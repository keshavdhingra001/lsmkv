//! Leveled compaction (DESIGN.md D8).
//!
//! - Level 0 compacts when it has `l0_compaction_trigger` tables. ALL level-0
//!   tables go in at once: they overlap, so moving only some of them down
//!   could leave an older version of a key above a newer one.
//! - Level n >= 1 compacts when its bytes exceed its limit. One table goes in,
//!   chosen round-robin through the key space (`compact_pointer`).
//! - The chosen tables plus every overlapping table in the next level are
//!   merged in internal key order, and the result is written to the next
//!   level as new tables of about `target_file_size`. A table only ends
//!   between two user keys, so each key's versions stay in one table and
//!   tables in a level never overlap.
//! - A version is dropped once a newer version of its key is visible to
//!   every reader (`Shadowed`, DESIGN.md D18).
//! - A tombstone is also dropped when every reader sees it and no deeper
//!   level could hold an older version of its key. Dropping it earlier would
//!   bring that version back.
//! - Commit: one manifest write adds the outputs and removes the inputs.

use std::collections::{BTreeMap, HashSet};

use std::sync::Arc;

use super::{open_table, remove_obsolete_files, table_for_key, table_path, State, Table};
use crate::error::Result;
use crate::key::{InternalKey, Shadowed};
use crate::manifest::{Edit, MAX_LEVELS};
use crate::memtable::Entry;
use crate::sstable::SstWriter;

/// Table ids to merge from `level` and from `level + 1`.
#[derive(Debug)]
struct Compaction {
    level: usize,
    inputs: Vec<u64>,
    next: Vec<u64>,
}

impl State {
    /// Runs compactions until no level is over its limit.
    pub(super) fn maybe_compact(&mut self) -> Result<()> {
        while let Some(level) = self.pick_level() {
            let c = self.compaction_for(level);
            self.run(c)?;
        }
        Ok(())
    }

    /// Flushes, then pushes every table down to the bottom level, which drops
    /// every overwritten value and every tombstone. Like RocksDB's
    /// `CompactRange` over the whole key space.
    pub(super) fn compact_all(&mut self) -> Result<()> {
        self.flush_memtable()?;
        for level in 0..MAX_LEVELS - 1 {
            while !self.current.levels[level].is_empty() {
                let c = self.compaction_for(level);
                self.run(c)?;
            }
        }
        Ok(())
    }

    /// The level whose score (fullness relative to its limit) is highest,
    /// if any level is at or over its limit. The bottom level never compacts.
    fn pick_level(&self) -> Option<usize> {
        let l0 = self.current.levels[0].len() as f64 / self.opts.l0_compaction_trigger as f64;
        let deeper = (1..MAX_LEVELS - 1).map(|n| {
            let bytes: u64 = self.current.levels[n]
                .iter()
                .map(|t| t.reader.file_size())
                .sum();
            (bytes as f64 / self.max_bytes_for(n) as f64, n)
        });
        std::iter::once((l0, 0))
            .chain(deeper)
            .filter(|&(score, _)| score >= 1.0)
            .max_by(|a, b| a.0.total_cmp(&b.0))
            .map(|(_, level)| level)
    }

    fn max_bytes_for(&self, level: usize) -> u64 {
        let mut max = self.opts.level1_max_bytes;
        for _ in 1..level {
            max = max.saturating_mul(self.opts.level_size_multiplier);
        }
        max
    }

    /// Chooses the input tables for compacting `level` (which must be non-empty).
    fn compaction_for(&self, level: usize) -> Compaction {
        let tables = &self.current.levels[level];
        let inputs: Vec<&Arc<Table>> = if level == 0 {
            tables.iter().collect()
        } else {
            let after_pointer = self.compact_pointer[level]
                .as_deref()
                .and_then(|p| tables.iter().find(|t| t.smallest() > p));
            vec![after_pointer.unwrap_or(&tables[0])]
        };

        let lo = inputs
            .iter()
            .map(|t| t.smallest())
            .min()
            .unwrap_or_default();
        let hi = inputs.iter().map(|t| t.largest()).max().unwrap_or_default();
        let next = self.current.levels[level + 1]
            .iter()
            .filter(|t| t.largest() >= lo && t.smallest() <= hi)
            .map(|t| t.id)
            .collect();

        Compaction {
            level,
            inputs: inputs.iter().map(|t| t.id).collect(),
            next,
        }
    }

    fn run(&mut self, c: Compaction) -> Result<()> {
        self.check_writable()?;
        if c.inputs.len() == 1 && c.next.is_empty() {
            return self.trivial_move(c.level, c.inputs[0]);
        }
        let out_level = c.level + 1;

        // Merge every version into internal key order: per key, newest first.
        // Sequence numbers are unique, so no two inputs hold the same version.
        let mut merged: BTreeMap<InternalKey, Entry> = BTreeMap::new();
        for (level, ids) in [(c.level, &c.inputs), (out_level, &c.next)] {
            for id in ids {
                let table = self.current.levels[level]
                    .iter()
                    .find(|t| t.id == *id)
                    .expect("compaction input is live");
                for (key, seq, entry) in table.reader.entries()? {
                    merged.insert(InternalKey { user_key: key, seq }, entry);
                }
            }
        }

        // Write the outputs. Not live until the commit, so a crash before it
        // only leaves orphan files, which the next open deletes.
        let oldest_snapshot = self.oldest_snapshot();
        let mut shadowed = Shadowed::new(oldest_snapshot);
        let deeper = &self.current.levels[out_level + 1..];
        let mut outputs: Vec<Arc<Table>> = Vec::new();
        // (table id, writer, bytes so far, last user key written)
        let mut current: Option<(u64, SstWriter, usize, Vec<u8>)> = None;
        for (InternalKey { user_key: key, seq }, entry) in merged {
            if shadowed.check(&key, seq) {
                continue;
            }
            let shadows_nothing = !deeper.iter().any(|l| table_for_key(l, &key).is_some());
            if entry == Entry::Tombstone && seq <= oldest_snapshot && shadows_nothing {
                continue;
            }
            // A full table ends at the first new user key.
            if let Some((_, _, size, last)) = &current {
                if *size >= self.opts.target_file_size && *last != key {
                    let (id, writer, _, _) = current.take().expect("just checked");
                    writer.finish()?;
                    outputs.push(Arc::new(Table {
                        id,
                        reader: open_table(&self.dir, id, &self.read_ctx)?,
                    }));
                }
            }
            if current.is_none() {
                let id = self.next_file;
                self.next_file += 1;
                current = Some((
                    id,
                    SstWriter::with_options(&table_path(&self.dir, id), self.writer_options())?,
                    0,
                    Vec::new(),
                ));
            }
            let (_, writer, size, last) = current.as_mut().expect("just set");
            writer.add(&key, seq, &entry)?;
            *size += key.len() + entry_len(&entry);
            *last = key;
        }
        if let Some((id, writer, _, _)) = current.take() {
            writer.finish()?;
            outputs.push(Arc::new(Table {
                id,
                reader: open_table(&self.dir, id, &self.read_ctx)?,
            }));
        }
        self.failpoint("compact:after_tables")?;

        let removed: HashSet<u64> = c.inputs.iter().chain(&c.next).copied().collect();
        let mut edits: Vec<Edit> = removed.iter().map(|&id| Edit::RemoveTable(id)).collect();
        edits.extend(outputs.iter().map(|t| Edit::AddTable {
            id: t.id,
            level: out_level as u8,
        }));
        self.commit(&edits, "compact")?;

        if c.level > 0 {
            self.compact_pointer[c.level] = self.current.levels[c.level]
                .iter()
                .find(|t| t.id == c.inputs[0])
                .map(|t| t.largest().to_vec());
        }
        self.compaction_bytes += outputs.iter().map(|t| t.reader.file_size()).sum::<u64>();
        let mut levels = self.current.levels.clone();
        for level in [c.level, out_level] {
            levels[level].retain(|t| !removed.contains(&t.id));
        }
        levels[out_level].extend(outputs);
        levels[out_level].sort_by(|a, b| a.smallest().cmp(b.smallest()));
        let mem = Arc::clone(&self.current.mem);
        self.install(mem, levels);
        // Readers still holding the old SuperVersion keep reading the inputs:
        // an unlinked file stays readable through a descriptor that's already
        // open (POSIX), and the descriptor closes with the last `Arc<Table>`.
        let _ = remove_obsolete_files(&self.dir, &self.version);
        Ok(())
    }

    /// A single table with nothing overlapping it in the next level moves
    /// down by a manifest edit alone: no data is rewritten.
    fn trivial_move(&mut self, level: usize, id: u64) -> Result<()> {
        let edits = [
            Edit::RemoveTable(id),
            Edit::AddTable {
                id,
                level: (level + 1) as u8,
            },
        ];
        self.commit(&edits, "compact")?;

        let mut levels = self.current.levels.clone();
        let i = levels[level]
            .iter()
            .position(|t| t.id == id)
            .expect("moved table is live");
        let table = levels[level].remove(i);
        if level > 0 {
            self.compact_pointer[level] = Some(table.largest().to_vec());
        }
        let next = &mut levels[level + 1];
        let at = next.partition_point(|t| t.smallest() < table.smallest());
        next.insert(at, table);
        let mem = Arc::clone(&self.current.mem);
        self.install(mem, levels);
        Ok(())
    }
}

/// Encoded size of an entry's value plus the fixed entry header
/// (kind, seq, two lengths).
fn entry_len(entry: &Entry) -> usize {
    17 + match entry {
        Entry::Value(v) => v.len(),
        Entry::Tombstone => 0,
    }
}
