//! Leveled compaction (DESIGN.md D8).
//!
//! - Level 0 compacts when it has `l0_compaction_trigger` tables. ALL level-0
//!   tables go in at once: they overlap, so moving only some of them down
//!   could leave an older version of a key above a newer one.
//! - Level n >= 1 compacts when its bytes exceed its limit. One table goes in,
//!   chosen round-robin through the key space (`compact_pointer`).
//! - The chosen tables plus every overlapping table in the next level are
//!   merged, newest version of each key wins, and the result is written to the
//!   next level as new tables of about `target_file_size`.
//! - A tombstone is dropped only when no deeper level could hold an older
//!   version of its key. Dropping it earlier would bring that version back.
//! - Commit: one manifest write adds the outputs and removes the inputs.

use std::collections::{BTreeMap, HashSet};

use super::{open_table, remove_obsolete_files, table_for_key, table_path, Db, Table};
use crate::error::Result;
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

impl Db {
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
    pub fn compact_all(&mut self) -> Result<()> {
        self.flush_memtable()?;
        for level in 0..MAX_LEVELS - 1 {
            while !self.levels[level].is_empty() {
                let c = self.compaction_for(level);
                self.run(c)?;
            }
        }
        Ok(())
    }

    /// The level whose score (fullness relative to its limit) is highest,
    /// if any level is at or over its limit. The bottom level never compacts.
    fn pick_level(&self) -> Option<usize> {
        let l0 = self.levels[0].len() as f64 / self.opts.l0_compaction_trigger as f64;
        let deeper = (1..MAX_LEVELS - 1).map(|n| {
            let bytes: u64 = self.levels[n].iter().map(|t| t.reader.file_size()).sum();
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
        let tables = &self.levels[level];
        let inputs: Vec<&Table> = if level == 0 {
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
        let next = self.levels[level + 1]
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

        // Merge, newest first: the input level (level 0 is already newest
        // first), then the next level. The first version seen of a key wins.
        let mut merged: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
        for (level, ids) in [(c.level, &c.inputs), (out_level, &c.next)] {
            for id in ids {
                let table = self.levels[level]
                    .iter()
                    .find(|t| t.id == *id)
                    .expect("compaction input is live");
                for (key, entry) in table.reader.entries()? {
                    merged.entry(key).or_insert(entry);
                }
            }
        }

        // Write the outputs. Not live until the commit, so a crash before it
        // only leaves orphan files, which the next open deletes.
        let deeper = &self.levels[out_level + 1..];
        let mut outputs: Vec<Table> = Vec::new();
        let mut current: Option<(u64, SstWriter, usize)> = None;
        for (key, entry) in merged {
            let shadows_nothing = !deeper.iter().any(|l| table_for_key(l, &key).is_some());
            if entry == Entry::Tombstone && shadows_nothing {
                continue;
            }
            if current.is_none() {
                let id = self.next_file;
                self.next_file += 1;
                current = Some((
                    id,
                    SstWriter::with_options(&table_path(&self.dir, id), self.writer_options())?,
                    0,
                ));
            }
            let (_, writer, size) = current.as_mut().expect("just set");
            writer.add(&key, &entry)?;
            *size += key.len() + entry_len(&entry);
            if *size >= self.opts.target_file_size {
                let (id, writer, _) = current.take().expect("just used");
                writer.finish()?;
                outputs.push(Table {
                    id,
                    reader: open_table(&self.dir, id, &self.read_ctx)?,
                });
            }
        }
        if let Some((id, writer, _)) = current.take() {
            writer.finish()?;
            outputs.push(Table {
                id,
                reader: open_table(&self.dir, id, &self.read_ctx)?,
            });
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
            self.compact_pointer[c.level] = self.levels[c.level]
                .iter()
                .find(|t| t.id == c.inputs[0])
                .map(|t| t.largest().to_vec());
        }
        self.compaction_bytes += outputs.iter().map(|t| t.reader.file_size()).sum::<u64>();
        for level in [c.level, out_level] {
            self.levels[level].retain(|t| !removed.contains(&t.id));
        }
        self.levels[out_level].extend(outputs);
        self.levels[out_level].sort_by(|a, b| a.smallest().cmp(b.smallest()));
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

        let i = self.levels[level]
            .iter()
            .position(|t| t.id == id)
            .expect("moved table is live");
        let table = self.levels[level].remove(i);
        if level > 0 {
            self.compact_pointer[level] = Some(table.largest().to_vec());
        }
        let next = &mut self.levels[level + 1];
        let at = next.partition_point(|t| t.smallest() < table.smallest());
        next.insert(at, table);
        Ok(())
    }
}

/// Encoded size of an entry's value plus the fixed entry header.
fn entry_len(entry: &Entry) -> usize {
    9 + match entry {
        Entry::Value(v) => v.len(),
        Entry::Tombstone => 0,
    }
}
