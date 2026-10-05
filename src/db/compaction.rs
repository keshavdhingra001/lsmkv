//! Leveled compaction (DESIGN.md D8), run by the background thread (D15).
//!
//! - Level 0 compacts when it has `l0_compaction_trigger` tables. ALL level-0
//!   tables go in at once: they overlap, so moving only some of them down
//!   could leave an older version of a key above a newer one.
//! - Level n >= 1 compacts when its bytes exceed its limit. One table goes in,
//!   chosen round-robin through the key space (`compact_pointer`).
//! - The chosen tables plus every overlapping table in the next level are
//!   merged in internal key order (a streaming k-way merge, as for scans),
//!   and the result is written to the next
//!   level as new tables of about `target_file_size`. A table only ends
//!   between two user keys, so each key's versions stay in one table and
//!   tables in a level never overlap.
//! - A version is dropped once a newer version of its key is visible to
//!   every reader (`Shadowed`, DESIGN.md D18).
//! - A tombstone is also dropped when every reader sees it and no deeper
//!   level could hold an older version of its key. Dropping it earlier would
//!   bring that version back.
//! - Commit: one manifest write adds the outputs and removes the inputs.
//! - The bottom level never compacts on its own. `compact_all` ends by
//!   rewriting it in place (RocksDB's `bottommost_level_compaction = kForce`),
//!   which drops what snapshots kept there and have since released.
//!
//! Three phases, so the slow part holds no lock: `start_compaction` (state
//! lock held) picks the inputs and copies out what the merge needs;
//! `CompactionJob::run` (no lock) merges and writes the outputs;
//! `finish_compaction` (lock held again) commits and installs them. The
//! levels can't change in between: only the background thread changes them,
//! and it runs one job at a time.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::background::JobEnv;
use super::iter::{MergeIter, Source};
use super::{bytes, remove_obsolete_files, table_for_key, table_path, State, SuperVersion, Table};
use crate::error::Result;
use crate::key::Shadowed;
use crate::manifest::{Edit, MAX_LEVELS};
use crate::memtable::Entry;
use crate::sstable::block::ENTRY_HEADER_LEN;
use crate::sstable::SstWriter;

/// Table ids to merge from `level` and from `out_level`, into `out_level`.
#[derive(Debug)]
pub(super) struct Compaction {
    level: usize,
    /// `level + 1`, or `level` itself for the bottom level's in-place rewrite.
    out_level: usize,
    inputs: Vec<u64>,
    next: Vec<u64>,
}

const BOTTOM: usize = MAX_LEVELS - 1;

/// A compaction's merge, with everything it reads copied out of `State`.
pub(super) struct CompactionJob {
    c: Compaction,
    /// The input tables: the input level's (level 0's newest first), then
    /// the next level's.
    tables: Vec<Arc<Table>>,
    /// The levels as of the start, to check what lies below the output level.
    current: Arc<SuperVersion>,
    env: JobEnv,
}

impl State {
    /// The next compaction for the background thread, if any: a step of a
    /// requested `compact_all` first, otherwise the most urgent level.
    pub(super) fn pick_compaction(&mut self) -> Option<Compaction> {
        #[cfg(test)]
        if self.pause_compactions {
            return None;
        }
        if let Some(from) = self.manual_compaction {
            // Lowest non-empty level from where the request got to, so a
            // table flushed meanwhile into level 0 can't keep it going forever.
            let level = (from..=BOTTOM).find(|&l| !self.current.levels[l].is_empty());
            // The bottom level is rewritten once, and that ends the request.
            self.manual_compaction = level.filter(|&l| l < BOTTOM);
            if let Some(level) = level {
                return Some(self.compaction_for(level));
            }
        }
        self.pick_level().map(|level| self.compaction_for(level))
    }

    /// Whether the background thread has compaction work it would start now.
    pub(super) fn compaction_wanted(&self) -> bool {
        #[cfg(test)]
        if self.pause_compactions {
            return false;
        }
        self.manual_compaction.is_some() || self.pick_level().is_some()
    }

    /// The level whose score (fullness relative to its limit) is highest,
    /// if any level is at or over its limit. The bottom level never compacts.
    fn pick_level(&self) -> Option<usize> {
        let l0 = self.current.levels[0].len() as f64 / self.opts.l0_compaction_trigger as f64;
        let deeper = (1..MAX_LEVELS - 1).map(|n| {
            let bytes = bytes(&self.current.levels[n]);
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
        if level == BOTTOM {
            return Compaction {
                level,
                out_level: level,
                inputs: tables.iter().map(|t| t.id).collect(),
                next: Vec::new(),
            };
        }
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
            out_level: level + 1,
            inputs: inputs.iter().map(|t| t.id).collect(),
            next,
        }
    }

    /// Phase 1, lock held. A trivial move is done right here (it's only a
    /// manifest edit); anything else becomes a job to run without the lock.
    pub(super) fn start_compaction(&mut self, c: Compaction) -> Result<Option<CompactionJob>> {
        self.check_writable()?;
        let mut tables = Vec::new();
        for (level, ids) in [(c.level, &c.inputs), (c.out_level, &c.next)] {
            for id in ids {
                let table = self.current.levels[level]
                    .iter()
                    .find(|t| t.id == *id)
                    .expect("compaction input is live");
                tables.push(Arc::clone(table));
            }
        }
        if c.out_level > c.level && c.next.is_empty() && can_move(&tables) {
            self.trivial_move(c.level, tables)?;
            return Ok(None);
        }
        Ok(Some(CompactionJob {
            c,
            tables,
            current: Arc::clone(&self.current),
            env: self.job_env(),
        }))
    }

    /// Phase 3, lock held: commit the outputs in place of the inputs.
    pub(super) fn finish_compaction(
        &mut self,
        job: CompactionJob,
        outputs: Vec<Arc<Table>>,
    ) -> Result<()> {
        let c = job.c;
        let out_level = c.out_level;
        self.failpoint("compact:after_tables")?;

        // Sorted, so the manifest records come out in the same order every
        // run (a HashSet's order changes per process; D28 needs determinism).
        let removed: BTreeSet<u64> = c.inputs.iter().chain(&c.next).copied().collect();
        let mut edits: Vec<Edit> = removed.iter().map(|&id| Edit::RemoveTable(id)).collect();
        edits.extend(outputs.iter().map(|t| Edit::AddTable {
            id: t.id,
            level: out_level as u8,
        }));
        self.commit(&edits, "compact")?;

        if c.level > 0 && out_level > c.level {
            self.compact_pointer[c.level] = self.current.levels[c.level]
                .iter()
                .find(|t| t.id == c.inputs[0])
                .map(|t| t.largest().to_vec());
        }
        self.compaction_bytes += bytes(&outputs);
        let mut levels = self.current.levels.clone();
        for level in [c.level, out_level] {
            levels[level].retain(|t| !removed.contains(&t.id));
        }
        levels[out_level].extend(outputs);
        levels[out_level].sort_by(|a, b| a.smallest().cmp(b.smallest()));
        self.install_levels(levels);
        // Readers still holding the old SuperVersion keep reading the inputs:
        // an unlinked file stays readable through a descriptor that's already
        // open (POSIX), and the descriptor closes with the last `Arc<Table>`.
        let _ = remove_obsolete_files(&*self.opts.fs, &self.dir, &self.version);
        Ok(())
    }

    /// Tables with nothing overlapping them in the next level, and not each
    /// other, move down by a manifest edit alone: no data is rewritten. For
    /// level 0 that's all of its tables at once (D29): with sequential keys,
    /// every flush makes a table past the ones before it, so they never merge.
    fn trivial_move(&mut self, level: usize, tables: Vec<Arc<Table>>) -> Result<()> {
        let mut edits: Vec<Edit> = tables.iter().map(|t| Edit::RemoveTable(t.id)).collect();
        edits.extend(tables.iter().map(|t| Edit::AddTable {
            id: t.id,
            level: (level + 1) as u8,
        }));
        self.commit(&edits, "compact")?;

        let mut levels = self.current.levels.clone();
        levels[level].retain(|t| !tables.iter().any(|m| m.id == t.id));
        if level > 0 {
            let largest = tables.iter().map(|t| t.largest()).max();
            self.compact_pointer[level] = largest.map(<[u8]>::to_vec);
        }
        let next = &mut levels[level + 1];
        next.extend(tables);
        next.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        self.install_levels(levels);
        Ok(())
    }
}

/// Whether `tables` can move down a level as they are: none is empty (levels
/// 1+ never hold an empty table) and no two share a user key, not even at
/// their edges, since a key's versions may be split across level-0 tables.
fn can_move(tables: &[Arc<Table>]) -> bool {
    if tables.iter().any(|t| t.reader.entry_count() == 0) {
        return false;
    }
    let mut by_start: Vec<&Arc<Table>> = tables.iter().collect();
    by_start.sort_by(|a, b| a.smallest().cmp(b.smallest()));
    by_start
        .windows(2)
        .all(|w| w[0].largest() < w[1].smallest())
}

impl CompactionJob {
    /// Phase 2, no lock held: merge the inputs and write the outputs.
    /// `new_file` hands out file numbers (it takes the state lock briefly).
    /// The outputs aren't live until `finish_compaction` commits them, so a
    /// crash before that only leaves orphan files, which the next open deletes.
    pub(super) fn run(&self, mut new_file: impl FnMut() -> u64) -> Result<Vec<Arc<Table>>> {
        #[cfg(test)]
        std::thread::sleep(self.env.slow);
        // Stream every version in internal key order (per key, newest
        // first), merging the inputs as scans do: one block per input in
        // memory, however big the compaction (DESIGN.md D20).
        let mut sources: Vec<Source> = Vec::with_capacity(self.tables.len());
        for table in &self.tables {
            sources.push(Box::new(table.reader.iter(None)?));
        }
        let merged = MergeIter::new(sources)?;

        let env = &self.env;
        let mut shadowed = Shadowed::new(env.oldest_snapshot);
        let deeper = &self.current.levels[self.c.out_level + 1..];
        let mut outputs: Vec<Arc<Table>> = Vec::new();
        // Finishes an output table and opens it.
        let seal = |id, writer: SstWriter| {
            writer.finish()?;
            Table::open(&*env.fs, &env.dir, id, &env.read_ctx)
        };
        // (table id, writer, bytes so far, last user key written)
        let mut current: Option<(u64, SstWriter, usize, Vec<u8>)> = None;
        for item in merged {
            let (key, seq, entry) = item?;
            if shadowed.check(&key, seq) {
                continue;
            }
            let shadows_nothing = !deeper.iter().any(|l| table_for_key(l, &key).is_some());
            if entry == Entry::Tombstone && seq <= env.oldest_snapshot && shadows_nothing {
                continue;
            }
            // A full table ends at the first new user key.
            if let Some((_, _, size, last)) = &current {
                if *size >= env.target_file_size && *last != key {
                    let (id, writer, _, _) = current.take().expect("just checked");
                    outputs.push(seal(id, writer)?);
                }
            }
            if current.is_none() {
                let id = new_file();
                let path = table_path(&env.dir, id);
                current = Some((
                    id,
                    SstWriter::with_options_in(Arc::clone(&env.fs), &path, env.writer)?,
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
            outputs.push(seal(id, writer)?);
        }
        Ok(outputs)
    }
}

/// Encoded size of an entry's value plus the fixed entry header
/// (kind, seq, two lengths).
fn entry_len(entry: &Entry) -> usize {
    ENTRY_HEADER_LEN
        + match entry {
            Entry::Value(v) => v.len(),
            Entry::Tombstone => 0,
        }
}
