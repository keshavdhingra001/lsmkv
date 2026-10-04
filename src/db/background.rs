//! The background thread: flushes the immutable memtable, then compacts
//! (DESIGN.md D14, D15). One thread, one job at a time, flush first, like
//! LevelDB.
//!
//! Each job takes the state lock to start (copying out what it needs), runs
//! its I/O with no lock held, and takes the lock again to commit and install
//! the result. Writers keep writing into the new memtable, and readers keep
//! reading, while a job runs.
//!
//! A failed job poisons the database: writes are refused from then on, reads
//! keep working, and a reopen recovers from what reached the disk.

use std::path::PathBuf;
use std::sync::{Arc, MutexGuard};
#[cfg(test)]
use std::time::Duration;

use super::{lock, open_table, remove_obsolete_files, table_path, Shared, State, Table};
use crate::error::Result;
use crate::key::{SeqNo, Shadowed};
use crate::manifest::Edit;
use crate::memtable::MemTable;
use crate::sstable::{ReadContext, SstReader, SstWriter, WriterOptions};

/// What a job needs from `State`, copied out so it can run with no lock held.
pub(super) struct JobEnv {
    pub dir: PathBuf,
    pub writer: WriterOptions,
    pub target_file_size: usize,
    pub read_ctx: Arc<ReadContext>,
    /// Versions only snapshots older than this could see may be dropped.
    pub oldest_snapshot: SeqNo,
    /// Test-only slow disk: the job sleeps this long before its I/O.
    #[cfg(test)]
    pub slow: Duration,
}

/// The thread's whole life: wait for work, do it, repeat until the `Db` drops.
pub(super) fn run(shared: &Shared) {
    let mut st = lock(&shared.state);
    loop {
        if st.stopping {
            return;
        }
        if st.poisoned.is_some() {
            st = shared.bg_work.wait(st).expect("db lock poisoned");
            continue;
        }
        st.bg_busy = true;
        let (guard, did_work) = if st.current.imm.is_some() {
            flush(shared, st)
        } else if let Some(c) = st.pick_compaction() {
            compact(shared, st, c)
        } else {
            (st, Ok(false))
        };
        st = guard;
        st.bg_busy = false;
        match did_work {
            Ok(true) => {}
            Ok(false) => {
                // Nothing to do: tell anyone waiting for the background to
                // settle, then sleep until a memtable switch or a request.
                shared.turn.notify_all();
                st = shared.bg_work.wait(st).expect("db lock poisoned");
                continue;
            }
            Err(e) => {
                st.poison(e);
            }
        }
        // Stalled writers and `flush`/`compact_all` callers re-check.
        shared.turn.notify_all();
    }
}

/// Writes the immutable memtable to a new level-0 table.
///
/// Steps, ordered so a crash after any of them loses nothing (the memtable
/// switch already sealed the old WAL and started a new one):
/// 1. Write the table (tmp + fsync + rename), no lock held. Unlisted, so a
///    crash leaves an orphan, which the next open deletes.
/// 2. **Commit point:** one manifest write adds the table and retires the old
///    WAL. Before this, recovery replays the old WAL; after it, the table is live.
/// 3. Install the table in place of the immutable memtable (one step, so a
///    reader finds the versions in exactly one of them), then delete the old
///    WAL (best effort; the next open retries).
fn flush<'a>(
    shared: &'a Shared,
    mut st: MutexGuard<'a, State>,
) -> (MutexGuard<'a, State>, Result<bool>) {
    let imm = Arc::clone(st.current.imm.as_ref().expect("checked by caller"));
    let table_id = st.next_file;
    st.next_file += 1;
    let env = st.job_env();
    drop(st);

    let written = write_table(&env, &imm, table_id);

    let mut st = lock(&shared.state);
    let done = written.and_then(|reader| st.finish_flush(table_id, reader));
    (st, done.map(|()| true))
}

/// Step 1 of a flush, no lock held.
fn write_table(env: &JobEnv, imm: &MemTable, table_id: u64) -> Result<SstReader> {
    #[cfg(test)]
    std::thread::sleep(env.slow);
    let path = table_path(&env.dir, table_id);
    let mut writer = SstWriter::with_options(&path, env.writer)?;
    // Overwritten versions no reader can see stay behind. Tombstones all go
    // in: older tables may hold what they delete.
    let mut shadowed = Shadowed::new(env.oldest_snapshot);
    for e in imm.iter() {
        let (key, seq) = (&e.key().user_key, e.key().seq);
        if !shadowed.check(key, seq) {
            writer.add(key, seq, e.value())?;
        }
    }
    writer.finish()?;
    open_table(&env.dir, table_id, &env.read_ctx)
}

/// One compaction: start (lock held), merge (no lock), finish (lock held).
fn compact<'a>(
    shared: &'a Shared,
    mut st: MutexGuard<'a, State>,
    c: super::compaction::Compaction,
) -> (MutexGuard<'a, State>, Result<bool>) {
    let job = match st.start_compaction(c) {
        Ok(Some(job)) => job,
        // A trivial move, already done.
        Ok(None) => return (st, Ok(true)),
        Err(e) => return (st, Err(e)),
    };
    drop(st);

    let outputs = job.run(|| {
        let mut st = lock(&shared.state);
        st.next_file += 1;
        st.next_file - 1
    });

    let mut st = lock(&shared.state);
    let done = outputs.and_then(|outputs| st.finish_compaction(job, outputs));
    (st, done.map(|()| true))
}

impl State {
    pub(super) fn job_env(&self) -> JobEnv {
        JobEnv {
            dir: self.dir.clone(),
            writer: self.writer_options(),
            target_file_size: self.opts.target_file_size,
            read_ctx: Arc::clone(&self.read_ctx),
            oldest_snapshot: self.oldest_snapshot(),
            #[cfg(test)]
            slow: self.slow_background,
        }
    }

    /// Steps 2 and 3 of a flush, lock held.
    fn finish_flush(&mut self, table_id: u64, reader: SstReader) -> Result<()> {
        self.failpoint("flush:after_table")?;
        let edits = [
            Edit::AddTable {
                id: table_id,
                level: 0,
            },
            // Logs before the active one hold only the flushed writes.
            Edit::SetLogNumber(self.wal_number),
            // ...and those logs, which held these writes' numbers, are going away.
            Edit::SetLastSequence(self.imm_last_seq),
        ];
        self.commit(&edits, "flush")?;

        self.flush_bytes += reader.file_size();
        let mut levels = self.current.levels.clone();
        levels[0].insert(
            0,
            Arc::new(Table {
                id: table_id,
                reader,
            }),
        );
        self.install(super::SuperVersion {
            mem: Arc::clone(&self.current.mem),
            imm: None,
            levels,
        });
        let _ = remove_obsolete_files(&self.dir, &self.version);
        Ok(())
    }
}
