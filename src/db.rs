//! Public database handle: ties together the WAL, memtable, SSTables and manifest.
//!
//! Files in the database directory:
//! - `MANIFEST`: which tables and which WALs are live (see `manifest.rs`).
//! - `NNNNNN.log`: write-ahead logs. Live if numbered >= the manifest's log number.
//! - `NNNNNN.sst`: SSTables. Live if listed in the manifest. Higher number = newer.
//!
//! Logs and tables share one number sequence, so "higher = newer" holds across
//! both. Anything else the engine owns (orphans from a crash mid-flush, `.tmp`
//! files) is deleted on open. Unrecognized files are left alone.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::fsutil::sync_dir;
use crate::manifest::{Edit, Manifest, Version, MAX_LEVELS};
use crate::memtable::{Entry, MemTable};
use crate::sstable::filter::DEFAULT_BITS_PER_KEY;
use crate::sstable::{
    ReadContext, ReadStats, SstReader, SstWriter, WriterOptions, DEFAULT_BLOCK_SIZE,
};
use crate::wal::{Record, Wal};

mod compaction;

/// Tuning knobs. Defaults follow LevelDB.
#[derive(Debug, Clone)]
pub struct Options {
    /// Flush the memtable to an SSTable once its approximate size reaches this.
    pub memtable_size: usize,
    /// Compact level 0 into level 1 once level 0 has this many tables.
    pub l0_compaction_trigger: usize,
    /// Size limit for level 1. Each deeper level's limit is
    /// `level_size_multiplier` times the one above it.
    pub level1_max_bytes: u64,
    pub level_size_multiplier: u64,
    /// Compaction output is split into tables of roughly this size.
    pub target_file_size: usize,
    /// Bloom filter bits per key in new tables; 0 turns filters off. Tables
    /// already on disk keep whatever filter they were written with.
    pub bloom_bits_per_key: usize,
    /// Block cache size, shared by all tables; 0 turns it off.
    pub block_cache_bytes: usize,
    /// When a write counts as durable (DESIGN.md D11).
    pub sync_mode: SyncMode,
}

/// When `put`/`delete` return, relative to the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// fsync the WAL before acknowledging. Survives power loss. Concurrent
    /// writers share one fsync (group commit).
    Always,
    /// Acknowledge once the WAL bytes reach the OS; a background thread
    /// fsyncs every interval. Survives a crash of this process, but a power
    /// cut or kernel crash can lose up to one interval of acknowledged writes.
    Periodic(Duration),
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size: 4 << 20,
            l0_compaction_trigger: 4,
            level1_max_bytes: 10 << 20,
            level_size_multiplier: 10,
            target_file_size: 2 << 20,
            bloom_bits_per_key: DEFAULT_BITS_PER_KEY,
            block_cache_bytes: 8 << 20,
            sync_mode: SyncMode::Always,
        }
    }
}

/// Point-in-time numbers for the REPL and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    /// Total live tables across all levels.
    pub tables: usize,
    /// Tables per level, index = level.
    pub level_files: Vec<usize>,
    /// Bytes per level, index = level.
    pub level_bytes: Vec<u64>,
    pub log_number: u64,
    /// Writes (puts and deletes) acknowledged since open.
    pub writes: u64,
    /// WAL write groups since open: each one a single append-and-sync for
    /// one or more writes.
    pub write_groups: u64,
    /// WAL fsyncs since open (by write groups, or by the periodic thread).
    pub wal_syncs: u64,
    /// Key + value bytes written by callers since open.
    pub user_bytes: u64,
    /// SSTable bytes written by flushes since open.
    pub flush_bytes: u64,
    /// SSTable bytes written by compactions since open.
    pub compaction_bytes: u64,
    /// Data blocks `get` read from disk since open (block cache misses).
    pub block_reads: u64,
    /// Data blocks `get` found in the block cache since open.
    pub cache_hits: u64,
    /// Bytes currently in the block cache.
    pub cache_bytes: usize,
    /// Table lookups a bloom filter answered without a block read.
    pub filter_negatives: u64,
    /// Table lookups where the filter said "maybe" but the key wasn't there.
    pub filter_false_positives: u64,
}

impl Stats {
    /// SSTable bytes written per user byte (WAL writes not counted).
    pub fn write_amplification(&self) -> f64 {
        if self.user_bytes == 0 {
            return 0.0;
        }
        (self.flush_bytes + self.compaction_bytes) as f64 / self.user_bytes as f64
    }
}

/// A live table: its file number plus an open reader.
struct Table {
    id: u64,
    reader: SstReader,
}

impl Table {
    fn smallest(&self) -> &[u8] {
        self.reader.smallest_key().unwrap_or_default()
    }

    fn largest(&self) -> &[u8] {
        self.reader.largest_key().unwrap_or_default()
    }
}

/// A write group is cut off at this many bytes of records, so one writer's
/// latency isn't stretched by an unbounded pile of others (LevelDB: 1 MiB).
const MAX_GROUP_BYTES: usize = 1 << 20;

/// A thread-safe handle: share it between threads as `Arc<Db>`.
///
/// Writes go through a queue (group commit). The writer at the front of the
/// queue becomes the *leader*: it takes every queued record (up to
/// `MAX_GROUP_BYTES`), appends them all to the WAL and syncs once, with the
/// state lock released, then applies them to the memtable and wakes the
/// others (the *followers*), whose writes are now done. Writers that arrive
/// meanwhile queue up and form the next group. See DESIGN.md D11.
///
/// Lock order: `state`, then the WAL. Nothing takes `state` while holding the
/// WAL, so the two can't deadlock.
pub struct Db {
    shared: Arc<Shared>,
    /// The background fsync thread in `SyncMode::Periodic`.
    syncer: Option<JoinHandle<()>>,
}

struct Shared {
    dir: PathBuf,
    state: Mutex<State>,
    /// Writers wait here for their turn to lead or for their leader to finish
    /// their write; flush and compaction wait here for a group to finish.
    turn: Condvar,
    /// Set when the `Db` is dropped, to stop the periodic sync thread.
    stop: Mutex<bool>,
    stop_signal: Condvar,
    /// fsyncs done by the periodic thread. An atomic, not a `State` field,
    /// so that thread never needs the state lock (see `sync_periodically`).
    periodic_syncs: AtomicU64,
    /// Test-only: the periodic thread's next fsync fails.
    #[cfg(test)]
    fail_periodic_sync: std::sync::atomic::AtomicBool,
}

/// Everything behind the state lock: the engine as it was before M7, plus the
/// writer queue.
struct State {
    dir: PathBuf,
    opts: Options,
    memtable: MemTable,
    /// The active log. Its own lock, so a leader can append and fsync while
    /// readers and queueing writers use `State`. Only a leader (with `writing`
    /// set) or a holder of the state lock with no group in flight touches it.
    wal: Arc<Mutex<Wal>>,
    wal_number: u64,
    manifest: Manifest,
    version: Version,
    /// Live tables by level. Level 0: newest first, ranges may overlap.
    /// Levels 1+: sorted by key, ranges never overlap.
    levels: Vec<Vec<Table>>,
    /// Next unused file number, for both logs and tables.
    next_file: u64,
    /// Block cache and read counters, shared by every open table.
    read_ctx: Arc<ReadContext>,
    /// Set after a WAL or manifest write fails; see `Error::Poisoned`.
    poisoned: Option<String>,
    /// Per level: largest key of the last table compacted out of it, so
    /// successive compactions rotate through the key space.
    compact_pointer: Vec<Option<Vec<u8>>>,
    /// Writes waiting to be logged, oldest first, tagged with a ticket.
    queue: VecDeque<(u64, Record)>,
    next_ticket: u64,
    /// A leader is logging a group right now (with the state lock released).
    writing: bool,
    /// Outcomes a leader left for its followers, by ticket.
    finished: HashMap<u64, Result<()>>,
    writes: u64,
    write_groups: u64,
    wal_syncs: u64,
    user_bytes: u64,
    flush_bytes: u64,
    compaction_bytes: u64,
    /// Test-only crash injection: the named failpoint returns an error.
    #[cfg(test)]
    fail_at: Option<&'static str>,
    /// Test-only slow disk: the leader sleeps this long while it holds the
    /// WAL, so concurrent writers reliably pile up into groups.
    #[cfg(test)]
    slow_wal: Duration,
}

impl Db {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(dir, Options::default())
    }

    /// Opens the database in `dir`, creating it if needed, then recovers:
    /// 1. Replay the manifest to learn the live tables and log number.
    /// 2. Delete files the manifest says aren't live (leftovers from a crash).
    /// 3. Replay the live WALs, oldest first, into a fresh memtable.
    pub fn open_with(dir: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let (mut manifest, mut version) = Manifest::open(&dir)?;

        remove_obsolete_files(&dir, &version)?;
        let live_logs: Vec<u64> = list_files(&dir)?
            .into_iter()
            .filter_map(|(f, _)| match f {
                DbFile::Log(n) => Some(n),
                _ => None,
            })
            .collect();

        let mut next_file = 1 + version
            .tables
            .keys()
            .chain(&live_logs)
            .chain([&version.log_number])
            .copied()
            .max()
            .unwrap_or(0);

        let read_ctx = Arc::new(ReadContext::new(opts.block_cache_bytes));
        let levels = open_levels(&dir, &version, &read_ctx)?;

        let mut memtable = MemTable::new();
        for (i, &n) in live_logs.iter().enumerate() {
            let path = log_path(&dir, n);
            let replay = Wal::replay(&path)?;
            for rec in replay.records {
                apply(&mut memtable, rec);
            }
            // Only the newest log gets appended to, so only it needs its torn
            // tail cut off (new writes must not land after garbage).
            let is_active = i + 1 == live_logs.len();
            if is_active && fs::metadata(&path)?.len() > replay.valid_len {
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(replay.valid_len)?;
                f.sync_all()?;
            }
        }

        let wal_number = match live_logs.last() {
            Some(&n) => n,
            None if version.log_number > 0 => version.log_number,
            None => {
                // Fresh database: record its first log in the manifest.
                let n = next_file;
                next_file += 1;
                let edit = Edit::SetLogNumber(n);
                manifest.append(&[edit])?;
                version.apply(edit).map_err(Error::Corruption)?;
                n
            }
        };
        let wal = Wal::open(&log_path(&dir, wal_number))?;
        sync_dir(&dir)?;

        let wal = Arc::new(Mutex::new(wal));
        let state = State {
            dir: dir.clone(),
            opts: opts.clone(),
            memtable,
            wal: Arc::clone(&wal),
            wal_number,
            manifest,
            version,
            levels,
            next_file,
            read_ctx,
            poisoned: None,
            compact_pointer: vec![None; MAX_LEVELS],
            queue: VecDeque::new(),
            next_ticket: 0,
            writing: false,
            finished: HashMap::new(),
            writes: 0,
            write_groups: 0,
            wal_syncs: 0,
            user_bytes: 0,
            flush_bytes: 0,
            compaction_bytes: 0,
            #[cfg(test)]
            fail_at: None,
            #[cfg(test)]
            slow_wal: Duration::ZERO,
        };
        let shared = Arc::new(Shared {
            dir,
            state: Mutex::new(state),
            turn: Condvar::new(),
            stop: Mutex::new(false),
            stop_signal: Condvar::new(),
            periodic_syncs: AtomicU64::new(0),
            #[cfg(test)]
            fail_periodic_sync: Default::default(),
        });
        let syncer = match opts.sync_mode {
            SyncMode::Always => None,
            SyncMode::Periodic(every) => {
                let shared = Arc::clone(&shared);
                Some(thread::spawn(move || {
                    sync_periodically(&shared, &wal, every)
                }))
            }
        };
        Ok(Self { shared, syncer })
    }

    /// Writes `key = value`. Returns once the write is as durable as the
    /// `SyncMode` promises. If this write fills the memtable, it also
    /// flushes; an `Err` from that flush still leaves the write itself logged.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(Record::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write(Record::Delete { key: key.to_vec() })
    }

    /// Newest data first: memtable, then every level-0 table (newest first),
    /// then at most one table per deeper level. The first hit wins, and a
    /// tombstone hit means "deleted": older data is not consulted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.lock().get(key)
    }

    /// Flushes the memtable, then compacts until no level is over its limit.
    pub fn flush(&self) -> Result<()> {
        self.exclusive().flush()
    }

    /// Flushes, then pushes every table down to the bottom level, which drops
    /// every overwritten value and every tombstone.
    pub fn compact_all(&self) -> Result<()> {
        self.exclusive().compact_all()
    }

    pub fn stats(&self) -> Stats {
        let mut stats = self.lock().stats();
        stats.wal_syncs += self.shared.periodic_syncs.load(Ordering::Relaxed);
        stats
    }

    pub fn dir(&self) -> &Path {
        &self.shared.dir
    }

    fn write(&self, rec: Record) -> Result<()> {
        let mut st = self.lock();
        st.check_writable()?;
        let ticket = st.next_ticket;
        st.next_ticket += 1;
        st.queue.push_back((ticket, rec));
        loop {
            // A leader already logged this write (or failed to).
            if let Some(result) = st.finished.remove(&ticket) {
                return result;
            }
            // First in line with no group in flight: this writer leads.
            if !st.writing && st.queue.front().map(|(t, _)| *t) == Some(ticket) {
                break;
            }
            st = self.wait(st);
        }

        let group = st.take_group();
        // The database may have been poisoned while this group waited.
        let ready = st.check_writable();
        let injected = st.failpoint("wal:sync");
        let sync = st.opts.sync_mode == SyncMode::Always;
        #[cfg(test)]
        let slow = st.slow_wal;
        st.writing = true;
        let wal = Arc::clone(&st.wal);
        drop(st);

        // The slow part, without the state lock: readers keep reading and new
        // writers keep queueing (they become the next group).
        let logged = ready.and_then(|()| {
            let mut wal = lock(&wal);
            let syncs_before = wal.sync_count();
            for (_, rec) in &group {
                wal.append(rec)?;
            }
            #[cfg(test)]
            thread::sleep(slow);
            injected?;
            // `Periodic` still pushes the bytes to the OS before acking: that
            // makes the write survive a crash of this process (the kernel
            // holds it), just not a power cut.
            if sync {
                wal.sync()?;
            } else {
                wal.flush()?;
            }
            Ok(wal.sync_count() - syncs_before)
        });

        let mut st = self.lock();
        st.writing = false;
        if let Ok(synced) = logged {
            st.wal_syncs += synced;
        }
        let result = st.finish_group(group, logged.map(drop));
        drop(st);
        self.shared.turn.notify_all();
        result
    }

    /// The state lock, once no write group is in flight. Flush and compaction
    /// switch the WAL and memtable, which must never happen while a leader is
    /// appending to the old WAL: its records would land in a retired log.
    fn exclusive(&self) -> MutexGuard<'_, State> {
        let mut st = self.lock();
        while st.writing {
            st = self.wait(st);
        }
        st
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        lock(&self.shared.state)
    }

    fn wait<'a>(&self, st: MutexGuard<'a, State>) -> MutexGuard<'a, State> {
        self.shared.turn.wait(st).expect("db lock poisoned")
    }

    #[cfg(test)]
    fn state(&self) -> MutexGuard<'_, State> {
        self.lock()
    }
}

impl Drop for Db {
    /// Stops the periodic sync thread, then syncs the WAL once more, so a
    /// clean close never loses an acknowledged write in any mode.
    fn drop(&mut self) {
        if let Some(syncer) = self.syncer.take() {
            *lock(&self.shared.stop) = true;
            self.shared.stop_signal.notify_all();
            let _ = syncer.join();
            let st = self.lock();
            if st.poisoned.is_none() {
                let _ = lock(&st.wal).sync();
            }
        }
    }
}

/// `SyncMode::Periodic`'s thread: fsync the WAL every `every` until the `Db`
/// is dropped. A failed fsync poisons the database, like a failed write.
///
/// It never takes the state lock on the normal path, so a long flush or
/// compaction (which holds that lock) can't stretch the interval, and it
/// holds the WAL lock only to clone the file handle, never during the fsync
/// itself, so writers keep appending while the disk catches up.
fn sync_periodically(shared: &Shared, wal: &Mutex<Wal>, every: Duration) {
    let mut stopped = lock(&shared.stop);
    loop {
        // `_while` checks the flag BEFORE sleeping. A plain `wait_timeout`
        // misses a stop that `Drop` signalled before this thread got here
        // (nobody was waiting yet, so the signal is lost), and then sleeps
        // a whole interval: a lost wakeup.
        stopped = shared
            .stop_signal
            .wait_timeout_while(stopped, every, |stopped| !*stopped)
            .expect("stop lock poisoned")
            .0;
        if *stopped {
            return;
        }
        drop(stopped);

        // The WAL guard is a temporary: released at the end of this line.
        let handle = lock(wal).sync_handle();
        let synced = handle.and_then(|file| {
            #[cfg(test)]
            if shared.fail_periodic_sync.load(Ordering::Relaxed) {
                return Err(Error::Io(std::io::Error::other("injected fsync failure")));
            }
            Ok(file.sync_data()?)
        });
        if let Err(e) = synced {
            lock(&shared.state).poison(e);
            return;
        }
        shared.periodic_syncs.fetch_add(1, Ordering::Relaxed);
        stopped = lock(&shared.stop);
    }
}

/// Locks a mutex. A poisoned lock means a thread panicked mid-update and the
/// state behind it is unknown, so this panics too rather than carry on.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("db lock poisoned")
}

impl State {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Some(Entry::Value(v)) => return Ok(Some(v.clone())),
            Some(Entry::Tombstone) => return Ok(None),
            None => {}
        }
        let level0 = self.levels[0].iter();
        let deeper = self.levels[1..]
            .iter()
            .filter_map(|level| table_for_key(level, key));
        for table in level0.chain(deeper) {
            match table.reader.get(key)? {
                Some(Entry::Value(v)) => return Ok(Some(v)),
                Some(Entry::Tombstone) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    /// Takes the next write group off the front of the queue: at least one
    /// record, then more while the group stays under `MAX_GROUP_BYTES`.
    fn take_group(&mut self) -> Vec<(u64, Record)> {
        let mut group = Vec::new();
        let mut bytes = 0;
        while let Some((_, rec)) = self.queue.front() {
            let size = record_len(rec);
            if !group.is_empty() && bytes + size > MAX_GROUP_BYTES {
                break;
            }
            bytes += size;
            group.push(self.queue.pop_front().expect("front exists"));
        }
        group
    }

    /// After the leader logged a group: apply it to the memtable in queue
    /// order (the same order as in the WAL, so a replay rebuilds exactly
    /// this memtable) and record each follower's outcome. If logging failed,
    /// none of the group was acknowledged, and the database is poisoned: the
    /// log may now end in a partial record.
    fn finish_group(&mut self, group: Vec<(u64, Record)>, logged: Result<()>) -> Result<()> {
        let leader = group[0].0;
        if let Err(e) = logged {
            let e = self.poison(e);
            let why = self.poisoned.clone().expect("just poisoned");
            for (ticket, _) in &group[1..] {
                self.finished
                    .insert(*ticket, Err(Error::Poisoned(why.clone())));
            }
            return Err(e);
        }
        self.write_groups += 1;
        for (ticket, rec) in group {
            self.writes += 1;
            self.user_bytes += record_len(&rec) as u64;
            apply(&mut self.memtable, rec);
            if ticket != leader {
                self.finished.insert(ticket, Ok(()));
            }
        }
        // The leader pays for the flush; its followers' writes are done.
        if self.memtable.approx_size() >= self.opts.memtable_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.flush_memtable()?;
        self.maybe_compact()
    }

    /// Writes the memtable to a new level-0 SSTable and starts a fresh WAL.
    ///
    /// Steps, ordered so a crash after any of them loses nothing:
    /// 1. Write the table (tmp + fsync + rename). Unlisted, so a crash leaves an orphan.
    /// 2. Create the new WAL. Empty, so a crash leaves a harmless extra log.
    /// 3. **Commit point:** one manifest write adds the table and retires the
    ///    old WAL. Before this, recovery replays the old WAL; after it, the table
    ///    is live.
    /// 4. Switch in-memory state, then delete the now-obsolete WAL (best effort;
    ///    the next open retries).
    ///
    /// A failure before the commit point leaves the database usable; the
    /// orphans are cleaned up on the next open. A failure at or after it
    /// poisons the database (see `Error::Poisoned`).
    ///
    /// Callers hold the state lock with no write group in flight, so no
    /// leader is appending to the WAL being retired.
    fn flush_memtable(&mut self) -> Result<()> {
        self.check_writable()?;
        if self.memtable.is_empty() {
            return Ok(());
        }
        let table_id = self.next_file;
        let log_id = table_id + 1;
        // Reserve both numbers up front so a failed flush never reuses one
        // that an orphan file on disk might still hold.
        self.next_file += 2;

        let table_path = table_path(&self.dir, table_id);
        let mut writer = SstWriter::with_options(&table_path, self.writer_options())?;
        for (key, entry) in self.memtable.iter() {
            writer.add(key, entry)?;
        }
        writer.finish()?;
        let reader = open_table(&self.dir, table_id, &self.read_ctx)?;
        self.failpoint("flush:after_table")?;

        let new_wal = Wal::open(&log_path(&self.dir, log_id))?;
        sync_dir(&self.dir)?;
        self.failpoint("flush:after_new_log")?;

        let edits = [
            Edit::AddTable {
                id: table_id,
                level: 0,
            },
            Edit::SetLogNumber(log_id),
        ];
        self.commit(&edits, "flush")?;

        self.flush_bytes += reader.file_size();
        self.levels[0].insert(
            0,
            Table {
                id: table_id,
                reader,
            },
        );
        self.memtable = MemTable::new();
        *lock(&self.wal) = new_wal;
        self.wal_number = log_id;
        let _ = remove_obsolete_files(&self.dir, &self.version);
        Ok(())
    }

    fn stats(&self) -> Stats {
        let r = &self.read_ctx.stats;
        Stats {
            memtable_entries: self.memtable.len(),
            memtable_bytes: self.memtable.approx_size(),
            tables: self.levels.iter().map(Vec::len).sum(),
            level_files: self.levels.iter().map(Vec::len).collect(),
            level_bytes: self
                .levels
                .iter()
                .map(|l| l.iter().map(|t| t.reader.file_size()).sum())
                .collect(),
            log_number: self.wal_number,
            writes: self.writes,
            write_groups: self.write_groups,
            wal_syncs: self.wal_syncs,
            user_bytes: self.user_bytes,
            flush_bytes: self.flush_bytes,
            compaction_bytes: self.compaction_bytes,
            block_reads: ReadStats::get(&r.block_reads),
            cache_hits: ReadStats::get(&r.cache_hits),
            cache_bytes: self.read_ctx.cache.used(),
            filter_negatives: ReadStats::get(&r.filter_negatives),
            filter_false_positives: ReadStats::get(&r.filter_false_positives),
        }
    }

    fn writer_options(&self) -> WriterOptions {
        WriterOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            bloom_bits_per_key: self.opts.bloom_bits_per_key,
        }
    }

    /// The commit point shared by flush and compaction: one durable manifest
    /// write. If it fails, the edits may still have reached the disk (for a
    /// flush, that makes the current WAL obsolete), so the in-memory state can
    /// no longer be trusted to match the disk: poison.
    fn commit(&mut self, edits: &[Edit], op: &'static str) -> Result<()> {
        let (at, after) = match op {
            "flush" => ("flush:manifest", "flush:after_manifest"),
            _ => ("compact:manifest", "compact:after_manifest"),
        };
        let committed = self
            .failpoint(at)
            .and_then(|()| self.manifest.append(edits))
            .and_then(|()| self.failpoint(after));
        if let Err(e) = committed {
            return Err(self.poison(e));
        }
        for &edit in edits {
            self.version.apply(edit).map_err(Error::Corruption)?;
        }
        Ok(())
    }

    fn check_writable(&self) -> Result<()> {
        match &self.poisoned {
            Some(why) => Err(Error::Poisoned(why.clone())),
            None => Ok(()),
        }
    }

    /// Marks the database read-only and passes the original error through.
    /// The first failure is the one remembered.
    fn poison(&mut self, e: Error) -> Error {
        self.poisoned.get_or_insert_with(|| e.to_string());
        e
    }

    #[cfg(test)]
    fn failpoint(&self, name: &'static str) -> Result<()> {
        if self.fail_at == Some(name) {
            return Err(Error::Io(std::io::Error::other(format!(
                "failpoint {name}"
            ))));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn failpoint(&self, _name: &'static str) -> Result<()> {
        Ok(())
    }
}

/// Key + value bytes of a write (what `Stats::user_bytes` counts).
fn record_len(rec: &Record) -> usize {
    match rec {
        Record::Put { key, value } => key.len() + value.len(),
        Record::Delete { key } => key.len(),
    }
}

fn apply(memtable: &mut MemTable, rec: Record) {
    match rec {
        Record::Put { key, value } => memtable.put(&key, &value),
        Record::Delete { key } => memtable.delete(&key),
    }
}

fn log_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("{n:06}.log"))
}

fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst"))
}

/// In a level >= 1 (sorted, non-overlapping), the only table that can hold `key`.
fn table_for_key<'a>(level: &'a [Table], key: &[u8]) -> Option<&'a Table> {
    let i = level.partition_point(|t| t.largest() < key);
    level.get(i).filter(|t| t.smallest() <= key)
}

/// Opens every live table and arranges them by level, checking that levels
/// 1+ are non-overlapping (the invariant `table_for_key` relies on).
fn open_levels(dir: &Path, version: &Version, ctx: &Arc<ReadContext>) -> Result<Vec<Vec<Table>>> {
    let mut levels: Vec<Vec<Table>> = (0..MAX_LEVELS).map(|_| Vec::new()).collect();
    for (&id, &level) in &version.tables {
        let reader = open_table(dir, id, ctx)?;
        levels[level as usize].push(Table { id, reader });
    }
    levels[0].sort_by_key(|t| std::cmp::Reverse(t.id));
    for (n, level) in levels.iter_mut().enumerate().skip(1) {
        if let Some(t) = level.iter().find(|t| t.reader.entry_count() == 0) {
            return Err(Error::Corruption(format!(
                "table {} at level {n} is empty",
                t.id
            )));
        }
        level.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        for pair in level.windows(2) {
            if pair[0].largest() >= pair[1].smallest() {
                return Err(Error::Corruption(format!(
                    "tables {} and {} overlap at level {n}",
                    pair[0].id, pair[1].id
                )));
            }
        }
    }
    Ok(levels)
}

fn open_table(dir: &Path, id: u64, ctx: &Arc<ReadContext>) -> Result<SstReader> {
    SstReader::open_with(&table_path(dir, id), id, Arc::clone(ctx)).map_err(|e| match e {
        Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => Error::Corruption(format!(
            "manifest lists table {id}, but {id:06}.sst is missing"
        )),
        other => other,
    })
}

/// Files the engine owns, recognized by name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DbFile {
    Log(u64),
    Table(u64),
    Temp,
}

fn classify(name: &str) -> Option<DbFile> {
    if name.ends_with(".tmp") {
        return Some(DbFile::Temp);
    }
    if let Some(stem) = name.strip_suffix(".log") {
        return stem.parse().ok().map(DbFile::Log);
    }
    if let Some(stem) = name.strip_suffix(".sst") {
        return stem.parse().ok().map(DbFile::Table);
    }
    None
}

/// Engine-owned files in `dir`, sorted (logs by number, then tables, then temps).
fn list_files(dir: &Path) -> Result<Vec<(DbFile, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(kind) = entry.file_name().to_str().and_then(classify) {
            out.push((kind, entry.path()));
        }
    }
    out.sort();
    Ok(out)
}

/// Deletes WALs below the log number, tables not in the version, and temp files.
fn remove_obsolete_files(dir: &Path, version: &Version) -> Result<()> {
    let mut removed = false;
    for (kind, path) in list_files(dir)? {
        let obsolete = match kind {
            DbFile::Log(n) => n < version.log_number,
            DbFile::Table(id) => !version.tables.contains_key(&id),
            DbFile::Temp => true,
        };
        if obsolete {
            fs::remove_file(&path)?;
            removed = true;
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::Rng;
    use std::collections::BTreeMap;

    fn small() -> Options {
        Options {
            memtable_size: 1024,
            ..Options::default()
        }
    }

    /// Small enough that a few thousand writes reach levels 2-4.
    fn tiny() -> Options {
        Options {
            memtable_size: 512,
            l0_compaction_trigger: 2,
            level1_max_bytes: 4096,
            level_size_multiplier: 3,
            target_file_size: 1024,
            ..Options::default()
        }
    }

    fn files(dir: &Path) -> Vec<DbFile> {
        list_files(dir)
            .unwrap()
            .into_iter()
            .map(|(f, _)| f)
            .collect()
    }

    fn only_log(dir: &Path) -> PathBuf {
        let logs: Vec<PathBuf> = list_files(dir)
            .unwrap()
            .into_iter()
            .filter(|(f, _)| matches!(f, DbFile::Log(_)))
            .map(|(_, p)| p)
            .collect();
        assert_eq!(logs.len(), 1, "expected exactly one log: {logs:?}");
        logs.into_iter().next().unwrap()
    }

    fn key(i: usize) -> Vec<u8> {
        format!("key{i:05}").into_bytes()
    }

    #[test]
    fn writes_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
            db.delete(b"a").unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn open_refuses_mid_log_corruption() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
        }
        let wal_path = only_log(dir.path());
        let mut bytes = fs::read(&wal_path).unwrap();
        bytes[crate::wal::HEADER_LEN] ^= 0xFF; // corrupt the first record's key
        fs::write(&wal_path, &bytes).unwrap();

        assert!(matches!(Db::open(dir.path()), Err(Error::Corruption(_))));
        // The log must be left untouched for a human to inspect.
        assert_eq!(fs::read(&wal_path).unwrap(), bytes);
    }

    #[test]
    fn writes_after_torn_tail_are_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
        }
        // Crash mid-write of "b".
        let wal_path = only_log(dir.path());
        let len = fs::metadata(&wal_path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap()
            .set_len(len - 2)
            .unwrap();

        {
            let db = Db::open(dir.path()).unwrap();
            assert_eq!(db.get(b"b").unwrap(), None);
            db.put(b"c", b"3").unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn fresh_db_layout() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(files(dir.path()), vec![DbFile::Log(1)]);
        assert!(dir.path().join(crate::manifest::MANIFEST_FILE).exists());
        assert_eq!(db.stats().log_number, 1);
    }

    #[test]
    fn automatic_flushes_keep_every_key_readable() {
        let dir = tempfile::tempdir().unwrap();
        let n = 2000;
        {
            let db = Db::open_with(dir.path(), small()).unwrap();
            for i in 0..n {
                db.put(&key(i), format!("v{i}").as_bytes()).unwrap();
            }
            let st = db.stats();
            // ~32 flushes: level 0 kept under its trigger by compaction into L1.
            assert!(st.level_files[0] < 4 && st.level_files[1] > 0, "{st:?}");
            for i in 0..n {
                assert_eq!(db.get(&key(i)).unwrap(), Some(format!("v{i}").into_bytes()));
            }
        }
        let db = Db::open_with(dir.path(), small()).unwrap();
        for i in 0..n {
            assert_eq!(db.get(&key(i)).unwrap(), Some(format!("v{i}").into_bytes()));
        }
        // Old logs are deleted after each flush.
        let logs = files(dir.path())
            .into_iter()
            .filter(|f| matches!(f, DbFile::Log(_)))
            .count();
        assert_eq!(logs, 1);
    }

    #[test]
    fn newer_data_shadows_older_tables() {
        let dir = tempfile::tempdir().unwrap();
        let check = |db: &Db| {
            assert_eq!(db.get(b"a").unwrap(), None, "a: tombstone in newer table");
            assert_eq!(db.get(b"b").unwrap(), None, "b: tombstone in memtable/log");
            assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()), "c: overwritten");
        };
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"1").unwrap();
            db.put(b"c", b"1").unwrap();
            db.flush().unwrap();
            db.put(b"c", b"3").unwrap();
            db.delete(b"a").unwrap();
            db.flush().unwrap();
            db.delete(b"b").unwrap(); // stays in the memtable
            assert_eq!(db.stats().tables, 2);
            check(&db);
        }
        check(&Db::open(dir.path()).unwrap());
    }

    #[test]
    fn flushing_an_empty_memtable_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.flush().unwrap();
        assert_eq!(db.stats().tables, 0);
        assert_eq!(files(dir.path()), vec![DbFile::Log(1)]);
    }

    #[test]
    fn open_removes_crash_leftovers_but_not_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.flush().unwrap(); // table 2, log 3; log 1 is obsolete
        }
        for junk in ["000001.log", "000950.sst", "000951.sst.tmp"] {
            fs::write(dir.path().join(junk), b"junk").unwrap();
        }
        fs::write(dir.path().join("notes.txt"), b"mine").unwrap();

        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(files(dir.path()), vec![DbFile::Log(3), DbFile::Table(2)]);
        assert!(dir.path().join("notes.txt").exists());
    }

    #[test]
    fn missing_live_table_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.flush().unwrap();
        }
        fs::remove_file(dir.path().join("000002.sst")).unwrap();
        match Db::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("000002.sst"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.err()),
        }
    }

    /// Writes a table file directly and registers it at `level`, bypassing
    /// flush/compaction, so tests can set up exact level layouts.
    fn place_table(dir: &Path, id: u64, level: u8, entries: &[(&str, Option<&str>)]) {
        let mut w = SstWriter::create(&table_path(dir, id)).unwrap();
        for (k, v) in entries {
            let e = match v {
                Some(v) => Entry::Value(v.as_bytes().to_vec()),
                None => Entry::Tombstone,
            };
            w.add(k.as_bytes(), &e).unwrap();
        }
        w.finish().unwrap();
        let (mut m, _) = Manifest::open(dir).unwrap();
        m.append(&[Edit::AddTable { id, level }]).unwrap();
    }

    #[test]
    fn reads_walk_levels_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        // Oldest data at the bottom; each level up overrides some keys.
        place_table(
            d,
            10,
            3,
            &[
                ("a", Some("L3")),
                ("b", Some("L3")),
                ("c", Some("L3")),
                ("z", Some("L3")),
            ],
        );
        place_table(d, 11, 2, &[("b", Some("L2")), ("d", Some("L2"))]);
        place_table(d, 12, 2, &[("m", None), ("n", Some("L2"))]);
        place_table(d, 13, 1, &[("c", None), ("m", Some("L1"))]);
        place_table(d, 14, 0, &[("a", Some("L0-old")), ("d", Some("L0-old"))]);
        place_table(d, 15, 0, &[("a", Some("L0-new"))]);

        let db = Db::open(d).unwrap();
        db.put(b"z", b"mem").unwrap();
        let get = |db: &Db, k: &str| {
            db.get(k.as_bytes())
                .unwrap()
                .map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(get(&db, "a").as_deref(), Some("L0-new"), "newest L0 wins");
        assert_eq!(get(&db, "b").as_deref(), Some("L2"), "L2 over L3");
        assert_eq!(get(&db, "c"), None, "L1 tombstone hides L3");
        assert_eq!(get(&db, "d").as_deref(), Some("L0-old"), "L0 over L2");
        assert_eq!(get(&db, "m").as_deref(), Some("L1"), "L1 over L2 tombstone");
        assert_eq!(get(&db, "n").as_deref(), Some("L2"));
        assert_eq!(get(&db, "z").as_deref(), Some("mem"), "memtable over all");
        assert_eq!(get(&db, "e"), None);
        assert_eq!(db.stats().level_files, vec![2, 1, 2, 1, 0, 0, 0]);
    }

    #[test]
    fn overlapping_tables_in_a_deep_level_are_corruption() {
        let dir = tempfile::tempdir().unwrap();
        place_table(dir.path(), 10, 1, &[("a", Some("1")), ("m", Some("1"))]);
        place_table(dir.path(), 11, 1, &[("k", Some("2")), ("z", Some("2"))]);
        match Db::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("overlap at level 1"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.err()),
        }
    }

    /// Entries stored across all live tables (all versions, plus tombstones).
    fn stored_entries(db: &Db) -> u64 {
        db.state()
            .levels
            .iter()
            .flatten()
            .map(|t| t.reader.entry_count())
            .sum()
    }

    #[test]
    fn compaction_bounds_level0_and_fills_deep_levels() {
        let dir = tempfile::tempdir().unwrap();
        let n = 4000;
        {
            let db = Db::open_with(dir.path(), tiny()).unwrap();
            for i in 0..n {
                db.put(&key(i), &key(i)).unwrap();
                assert!(
                    db.stats().level_files[0] < 2,
                    "L0 over trigger after put {i}"
                );
            }
            let st = db.stats();
            assert!(st.level_files[3] > 0, "never reached L3: {st:?}");
            assert!(st.write_amplification() > 1.0, "{st:?}");
            assert_keys(&db, 0..n, "before reopen");
        }
        // Reopen re-validates that levels 1+ don't overlap.
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        assert_keys(&db, 0..n, "after reopen");
    }

    #[test]
    fn overwritten_versions_are_garbage_collected() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        for round in 0..40 {
            for i in 0..100 {
                db.put(&key(i), format!("r{round}").as_bytes()).unwrap();
            }
        }
        // 4000 versions were written; compaction keeps few stale ones.
        assert!(stored_entries(&db) < 800, "{} stored", stored_entries(&db));
        db.compact_all().unwrap();
        assert_eq!(stored_entries(&db), 100);
        for i in 0..100 {
            assert_eq!(db.get(&key(i)).unwrap(), Some(b"r39".to_vec()));
        }
    }

    #[test]
    fn compact_all_drops_deleted_data_entirely() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        for i in 0..1000 {
            db.put(&key(i), &key(i)).unwrap();
        }
        for i in 0..1000 {
            db.delete(&key(i)).unwrap();
        }
        db.compact_all().unwrap();
        assert_eq!(stored_entries(&db), 0, "{:?}", db.stats());
        assert_eq!(db.stats().tables, 0);
        assert_eq!(db.get(&key(5)).unwrap(), None);
        drop(db);
        let tables = files(dir.path())
            .into_iter()
            .filter(|f| matches!(f, DbFile::Table(_)))
            .count();
        assert_eq!(tables, 0, "deleted tables left on disk");
    }

    #[test]
    fn tombstone_survives_while_older_data_is_deeper() {
        let dir = tempfile::tempdir().unwrap();
        place_table(dir.path(), 10, 3, &[("a", Some("old"))]);
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        db.delete(b"a").unwrap();
        db.flush().unwrap();
        db.put(b"b", b"1").unwrap();
        db.flush().unwrap(); // L0 hits the trigger (2): L0 -> L1
        assert_eq!(db.stats().level_files[..2], [0, 1], "{:?}", db.stats());
        // Dropping the tombstone in L1 would resurrect "old" from L3.
        assert_eq!(db.get(b"a").unwrap(), None);
        drop(db);
        assert_eq!(
            Db::open_with(dir.path(), tiny())
                .unwrap()
                .get(b"a")
                .unwrap(),
            None
        );
    }

    #[test]
    fn lone_table_moves_down_without_rewriting() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.compact_all().unwrap(); // flush, then 6 trivial moves to the bottom
        let st = db.stats();
        assert_eq!(st.level_files, vec![0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(st.compaction_bytes, 0, "a trivial move rewrote data");
        drop(db);
        assert_eq!(
            Db::open(dir.path()).unwrap().get(b"a").unwrap(),
            Some(b"1".to_vec())
        );
    }

    const FLUSH_FAILPOINTS: [&str; 4] = [
        "flush:after_table",
        "flush:after_new_log",
        "flush:manifest",
        "flush:after_manifest",
    ];

    /// Writes key(0), key(1), ... until a put fails (the flush hit the
    /// failpoint). Returns how many keys were written; the failing key's own
    /// WAL record is durable, so it counts too.
    fn write_until_failpoint(db: &mut Db, fp: &'static str) -> usize {
        db.state().fail_at = Some(fp);
        for i in 0..100_000 {
            if db.put(&key(i), &key(i)).is_err() {
                return i + 1;
            }
        }
        panic!("{fp} never triggered");
    }

    fn assert_keys(db: &Db, range: std::ops::Range<usize>, ctx: &str) {
        for i in range {
            assert_eq!(db.get(&key(i)).unwrap(), Some(key(i)), "{ctx}: key {i}");
        }
    }

    /// No temp files, and every table on disk is live.
    fn assert_no_orphans(dir: &Path, db: &Db, ctx: &str) {
        let on_disk = files(dir);
        assert!(!on_disk.contains(&DbFile::Temp), "{ctx}: {on_disk:?}");
        let tables = on_disk
            .iter()
            .filter(|f| matches!(f, DbFile::Table(_)))
            .count();
        assert_eq!(tables, db.stats().tables, "{ctx}: {on_disk:?}");
    }

    #[test]
    fn crash_at_every_flush_step_loses_nothing() {
        for fp in FLUSH_FAILPOINTS {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Db::open_with(dir.path(), small()).unwrap();
            db.put(b"before", b"flush").unwrap();
            db.flush().unwrap(); // so there's an older table and log in play
            let n = write_until_failpoint(&mut db, fp);
            drop(db); // the "crash": nothing after the failpoint runs

            let db = Db::open_with(dir.path(), small()).unwrap();
            assert_eq!(db.get(b"before").unwrap(), Some(b"flush".to_vec()), "{fp}");
            assert_keys(&db, 0..n, fp);
            assert_no_orphans(dir.path(), &db, fp);

            // The recovered database must be fully writable, with no file
            // number collisions, across more flushes and another reopen.
            for i in n..n + 300 {
                db.put(&key(i), &key(i)).unwrap();
            }
            db.flush().unwrap();
            drop(db);
            let db = Db::open_with(dir.path(), small()).unwrap();
            assert_keys(&db, 0..n + 300, &format!("{fp}, after more writes"));
            assert_no_orphans(dir.path(), &db, fp);
        }
    }

    #[test]
    fn crash_at_every_compaction_step_loses_nothing() {
        for fp in [
            "compact:after_tables",
            "compact:manifest",
            "compact:after_manifest",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = Db::open_with(dir.path(), tiny()).unwrap();
            // Let a few compactions succeed first, so deeper levels exist.
            for i in 0..500 {
                db.put(&key(i), &key(i)).unwrap();
            }
            assert!(db.stats().level_files[2] > 0, "{fp}: {:?}", db.stats());
            let start = 500;
            db.state().fail_at = Some(fp);
            let mut n = start;
            while db.put(&key(n), &key(n)).is_ok() {
                n += 1;
            }
            let n = n + 1; // the failing put's WAL record is durable
            drop(db);

            let db = Db::open_with(dir.path(), tiny()).unwrap();
            assert_keys(&db, 0..n, fp);
            assert_no_orphans(dir.path(), &db, fp);

            for i in n..n + 1000 {
                db.put(&key(i), &key(i)).unwrap();
            }
            db.compact_all().unwrap();
            drop(db);
            let db = Db::open_with(dir.path(), tiny()).unwrap();
            assert_keys(&db, 0..n + 1000, &format!("{fp}, after more writes"));
            assert_no_orphans(dir.path(), &db, fp);
        }
    }

    #[test]
    fn failure_before_commit_is_retryable_without_reopen() {
        for fp in ["flush:after_table", "flush:after_new_log"] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Db::open_with(dir.path(), small()).unwrap();
            let n = write_until_failpoint(&mut db, fp);

            db.state().fail_at = None;
            db.flush().unwrap();
            for i in n..n + 300 {
                db.put(&key(i), &key(i)).unwrap();
            }
            assert_keys(&db, 0..n + 300, fp);
            drop(db);

            let db = Db::open_with(dir.path(), small()).unwrap();
            assert_keys(&db, 0..n + 300, &format!("{fp}, reopened"));
            assert_no_orphans(dir.path(), &db, fp);
        }
    }

    #[test]
    fn failure_at_commit_poisons_writes_but_not_reads() {
        for fp in ["flush:manifest", "flush:after_manifest"] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Db::open_with(dir.path(), small()).unwrap();
            let n = write_until_failpoint(&mut db, fp);
            db.state().fail_at = None;

            assert!(
                matches!(db.put(b"x", b"y"), Err(Error::Poisoned(_))),
                "{fp}"
            );
            assert!(matches!(db.delete(b"x"), Err(Error::Poisoned(_))), "{fp}");
            assert!(matches!(db.flush(), Err(Error::Poisoned(_))), "{fp}");
            assert_keys(&db, 0..n, &format!("{fp}, reads while poisoned"));
            drop(db);

            let db = Db::open_with(dir.path(), small()).unwrap();
            assert_keys(&db, 0..n, &format!("{fp}, reopened"));
            assert_eq!(db.get(b"x").unwrap(), None, "{fp}: refused write leaked");
            db.put(b"x", b"y").unwrap();
        }
    }

    /// Eight level-0 tables that each span the whole key range, so every
    /// lookup must consult all of them. Returns the database, with the key
    /// numbers 0..n spread over the tables.
    fn eight_overlapping_tables(dir: &Path, bloom_bits_per_key: usize, n: usize) -> Db {
        let opts = Options {
            l0_compaction_trigger: 100,
            bloom_bits_per_key,
            // Cache off, so every block a lookup needs is a disk read.
            block_cache_bytes: 0,
            ..Options::default()
        };
        let db = Db::open_with(dir, opts).unwrap();
        for round in 0..8 {
            for i in (round..n).step_by(8) {
                db.put(format!("key{i:06}").as_bytes(), b"v").unwrap();
            }
            db.flush().unwrap();
        }
        assert_eq!(db.stats().level_files[0], 8);
        db
    }

    #[test]
    fn bloom_filters_cut_block_reads_for_missing_keys() {
        let n = 4000;
        let mut reads = Vec::new();
        for bits in [0, 10] {
            let dir = tempfile::tempdir().unwrap();
            let db = eight_overlapping_tables(dir.path(), bits, n);
            let before = db.stats();
            // Between two real keys. Stopping 8 short of n keeps each probe
            // below every table's last key, so the index alone can't rule it
            // out: without a filter, each table costs one block read.
            let misses = n - 8;
            for i in 0..misses {
                assert_eq!(db.get(format!("key{i:06}x").as_bytes()).unwrap(), None);
            }
            let after = db.stats();
            let block_reads = after.block_reads - before.block_reads;
            let negatives = after.filter_negatives - before.filter_negatives;
            let fps = after.filter_false_positives - before.filter_false_positives;
            let lookups = 8 * misses as u64;
            println!(
                "bits/key {bits:2}: {block_reads} block reads for {misses} missing keys \
                 ({negatives} filter negatives, {fps} false positives = {:.2}%)",
                100.0 * fps as f64 / lookups as f64
            );
            if bits == 0 {
                assert_eq!(block_reads, lookups, "one read per table");
                assert_eq!(negatives + fps, 0);
            } else {
                assert_eq!(negatives + fps, lookups, "every table consulted its filter");
                assert_eq!(block_reads, fps, "only false positives read a block");
            }
            reads.push(block_reads);
        }
        assert!(
            reads[1] * 50 < reads[0],
            "filters saved too little: {reads:?}"
        );
    }

    #[test]
    fn deleted_key_in_a_filtered_table_shadows_older_tables() {
        // The tombstone must pass the newer table's filter; if tombstones were
        // left out of filters, the lookup would fall through to the old value.
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), small()).unwrap();
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap();
        db.delete(b"k").unwrap();
        db.flush().unwrap();
        assert_eq!(db.stats().level_files[0], 2);
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn tables_with_and_without_filters_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let no_filters = Options {
            bloom_bits_per_key: 0,
            block_cache_bytes: 0,
            l0_compaction_trigger: 100,
            ..small()
        };
        let db = Db::open_with(dir.path(), no_filters.clone()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap();
        drop(db);

        let with_filters = Options {
            bloom_bits_per_key: 10,
            ..no_filters
        };
        let db = Db::open_with(dir.path(), with_filters).unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
        let filtered: Vec<bool> = db.state().levels[0]
            .iter()
            .map(|t| t.reader.has_filter())
            .collect();
        assert_eq!(filtered, [true, false], "newest first");

        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        let before = db.stats();
        // "0" sorts before every key, so the index alone can't rule it out.
        assert_eq!(db.get(b"0").unwrap(), None);
        let after = db.stats();
        // The filtered table rules it out; the old one has to read a block.
        assert_eq!(after.filter_negatives - before.filter_negatives, 1);
        assert_eq!(after.block_reads - before.block_reads, 1);
    }

    /// Skewed reads (90% of lookups hit the first 5% of keys), with the cache
    /// off and then on.
    #[test]
    fn hot_keys_are_served_from_the_cache() {
        let n = 20_000;
        let value = [b'v'; 100];
        let mut results = Vec::new();
        for cache in [0, 256 << 10] {
            let dir = tempfile::tempdir().unwrap();
            let opts = Options {
                block_cache_bytes: cache,
                ..Options::default()
            };
            let db = Db::open_with(dir.path(), opts).unwrap();
            for i in 0..n {
                db.put(format!("key{i:06}").as_bytes(), &value).unwrap();
            }
            db.compact_all().unwrap();

            let mut rng = Rng::new(7);
            let before = db.stats();
            let reads = 20_000;
            for _ in 0..reads {
                let i = if rng.below(10) < 9 {
                    rng.below(n / 20)
                } else {
                    rng.below(n)
                };
                let k = format!("key{i:06}");
                assert_eq!(db.get(k.as_bytes()).unwrap().as_deref(), Some(&value[..]));
            }
            let after = db.stats();
            let disk = after.block_reads - before.block_reads;
            let hits = after.cache_hits - before.cache_hits;
            println!(
                "cache {:>3} KiB: {disk} disk reads, {hits} hits ({:.1}% hit rate) for {reads} gets",
                cache >> 10,
                100.0 * hits as f64 / reads as f64
            );
            assert_eq!(disk + hits, reads, "one block per get, from somewhere");
            assert!(after.cache_bytes <= cache);
            results.push(disk);
        }
        assert_eq!(results[0], 20_000);
        assert!(
            results[1] * 5 < results[0],
            "cache saved too little: {results:?}"
        );
    }

    // ---- M7: group commit and sync modes ----

    #[test]
    fn db_is_send_and_sync() {
        fn shareable<T: Send + Sync>() {}
        shareable::<Db>();
    }

    #[test]
    fn concurrent_writers_share_fsyncs() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.state().slow_wal = Duration::from_millis(2);
        let (threads, per) = (8, 40);
        thread::scope(|s| {
            for t in 0..threads {
                let db = &db;
                s.spawn(move || {
                    for i in 0..per {
                        db.put(format!("t{t}-{i}").as_bytes(), b"v").unwrap();
                    }
                });
            }
        });
        let st = db.stats();
        println!(
            "{} writes from {threads} threads in {} groups ({} fsyncs)",
            st.writes, st.write_groups, st.wal_syncs
        );
        assert_eq!(st.writes, threads * per);
        assert_eq!(st.wal_syncs, st.write_groups, "one fsync per group");
        assert!(
            st.write_groups * 3 < st.writes,
            "writers didn't share fsyncs"
        );

        drop(db);
        let db = Db::open(dir.path()).unwrap();
        for t in 0..threads {
            for i in 0..per {
                assert!(db.get(format!("t{t}-{i}").as_bytes()).unwrap().is_some());
            }
        }
    }

    #[test]
    fn failed_group_sync_fails_every_writer_in_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.state().slow_wal = Duration::from_millis(100);
        let pause = || thread::sleep(Duration::from_millis(20));
        let results: Vec<Result<()>> = thread::scope(|s| {
            // The first writer leads alone and holds the WAL for 100 ms...
            let first = s.spawn(|| db.put(b"first", b"1"));
            pause();
            // ...while seven more queue up behind it as one group...
            let rest: Vec<_> = (0..7)
                .map(|i| {
                    let db = &db;
                    s.spawn(move || db.put(format!("k{i}").as_bytes(), b"v"))
                })
                .collect();
            pause();
            // ...whose fsync will fail. (The first group already passed this
            // failpoint before it released the state lock.)
            db.state().fail_at = Some("wal:sync");
            std::iter::once(first)
                .chain(rest)
                .map(|h| h.join().unwrap())
                .collect()
        });

        assert!(
            results[0].is_ok(),
            "the first group synced: {:?}",
            results[0]
        );
        let failed = &results[1..];
        assert!(
            failed.iter().all(Result::is_err),
            "acked without an fsync: {failed:?}"
        );
        let followers = failed
            .iter()
            .filter(|r| matches!(r, Err(Error::Poisoned(_))))
            .count();
        assert_eq!(
            followers, 6,
            "the leader gets the I/O error, followers Poisoned"
        );
        assert_eq!(db.stats().write_groups, 1);
        assert!(matches!(db.put(b"x", b"y"), Err(Error::Poisoned(_))));
        assert_eq!(db.get(b"first").unwrap(), Some(b"1".to_vec()));

        drop(db);
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"first").unwrap(), Some(b"1".to_vec()));
        db.put(b"x", b"y").unwrap();
    }

    /// Writers race each other and explicit flushes (with automatic flushes
    /// and compactions too). Two checks after a reopen: every acknowledged
    /// unique key is there, and every shared key reads exactly as before the
    /// close, which needs the WAL and memtable to agree on write order.
    #[test]
    fn concurrent_writes_and_flushes_survive_reopen_identically() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        db.state().slow_wal = Duration::from_micros(300);
        let threads = 6;
        thread::scope(|s| {
            for t in 0..threads {
                let db = &db;
                s.spawn(move || {
                    let mut rng = Rng::new(t + 1);
                    for i in 0..300 {
                        db.put(format!("u{t}-{i}").as_bytes(), b"unique").unwrap();
                        let shared = format!("s{}", rng.below(20));
                        if rng.below(5) == 0 {
                            db.delete(shared.as_bytes()).unwrap();
                        } else {
                            db.put(shared.as_bytes(), format!("{t}-{i}").as_bytes())
                                .unwrap();
                        }
                        if i % 50 == 25 {
                            db.flush().unwrap();
                        }
                    }
                });
            }
        });
        let shared: Vec<_> = (0..20)
            .map(|k| db.get(format!("s{k}").as_bytes()).unwrap())
            .collect();
        drop(db);

        let db = Db::open_with(dir.path(), tiny()).unwrap();
        for t in 0..threads {
            for i in 0..300 {
                let k = format!("u{t}-{i}");
                assert!(db.get(k.as_bytes()).unwrap().is_some(), "lost {k}");
            }
        }
        for (k, before) in shared.iter().enumerate() {
            let after = db.get(format!("s{k}").as_bytes()).unwrap();
            assert_eq!(&after, before, "s{k} changed across reopen");
        }
    }

    /// For the staged tests below: the leader holds the WAL for 150 ms, and
    /// each step waits 30 ms so the previous one has reached its position.
    const SLOW: Duration = Duration::from_millis(150);
    fn step() {
        thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn flush_waits_for_the_group_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.put(b"before", b"1").unwrap();
        db.state().slow_wal = SLOW;
        thread::scope(|s| {
            // "a" is being logged into the current WAL...
            s.spawn(|| db.put(b"a", b"1").unwrap());
            step();
            // ...so this flush must wait. If it went ahead, it would retire
            // that WAL, and "a" would then land in a memtable whose log
            // doesn't hold it.
            db.flush().unwrap();
        });
        drop(db);
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"before").unwrap(), Some(b"1".to_vec()));
        assert_eq!(
            db.get(b"a").unwrap(),
            Some(b"1".to_vec()),
            "acked write lost"
        );
    }

    #[test]
    fn group_applies_in_queue_order() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.state().slow_wal = SLOW;
        thread::scope(|s| {
            s.spawn(|| db.put(b"lead", b"x").unwrap());
            step();
            // Two writes to one key queue up, in this order, into one group.
            s.spawn(|| db.put(b"k", b"first").unwrap());
            step();
            s.spawn(|| db.put(b"k", b"second").unwrap());
        });
        assert_eq!(db.stats().write_groups, 2);
        // Later in the queue = later in the WAL = the value a replay ends on.
        // The memtable must agree, or a reopen changes what readers see.
        assert_eq!(db.get(b"k").unwrap(), Some(b"second".to_vec()));
        drop(db);
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"second".to_vec()));
    }

    #[test]
    fn writers_queued_behind_a_failed_group_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        {
            let mut st = db.state();
            st.slow_wal = SLOW;
            st.fail_at = Some("wal:sync");
        }
        let results: Vec<Result<()>> = thread::scope(|s| {
            let first = s.spawn(|| db.put(b"first", b"1"));
            step();
            let rest: Vec<_> = (0..3)
                .map(|i| {
                    let db = &db;
                    s.spawn(move || db.put(format!("k{i}").as_bytes(), b"v"))
                })
                .collect();
            step();
            // The disk "recovers" before the queued group leads. It must
            // still be refused: the first group may have left a partial
            // record at the end of the log, and appending after it would
            // turn a torn tail into mid-log corruption (D2).
            db.state().fail_at = None;
            std::iter::once(first)
                .chain(rest)
                .map(|h| h.join().unwrap())
                .collect()
        });
        assert!(matches!(results[0], Err(Error::Io(_))), "{:?}", results[0]);
        for r in &results[1..] {
            assert!(matches!(r, Err(Error::Poisoned(_))), "{r:?}");
        }
        assert_eq!(db.stats().write_groups, 0);
    }

    fn periodic(every: Duration) -> Options {
        Options {
            sync_mode: SyncMode::Periodic(every),
            ..Options::default()
        }
    }

    /// Waits up to 5 s for `cond`, for tests that depend on a background thread.
    fn eventually(mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if cond() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        false
    }

    #[test]
    fn periodic_mode_acks_once_the_os_has_the_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), periodic(Duration::from_secs(3600))).unwrap();
        for i in 0..3 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        assert_eq!(db.stats().wal_syncs, 0, "no fsync yet");
        // Not fsynced, but already written to the OS: another reader of the
        // file (here, a replay) sees every acknowledged record.
        let replay = Wal::replay(&only_log(dir.path())).unwrap();
        assert_eq!(replay.records.len(), 3);

        // Dropping must stop the thread now, not after its hour-long wait.
        let start = std::time::Instant::now();
        drop(db);
        assert!(start.elapsed() < Duration::from_secs(2));
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v".to_vec()));
    }

    /// Regression test for a lost wakeup: `Drop` signalled "stop" before the
    /// new sync thread was waiting, and the thread then slept a full interval
    /// (an hour here), hanging the drop. Opening and dropping at once, many
    /// times, makes that ordering near-certain to occur.
    #[test]
    fn drop_right_after_open_does_not_wait_out_the_interval() {
        let dir = tempfile::tempdir().unwrap();
        let start = std::time::Instant::now();
        for _ in 0..200 {
            drop(Db::open_with(dir.path(), periodic(Duration::from_secs(3600))).unwrap());
        }
        assert!(start.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn periodic_thread_syncs_in_the_background() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), periodic(Duration::from_millis(5))).unwrap();
        db.put(b"k", b"v").unwrap();
        assert!(eventually(|| db.stats().wal_syncs >= 2));
        assert_eq!(db.stats().write_groups, 1);
    }

    /// The periodic fsync must not wait behind a long flush or compaction,
    /// or the "lose at most one interval" bound stretches under load.
    #[test]
    fn periodic_sync_never_waits_for_the_state_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), periodic(Duration::from_millis(5))).unwrap();
        db.put(b"k", b"v").unwrap();
        let syncs = || db.shared.periodic_syncs.load(Ordering::Relaxed);
        let st = db.state(); // stands in for a 300 ms compaction
        let before = syncs();
        thread::sleep(Duration::from_millis(300));
        assert!(
            syncs() >= before + 5,
            "{} syncs in 300 ms",
            syncs() - before
        );
        drop(st);
    }

    #[test]
    fn failed_periodic_sync_poisons() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), periodic(Duration::from_millis(5))).unwrap();
        db.put(b"k", b"v").unwrap();
        db.shared.fail_periodic_sync.store(true, Ordering::Relaxed);
        assert!(eventually(|| matches!(
            db.put(b"k2", b"v"),
            Err(Error::Poisoned(_))
        )));
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    /// Random puts, deletes, flushes, reads and reopens, checked against a
    /// BTreeMap after every read and at the end of each run.
    #[test]
    fn randomized_ops_match_a_btreemap() {
        for seed in 1..=12u64 {
            let mut rng = Rng::new(seed);
            let dir = tempfile::tempdir().unwrap();
            // Filter size changes on every reopen, so one database mixes
            // tables with no filter, weak filters and normal ones.
            let bloom_sizes = [0, 1, 4, 10];
            let mut opts = Options {
                memtable_size: 64 + rng.below(4000) as usize,
                l0_compaction_trigger: 2 + rng.below(4) as usize,
                level1_max_bytes: 512 + rng.below(8192),
                level_size_multiplier: 2 + rng.below(9),
                target_file_size: 256 + rng.below(4096) as usize,
                bloom_bits_per_key: bloom_sizes[rng.below(4) as usize],
                // Off, tiny (constant eviction) or roomy.
                block_cache_bytes: [0, 300, 4096, 1 << 20][rng.below(4) as usize],
                sync_mode: if rng.below(2) == 0 {
                    SyncMode::Always
                } else {
                    SyncMode::Periodic(Duration::from_millis(1))
                },
            };
            let mut db = Db::open_with(dir.path(), opts.clone()).unwrap();
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

            for _ in 0..2500 {
                let k = rng.key();
                match rng.below(100) {
                    0..=59 => {
                        let v = rng.value();
                        db.put(&k, &v).unwrap();
                        model.insert(k, v);
                    }
                    60..=84 => {
                        db.delete(&k).unwrap();
                        model.remove(&k);
                    }
                    85..=89 => db.flush().unwrap(),
                    90..=92 => {
                        drop(db);
                        opts.bloom_bits_per_key = bloom_sizes[rng.below(4) as usize];
                        db = Db::open_with(dir.path(), opts.clone()).unwrap();
                    }
                    _ => assert_eq!(db.get(&k).unwrap().as_ref(), model.get(&k), "seed {seed}"),
                }
            }
            drop(db);
            let db = Db::open_with(dir.path(), opts).unwrap();
            for (k, v) in &model {
                assert_eq!(
                    db.get(k).unwrap().as_ref(),
                    Some(v),
                    "seed {seed} key {k:?}"
                );
            }
            for _ in 0..500 {
                let k = rng.key();
                assert_eq!(
                    db.get(&k).unwrap().as_ref(),
                    model.get(&k),
                    "seed {seed} probe {k:?}"
                );
            }
        }
    }
}
