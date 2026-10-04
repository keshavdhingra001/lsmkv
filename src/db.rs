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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::fsutil::sync_dir;
use crate::key::SeqNo;
use crate::manifest::{Edit, Manifest, Version, MAX_LEVELS};
use crate::memtable::{Entry, MemTable};
use crate::sstable::filter::DEFAULT_BITS_PER_KEY;
use crate::sstable::{ReadContext, ReadStats, SstReader, WriterOptions, DEFAULT_BLOCK_SIZE};
use crate::wal::{Record, Wal};

mod background;
mod compaction;
mod iter;
mod snapshot;

pub use iter::DbIter;
pub use snapshot::Snapshot;

/// Tuning knobs. Defaults follow LevelDB.
#[derive(Debug, Clone)]
pub struct Options {
    /// Flush the memtable to an SSTable once its approximate size reaches this.
    pub memtable_size: usize,
    /// Compact level 0 into level 1 once level 0 has this many tables.
    pub l0_compaction_trigger: usize,
    /// With this many level-0 tables, each write first sleeps 1 ms, handing
    /// the background thread time to catch up (DESIGN.md D16).
    pub l0_slowdown_trigger: usize,
    /// With this many level-0 tables, a write that needs a fresh memtable
    /// waits until compaction brings level 0 back down.
    pub l0_stop_trigger: usize,
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
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
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
    /// Versions in the memtable (each write adds one).
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    /// Versions in the immutable memtable waiting to be flushed (0 if none).
    pub immutable_entries: usize,
    /// Total live tables across all levels.
    pub tables: usize,
    /// Tables per level, index = level.
    pub level_files: Vec<usize>,
    /// Bytes per level, index = level.
    pub level_bytes: Vec<u64>,
    pub log_number: u64,
    /// Sequence number of the newest write readers can see.
    pub last_sequence: SeqNo,
    /// Live `Snapshot` handles, and the oldest one's sequence number (the
    /// versions it can see are being kept).
    pub snapshots: usize,
    pub oldest_snapshot: Option<SeqNo>,
    /// Writes (puts and deletes) acknowledged since open.
    pub writes: u64,
    /// WAL write groups since open: each one a single append-and-sync for
    /// one or more writes.
    pub write_groups: u64,
    /// WAL fsyncs since open (by write groups, memtable switches, or the
    /// periodic thread).
    pub wal_syncs: u64,
    /// Write groups delayed 1 ms because level 0 reached the slowdown trigger.
    pub write_slowdowns: u64,
    /// Write groups that had to wait for the background thread (a flush
    /// still running, or level 0 at the stop trigger), and the total wait.
    pub write_stalls: u64,
    pub stall_micros: u64,
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
    /// Shared, so a scan's table iterator can own it (M9).
    reader: Arc<SstReader>,
}

impl Table {
    fn smallest(&self) -> &[u8] {
        self.reader.smallest_key().unwrap_or_default()
    }

    fn largest(&self) -> &[u8] {
        self.reader.largest_key().unwrap_or_default()
    }
}

/// Live tables by level. Level 0: newest first, ranges may overlap.
/// Levels 1+: sorted by key, ranges never overlap.
type Levels = Vec<Vec<Arc<Table>>>;

/// Everything a read needs, as one immutable unit (RocksDB's SuperVersion,
/// DESIGN.md D12). A flush or compaction never changes one; it builds the
/// next one and `State::install`s it. A reader holding an older one keeps
/// reading it safely: its memtable and tables stay alive (and their files
/// open) for as long as the reader holds the `Arc`.
struct SuperVersion {
    /// Takes new writes.
    mem: Arc<MemTable>,
    /// Full, and waiting for the background thread to flush it (D14). Every
    /// version in it is older than every version in `mem`.
    imm: Option<Arc<MemTable>>,
    levels: Levels,
}

impl SuperVersion {
    /// The newest version of `key` at or below `snapshot`. Sources are
    /// searched newest first, and every version in a newer source is newer
    /// than every version in an older one, so the first version found that
    /// the snapshot may see is the answer.
    fn get(&self, key: &[u8], snapshot: SeqNo) -> Result<Option<Vec<u8>>> {
        for mem in std::iter::once(&self.mem).chain(&self.imm) {
            match mem.get(key, snapshot) {
                Some((_, Entry::Value(v))) => return Ok(Some(v)),
                Some((_, Entry::Tombstone)) => return Ok(None),
                None => {}
            }
        }
        let level0 = self.levels[0].iter();
        let deeper = self.levels[1..]
            .iter()
            .filter_map(|level| table_for_key(level, key));
        for table in level0.chain(deeper) {
            match table.reader.get(key, snapshot)? {
                Some(Entry::Value(v)) => return Ok(Some(v)),
                Some(Entry::Tombstone) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }
}

/// What readers take, in one brief lock and never the state lock: the
/// current SuperVersion and the snapshot to read it at.
struct ReadView {
    current: Arc<SuperVersion>,
    /// `State::last_seq`, as of the last group applied.
    last_seq: SeqNo,
    /// Live snapshots: sequence number -> how many handles read at it.
    snapshots: BTreeMap<SeqNo, usize>,
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
/// Reads never take the state lock: they copy an `Arc` out of the read view
/// and search it with no lock held (DESIGN.md D12).
///
/// Lock order: `state`, then the WAL or the read view. Nothing takes `state`
/// while holding either, so they can't deadlock.
pub struct Db {
    shared: Arc<Shared>,
    /// The background fsync thread in `SyncMode::Periodic`.
    syncer: Option<JoinHandle<()>>,
    /// The flush and compaction thread (`background.rs`).
    background: Option<JoinHandle<()>>,
}

struct Shared {
    dir: PathBuf,
    state: Mutex<State>,
    /// What readers read. Its own lock, held only to copy out an `Arc` and a
    /// number, so a reader never waits behind a write, flush or compaction.
    view: Arc<Mutex<ReadView>>,
    /// Notified whenever a write group or a background job finishes, or the
    /// database is poisoned. Stalled leaders and `flush` / `compact_all`
    /// callers wait here. Queued writers don't: each sleeps on its own
    /// condition variable, in the queue (D17).
    turn: Condvar,
    /// Wakes the background thread: a memtable was switched, a manual
    /// compaction was requested, or the `Db` is closing.
    bg_work: Condvar,
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
    /// The live memtable and tables. Changed only by `install`, which also
    /// publishes the new one to readers.
    current: Arc<SuperVersion>,
    view: Arc<Mutex<ReadView>>,
    /// The active log. Its own lock, so a leader can append and fsync while
    /// readers and queueing writers use `State`. Only a leader (with `writing`
    /// set) or a holder of the state lock with no group in flight touches it.
    wal: Arc<Mutex<Wal>>,
    wal_number: u64,
    manifest: Manifest,
    version: Version,
    /// Next unused file number, for both logs and tables.
    next_file: u64,
    /// Sequence number of the newest write applied to the memtable. Reads
    /// see everything up to here. A group in flight has numbers above it,
    /// and becomes visible all at once when it's applied (DESIGN.md D18).
    last_seq: SeqNo,
    /// `last_seq` when the immutable memtable was switched out: its newest write.
    imm_last_seq: SeqNo,
    /// The `Db` is closing: the background thread exits.
    stopping: bool,
    /// The background thread is running a job.
    bg_busy: bool,
    /// A `compact_all` in progress: the lowest level it may still have to
    /// push down.
    manual_compaction: Option<usize>,
    /// Block cache and read counters, shared by every open table.
    read_ctx: Arc<ReadContext>,
    /// Set after a WAL or manifest write fails; see `Error::Poisoned`.
    poisoned: Option<String>,
    /// Per level: largest key of the last table compacted out of it, so
    /// successive compactions rotate through the key space.
    compact_pointer: Vec<Option<Vec<u8>>>,
    /// Writes waiting to be logged, oldest first, tagged with a ticket.
    /// Each with the condition variable its writer sleeps on (D17).
    queue: VecDeque<(u64, Record, Arc<Condvar>)>,
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
    write_slowdowns: u64,
    write_stalls: u64,
    stall_micros: u64,
    /// Test-only crash injection: the named failpoint returns an error.
    #[cfg(test)]
    fail_at: Option<&'static str>,
    /// Test-only slow disk: the leader sleeps this long while it holds the
    /// WAL, so concurrent writers reliably pile up into groups.
    #[cfg(test)]
    slow_wal: Duration,
    /// Test-only slow disk for the background thread: every flush and
    /// compaction sleeps this long, with no lock held, before its I/O.
    #[cfg(test)]
    slow_background: Duration,
    /// Test-only: the background thread flushes but doesn't compact.
    #[cfg(test)]
    pause_compactions: bool,
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
        // Otherwise writes could stop at a level-0 size that never triggers
        // the compaction that would let them continue.
        if !(opts.l0_compaction_trigger <= opts.l0_slowdown_trigger
            && opts.l0_slowdown_trigger <= opts.l0_stop_trigger)
        {
            return Err(Error::InvalidArgument(format!(
                "level-0 triggers must satisfy compaction ({}) <= slowdown ({}) <= stop ({})",
                opts.l0_compaction_trigger, opts.l0_slowdown_trigger, opts.l0_stop_trigger
            )));
        }
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

        let memtable = MemTable::new();
        // Writes in tables are numbered up to `last_sequence`; anything newer
        // is in the logs.
        let mut last_seq = version.last_sequence;
        for (i, &n) in live_logs.iter().enumerate() {
            let path = log_path(&dir, n);
            let replay = Wal::replay(&path)?;
            for (seq, rec) in replay.records {
                last_seq = last_seq.max(seq);
                apply(&memtable, seq, rec);
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
        let current = Arc::new(SuperVersion {
            mem: Arc::new(memtable),
            imm: None,
            levels,
        });
        let view = Arc::new(Mutex::new(ReadView {
            current: Arc::clone(&current),
            last_seq,
            snapshots: BTreeMap::new(),
        }));
        let state = State {
            dir: dir.clone(),
            opts: opts.clone(),
            current,
            view: Arc::clone(&view),
            wal: Arc::clone(&wal),
            wal_number,
            manifest,
            version,
            next_file,
            last_seq,
            imm_last_seq: 0,
            stopping: false,
            bg_busy: false,
            manual_compaction: None,
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
            write_slowdowns: 0,
            write_stalls: 0,
            stall_micros: 0,
            #[cfg(test)]
            fail_at: None,
            #[cfg(test)]
            slow_wal: Duration::ZERO,
            #[cfg(test)]
            slow_background: Duration::ZERO,
            #[cfg(test)]
            pause_compactions: false,
        };
        let shared = Arc::new(Shared {
            dir,
            state: Mutex::new(state),
            view,
            turn: Condvar::new(),
            bg_work: Condvar::new(),
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
        let background = {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("lsmkv-background".into())
                .spawn(move || background::run(&shared))?
        };
        Ok(Self {
            shared,
            syncer,
            background: Some(background),
        })
    }

    /// Writes `key = value`. Returns once the write is as durable as the
    /// `SyncMode` promises. An `Err` means the write was not logged.
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
    ///
    /// Takes no lock beyond copying out the read view, so it never waits for
    /// a write group, flush or compaction (DESIGN.md D12).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (current, snapshot) = {
            let view = lock(&self.shared.view);
            (Arc::clone(&view.current), view.last_seq)
        };
        current.get(key, snapshot)
    }

    /// The live keys in `range`, in key order, with their values, as of now:
    /// a scan sees every write acknowledged before this call and none after
    /// it, however long it runs. Like `get`, it never waits for writes,
    /// flushes or compactions (DESIGN.md D19).
    ///
    /// Any range of byte strings works, e.g. `&str` or `Vec<u8>` keys:
    ///
    /// ```no_run
    /// # let db = lsmkv::Db::open("data")?;
    /// for item in db.scan("user:".."user;")? {
    ///     let (key, value) = item?;
    /// }
    /// # Ok::<(), lsmkv::Error>(())
    /// ```
    pub fn scan<K: AsRef<[u8]> + ?Sized>(&self, range: impl RangeBounds<K>) -> Result<DbIter> {
        let (current, snapshot) = {
            let view = lock(&self.shared.view);
            (Arc::clone(&view.current), view.last_seq)
        };
        DbIter::new(&current, snapshot, range)
    }

    /// Every live key, in order: `scan` over the whole key space.
    pub fn iter(&self) -> Result<DbIter> {
        self.scan::<[u8]>(..)
    }

    /// Flushes the memtable, and returns once the background thread has
    /// written it and compacted until no level is over its limit.
    pub fn flush(&self) -> Result<()> {
        let mut st = self.lock();
        // Like a write group's leader: no group may be appending to the WAL
        // being sealed, and an older immutable memtable must be flushed first.
        loop {
            st.check_writable()?;
            if !st.writing && st.current.imm.is_none() {
                break;
            }
            st = self.wait(st);
        }
        if !st.current.mem.is_empty() {
            st.switch_memtable()?;
            self.shared.bg_work.notify_one();
        }
        self.wait_for_background(st)
    }

    /// Flushes, then pushes every table down to the bottom level, which drops
    /// every overwritten value and every tombstone no snapshot needs. Like
    /// RocksDB's `CompactRange` over the whole key space.
    pub fn compact_all(&self) -> Result<()> {
        self.flush()?;
        let mut st = self.lock();
        st.manual_compaction = Some(0);
        self.shared.bg_work.notify_one();
        loop {
            st.check_writable()?;
            if st.manual_compaction.is_none() && !st.bg_busy {
                return Ok(());
            }
            st = self.wait(st);
        }
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
        // Its own condition variable, so a leader wakes exactly the writers
        // whose state it changed, not every waiter (DESIGN.md D17).
        let wake = Arc::new(Condvar::new());
        st.queue.push_back((ticket, rec, Arc::clone(&wake)));
        loop {
            // A leader already logged this write (or failed to).
            if let Some(result) = st.finished.remove(&ticket) {
                return result;
            }
            // First in line with no group in flight: this writer leads.
            if !st.writing && st.queue.front().map(|q| q.0) == Some(ticket) {
                break;
            }
            st = wake.wait(st).expect("db lock poisoned");
        }

        let (mut st, room) = self.make_room(st);
        let (group, followers) = st.take_group();
        // Numbered in queue order. Only one group is ever in flight, so the
        // numbers right after the last applied write are free.
        let first_seq = st.last_seq + 1;
        // The database may have been poisoned while this group waited.
        let ready = room.and_then(|()| st.check_writable());
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
            for (seq, (_, rec)) in (first_seq..).zip(&group) {
                wal.append(seq, rec)?;
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
        let result = st.finish_group(group, first_seq, logged.map(drop));
        // Whoever is first in line now leads the next group: it waited
        // because this group was in flight.
        let next = st.queue.front().map(|q| Arc::clone(&q.2));
        drop(st);
        for writer in followers.iter().chain(&next) {
            writer.notify_one();
        }
        // `flush` callers wait for no group to be in flight.
        self.shared.turn.notify_all();
        result
    }

    /// Run by a leader before it takes its group: makes sure the memtable has
    /// room, switching a full one out for the background thread to flush
    /// (DESIGN.md D14, D16). It may release the state lock to wait; the
    /// caller stays the leader throughout, since its write is still at the
    /// front of the queue and no group is in flight.
    fn make_room<'a>(
        &'a self,
        mut st: MutexGuard<'a, State>,
    ) -> (MutexGuard<'a, State>, Result<()>) {
        let mut slowed = false;
        let mut stalled_since = None;
        let room = loop {
            if let Err(e) = st.check_writable() {
                break Err(e);
            }
            let l0 = st.current.levels[0].len();
            if !slowed && l0 >= st.opts.l0_slowdown_trigger {
                // A 1 ms delay on many writes, instead of one long stall
                // later: hands the background thread time to compact.
                slowed = true;
                st.write_slowdowns += 1;
                drop(st);
                thread::sleep(Duration::from_millis(1));
                st = self.lock();
                continue;
            }
            if st.current.mem.approx_size() < st.opts.memtable_size {
                break Ok(());
            }
            if st.current.imm.is_some() || l0 >= st.opts.l0_stop_trigger {
                // The previous memtable is still being flushed, or level 0
                // is too deep to add to: wait for the background thread.
                stalled_since.get_or_insert_with(Instant::now);
                st = self.wait(st);
                continue;
            }
            let switched = st.switch_memtable();
            self.shared.bg_work.notify_one();
            break switched;
        };
        if let Some(since) = stalled_since {
            st.write_stalls += 1;
            st.stall_micros += since.elapsed().as_micros() as u64;
        }
        (st, room)
    }

    /// Waits until the background thread is idle with nothing left to do:
    /// no immutable memtable and no level over its limit.
    fn wait_for_background(&self, mut st: MutexGuard<'_, State>) -> Result<()> {
        loop {
            st.check_writable()?;
            if st.current.imm.is_none() && !st.bg_busy && !st.compaction_wanted() {
                return Ok(());
            }
            st = self.wait(st);
        }
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
    /// Stops the background thread (after the job it's on, if any; an
    /// unflushed memtable is still in its WAL), then the periodic sync
    /// thread, then syncs the WAL once more, so a clean close never loses an
    /// acknowledged write in any mode.
    fn drop(&mut self) {
        if let Some(background) = self.background.take() {
            self.lock().stopping = true;
            self.shared.bg_work.notify_all();
            let _ = background.join();
        }
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
            // Writers stalled on the background thread must see it.
            shared.turn.notify_all();
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
    /// Takes the next write group off the front of the queue: at least one
    /// record, then more while the group stays under `MAX_GROUP_BYTES`.
    /// Returns it with its followers' condition variables (all but the
    /// leader's, which is first).
    fn take_group(&mut self) -> (Vec<(u64, Record)>, Vec<Arc<Condvar>>) {
        let mut group = Vec::new();
        let mut followers = Vec::new();
        let mut bytes = 0;
        while let Some((_, rec, _)) = self.queue.front() {
            let size = record_len(rec);
            if !group.is_empty() && bytes + size > MAX_GROUP_BYTES {
                break;
            }
            bytes += size;
            let (ticket, rec, wake) = self.queue.pop_front().expect("front exists");
            if !group.is_empty() {
                followers.push(wake);
            }
            group.push((ticket, rec));
        }
        (group, followers)
    }

    /// After the leader logged a group: apply it to the memtable in queue
    /// order with the numbers it was logged with (so a replay rebuilds
    /// exactly this memtable), make it visible, and record each follower's
    /// outcome. If logging failed,
    /// none of the group was acknowledged, and the database is poisoned: the
    /// log may now end in a partial record.
    fn finish_group(
        &mut self,
        group: Vec<(u64, Record)>,
        first_seq: SeqNo,
        logged: Result<()>,
    ) -> Result<()> {
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
        for (seq, (ticket, rec)) in (first_seq..).zip(group) {
            self.writes += 1;
            self.user_bytes += record_len(&rec) as u64;
            apply(&self.current.mem, seq, rec);
            self.last_seq = seq;
            if ticket != leader {
                self.finished.insert(ticket, Ok(()));
            }
        }
        // Now the whole group is visible, at once.
        lock(&self.view).last_seq = self.last_seq;
        Ok(())
    }

    /// Makes the memtable immutable, for the background thread to flush,
    /// and starts a fresh one with a fresh WAL (DESIGN.md D14).
    ///
    /// Callers hold the state lock with no group in flight (so no leader is
    /// appending to the WAL being sealed) and no immutable memtable. The old
    /// WAL stays live, and recovery replays it, until the flush commits.
    /// Any failure poisons: the WALs on disk may no longer match memory.
    fn switch_memtable(&mut self) -> Result<()> {
        debug_assert!(!self.writing && self.current.imm.is_none());
        let log_id = self.next_file;
        self.next_file += 1;
        let switched = self.failpoint("switch:new_log").and_then(|()| {
            let new_wal = Wal::open(&log_path(&self.dir, log_id))?;
            sync_dir(&self.dir)?;
            // In `Periodic` mode the old log's tail may not be on disk yet,
            // and nothing would sync it once it's swapped out. It holds the
            // immutable memtable's writes until their table commits.
            let mut wal = lock(&self.wal);
            let before = wal.sync_count();
            wal.sync()?;
            let synced = wal.sync_count() - before;
            *wal = new_wal;
            Ok(synced)
        });
        match switched {
            Ok(synced) => self.wal_syncs += synced,
            Err(e) => return Err(self.poison(e)),
        }
        self.wal_number = log_id;
        self.imm_last_seq = self.last_seq;
        self.install(SuperVersion {
            mem: Arc::new(MemTable::new()),
            imm: Some(Arc::clone(&self.current.mem)),
            levels: self.current.levels.clone(),
        });
        Ok(())
    }

    fn stats(&self) -> Stats {
        let r = &self.read_ctx.stats;
        // One lock, taken and released here. Inside the struct literal below,
        // a guard would live to the end of the whole statement, and a second
        // `lock(&self.view)` there would deadlock on it.
        let (snapshots, oldest_snapshot) = {
            let view = lock(&self.view);
            let oldest = view.snapshots.keys().next().copied();
            (view.snapshots.values().sum(), oldest)
        };
        Stats {
            memtable_entries: self.current.mem.len(),
            memtable_bytes: self.current.mem.approx_size(),
            immutable_entries: self.current.imm.as_ref().map_or(0, |m| m.len()),
            tables: self.current.levels.iter().map(Vec::len).sum(),
            level_files: self.current.levels.iter().map(Vec::len).collect(),
            level_bytes: self
                .current
                .levels
                .iter()
                .map(|l| l.iter().map(|t| t.reader.file_size()).sum())
                .collect(),
            log_number: self.wal_number,
            last_sequence: self.last_seq,
            snapshots,
            oldest_snapshot,
            writes: self.writes,
            write_groups: self.write_groups,
            wal_syncs: self.wal_syncs,
            write_slowdowns: self.write_slowdowns,
            write_stalls: self.write_stalls,
            stall_micros: self.stall_micros,
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

    /// Makes `sv` the live data, for this state and for readers. The only
    /// way the memtables or levels change after open.
    fn install(&mut self, sv: SuperVersion) {
        self.current = Arc::new(sv);
        lock(&self.view).current = Arc::clone(&self.current);
    }

    /// `install` with new levels and the same memtables.
    fn install_levels(&mut self, levels: Levels) {
        self.install(SuperVersion {
            mem: Arc::clone(&self.current.mem),
            imm: self.current.imm.clone(),
            levels,
        });
    }

    /// The oldest snapshot any reader may still read at: the oldest live
    /// `Snapshot`, or the latest write if there is none (what `get` reads
    /// at). Versions only an older reader could see may be dropped.
    fn oldest_snapshot(&self) -> SeqNo {
        let view = lock(&self.view);
        let oldest = view.snapshots.keys().next().copied();
        oldest.map_or(self.last_seq, |s| s.min(self.last_seq))
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

fn apply(memtable: &MemTable, seq: SeqNo, rec: Record) {
    match rec {
        Record::Put { key, value } => memtable.put(&key, seq, &value),
        Record::Delete { key } => memtable.delete(&key, seq),
    }
}

fn log_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("{n:06}.log"))
}

fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst"))
}

/// In a level >= 1 (sorted, non-overlapping), the only table that can hold `key`.
fn table_for_key<'a>(level: &'a [Arc<Table>], key: &[u8]) -> Option<&'a Arc<Table>> {
    let i = level.partition_point(|t| t.largest() < key);
    level.get(i).filter(|t| t.smallest() <= key)
}

/// Opens every live table and arranges them by level, checking that levels
/// 1+ are non-overlapping (the invariant `table_for_key` relies on).
fn open_levels(dir: &Path, version: &Version, ctx: &Arc<ReadContext>) -> Result<Levels> {
    let mut levels: Levels = (0..MAX_LEVELS).map(|_| Vec::new()).collect();
    for (&id, &level) in &version.tables {
        let reader = open_table(dir, id, ctx)?;
        levels[level as usize].push(Arc::new(Table {
            id,
            reader: Arc::new(reader),
        }));
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
    use crate::sstable::SstWriter;
    use crate::test_util::Rng;
    use std::collections::{BTreeMap, HashSet};
    use std::ops::Bound;

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
            // Close behind, so writes get slowed and stalled for real.
            l0_slowdown_trigger: 4,
            l0_stop_trigger: 6,
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
    fn sequence_numbers_survive_replay_and_flush() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            for i in 0..3 {
                db.put(b"a", format!("v{i}").as_bytes()).unwrap();
            }
            assert_eq!(db.stats().last_sequence, 3);
        }
        // From the WAL alone.
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.stats().last_sequence, 3);
        assert_eq!(db.get(b"a").unwrap(), Some(b"v2".to_vec()));

        // After a flush the WAL that held 1..=3 is gone; the manifest has to
        // remember them, or numbering restarts below what the table holds and
        // reads at the restarted numbers can't see the table's versions.
        db.flush().unwrap();
        drop(db);
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.stats().last_sequence, 3);
        assert_eq!(db.get(b"a").unwrap(), Some(b"v2".to_vec()));
        db.put(b"a", b"v3").unwrap();
        assert_eq!(db.stats().last_sequence, 4);
        db.compact_all().unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn flush_keeps_only_versions_a_reader_can_see() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        for i in 0..5 {
            db.put(b"a", format!("v{i}").as_bytes()).unwrap();
        }
        db.delete(b"b").unwrap();
        db.put(b"b", b"back").unwrap();
        db.delete(b"c").unwrap();
        assert_eq!(
            db.stats().memtable_entries,
            8,
            "every version is kept in memory"
        );
        db.flush().unwrap();
        let st = db.state();
        let entries = st.current.levels[0][0].reader.entries().unwrap();
        let kept: Vec<(&[u8], SeqNo)> = entries.iter().map(|(k, s, _)| (&k[..], *s)).collect();
        // Newest of a and b; c's tombstone stays (older tables may hold c).
        assert_eq!(kept, vec![(&b"a"[..], 5), (b"b", 7), (b"c", 8)]);
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
            db.flush().unwrap(); // log 2, then table 3; log 1 is obsolete
        }
        for junk in ["000001.log", "000950.sst", "000951.sst.tmp"] {
            fs::write(dir.path().join(junk), b"junk").unwrap();
        }
        fs::write(dir.path().join("notes.txt"), b"mine").unwrap();

        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(files(dir.path()), vec![DbFile::Log(2), DbFile::Table(3)]);
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
        fs::remove_file(dir.path().join("000003.sst")).unwrap();
        match Db::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("000003.sst"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.err()),
        }
    }

    /// Writes a table file directly and registers it at `level`, bypassing
    /// flush/compaction, so tests can set up exact level layouts. Its
    /// entries are numbered `id`, so place newer data with higher ids.
    fn place_table(dir: &Path, id: u64, level: u8, entries: &[(&str, Option<&str>)]) {
        let mut w = SstWriter::create(&table_path(dir, id)).unwrap();
        for (k, v) in entries {
            let e = match v {
                Some(v) => Entry::Value(v.as_bytes().to_vec()),
                None => Entry::Tombstone,
            };
            w.add(k.as_bytes(), id, &e).unwrap();
        }
        w.finish().unwrap();
        let (mut m, _) = Manifest::open(dir).unwrap();
        m.append(&[Edit::AddTable { id, level }, Edit::SetLastSequence(id)])
            .unwrap();
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
            .current
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
            let stop = tiny().l0_stop_trigger;
            for i in 0..n {
                db.put(&key(i), &key(i)).unwrap();
                // Compaction runs in the background, so level 0 may grow
                // past its trigger, but the stop trigger bounds it.
                let l0 = db.stats().level_files[0];
                assert!(l0 <= stop, "L0 at {l0} after put {i}");
            }
            db.flush().unwrap();
            let st = db.stats();
            assert!(
                st.level_files[0] < 2,
                "L0 over trigger once settled: {st:?}"
            );
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
        // Flush, then 6 trivial moves to the bottom, then the one rewrite
        // `compact_all` does at the bottom (D18).
        db.compact_all().unwrap();
        let st = db.stats();
        assert_eq!(st.level_files, vec![0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(
            st.compaction_bytes, st.flush_bytes,
            "a trivial move rewrote data"
        );
        drop(db);
        assert_eq!(
            Db::open(dir.path()).unwrap().get(b"a").unwrap(),
            Some(b"1".to_vec())
        );
    }

    /// Every step of a memtable switch and its background flush that can fail.
    const FLUSH_FAILPOINTS: [&str; 4] = [
        "switch:new_log",
        "flush:after_table",
        "flush:manifest",
        "flush:after_manifest",
    ];

    /// Writes key(0), key(1), ... until a put fails (the switch or the
    /// background flush hit the failpoint and poisoned the database). Returns
    /// how many puts succeeded: a refused put was never logged.
    fn write_until_failpoint(db: &mut Db, fp: &'static str) -> usize {
        db.state().fail_at = Some(fp);
        for i in 0..100_000 {
            if db.put(&key(i), &key(i)).is_err() {
                return i;
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
            // n puts were acknowledged; the failing one was refused unlogged.
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
    fn background_failure_poisons_writes_but_not_reads() {
        // Before the commit too: a background job has no caller to hand a
        // retryable error to (DESIGN.md D15).
        for fp in FLUSH_FAILPOINTS {
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
            l0_slowdown_trigger: 100,
            l0_stop_trigger: 100,
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
            l0_slowdown_trigger: 100,
            l0_stop_trigger: 100,
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
        let filtered: Vec<bool> = db.state().current.levels[0]
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
            // Close together, so slowdowns and stalls happen too.
            let l0_compaction_trigger = 2 + rng.below(4) as usize;
            let l0_slowdown_trigger = l0_compaction_trigger + rng.below(3) as usize;
            let mut opts = Options {
                memtable_size: 64 + rng.below(4000) as usize,
                l0_compaction_trigger,
                l0_slowdown_trigger,
                l0_stop_trigger: l0_slowdown_trigger + rng.below(3) as usize,
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

    // ---- M8: reads without the state lock ----

    /// The read view, as `Db::get` takes it.
    fn read_view(db: &Db) -> (Arc<SuperVersion>, SeqNo) {
        let view = lock(&db.shared.view);
        (Arc::clone(&view.current), view.last_seq)
    }

    #[test]
    fn reads_never_wait_for_the_state_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Db::open(dir.path()).unwrap());
        db.put(b"in-table", b"1").unwrap();
        db.flush().unwrap();
        db.put(b"in-memtable", b"2").unwrap();

        // Stand-in for a long flush or compaction: hold the state lock.
        let st = db.state();
        let reader = {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                (
                    db.get(b"in-table").unwrap(),
                    db.get(b"in-memtable").unwrap(),
                    db.get(b"missing").unwrap(),
                )
            })
        };
        let finished = eventually(|| reader.is_finished());
        drop(st);
        assert!(finished, "a read waited for the state lock");
        let got = reader.join().unwrap();
        assert_eq!(got, (Some(b"1".to_vec()), Some(b"2".to_vec()), None));
    }

    #[test]
    fn an_old_super_version_stays_readable_after_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), small()).unwrap();
        for i in 0..200 {
            db.put(&key(i), b"old").unwrap();
        }
        db.flush().unwrap();
        // A reader that grabbed the view before the compaction...
        let (old, snapshot) = read_view(&db);
        let old_ids: Vec<u64> = old.levels.iter().flatten().map(|t| t.id).collect();
        for i in 0..200 {
            db.put(&key(i), b"new").unwrap();
        }
        db.compact_all().unwrap();
        // ...whose table files the compaction has since deleted...
        for id in &old_ids {
            assert!(
                !table_path(dir.path(), *id).exists(),
                "table {id} not deleted"
            );
        }
        // ...still reads its point in time, through the descriptors it holds.
        for i in (0..200).step_by(7) {
            assert_eq!(old.get(&key(i), snapshot).unwrap(), Some(b"old".to_vec()));
            assert_eq!(db.get(&key(i)).unwrap(), Some(b"new".to_vec()));
        }
    }

    /// One writer counts a key up from 0 while flushes and compactions run.
    /// Readers check two things on every read: the value never goes
    /// backwards, and it's never below the last value acknowledged before the
    /// read started. A reader that saw a memtable swapped out before its
    /// table was in place, or a stale view, would break one of them.
    #[test]
    fn readers_see_every_acknowledged_write_across_flushes_and_compactions() {
        use std::sync::atomic::AtomicBool;
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Db::open_with(dir.path(), tiny()).unwrap());
        let acked = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        db.put(b"counter", &0u64.to_be_bytes()).unwrap();

        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (db, acked, done) = (Arc::clone(&db), Arc::clone(&acked), Arc::clone(&done));
                thread::spawn(move || {
                    let mut last = 0;
                    let mut reads = 0u64;
                    while !done.load(Ordering::Acquire) {
                        let floor = acked.load(Ordering::Acquire);
                        let v = db.get(b"counter").unwrap().expect("counter vanished");
                        let v = u64::from_be_bytes(v.try_into().unwrap());
                        assert!(v >= floor, "read {v} after {floor} was acknowledged");
                        assert!(v >= last, "went backwards: {last} then {v}");
                        last = v;
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        let n = 3000u64;
        for i in 1..=n {
            db.put(b"counter", &i.to_be_bytes()).unwrap();
            // Filler, so the memtable fills and flushes and compactions run.
            db.put(&key(i as usize % 500), &[b'x'; 40]).unwrap();
            acked.store(i, Ordering::Release);
        }
        done.store(true, Ordering::Release);
        let reads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
        let st = db.stats();
        assert!(st.compaction_bytes > 0 && st.flush_bytes > 0, "{st:?}");
        assert!(reads > 1000, "only {reads} reads");
    }

    // ---- M8: background flush and compaction ----

    /// The longest single call to `op`, over `n` calls.
    fn slowest(n: usize, mut op: impl FnMut(usize)) -> Duration {
        (0..n)
            .map(|i| {
                let start = std::time::Instant::now();
                op(i);
                start.elapsed()
            })
            .max()
            .unwrap_or_default()
    }

    #[test]
    fn writes_and_reads_continue_while_a_flush_runs() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size: 16 << 10,
            ..Options::default()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        db.state().slow_background = Duration::from_millis(400);
        // Fill the memtable until the switch: it's now immutable, and the
        // background thread is (slowly) flushing it. Watched through the read
        // view, which (unlike `stats`) doesn't take the state lock.
        let flushing = |db: &Db| read_view(db).0.imm.is_some();
        let mut n = 0;
        while !flushing(&db) {
            db.put(&key(n), &key(n)).unwrap();
            n += 1;
        }
        // Writes into the fresh memtable and reads of the flushing one (and
        // of the new one) don't wait for the flush.
        let put_max = slowest(50, |i| db.put(&key(n + i), &key(n + i)).unwrap());
        let get_max = slowest(n + 50, |i| {
            assert_eq!(db.get(&key(i)).unwrap(), Some(key(i)))
        });
        assert!(
            put_max < Duration::from_millis(100),
            "a put took {put_max:?}"
        );
        assert!(
            get_max < Duration::from_millis(100),
            "a get took {get_max:?}"
        );
        assert!(flushing(&db), "flush finished too soon to tell");
        assert_eq!(db.stats().write_stalls, 0);

        // Filling the second memtable before the first is flushed must wait.
        n += 50;
        while db.stats().write_stalls == 0 {
            db.put(&key(n), &key(n)).unwrap();
            n += 1;
        }
        let st = db.stats();
        assert!(st.stall_micros > 50_000, "{st:?}");
        drop(db);
        let db = Db::open(dir.path()).unwrap();
        assert_keys(&db, 0..n, "reopened");
    }

    #[test]
    fn writes_and_reads_continue_while_a_compaction_runs() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), small()).unwrap();
        db.state().pause_compactions = true;
        for round in 0..4 {
            for i in 0..50 {
                db.put(&key(i), format!("r{round}").as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }
        assert_eq!(db.stats().level_files[0], 4);
        {
            let mut st = db.state();
            st.pause_compactions = false;
            st.slow_background = Duration::from_millis(400);
        }
        db.shared.bg_work.notify_one();
        assert!(eventually(|| db.state().bg_busy));

        // Fewer bytes than a memtable holds, so no switch is needed.
        let put_max = slowest(20, |i| db.put(&key(100 + i), b"new").unwrap());
        let get_max = slowest(50, |i| {
            assert_eq!(db.get(&key(i)).unwrap(), Some(b"r3".to_vec()));
        });
        assert!(db.state().bg_busy, "compaction finished too soon to tell");
        assert!(
            put_max < Duration::from_millis(100),
            "a put took {put_max:?}"
        );
        assert!(
            get_max < Duration::from_millis(100),
            "a get took {get_max:?}"
        );

        db.flush().unwrap(); // waits for the compaction too
        let st = db.stats();
        // The 4 old tables went down; the one new table is the 20 puts.
        assert_eq!(st.level_files[0], 1, "{st:?}");
        assert!(st.compaction_bytes > 0, "{st:?}");
        assert_eq!(db.get(&key(7)).unwrap(), Some(b"r3".to_vec()));
    }

    #[test]
    fn deep_level_0_slows_then_stops_writes_until_compaction_catches_up() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size: 1024,
            l0_compaction_trigger: 2,
            l0_slowdown_trigger: 3,
            l0_stop_trigger: 4,
            ..Options::default()
        };
        let db = Arc::new(Db::open_with(dir.path(), opts).unwrap());
        db.state().pause_compactions = true;
        let written = Arc::new(AtomicU64::new(0));
        let writer = {
            let (db, written) = (Arc::clone(&db), Arc::clone(&written));
            thread::spawn(move || {
                for i in 0..2000 {
                    db.put(&key(i), &[b'v'; 50]).unwrap();
                    written.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        // Level 0 fills to the stop trigger. Writes go on into the memtable
        // until it's full; then the switch it needs waits, and they stop.
        let stopped = eventually(|| {
            let before = written.load(Ordering::Relaxed);
            thread::sleep(Duration::from_millis(100));
            before == written.load(Ordering::Relaxed)
        });
        assert!(stopped && !writer.is_finished(), "writes never stopped");
        assert_eq!(db.stats().level_files[0], 4);
        assert!(db.stats().write_slowdowns > 0, "{:?}", db.stats());

        // Compaction drains level 0, and the writer finishes.
        db.state().pause_compactions = false;
        db.shared.bg_work.notify_one();
        writer.join().unwrap();
        db.flush().unwrap();
        let st = db.stats();
        assert!(st.write_stalls > 0, "{st:?}");
        assert!(st.level_files[0] < 2, "{st:?}");
        assert_eq!(db.get(&key(1999)).unwrap(), Some(vec![b'v'; 50]));
    }

    #[test]
    fn level_0_triggers_must_be_ordered() {
        let dir = tempfile::tempdir().unwrap();
        for (compaction, slowdown, stop) in [(4, 3, 12), (4, 8, 7)] {
            let opts = Options {
                l0_compaction_trigger: compaction,
                l0_slowdown_trigger: slowdown,
                l0_stop_trigger: stop,
                ..Options::default()
            };
            assert!(matches!(
                Db::open_with(dir.path(), opts),
                Err(Error::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn switch_syncs_the_old_log_even_in_periodic_mode() {
        // An hour-long interval: no periodic fsync will happen in this test.
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size: 1024,
            ..periodic(Duration::from_secs(3600))
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        let mut n = 0;
        while db.stats().immutable_entries == 0 && db.stats().flush_bytes == 0 {
            db.put(&key(n), &key(n)).unwrap();
            n += 1;
        }
        // The one fsync is the switch sealing the old log, which still holds
        // every acknowledged write until its table commits. Without it, a
        // power cut before the flush finished could lose more than the
        // interval allows.
        assert_eq!(db.stats().wal_syncs, 1, "{:?}", db.stats());
    }

    // ---- M8: snapshots ----

    #[test]
    fn snapshot_reads_a_point_in_time_through_flush_and_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        for i in 0..100 {
            db.put(&key(i), b"old").unwrap();
        }
        let snap = db.snapshot();
        assert_eq!(snap.sequence(), 100);
        for i in 0..100 {
            if i % 2 == 0 {
                db.put(&key(i), b"new").unwrap();
            } else {
                db.delete(&key(i)).unwrap();
            }
        }
        db.put(b"born-later", b"x").unwrap();
        db.compact_all().unwrap();

        for i in 0..100 {
            assert_eq!(snap.get(&key(i)).unwrap(), Some(b"old".to_vec()), "{i}");
            let now = (i % 2 == 0).then(|| b"new".to_vec());
            assert_eq!(db.get(&key(i)).unwrap(), now, "{i}");
        }
        assert_eq!(snap.get(b"born-later").unwrap(), None);
        let st = db.stats();
        assert_eq!((st.snapshots, st.oldest_snapshot), (1, Some(100)));
        // Both versions of every key are still stored, for the snapshot.
        let kept = stored_entries(&db);
        assert!(kept >= 200, "{kept}");

        // Once it's gone, compaction may drop what only it could see:
        // the old values, and the tombstones with the keys they deleted.
        drop(snap);
        assert_eq!(db.stats().snapshots, 0);
        db.compact_all().unwrap();
        assert_eq!(stored_entries(&db), 51, "50 new values and born-later");
    }

    /// Section 1's surviving mutation: a compaction may split its output
    /// only between user keys. Snapshots keep many big versions of one key
    /// alive, more than one output table holds, so a split would put that
    /// key in two tables of one level: reads would miss versions, and a
    /// reopen would refuse the overlapping tables.
    #[test]
    fn one_keys_versions_never_span_two_tables() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open_with(dir.path(), tiny()).unwrap();
            let mut snaps = Vec::new();
            for v in 0..20u8 {
                db.put(b"hot", &[v; 200]).unwrap();
                db.put(&key(v as usize), b"filler").unwrap();
                snaps.push(db.snapshot());
            }
            db.compact_all().unwrap();
            let st = db.stats();
            assert!(st.level_files[6] > 1, "too few tables to split: {st:?}");
            for (v, snap) in snaps.iter().enumerate() {
                assert_eq!(snap.get(b"hot").unwrap(), Some(vec![v as u8; 200]));
            }
        }
        Db::open_with(dir.path(), tiny()).expect("levels overlap after reopen");
    }

    /// A writer sets a = i, then b = i, for i = 1, 2, ... So in any
    /// consistent view, b <= a. Readers read a, then b, through one
    /// snapshot; separate gets could see a newer b than a.
    #[test]
    fn reads_through_one_snapshot_agree_under_concurrent_writes() {
        use std::sync::atomic::AtomicBool;
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Db::open_with(dir.path(), tiny()).unwrap());
        let done = Arc::new(AtomicBool::new(false));
        let num = |v: Option<Vec<u8>>| v.map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap()));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (db, done) = (Arc::clone(&db), Arc::clone(&done));
                thread::spawn(move || {
                    let mut checks = 0u64;
                    while !done.load(Ordering::Acquire) {
                        let snap = db.snapshot();
                        let a = num(snap.get(b"a").unwrap());
                        thread::yield_now(); // let the writer get ahead
                        let b = num(snap.get(b"b").unwrap());
                        assert!(b <= a, "b = {b} but a = {a} in one snapshot");
                        checks += 1;
                    }
                    checks
                })
            })
            .collect();
        for i in 1..=3000u64 {
            db.put(b"a", &i.to_be_bytes()).unwrap();
            db.put(b"b", &i.to_be_bytes()).unwrap();
        }
        done.store(true, Ordering::Release);
        let checks: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(checks > 100, "only {checks} checks");
        assert!(db.stats().compaction_bytes > 0, "{:?}", db.stats());
    }

    type Model = BTreeMap<Vec<u8>, Vec<u8>>;

    /// Random writes, snapshots taken and dropped, flushes and compactions;
    /// every live snapshot is checked against a copy of the model taken with it.
    #[test]
    fn randomized_snapshots_match_a_model() {
        for seed in 1..=10u64 {
            let mut rng = Rng::new(seed);
            let dir = tempfile::tempdir().unwrap();
            let opts = Options {
                memtable_size: 256 + rng.below(2048) as usize,
                target_file_size: 256 + rng.below(2048) as usize,
                ..tiny()
            };
            let db = Db::open_with(dir.path(), opts).unwrap();
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            // Each live snapshot, with the model as it was when it was taken.
            let mut snaps: Vec<(Snapshot, Model)> = Vec::new();
            for step in 0..3000 {
                let k = rng.key();
                match rng.below(100) {
                    0..=49 => {
                        let v = rng.value();
                        db.put(&k, &v).unwrap();
                        model.insert(k, v);
                    }
                    50..=64 => {
                        db.delete(&k).unwrap();
                        model.remove(&k);
                    }
                    65..=69 => snaps.push((db.snapshot(), model.clone())),
                    70..=73 if !snaps.is_empty() => {
                        let i = rng.below(snaps.len() as u64) as usize;
                        snaps.swap_remove(i);
                    }
                    74 => db.flush().unwrap(),
                    75 => db.compact_all().unwrap(),
                    _ if !snaps.is_empty() => {
                        let i = rng.below(snaps.len() as u64) as usize;
                        let (snap, then) = &snaps[i];
                        assert_eq!(
                            snap.get(&k).unwrap().as_ref(),
                            then.get(&k),
                            "seed {seed} step {step} key {k:?} at {}",
                            snap.sequence()
                        );
                    }
                    _ => assert_eq!(db.get(&k).unwrap().as_ref(), model.get(&k)),
                }
            }
            db.compact_all().unwrap();
            for (snap, then) in &snaps {
                for (k, v) in then {
                    assert_eq!(snap.get(k).unwrap().as_ref(), Some(v), "seed {seed}");
                }
            }
        }
    }

    // ---- M9: range scans ----

    type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

    fn collect(it: DbIter) -> Pairs {
        it.map(|item| item.unwrap()).collect()
    }

    fn pairs<K: AsRef<[u8]> + ?Sized>(model: &Model, range: impl RangeBounds<K>) -> Pairs {
        let range = (
            range.start_bound().map(|k| k.as_ref()),
            range.end_bound().map(|k| k.as_ref()),
        );
        model
            .iter()
            .filter(|(k, _)| range.contains(k.as_slice()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Versions spread over every kind of source: L1+ tables, level-0
    /// tables, the immutable memtable is covered by the concurrent tests.
    #[test]
    fn scan_merges_every_source_and_honors_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), small()).unwrap();
        let mut model = Model::new();
        let mut put = |db: &Db, k: &str, v: &str| {
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
            model.insert(k.as_bytes().to_vec(), v.as_bytes().to_vec());
        };
        for i in 0..40 {
            put(&db, &format!("k{i:02}"), "deep");
        }
        db.compact_all().unwrap();
        for i in (0..40).step_by(3) {
            put(&db, &format!("k{i:02}"), "l0");
        }
        db.flush().unwrap();
        for i in (0..40).step_by(5) {
            put(&db, &format!("k{i:02}"), "mem");
        }
        for k in ["k07", "k09", "k10"] {
            db.delete(k.as_bytes()).unwrap();
            model.remove(k.as_bytes());
        }
        let (current, _) = read_view(&db);
        assert!(!current.levels[0].is_empty() && !current.mem.is_empty());
        assert!(current.levels[1..].iter().any(|l| !l.is_empty()));

        let b = |s: &'static str| s.as_bytes();
        assert_eq!(collect(db.iter().unwrap()), pairs::<[u8]>(&model, ..));
        assert_eq!(
            collect(db.scan(b("k05")..b("k12")).unwrap()),
            pairs(&model, b("k05")..b("k12"))
        );
        assert_eq!(
            collect(db.scan(b("k05")..=b("k12")).unwrap()),
            pairs(&model, b("k05")..=b("k12"))
        );
        assert_eq!(
            collect(db.scan(b("k3")..).unwrap()),
            pairs(&model, b("k3")..)
        );
        assert_eq!(
            collect(db.scan(..b("k02")).unwrap()),
            pairs(&model, ..b("k02"))
        );
        let excl = (Bound::Excluded(b("k05")), Bound::Included(b("k08")));
        assert_eq!(
            collect(db.scan::<[u8]>(excl).unwrap()),
            pairs::<[u8]>(&model, excl)
        );
        // A deleted start key, ranges with nothing in them, and start > end.
        assert_eq!(
            collect(db.scan(b("k09")..b("k11")).unwrap()),
            pairs(&model, b("k09")..b("k11"))
        );
        assert!(collect(db.scan(b("x")..).unwrap()).is_empty());
        assert!(collect(db.scan(b("k20")..b("k10")).unwrap()).is_empty());
        assert!(collect(db.scan(b("k20")..b("k20")).unwrap()).is_empty());
    }

    #[test]
    fn scan_is_one_point_in_time() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        for i in 0..300 {
            db.put(format!("k{i:03}").as_bytes(), b"before").unwrap();
        }
        let mut it = db.iter().unwrap();
        let first = it.next().unwrap().unwrap();
        // After the scan starts: overwrite, delete and add keys, then push
        // everything down so the tables the scan is reading are deleted.
        for i in 0..300 {
            match i % 3 {
                0 => db.put(format!("k{i:03}").as_bytes(), b"after").unwrap(),
                1 => db.delete(format!("k{i:03}").as_bytes()).unwrap(),
                _ => db.put(format!("k{i:03}x").as_bytes(), b"new").unwrap(),
            }
        }
        db.compact_all().unwrap();
        let rest = collect(it);
        let mut seen = vec![first];
        seen.extend(rest);
        let want: Pairs = (0..300)
            .map(|i| (format!("k{i:03}").into_bytes(), b"before".to_vec()))
            .collect();
        assert_eq!(seen, want);
        // A new scan sees the new state: 100 overwritten, 100 deleted, 100
        // untouched and 100 added.
        let now = collect(db.iter().unwrap());
        let count = |v: &[u8]| now.iter().filter(|(_, x)| x == v).count();
        assert_eq!(
            (count(b"after"), count(b"before"), count(b"new")),
            (100, 100, 100)
        );
    }

    #[test]
    fn scan_keeps_reading_tables_compaction_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), small()).unwrap();
        for i in 0..200 {
            db.put(format!("k{i:03}").as_bytes(), &[b'v'; 50]).unwrap();
        }
        db.flush().unwrap();
        let sst = |dir: &Path| -> HashSet<PathBuf> {
            fs::read_dir(dir)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.extension().is_some_and(|x| x == "sst"))
                .collect()
        };
        let before = sst(dir.path());
        let it = db.iter().unwrap();
        for i in 0..200 {
            db.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
        }
        db.compact_all().unwrap();
        assert!(
            sst(dir.path()).is_disjoint(&before),
            "the old tables are gone"
        );
        let got = collect(it);
        assert_eq!(got.len(), 200);
        assert!(got.iter().all(|(_, v)| v == &[b'v'; 50]));
    }

    #[test]
    fn snapshot_scan_sees_the_snapshot_after_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        for i in 0..100 {
            db.put(format!("k{i:02}").as_bytes(), b"old").unwrap();
        }
        let snap = db.snapshot();
        for i in 0..100 {
            if i % 2 == 0 {
                db.delete(format!("k{i:02}").as_bytes()).unwrap();
            } else {
                db.put(format!("k{i:02}").as_bytes(), b"new").unwrap();
            }
        }
        db.compact_all().unwrap();
        let got = collect(snap.scan(&b"k10"[..]..&b"k20"[..]).unwrap());
        let want: Pairs = (10..20)
            .map(|i| (format!("k{i:02}").into_bytes(), b"old".to_vec()))
            .collect();
        assert_eq!(got, want);
        let now = collect(db.scan(&b"k10"[..]..&b"k20"[..]).unwrap());
        assert_eq!(now.len(), 5);
        assert!(now.iter().all(|(_, v)| v == b"new"));
    }

    /// Staged: a flush is held in place, so the immutable memtable is one
    /// of the scan's sources, overlapping the new memtable's keys.
    #[test]
    fn scan_reads_the_immutable_memtable_during_a_flush() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size: 16 << 10,
            ..Options::default()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        db.state().slow_background = Duration::from_millis(400);
        let flushing = |db: &Db| read_view(db).0.imm.is_some();
        let mut n = 0;
        while !flushing(&db) {
            db.put(&key(n), b"imm").unwrap();
            n += 1;
        }
        // Overwrite every other key in the fresh memtable.
        for i in (0..n).step_by(2) {
            db.put(&key(i), b"mem").unwrap();
        }
        let got = collect(db.iter().unwrap());
        assert!(flushing(&db), "flush finished too soon to tell");
        let want: Pairs = (0..n)
            .map(|i| {
                (
                    key(i),
                    if i % 2 == 0 {
                        b"mem".to_vec()
                    } else {
                        b"imm".to_vec()
                    },
                )
            })
            .collect();
        assert_eq!(got, want);
    }

    /// A scan only opens tables whose key range overlaps it: with a table
    /// outside the range damaged on disk, a narrow scan still succeeds.
    #[test]
    fn narrow_scan_skips_tables_outside_its_range() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            target_file_size: 16 << 10,
            block_cache_bytes: 0,
            ..small()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        for i in 0..4000 {
            db.put(format!("k{i:04}").as_bytes(), &[b'v'; 20]).unwrap();
        }
        db.compact_all().unwrap();
        let (current, _) = read_view(&db);
        let level = current.levels.iter().rfind(|l| !l.is_empty()).unwrap();
        assert!(level.len() > 4, "{} tables", level.len());
        // Damage every table except the first, mid-file: past block 0, which
        // open already read.
        let mut damaged = 0;
        for t in level[1..].iter().filter(|t| t.reader.block_count() >= 3) {
            damaged += 1;
            let path = table_path(dir.path(), t.id);
            let mut bytes = fs::read(&path).unwrap();
            let mid = bytes.len() / 2;
            bytes[mid] ^= 0xff;
            fs::write(&path, &bytes).unwrap();
        }
        assert!(damaged >= 3, "{damaged} tables damaged");
        let first = &level[0];
        let (lo, hi) = (first.smallest().to_vec(), first.largest().to_vec());
        let got = collect(db.scan(&lo[..]..=&hi[..]).unwrap());
        assert_eq!(got.len() as u64, first.reader.entry_count());
        // A scan that reaches the damaged tables reports it.
        let err = db.iter().unwrap().find_map(|item| item.err());
        assert!(matches!(err, Some(Error::Corruption(_))), "{err:?}");
    }

    /// While a writer adds k0000, k0001, ... in order, with flushes and
    /// compactions running, every scan must see a gap-free prefix: it's one
    /// point in time, so it can't see a write without every earlier one.
    #[test]
    fn concurrent_scans_see_a_prefix_of_ordered_writes() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Db::open_with(dir.path(), tiny()).unwrap());
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let scanners: Vec<_> = (0..3)
            .map(|_| {
                let (db, done) = (Arc::clone(&db), Arc::clone(&done));
                thread::spawn(move || {
                    let mut scans = 0;
                    while !done.load(Ordering::Acquire) {
                        let keys: Vec<Vec<u8>> = collect(db.iter().unwrap())
                            .into_iter()
                            .map(|(k, _)| k)
                            .collect();
                        for (i, k) in keys.iter().enumerate() {
                            assert_eq!(k, format!("k{i:04}").as_bytes(), "gap in a scan");
                        }
                        scans += 1;
                    }
                    scans
                })
            })
            .collect();
        for i in 0..3000 {
            db.put(format!("k{i:04}").as_bytes(), &[b'v'; 16]).unwrap();
        }
        done.store(true, Ordering::Release);
        let scans: u64 = scanners.into_iter().map(|s| s.join().unwrap()).sum();
        assert!(scans > 10, "only {scans} scans");
        assert!(db.stats().compaction_bytes > 0);
    }

    /// Random writes, snapshots, flushes and compactions; scans with random
    /// bounds, at now or at a random live snapshot, checked against a model.
    #[test]
    fn randomized_scans_match_a_model() {
        for seed in 1..=10u64 {
            let mut rng = Rng::new(seed);
            let dir = tempfile::tempdir().unwrap();
            let opts = Options {
                memtable_size: 256 + rng.below(2048) as usize,
                target_file_size: 256 + rng.below(2048) as usize,
                ..tiny()
            };
            let db = Db::open_with(dir.path(), opts).unwrap();
            let mut model = Model::new();
            let mut snaps: Vec<(Snapshot, Model)> = Vec::new();
            let bound = |rng: &mut Rng| match rng.below(3) {
                0 => Bound::Included(rng.key()),
                1 => Bound::Excluded(rng.key()),
                _ => Bound::Unbounded,
            };
            for step in 0..3000 {
                let k = rng.key();
                match rng.below(100) {
                    0..=54 => {
                        let v = rng.value();
                        db.put(&k, &v).unwrap();
                        model.insert(k, v);
                    }
                    55..=69 => {
                        db.delete(&k).unwrap();
                        model.remove(&k);
                    }
                    70..=72 => snaps.push((db.snapshot(), model.clone())),
                    73..=75 if !snaps.is_empty() => {
                        let i = rng.below(snaps.len() as u64) as usize;
                        snaps.swap_remove(i);
                    }
                    76 => db.flush().unwrap(),
                    77 => db.compact_all().unwrap(),
                    _ => {
                        let (lo, hi) = (bound(&mut rng), bound(&mut rng));
                        let range = (
                            lo.as_ref().map(Vec::as_slice),
                            hi.as_ref().map(Vec::as_slice),
                        );
                        let (got, want) = if !snaps.is_empty() && rng.below(2) == 0 {
                            let (snap, then) = &snaps[rng.below(snaps.len() as u64) as usize];
                            (
                                collect(snap.scan::<[u8]>(range).unwrap()),
                                pairs::<[u8]>(then, range),
                            )
                        } else {
                            (
                                collect(db.scan::<[u8]>(range).unwrap()),
                                pairs::<[u8]>(&model, range),
                            )
                        };
                        assert_eq!(got, want, "seed {seed} step {step} range {range:?}");
                    }
                }
            }
        }
    }
}
