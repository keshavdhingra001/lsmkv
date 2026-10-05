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
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::key::{SeqNo, MAX_SEQ};
use crate::manifest::{Edit, Manifest, Version, MAX_LEVELS};
use crate::memtable::{Entry, MemTable};
use crate::sstable::filter::DEFAULT_BITS_PER_KEY;
use crate::sstable::{ReadContext, ReadStats, SstReader, WriterOptions, DEFAULT_BLOCK_SIZE};
use crate::vfs::{Fs, RealFs};
use crate::wal::{Record, Wal};
use batch::Pending;

mod background;
mod batch;
mod compaction;
mod iter;
mod snapshot;

pub use batch::{Transaction, WriteBatch};
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
    /// The filesystem every file goes through: the real one, or a simulated
    /// disk for testing (DESIGN.md D28).
    pub fs: Arc<dyn Fs>,
    /// No background or periodic-sync threads: flushes and compactions run
    /// on the writing thread, when a write needs room or on `flush` /
    /// `compact_all`, and the WAL is synced only by `sync_wal` (and memtable
    /// switches and close). With a single-threaded caller, every run is then
    /// deterministic, which is what simulation testing needs (D28). Slower
    /// writes; for testing.
    pub inline_background: bool,
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
            fs: RealFs::shared(),
            inline_background: false,
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
    /// Opens live table `id` in `dir`. A missing file is corruption: the
    /// manifest (or a job about to commit it) says it exists.
    fn open(fs: &dyn Fs, dir: &Path, id: u64, ctx: &Arc<ReadContext>) -> Result<Arc<Table>> {
        let reader = SstReader::open_in(fs, &table_path(dir, id), id, Arc::clone(ctx)).map_err(
            |e| match e {
                Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => Error::Corruption(
                    format!("manifest lists table {id}, but {id:06}.sst is missing"),
                ),
                other => other,
            },
        )?;
        Ok(Arc::new(Table {
            id,
            reader: Arc::new(reader),
        }))
    }

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
        Ok(match self.newest(key, snapshot)? {
            Some((_, Entry::Value(v))) => Some(v),
            Some((_, Entry::Tombstone)) | None => None,
        })
    }

    /// The sequence number of `key`'s newest version (a value or a
    /// tombstone), if it has one: what a transaction's conflict check needs.
    fn newest_seq(&self, key: &[u8]) -> Result<Option<SeqNo>> {
        Ok(self.newest(key, MAX_SEQ)?.map(|(seq, _)| seq))
    }

    /// `key`'s newest version at or below `snapshot`, with its number.
    fn newest(&self, key: &[u8], snapshot: SeqNo) -> Result<Option<(SeqNo, Entry)>> {
        for mem in std::iter::once(&self.mem).chain(&self.imm) {
            if let Some(found) = mem.get(key, snapshot) {
                return Ok(Some(found));
            }
        }
        let level0 = self.levels[0].iter();
        let deeper = self.levels[1..]
            .iter()
            .filter_map(|level| table_for_key(level, key));
        for table in level0.chain(deeper) {
            if let Some(found) = table.reader.get_versioned(key, snapshot)? {
                return Ok(Some(found));
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
    queue: VecDeque<(u64, Pending, Arc<Condvar>)>,
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
        let fs = Arc::clone(&opts.fs);
        fs.create_dir_all(&dir)?;
        let (mut manifest, mut version) = Manifest::open_in(&*fs, &dir)?;

        remove_obsolete_files(&*fs, &dir, &version)?;
        let live_logs: Vec<u64> = list_files(&*fs, &dir)?
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
        let levels = open_levels(&*fs, &dir, &version, &read_ctx)?;

        let memtable = MemTable::new();
        // Writes in tables are numbered up to `last_sequence`; anything newer
        // is in the logs.
        let mut last_seq = version.last_sequence;
        for (i, &n) in live_logs.iter().enumerate() {
            let path = log_path(&dir, n);
            let replay = Wal::replay_in(&*fs, &path)?;
            for (seq, rec) in replay.records {
                last_seq = last_seq.max(seq);
                apply(&memtable, seq, rec);
            }
            // Only the newest log gets appended to, so only it needs its torn
            // tail cut off (new writes must not land after garbage).
            let is_active = i + 1 == live_logs.len();
            let mut f = fs.open_append(&path)?;
            if is_active && replay.file_len > replay.valid_len {
                f.set_len(replay.valid_len)?;
            }
            // What was just replayed will be served from now on, so it must
            // be durable: some of it may only have been in the page cache
            // (an unsynced `Periodic` write, or a group whose fsync never
            // finished). Without this, a later power cut or failed fsync
            // could take back writes this database already served (D28; the
            // simulation found it).
            f.sync()?;
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
        let wal = Wal::open_in(&*fs, &log_path(&dir, wal_number))?;
        fs.sync_dir(&dir)?;

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
            // Inline mode starts no threads: `sync_wal` is the caller's job.
            SyncMode::Periodic(_) if opts.inline_background => None,
            SyncMode::Periodic(every) => {
                let shared = Arc::clone(&shared);
                Some(thread::spawn(move || {
                    sync_periodically(&shared, &wal, every)
                }))
            }
        };
        let background = if opts.inline_background {
            None
        } else {
            let shared = Arc::clone(&shared);
            Some(
                thread::Builder::new()
                    .name("lsmkv-background".into())
                    .spawn(move || background::run(&shared))?,
            )
        };
        Ok(Self {
            shared,
            syncer,
            background,
        })
    }

    /// Writes `key = value`. Returns once the write is as durable as the
    /// `SyncMode` promises. An `Err` means the write was not logged.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.commit(Pending::writes(vec![Record::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }]))
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.commit(Pending::writes(vec![Record::Delete { key: key.to_vec() }]))
    }

    /// Newest data first: memtable, then every level-0 table (newest first),
    /// then at most one table per deeper level. The first hit wins, and a
    /// tombstone hit means "deleted": older data is not consulted.
    ///
    /// Takes no lock beyond copying out the read view, so it never waits for
    /// a write group, flush or compaction (DESIGN.md D12).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (current, snapshot) = self.shared.read_view();
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
        let (current, snapshot) = self.shared.read_view();
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
            if st.opts.inline_background && !st.writing {
                // No background thread will flush it: do it here.
                st = self.run_inline(st);
                continue;
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
            if st.opts.inline_background {
                st = self.run_inline(st);
            }
            st.check_writable()?;
            if st.manual_compaction.is_none() && !st.bg_busy {
                return Ok(());
            }
            st = self.wait(st);
        }
    }

    /// fsyncs the WAL now, making every acknowledged write durable. Only
    /// useful in `Periodic` mode (in `Always` mode each write already is);
    /// with `inline_background` there's no periodic thread, so this is how
    /// the WAL gets synced.
    pub fn sync_wal(&self) -> Result<()> {
        let wal = {
            let st = self.lock();
            st.check_writable()?;
            Arc::clone(&st.wal)
        };
        let synced = lock(&wal).sync();
        match synced {
            Ok(()) => {
                self.shared.periodic_syncs.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            // As for the periodic thread: a failed fsync poisons (D7).
            Err(e) => Err(self.lock().poison(e)),
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

    /// Queues `pending` (a put, a delete, a batch or a transaction's writes)
    /// and returns once a leader has logged and applied it, or refused it.
    fn commit(&self, pending: Pending) -> Result<()> {
        // Nothing to write and nothing to check. (A transaction that only
        // locked keys still goes through the queue: its check must run.)
        if pending.ops.is_empty() && pending.also_check.is_empty() {
            return Ok(());
        }
        let mut st = self.lock();
        st.check_writable()?;
        let ticket = st.next_ticket;
        st.next_ticket += 1;
        // Its own condition variable, so a leader wakes exactly the writers
        // whose state it changed, not every waiter (DESIGN.md D17).
        let wake = Arc::new(Condvar::new());
        st.queue.push_back((ticket, pending, Arc::clone(&wake)));
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
        let current = Arc::clone(&st.current);
        drop(st);

        // The slow part, without the state lock: readers keep reading and new
        // writers keep queueing (they become the next group).
        // First, transactions' conflict checks (D26): an entry that fails one
        // is left out, and gets no sequence numbers.
        let verdicts = batch::check_conflicts(&current, &group);
        drop(current);
        let logged = ready.and_then(|()| {
            let mut wal = lock(&wal);
            let syncs_before = wal.sync_count();
            let mut seq = first_seq;
            for ((_, pending), verdict) in group.iter().zip(&verdicts) {
                if verdict.is_ok() {
                    // One WAL record per batch, so it survives a crash whole
                    // or not at all (D25).
                    wal.append_batch(seq, &pending.ops)?;
                    seq += pending.ops.len() as SeqNo;
                }
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
        let result = st.finish_group(group, verdicts, first_seq, logged.map(drop));
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
            // Inline mode does the background thread's work only where a
            // writer would otherwise wait for it, so between switches the
            // immutable memtable and its WAL stay unflushed for a while, as
            // with a slow background thread. (Flushing right after every
            // switch hid two planted durability bugs from the simulation.)
            if st.opts.inline_background
                && (l0 >= st.opts.l0_slowdown_trigger
                    || (st.current.mem.approx_size() >= st.opts.memtable_size
                        && st.current.imm.is_some()))
            {
                st = self.run_inline(st);
                if st.current.levels[0].len() >= st.opts.l0_slowdown_trigger
                    || st.current.imm.is_some()
                {
                    // Nothing more to do (poisoned, or no progress possible).
                    if let Err(e) = st.check_writable() {
                        break Err(e);
                    }
                }
            }
            if !slowed && l0 >= st.opts.l0_slowdown_trigger && !st.opts.inline_background {
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
                if st.opts.inline_background {
                    // Never wait for a thread that doesn't exist. The work
                    // above flushed the immutable memtable unless the
                    // database is poisoned (the loop's first check returns
                    // that); if level 0 still can't shrink, go ahead.
                    st = self.run_inline(st);
                    if st.current.imm.is_some() {
                        continue;
                    }
                } else {
                    // The previous memtable is still being flushed, or level 0
                    // is too deep to add to: wait for the background thread.
                    stalled_since.get_or_insert_with(Instant::now);
                    st = self.wait(st);
                    continue;
                }
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
    fn wait_for_background<'a>(&'a self, mut st: MutexGuard<'a, State>) -> Result<()> {
        loop {
            if st.opts.inline_background {
                st = self.run_inline(st);
            }
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

    /// `inline_background` mode: runs flushes and compactions on this thread
    /// until none is left, or one fails (which poisons, as on the thread).
    fn run_inline<'a>(&'a self, mut st: MutexGuard<'a, State>) -> MutexGuard<'a, State> {
        while st.poisoned.is_none() {
            let (guard, did_work) = background::run_one(&self.shared, st);
            st = guard;
            match did_work {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => {
                    st.poison(e);
                }
            }
        }
        st
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
        }
        let st = self.lock();
        if matches!(st.opts.sync_mode, SyncMode::Periodic(_)) && st.poisoned.is_none() {
            let _ = lock(&st.wal).sync();
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
            Ok(file.sync()?)
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

impl Shared {
    /// What a read needs, copied out under the read-view lock alone: the
    /// current SuperVersion and the last write it may see.
    fn read_view(&self) -> (Arc<SuperVersion>, SeqNo) {
        let view = lock(&self.view);
        (Arc::clone(&view.current), view.last_seq)
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
    fn take_group(&mut self) -> (Vec<(u64, Pending)>, Vec<Arc<Condvar>>) {
        let mut group = Vec::new();
        let mut followers = Vec::new();
        let mut bytes = 0;
        while let Some((_, pending, _)) = self.queue.front() {
            let size = pending.bytes();
            if !group.is_empty() && bytes + size > MAX_GROUP_BYTES {
                break;
            }
            bytes += size;
            let (ticket, pending, wake) = self.queue.pop_front().expect("front exists");
            if !group.is_empty() {
                followers.push(wake);
            }
            group.push((ticket, pending));
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
        group: Vec<(u64, Pending)>,
        verdicts: Vec<Result<()>>,
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
        let mut seq = first_seq;
        let mut leader_result = Ok(());
        for ((ticket, pending), verdict) in group.into_iter().zip(verdicts) {
            // Refused entries (conflicts) were never logged and take no numbers.
            if verdict.is_ok() {
                for op in pending.ops {
                    self.writes += 1;
                    self.user_bytes += op.size() as u64;
                    apply(&self.current.mem, seq, op);
                    self.last_seq = seq;
                    seq += 1;
                }
            }
            if ticket == leader {
                leader_result = verdict;
            } else {
                self.finished.insert(ticket, verdict);
            }
        }
        // Now the whole group is visible, at once: every batch in it whole.
        lock(&self.view).last_seq = self.last_seq;
        leader_result
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
        let log_id = self.new_file_number();
        let switched = self.failpoint("switch:new_log").and_then(|()| {
            let new_wal = Wal::open_in(&*self.opts.fs, &log_path(&self.dir, log_id))?;
            self.opts.fs.sync_dir(&self.dir)?;
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
            level_bytes: self.current.levels.iter().map(|l| bytes(l)).collect(),
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

    /// A file number never used before, for a log or a table.
    fn new_file_number(&mut self) -> u64 {
        self.next_file += 1;
        self.next_file - 1
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

/// Total file size of `tables`.
fn bytes(tables: &[Arc<Table>]) -> u64 {
    tables.iter().map(|t| t.reader.file_size()).sum()
}

/// In a level >= 1 (sorted, non-overlapping), the only table that can hold `key`.
fn table_for_key<'a>(level: &'a [Arc<Table>], key: &[u8]) -> Option<&'a Arc<Table>> {
    let i = level.partition_point(|t| t.largest() < key);
    level.get(i).filter(|t| t.smallest() <= key)
}

/// Opens every live table and arranges them by level, checking that levels
/// 1+ are non-overlapping (the invariant `table_for_key` relies on).
fn open_levels(
    fs: &dyn Fs,
    dir: &Path,
    version: &Version,
    ctx: &Arc<ReadContext>,
) -> Result<Levels> {
    let mut levels: Levels = (0..MAX_LEVELS).map(|_| Vec::new()).collect();
    for (&id, &level) in &version.tables {
        levels[level as usize].push(Table::open(fs, dir, id, ctx)?);
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
fn list_files(fs: &dyn Fs, dir: &Path) -> Result<Vec<(DbFile, PathBuf)>> {
    let mut out = Vec::new();
    for path in fs.list(dir)? {
        let name = path.file_name().and_then(|n| n.to_str());
        if let Some(kind) = name.and_then(classify) {
            out.push((kind, path));
        }
    }
    out.sort();
    Ok(out)
}

/// Deletes WALs below the log number, tables not in the version, and temp files.
fn remove_obsolete_files(fs: &dyn Fs, dir: &Path, version: &Version) -> Result<()> {
    let mut removed = false;
    for (kind, path) in list_files(fs, dir)? {
        let obsolete = match kind {
            DbFile::Log(n) => n < version.log_number,
            DbFile::Table(id) => !version.tables.contains_key(&id),
            DbFile::Temp => true,
        };
        if obsolete {
            fs.remove(&path)?;
            removed = true;
        }
    }
    if removed {
        fs.sync_dir(dir)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
