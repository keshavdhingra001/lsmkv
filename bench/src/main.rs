//! lsmkv vs RocksDB, on db_bench's workloads with matched settings
//! (DESIGN.md D24).
//!
//! Usage: `cargo run --release -- [keys] [dir]` from `bench/`
//! (default 1,000,000 keys, data under `../target/bench-rocks`).
//!
//! Both engines get the same shape: 16-byte keys, 100-byte values, a 4 MiB
//! memtable with one immutable memtable behind it, 10-bit bloom filters, an
//! 8 MiB block cache, 4 KiB blocks, no compression, LevelDB's level-0
//! triggers (4 / 8 / 12) and level sizes (10 MiB, x10), 2 MiB tables, one
//! background thread, static level sizes, and a WAL that's written but not fsynced per write
//! (lsmkv `Periodic(1s)`, RocksDB `sync = false`). A short `sync` run
//! compares fsync-per-write too.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const VALUE_LEN: usize = 100;

fn key(i: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k.copy_from_slice(format!("{i:016}").as_bytes());
    k
}

/// A bijection on 0..n, so a "random" pass visits every key exactly once.
fn shuffled(i: u64, n: u64) -> u64 {
    let bits = 64 - (n - 1).leading_zeros();
    let mask = (1u64 << bits) - 1;
    // Start inside 0..n: walking from outside it might never come back in.
    let mut x = i % n;
    loop {
        x = (x
            .wrapping_mul(0x9E37_79B9_7F4A_7C15 | 1)
            .wrapping_add(0x1234_5678))
            & mask;
        if x < n {
            return x;
        }
    }
}

/// What the benchmark needs from an engine.
trait Engine: Send + Sync + Sized {
    const NAME: &'static str;
    fn open(dir: &Path, sync: bool) -> Self;
    fn put(&self, key: &[u8], value: &[u8]);
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    /// Seeks to `start` and reads up to `n` pairs; returns how many it read.
    fn scan(&self, start: &[u8], n: usize) -> usize;
    /// Flushes the memtable and waits for compactions to settle.
    fn settle(&self);
    /// SSTable bytes written by flushes and compactions since open.
    fn table_bytes_written(&self) -> u64;
}

struct Lsmkv(lsmkv::Db);

impl Engine for Lsmkv {
    const NAME: &'static str = "lsmkv";

    fn open(dir: &Path, sync: bool) -> Self {
        let opts = lsmkv::Options {
            memtable_size: 4 << 20,
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            level1_max_bytes: 10 << 20,
            level_size_multiplier: 10,
            target_file_size: 2 << 20,
            bloom_bits_per_key: 10,
            block_cache_bytes: 8 << 20,
            sync_mode: if sync {
                lsmkv::SyncMode::Always
            } else {
                lsmkv::SyncMode::Periodic(Duration::from_secs(1))
            },
            ..lsmkv::Options::default()
        };
        Self(lsmkv::Db::open_with(dir, opts).unwrap())
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        self.0.put(key, value).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).unwrap()
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mut got = 0;
        for item in self.0.scan(start..).unwrap().take(n) {
            std::hint::black_box(item.unwrap());
            got += 1;
        }
        got
    }

    fn settle(&self) {
        self.0.flush().unwrap();
    }

    fn table_bytes_written(&self) -> u64 {
        let s = self.0.stats();
        s.flush_bytes + s.compaction_bytes
    }
}

struct Rocks {
    db: rocksdb::DB,
    opts: rocksdb::Options,
    write: rocksdb::WriteOptions,
}

impl Engine for Rocks {
    const NAME: &'static str = "RocksDB";

    fn open(dir: &Path, sync: bool) -> Self {
        let mut table = rocksdb::BlockBasedOptions::default();
        table.set_block_size(4096);
        table.set_bloom_filter(10.0, false);
        table.set_block_cache(&rocksdb::Cache::new_lru_cache(8 << 20));

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&table);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        opts.set_write_buffer_size(4 << 20);
        opts.set_max_write_buffer_number(2);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(8);
        opts.set_level_zero_stop_writes_trigger(12);
        opts.set_max_bytes_for_level_base(10 << 20);
        opts.set_max_bytes_for_level_multiplier(10.0);
        opts.set_target_file_size_base(2 << 20);
        opts.set_max_background_jobs(1);
        // Static level sizes like lsmkv's (LevelDB's). RocksDB's default
        // since 8.x sizes levels from the bottom up instead.
        opts.set_level_compaction_dynamic_level_bytes(false);
        // Counters only (for write amplification): the default level also
        // times every operation, which would slow RocksDB down.
        opts.enable_statistics();
        opts.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptHistogramOrTimers);

        let mut write = rocksdb::WriteOptions::default();
        write.set_sync(sync);
        let db = rocksdb::DB::open(&opts, dir).unwrap();
        Self { db, opts, write }
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        self.db.put_opt(key, value, &self.write).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).unwrap()
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mode = rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward);
        let mut got = 0;
        for item in self.db.iterator(mode).take(n) {
            std::hint::black_box(item.unwrap());
            got += 1;
        }
        got
    }

    fn settle(&self) {
        self.db.flush().unwrap();
        // Wait until no compaction is pending or running.
        loop {
            let pending = self
                .db
                .property_int_value("rocksdb.compaction-pending")
                .unwrap()
                .unwrap_or(0);
            let running = self
                .db
                .property_int_value("rocksdb.num-running-compactions")
                .unwrap()
                .unwrap_or(0);
            if pending == 0 && running == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn table_bytes_written(&self) -> u64 {
        self.opts
            .get_ticker_count(rocksdb::statistics::Ticker::FlushWriteBytes)
            + self
                .opts
                .get_ticker_count(rocksdb::statistics::Ticker::CompactWriteBytes)
    }
}

/// Per-op latencies, in nanoseconds.
struct Latencies(Vec<u32>);

impl Latencies {
    fn with_capacity(n: usize) -> Self {
        Self(Vec::with_capacity(n))
    }

    fn time<T>(&mut self, f: impl FnOnce() -> T) -> T {
        let t = Instant::now();
        let out = f();
        self.0
            .push(t.elapsed().as_nanos().min(u32::MAX as u128) as u32);
        out
    }

    fn pct(&mut self, p: f64) -> f64 {
        self.0.sort_unstable();
        let i = ((self.0.len() as f64 * p) as usize).min(self.0.len() - 1);
        self.0[i] as f64 / 1000.0
    }
}

struct Row {
    workload: &'static str,
    engine: &'static str,
    ops_per_sec: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    note: String,
}

fn row(
    workload: &'static str,
    engine: &'static str,
    ops: u64,
    d: Duration,
    lat: &mut Latencies,
    note: String,
) -> Row {
    Row {
        workload,
        engine,
        ops_per_sec: ops as f64 / d.as_secs_f64(),
        p50_us: lat.pct(0.50),
        p99_us: lat.pct(0.99),
        p999_us: lat.pct(0.999),
        note,
    }
}

fn fresh(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    let _ = std::fs::remove_dir_all(&path);
    path
}

/// Writes `n` keys in the given order; returns the row (write amplification
/// measured after the engine settles).
fn fill<E: Engine>(db: &E, workload: &'static str, n: u64, order: impl Fn(u64) -> u64) -> Row {
    let value = [b'v'; VALUE_LEN];
    let mut lat = Latencies::with_capacity(n as usize);
    let t = Instant::now();
    for i in 0..n {
        lat.time(|| db.put(&key(order(i)), &value));
    }
    let d = t.elapsed();
    db.settle();
    let user = n * (16 + VALUE_LEN as u64);
    let wa = db.table_bytes_written() as f64 / user as f64;
    row(
        workload,
        E::NAME,
        n,
        d,
        &mut lat,
        format!("write amp {wa:.2}"),
    )
}

fn run<E: Engine>(base: &Path, n: u64) -> Vec<Row> {
    let mut rows = Vec::new();
    eprintln!("[{}] fillseq", E::NAME);
    {
        let db = E::open(&fresh(base, &format!("{}-seq", E::NAME)), false);
        rows.push(fill(&db, "fillseq", n, |i| i));
    }

    eprintln!("[{}] fillrandom", E::NAME);
    let path = fresh(base, &format!("{}-random", E::NAME));
    let db = E::open(&path, false);
    rows.push(fill(&db, "fillrandom", n, |i| shuffled(i, n)));

    eprintln!("[{}] overwrite", E::NAME);
    let before = db.table_bytes_written();
    {
        let value = [b'o'; VALUE_LEN];
        let mut lat = Latencies::with_capacity(n as usize);
        let t = Instant::now();
        for i in 0..n {
            lat.time(|| db.put(&key(shuffled(i * 7 + 3, n)), &value));
        }
        let d = t.elapsed();
        db.settle();
        let wa = (db.table_bytes_written() - before) as f64 / (n * (16 + VALUE_LEN as u64)) as f64;
        rows.push(row(
            "overwrite",
            E::NAME,
            n,
            d,
            &mut lat,
            format!("write amp {wa:.2}"),
        ));
    }

    let reads = n.min(500_000);
    eprintln!("[{}] readrandom", E::NAME);
    {
        let mut lat = Latencies::with_capacity(reads as usize);
        let t = Instant::now();
        for i in 0..reads {
            let found = lat.time(|| db.get(&key(shuffled(i * 13 + 1, n))));
            assert!(found.is_some());
        }
        rows.push(row(
            "readrandom",
            E::NAME,
            reads,
            t.elapsed(),
            &mut lat,
            String::new(),
        ));
    }

    eprintln!("[{}] readmissing", E::NAME);
    {
        let mut lat = Latencies::with_capacity(reads as usize);
        let t = Instant::now();
        for i in 0..reads {
            // Same length as real keys, sorting among them, never written.
            let mut k = key(shuffled(i * 13 + 1, n));
            k[15] = b'x';
            let found = lat.time(|| db.get(&k));
            assert!(found.is_none());
        }
        rows.push(row(
            "readmissing",
            E::NAME,
            reads,
            t.elapsed(),
            &mut lat,
            String::new(),
        ));
    }

    let seeks = n.min(100_000) / 2;
    eprintln!("[{}] seekrandom", E::NAME);
    {
        let mut lat = Latencies::with_capacity(seeks as usize);
        let t = Instant::now();
        for i in 0..seeks {
            let got = lat.time(|| db.scan(&key(shuffled(i * 17 + 5, n)), 100));
            assert!(got > 0);
        }
        rows.push(row(
            "seekrandom (+100 next)",
            E::NAME,
            seeks,
            t.elapsed(),
            &mut lat,
            String::new(),
        ));
    }

    eprintln!("[{}] readwhilewriting", E::NAME);
    rows.extend(read_while_writing(&db, n));
    drop(db);

    eprintln!("[{}] fillrandom, sync", E::NAME);
    {
        let db = E::open(&fresh(base, &format!("{}-sync", E::NAME)), true);
        let m = n.min(20_000);
        let mut r = fill(&db, "fillrandom (fsync each)", m, |i| shuffled(i, m));
        r.note.clear();
        rows.push(r);
    }
    let _ = std::fs::remove_dir_all(&path);
    rows
}

/// 4 reader threads doing random gets while 1 writer overwrites random keys,
/// for a fixed time.
fn read_while_writing<E: Engine>(db: &E, n: u64) -> Vec<Row> {
    const READERS: usize = 4;
    let secs = Duration::from_secs(10);
    let stop = AtomicBool::new(false);
    let reads = AtomicU64::new(0);
    let (mut rlat, mut wlat, writes) = std::thread::scope(|s| {
        let readers: Vec<_> = (0..READERS)
            .map(|r| {
                let (stop, reads) = (&stop, &reads);
                s.spawn(move || {
                    let mut lat = Latencies::with_capacity(1 << 20);
                    let mut i = r as u64;
                    while !stop.load(Ordering::Relaxed) {
                        let k = key(shuffled(i % n, n));
                        std::hint::black_box(lat.time(|| db.get(&k)));
                        i += READERS as u64 * 7919;
                    }
                    reads.fetch_add(lat.0.len() as u64, Ordering::Relaxed);
                    lat
                })
            })
            .collect();
        let writer = s.spawn(|| {
            let value = [b'w'; VALUE_LEN];
            let mut lat = Latencies::with_capacity(1 << 20);
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                lat.time(|| db.put(&key(shuffled(i % n, n)), &value));
                i += 31;
            }
            lat
        });
        std::thread::sleep(secs);
        stop.store(true, Ordering::Relaxed);
        let mut all = Latencies(Vec::new());
        for r in readers {
            all.0.extend(r.join().unwrap().0);
        }
        let w = writer.join().unwrap();
        let writes = w.0.len() as u64;
        (all, w, writes)
    });
    let reads = reads.load(Ordering::Relaxed);
    vec![
        row(
            "readwhilewriting: reads (4 threads)",
            E::NAME,
            reads,
            secs,
            &mut rlat,
            String::new(),
        ),
        row(
            "readwhilewriting: writes (1 thread)",
            E::NAME,
            writes,
            secs,
            &mut wlat,
            String::new(),
        ),
    ]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let n: u64 = args
        .next()
        .map_or(1_000_000, |s| s.parse().expect("key count"));
    let base = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "../target/bench-rocks".into()),
    );
    std::fs::create_dir_all(&base).unwrap();

    let ours = run::<Lsmkv>(&base, n);
    let theirs = run::<Rocks>(&base, n);

    println!("{n} keys, 16 B keys, 100 B values\n");
    println!("| workload | lsmkv ops/s | RocksDB ops/s | lsmkv p50 / p99 / p99.9 (µs) | RocksDB p50 / p99 / p99.9 (µs) | notes |");
    println!("|---|---:|---:|---:|---:|---|");
    for (a, b) in ours.iter().zip(&theirs) {
        assert_eq!(a.workload, b.workload);
        assert_eq!((a.engine, b.engine), ("lsmkv", "RocksDB"));
        let notes = if a.note.is_empty() {
            String::new()
        } else {
            format!("lsmkv {}; RocksDB {}", a.note, b.note)
        };
        println!(
            "| {} | {} | {} | {:.1} / {:.1} / {:.1} | {:.1} / {:.1} / {:.1} | {} |",
            a.workload,
            human(a.ops_per_sec),
            human(b.ops_per_sec),
            a.p50_us,
            a.p99_us,
            a.p999_us,
            b.p50_us,
            b.p99_us,
            b.p999_us,
            notes
        );
    }
}

fn human(ops: f64) -> String {
    if ops >= 1e6 {
        format!("{:.2}M", ops / 1e6)
    } else if ops >= 1e4 {
        format!("{:.0}k", ops / 1e3)
    } else {
        format!("{ops:.0}")
    }
}
