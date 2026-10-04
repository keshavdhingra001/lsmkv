//! Reads and writes at the same time: what M8 (DESIGN.md D12–D17) is for.
//!
//! Preloads `KEYS` keys, then runs reader threads doing random gets while one
//! writer writes new keys as fast as it can (`Periodic` sync, so the engine,
//! not the disk, is the limit). The writer fills a memtable every ~0.1 s, so
//! flushes and compactions run throughout. Each row reports read throughput
//! and latency, and the writer's throughput and latency (max included: the
//! number inline flushes used to blow up).
//!
//! Usage: cargo run --release --example concurrency -- [dir] [seconds]
//!
//! Uses only API that M7 already had, so the same file runs against older
//! commits for a before/after comparison. `dir` must be on a real disk
//! (default `target/bench-concurrency`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use lsmkv::{Db, Options, SyncMode};

const KEYS: u64 = 200_000;
const VALUE: [u8; 100] = [b'v'; 100];

fn main() -> lsmkv::Result<()> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "target/bench-concurrency".into()),
    );
    let secs: u64 = args.next().map_or(3, |s| s.parse().expect("seconds"));
    let opts = Options {
        sync_mode: SyncMode::Periodic(Duration::from_millis(100)),
        ..Options::default()
    };

    let dir = root.join("db");
    let _ = std::fs::remove_dir_all(&dir);
    let db = Db::open_with(&dir, opts)?;
    for i in 0..KEYS {
        db.put(&key(i), &VALUE)?;
    }
    db.flush()?;

    println!(
        "dir: {}, {secs} s per run, {KEYS} preloaded keys, 100-byte values, Periodic(100ms)\n",
        root.display()
    );
    println!(
        "| readers | writer | reads/sec | read p50 | read p99 | read max \
         | writes/sec | write p99 | write p99.9 | write max |"
    );
    println!("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    let mut next_write = 0;
    for (readers, writer) in [(4, false), (1, true), (4, true), (8, true)] {
        let run = run(&db, readers, writer, secs, &mut next_write);
        let w = |r: &Option<Vec<Duration>>, p: f64| r.as_ref().map_or("-".into(), |l| pct(l, p));
        println!(
            "| {readers} | {} | {:.0} | {} | {} | {} | {} | {} | {} | {} |",
            if writer { "yes" } else { "no" },
            run.reads.len() as f64 / run.elapsed.as_secs_f64(),
            pct(&run.reads, 0.5),
            pct(&run.reads, 0.99),
            pct(&run.reads, 1.0),
            run.writes.as_ref().map_or("-".into(), |l| format!(
                "{:.0}",
                l.len() as f64 / run.elapsed.as_secs_f64()
            )),
            w(&run.writes, 0.99),
            w(&run.writes, 0.999),
            w(&run.writes, 1.0),
        );
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

struct Run {
    elapsed: Duration,
    /// Sorted latencies.
    reads: Vec<Duration>,
    writes: Option<Vec<Duration>>,
}

fn run(db: &Db, readers: usize, writer: bool, secs: u64, next_write: &mut u64) -> Run {
    let stop = AtomicBool::new(false);
    let start = Instant::now();
    let (mut reads, writes) = std::thread::scope(|s| {
        let stop = &stop;
        let handles: Vec<_> = (0..readers)
            .map(|t| {
                s.spawn(move || {
                    let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64 + 1);
                    let mut lat = Vec::new();
                    while !stop.load(Ordering::Relaxed) {
                        // xorshift64: cheap, and the same sequence every run.
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let k = key(rng % KEYS);
                        let begin = Instant::now();
                        let got = db.get(&k).unwrap();
                        lat.push(begin.elapsed());
                        assert!(got.is_some(), "preloaded key missing");
                    }
                    lat
                })
            })
            .collect();
        let first = *next_write;
        let writer = writer.then(|| {
            s.spawn(move || {
                let mut lat = Vec::new();
                let mut i = first;
                while !stop.load(Ordering::Relaxed) {
                    let k = format!("w{i:012}");
                    let begin = Instant::now();
                    db.put(k.as_bytes(), &VALUE).unwrap();
                    lat.push(begin.elapsed());
                    i += 1;
                }
                (lat, i)
            })
        });
        std::thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
        let reads: Vec<Duration> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let writes = writer.map(|h| {
            let (lat, next) = h.join().unwrap();
            *next_write = next;
            lat
        });
        (reads, writes)
    });
    let elapsed = start.elapsed();
    reads.sort();
    let writes = writes.map(|mut w| {
        w.sort();
        w
    });
    Run {
        elapsed,
        reads,
        writes,
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

/// The `p` quantile of sorted latencies, in µs (or ms when large).
fn pct(sorted: &[Duration], p: f64) -> String {
    let d = sorted[((sorted.len() - 1) as f64 * p) as usize];
    let us = d.as_secs_f64() * 1e6;
    if us >= 1000.0 {
        format!("{:.1} ms", us / 1000.0)
    } else {
        format!("{us:.1} µs")
    }
}
