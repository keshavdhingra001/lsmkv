//! Write throughput and latency for each sync mode and writer-thread count.
//!
//! Usage: cargo run --release --example durability -- [dir] [seconds]
//!
//! `dir` must be on a real disk. On tmpfs (often `/tmp`) fsync costs nothing,
//! and `Always` would look as fast as `Periodic`. The default is
//! `target/bench-durability`, inside the project.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use lsmkv::{Db, Options, SyncMode};

fn main() -> lsmkv::Result<()> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "target/bench-durability".into()),
    );
    let secs: u64 = args.next().map_or(3, |s| s.parse().expect("seconds"));
    let value = [b'v'; 100];

    println!(
        "dir: {}, {secs} s per run, 16-byte keys, 100-byte values\n",
        root.display()
    );
    println!("| mode | threads | ops/sec | p50 | p99 | p99.9 | max | writes per fsync |");
    println!("|---|---:|---:|---:|---:|---:|---:|---:|");

    let modes = [
        ("Always", SyncMode::Always),
        (
            "Periodic(100ms)",
            SyncMode::Periodic(Duration::from_millis(100)),
        ),
    ];
    for (name, mode) in modes {
        for threads in [1, 4, 16] {
            let dir = root.join(format!("{name}-{threads}"));
            let _ = std::fs::remove_dir_all(&dir);
            let opts = Options {
                sync_mode: mode,
                ..Options::default()
            };
            let db = Db::open_with(&dir, opts)?;

            let deadline = Instant::now() + Duration::from_secs(secs);
            let start = Instant::now();
            let mut latencies: Vec<Duration> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..threads)
                    .map(|t| {
                        let db = &db;
                        s.spawn(move || {
                            let mut lat = Vec::new();
                            let mut i = 0u64;
                            while Instant::now() < deadline {
                                let key = format!("t{t:02}-{i:010}");
                                let begin = Instant::now();
                                db.put(key.as_bytes(), &value).unwrap();
                                lat.push(begin.elapsed());
                                i += 1;
                            }
                            lat
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .flat_map(|h| h.join().unwrap())
                    .collect()
            });
            let elapsed = start.elapsed();
            latencies.sort();
            let pct = |p: f64| {
                let d = latencies[((latencies.len() - 1) as f64 * p) as usize];
                format!("{:.1} µs", d.as_secs_f64() * 1e6)
            };
            let st = db.stats();
            let per_sync = if st.wal_syncs == 0 {
                "-".to_string()
            } else {
                format!("{:.1}", st.writes as f64 / st.wal_syncs as f64)
            };
            println!(
                "| {name} | {threads} | {:.0} | {} | {} | {} | {} | {per_sync} |",
                latencies.len() as f64 / elapsed.as_secs_f64(),
                pct(0.50),
                pct(0.99),
                pct(0.999),
                pct(1.0),
            );
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
    Ok(())
}
