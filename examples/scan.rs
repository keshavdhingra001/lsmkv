//! Scan throughput, and the memory a full compaction needs.
//!
//! Usage: `cargo run --release --example scan -- [dir] [keys]`
//! (default `target/bench-scan`, 1,000,000 keys of 16 bytes with 100-byte values).
//!
//! 1. Fill: random-order puts (`Periodic` sync, as in db_bench's default).
//! 2. `compact_all`: rewrites everything into the bottom level. Peak memory
//!    is measured over just this step: `/proc/self/clear_refs` resets the
//!    process's peak RSS, and `VmHWM` reads it back (Linux only).
//! 3. A full scan, short scans (seek + 100 keys), and point reads to compare.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use lsmkv::{Db, Options, SyncMode};

fn key(i: u64) -> Vec<u8> {
    format!("{i:016}").into_bytes()
}

/// A permutation of 0..n (multiplying by an odd constant mod a power of
/// two is a bijection), so the fill visits every key once in random order.
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

fn peak_rss_mib() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024.0)
}

fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

fn rate(n: u64, d: Duration) -> String {
    format!("{:.0}k/s", n as f64 / d.as_secs_f64() / 1000.0)
}

fn main() -> lsmkv::Result<()> {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(args.next().unwrap_or_else(|| "target/bench-scan".into()));
    let n: u64 = args
        .next()
        .map_or(1_000_000, |s| s.parse().expect("key count"));
    let _ = std::fs::remove_dir_all(&dir);

    let opts = Options {
        sync_mode: SyncMode::Periodic(Duration::from_secs(1)),
        ..Options::default()
    };
    let db = Db::open_with(&dir, opts)?;
    let value = [b'v'; 100];

    let t = Instant::now();
    for i in 0..n {
        db.put(&key(shuffled(i, n)), &value)?;
    }
    db.flush()?;
    println!(
        "fill        {n} keys in {:.2?} ({})",
        t.elapsed(),
        rate(n, t.elapsed())
    );

    reset_peak_rss();
    let before = peak_rss_mib();
    let t = Instant::now();
    db.compact_all()?;
    let peak = peak_rss_mib();
    let s = db.stats();
    let bytes: u64 = s.level_bytes.iter().sum();
    print!(
        "compact_all {:.2?}, {} MiB of tables",
        t.elapsed(),
        bytes >> 20
    );
    match (before, peak) {
        (Some(b), Some(p)) => println!(", peak RSS {p:.0} MiB (was {b:.0} MiB before)"),
        _ => println!(),
    }

    let t = Instant::now();
    let mut count = 0u64;
    for item in db.iter()? {
        let (k, _) = item?;
        assert_eq!(k, key(count), "scan out of order");
        count += 1;
    }
    assert_eq!(count, n);
    println!(
        "full scan   {count} keys in {:.2?} ({})",
        t.elapsed(),
        rate(count, t.elapsed())
    );

    let seeks = 20_000u64;
    let t = Instant::now();
    let mut got = 0u64;
    for i in 0..seeks {
        let start = key(shuffled(i, n));
        for item in db.scan(start.as_slice()..)?.take(100) {
            item?;
            got += 1;
        }
    }
    println!(
        "short scans {seeks} x (seek + 100) in {:.2?} ({} seeks, {} keys)",
        t.elapsed(),
        rate(seeks, t.elapsed()),
        rate(got, t.elapsed())
    );

    let gets = 200_000u64;
    let t = Instant::now();
    for i in 0..gets {
        assert!(db.get(&key(shuffled(i * 7, n)))?.is_some());
    }
    println!(
        "point gets  {gets} in {:.2?} ({})",
        t.elapsed(),
        rate(gets, t.elapsed())
    );
    Ok(())
}
