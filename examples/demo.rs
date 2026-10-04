//! A scripted tour of lsmkv, a few seconds long:
//!
//! 1. Write a batch big enough to build several levels of tables.
//! 2. Crash: a child process writes and is killed with SIGKILL mid-stream;
//!    reopen and check that every acknowledged write is there.
//! 3. A snapshot keeps its view through overwrites, a delete and a full compaction.
//! 4. A range scan.
//! 5. An atomic batch, and a transaction that loses to a concurrent write.
//! 6. Engine stats: levels, write amplification, bloom filter and cache hits.
//! 7. A short benchmark.
//!
//! Usage: `cargo run --release --example demo -- [dir]` (default `target/demo`).

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use lsmkv::{Db, Error, Options, SyncMode, WriteBatch};

fn options() -> Options {
    Options {
        // Smaller than the default 4 MiB, so the demo builds several levels quickly.
        memtable_size: 256 << 10,
        target_file_size: 256 << 10,
        level1_max_bytes: 1 << 20,
        sync_mode: SyncMode::Always,
        ..Options::default()
    }
}

fn step(n: u32, title: &str) {
    println!("\n== {n}. {title} ==");
}

fn user(i: u64) -> String {
    format!("user:{i:06}")
}

fn main() -> lsmkv::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [flag, dir] = args.as_slice() {
        if flag == "--child" {
            child(Path::new(dir));
        }
    }
    let dir = PathBuf::from(
        args.first()
            .cloned()
            .unwrap_or_else(|| "target/demo".into()),
    );
    let _ = std::fs::remove_dir_all(&dir);
    println!("lsmkv demo, database in {}", dir.display());

    step(1, "write a batch");
    let n = 200_000;
    {
        let db = Db::open_with(
            &dir,
            Options {
                sync_mode: SyncMode::Periodic(Duration::from_millis(100)),
                ..options()
            },
        )?;
        let t = Instant::now();
        for i in 0..n {
            db.put(user(i).as_bytes(), format!("profile-{i}").as_bytes())?;
        }
        db.flush()?;
        let s = db.stats();
        println!(
            "{n} puts in {:.2?}; tables per level: {}",
            t.elapsed(),
            levels(&s)
        );
        println!(
            "write amplification so far: {:.2} ({} MiB written by callers -> {} MiB flushed + {} MiB compacted)",
            s.write_amplification(),
            s.user_bytes >> 20,
            s.flush_bytes >> 20,
            s.compaction_bytes >> 20
        );
    }

    step(2, "kill -9 a writer mid-stream, then recover");
    let (acked, last) = crash_a_writer(&dir);
    let t = Instant::now();
    let db = Db::open_with(&dir, options())?;
    let reopen = t.elapsed();
    let missing = (0..acked)
        .filter(|i| {
            db.get(format!("crash:{i:06}").as_bytes())
                .unwrap()
                .is_none()
        })
        .count();
    let extra = (acked..acked + 100)
        .take_while(|i| {
            db.get(format!("crash:{i:06}").as_bytes())
                .unwrap()
                .is_some()
        })
        .count();
    println!(
        "the child acknowledged {acked} writes (last: {last}) before SIGKILL; reopened in {reopen:.2?}"
    );
    println!(
        "acknowledged writes missing after recovery: {missing}{}",
        if extra > 0 {
            format!(" (plus {extra} in flight that made it to the log too)")
        } else {
            String::new()
        }
    );
    assert_eq!(missing, 0, "lost acknowledged writes");

    step(
        3,
        "a snapshot survives overwrites, a delete and a full compaction",
    );
    let snap = db.snapshot();
    println!("snapshot taken at sequence {}", snap.sequence());
    db.put(b"user:000042", b"OVERWRITTEN")?;
    db.delete(b"user:000043")?;
    db.compact_all()?;
    for k in ["user:000042", "user:000043"] {
        println!(
            "  {k}: now {:<16} snapshot {}",
            show(db.get(k.as_bytes())?),
            show(snap.get(k.as_bytes())?)
        );
    }
    drop(snap);

    step(4, "range scan user:000040 .. user:000046");
    for item in db.scan("user:000040".."user:000046")? {
        let (k, v) = item?;
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }
    let t = Instant::now();
    let count = db.iter()?.count();
    println!("full scan: {count} keys in {:.2?}", t.elapsed());

    step(5, "an atomic batch, then a transaction that conflicts");
    let mut batch = WriteBatch::new();
    batch.put(b"acct:alice", b"100").put(b"acct:bob", b"50");
    db.write(batch)?;
    println!("batch: alice=100, bob=50 written together (one WAL record)");
    let transfer = |db: &Db, amount: u64, meddle: bool| -> lsmkv::Result<()> {
        let mut tx = db.transaction();
        let read =
            |v: Option<Vec<u8>>| -> u64 { String::from_utf8(v.unwrap()).unwrap().parse().unwrap() };
        let alice = read(tx.get_for_update(b"acct:alice")?);
        let bob = read(tx.get_for_update(b"acct:bob")?);
        tx.put(b"acct:alice", (alice - amount).to_string().as_bytes());
        tx.put(b"acct:bob", (bob + amount).to_string().as_bytes());
        if meddle {
            // Another writer changes alice's balance after the snapshot.
            db.put(b"acct:alice", b"90")?;
            println!("  (meanwhile, another writer sets alice=90)");
        }
        tx.commit()
    };
    match transfer(&db, 30, true) {
        Err(Error::Conflict(why)) => println!("transfer of 30: conflict, nothing applied ({why})"),
        other => println!("transfer of 30: {other:?}"),
    }
    transfer(&db, 30, false)?;
    let show = |k: &[u8]| String::from_utf8(db.get(k).unwrap().unwrap()).unwrap();
    println!(
        "retried: alice={}, bob={} (total still 140)",
        show(b"acct:alice"),
        show(b"acct:bob")
    );

    step(6, "stats (counters since the reopen in step 2)");
    for i in (0..n).step_by(97) {
        db.get(user(i).as_bytes())?;
        db.get(format!("nobody:{i}").as_bytes())?;
    }
    let s = db.stats();
    println!("tables per level: {}", levels(&s));
    println!(
        "{} MiB of tables after compact_all; reads: {} blocks from disk, {} from the block cache; bloom filters skipped {} table lookups ({} false positives)",
        s.level_bytes.iter().sum::<u64>() >> 20,
        s.block_reads,
        s.cache_hits,
        s.filter_negatives,
        s.filter_false_positives
    );
    drop(db);

    step(7, "short benchmark (Periodic sync, one thread)");
    let db = Db::open_with(
        &dir,
        Options {
            sync_mode: SyncMode::Periodic(Duration::from_millis(100)),
            ..options()
        },
    )?;
    let ops = 100_000u64;
    let t = Instant::now();
    for i in 0..ops {
        db.put(user(i * 7 % n).as_bytes(), b"updated")?;
    }
    let puts = t.elapsed();
    let t = Instant::now();
    for i in 0..ops {
        db.get(user(i * 13 % n).as_bytes())?;
    }
    let gets = t.elapsed();
    println!(
        "{ops} random overwrites: {:.0}k/s; {ops} random gets: {:.0}k/s",
        ops as f64 / puts.as_secs_f64() / 1000.0,
        ops as f64 / gets.as_secs_f64() / 1000.0
    );
    println!("\ndone");
    Ok(())
}

/// Runs this program as a child writing `crash:000000`, `crash:000001`, ...
/// with fsync per write, kills it with SIGKILL after 300 ms of acknowledged
/// writes, and returns how many it acknowledged plus the last key.
fn crash_a_writer(dir: &Path) -> (u64, String) {
    let mut proc = Command::new(std::env::current_exe().unwrap())
        .args(["--child", dir.to_str().unwrap()])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(proc.stdout.take().unwrap()).lines();
    let mut acked = 0;
    let mut last = String::new();
    let mut started: Option<Instant> = None;
    for line in lines.by_ref() {
        let Ok(line) = line else { break };
        if let Some(key) = line.strip_prefix("ack ") {
            started.get_or_insert_with(Instant::now);
            acked += 1;
            last = key.to_string();
        }
        if started.is_some_and(|t| t.elapsed() > Duration::from_millis(300)) {
            proc.kill().unwrap(); // SIGKILL: no destructors, no final flush
            break;
        }
    }
    proc.wait().unwrap();
    (acked, last)
}

/// The child: write forever, printing each key once its write is acknowledged.
fn child(dir: &Path) -> ! {
    let db = Db::open_with(dir, options()).unwrap();
    let mut out = std::io::stdout().lock();
    let mut i = 0u64;
    loop {
        let key = format!("crash:{i:06}");
        db.put(key.as_bytes(), b"written before the crash").unwrap();
        writeln!(out, "ack {key}").unwrap();
        out.flush().unwrap();
        i += 1;
    }
}

fn levels(s: &lsmkv::Stats) -> String {
    s.level_files
        .iter()
        .enumerate()
        .filter(|(_, &n)| n > 0)
        .map(|(l, n)| format!("L{l}={n}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn show(v: Option<Vec<u8>>) -> String {
    match v {
        Some(v) => format!("{:?}", String::from_utf8_lossy(&v)),
        None => "(deleted)".into(),
    }
}
