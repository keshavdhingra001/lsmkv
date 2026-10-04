//! A real process crash: a child process writes from 4 threads and prints each
//! key once its write is acknowledged; the parent `kill -9`s it mid-stream,
//! reopens the database, and checks that every acknowledged key is there.
//!
//! The child is this same test binary, re-run with an environment variable
//! set, so the test needs no separate helper program.
//!
//! This covers a crash of the *process*: the kernel keeps whatever the process
//! had already written to it. A power cut (losing the kernel's page cache too)
//! needs a VM or a fault-injecting filesystem, and is left for M10.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use lsmkv::{Db, Options, SyncMode};

const CHILD_DIR: &str = "LSMKV_KILL9_DIR";
const CHILD_MODE: &str = "LSMKV_KILL9_MODE";
const TEST_NAME: &str = "kill_9_loses_no_acknowledged_write";

fn options(mode: &str) -> Options {
    Options {
        // Small, so flushes and compactions are running when the kill lands.
        memtable_size: 16 << 10,
        sync_mode: match mode {
            "always" => SyncMode::Always,
            // Never fsyncs within the test: only the OS holds the data.
            _ => SyncMode::Periodic(Duration::from_secs(3600)),
        },
        ..Options::default()
    }
}

/// The child: write forever, printing `ack <key>` after each acknowledged write.
fn child(dir: &Path, mode: &str) -> ! {
    let db = Db::open_with(dir, options(mode)).unwrap();
    std::thread::scope(|s| {
        for t in 0..4 {
            let db = &db;
            s.spawn(move || {
                let mut i = 0u64;
                loop {
                    let key = format!("t{t}-{i:08}");
                    db.put(key.as_bytes(), &[b'v'; 64]).unwrap();
                    let mut out = std::io::stdout().lock();
                    writeln!(out, "ack {key}").unwrap();
                    out.flush().unwrap();
                    i += 1;
                }
            });
        }
    });
    unreachable!("writers never stop; the parent kills this process")
}

#[test]
fn kill_9_loses_no_acknowledged_write() {
    if let (Ok(dir), Ok(mode)) = (std::env::var(CHILD_DIR), std::env::var(CHILD_MODE)) {
        child(Path::new(&dir), &mode);
    }

    for mode in ["always", "periodic"] {
        for round in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let mut proc = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST_NAME, "--nocapture"])
                .env(CHILD_DIR, dir.path())
                .env(CHILD_MODE, mode)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();

            // Read acknowledgements, then kill at a point that varies by round.
            let target = 2000 + round * 1500;
            let mut acked = Vec::new();
            for line in BufReader::new(proc.stdout.take().unwrap()).lines() {
                if let Some(key) = line.unwrap().strip_prefix("ack ") {
                    acked.push(key.to_string());
                    if acked.len() == target {
                        proc.kill().unwrap(); // SIGKILL: no destructors, no final sync
                        break;
                    }
                }
            }
            proc.wait().unwrap();
            assert_eq!(acked.len(), target, "{mode}: child exited early");

            let db = Db::open_with(dir.path(), options(mode)).unwrap();
            let lost: Vec<&String> = acked
                .iter()
                .filter(|k| db.get(k.as_bytes()).unwrap().is_none())
                .collect();
            assert!(
                lost.is_empty(),
                "{mode} round {round}: {} of {} acknowledged writes lost, e.g. {:?}",
                lost.len(),
                acked.len(),
                &lost[..lost.len().min(5)]
            );
            println!(
                "{mode} round {round}: killed after {} acks, all present ({} tables)",
                acked.len(),
                db.stats().tables
            );
        }
    }
}
