//! Crash harness (DESIGN.md D22): real `kill -9`s, round after round, against
//! one database directory.
//!
//! Each round, a child process (this same test binary, re-run with an
//! environment variable set) opens the database and runs 4 writer threads.
//! Each thread owns 2,000 keys and does random puts and deletes on them,
//! printing `S` before an operation and `A` once it's acknowledged. The parent
//! `kill -9`s the child at a random moment, reopens the database itself, and
//! checks every key exactly:
//!
//! - it must hold the result of the last acknowledged operation on it,
//! - or, if the thread's one unacknowledged operation was on this key, the
//!   result of that operation (it may or may not have reached the log).
//!
//! Anything else (a lost acknowledged write, an old value coming back, a key
//! nobody wrote) fails the test. A full scan must agree with the point reads.
//! The next round runs on the recovered directory, so recovery is tested on
//! top of earlier recoveries.
//!
//! The memtable is tiny, so flushes and compactions are always in flight when
//! the kill lands. Rounds alternate between `SyncMode::Always` and a
//! `Periodic` mode that never fsyncs within the test: a process crash keeps
//! whatever the kernel already has, so neither may lose anything.
//!
//! The child also checks itself while it runs: every thread rescans its own
//! keys (sometimes through a snapshot) and compares them with what it wrote.
//!
//! A power cut (losing the kernel's page cache too) is out of scope: it needs
//! a VM or a fault-injecting filesystem (DESIGN.md D22).
//!
//! `cargo test --test kill9` runs 6 rounds; the soak test runs 300
//! (`cargo test --release --test kill9 -- --ignored`, or set
//! `LSMKV_CRASH_ROUNDS`).

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use lsmkv::{Db, Options, SyncMode};

const CHILD_DIR: &str = "LSMKV_KILL9_DIR";
const CHILD_ROUND: &str = "LSMKV_KILL9_ROUND";
/// The child is always started through this test, whichever test runs the rounds.
const CHILD_TEST: &str = "kill_9_loses_no_acknowledged_write";
const THREADS: usize = 4;
const KEYS_PER_THREAD: usize = 2000;

fn options(round: u64) -> Options {
    Options {
        // Tiny, so flushes and compactions are running when the kill lands.
        memtable_size: 16 << 10,
        l0_compaction_trigger: 2,
        l0_slowdown_trigger: 4,
        l0_stop_trigger: 8,
        level1_max_bytes: 64 << 10,
        level_size_multiplier: 4,
        target_file_size: 16 << 10,
        sync_mode: if round.is_multiple_of(2) {
            SyncMode::Always
        } else {
            // Never fsyncs within the test: only the kernel holds the data.
            SyncMode::Periodic(Duration::from_secs(3600))
        },
        ..Options::default()
    }
}

fn key(t: usize, j: usize) -> String {
    format!("t{t}-{j:04}")
}

/// A value that names the operation that wrote it, padded to a varying length.
fn value(round: u64, t: usize, op: u64) -> Vec<u8> {
    let mut v = format!("r{round}.t{t}.o{op}.").into_bytes();
    v.resize(v.len() + (op as usize * 37) % 300, b'x');
    v
}

/// xorshift64, so each round's workload is reproducible from its number.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// A thread's keys and values as one map.
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// The child: write until killed.
fn child(dir: &Path, round: u64) -> ! {
    let db = Db::open_with(dir, options(round)).unwrap();
    // libtest has printed "test <name> ... " with no newline: this line
    // ends it, so the first `S` line arrives whole. It also tells the parent
    // that recovery is done.
    println!("ready");
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = &db;
            s.spawn(move || child_thread(db, round, t));
        }
    });
    unreachable!("writers never stop; the parent kills this process")
}

fn child_thread(db: &Db, round: u64, t: usize) {
    let mut rng = Rng::new(round * 100 + t as u64 + 1);
    let (lo, hi) = (format!("t{t}-"), format!("t{t}."));
    // What this thread's keys hold now: whatever earlier rounds left.
    let mut mine: Model = db
        .scan(lo.as_str()..hi.as_str())
        .unwrap()
        .map(|item| item.unwrap())
        .collect();
    let print = |line: String| {
        let mut out = std::io::stdout().lock();
        writeln!(out, "{line}").unwrap();
        out.flush().unwrap();
    };
    for op in 0u64.. {
        let k = key(t, rng.below(KEYS_PER_THREAD as u64) as usize);
        if rng.below(4) == 0 {
            print(format!("S {t} {op} {k} D"));
            db.delete(k.as_bytes()).unwrap();
            mine.remove(k.as_bytes());
        } else {
            print(format!("S {t} {op} {k} P"));
            let v = value(round, t, op);
            db.put(k.as_bytes(), &v).unwrap();
            mine.insert(k.into_bytes(), v);
        }
        print(format!("A {t} {op}"));

        // Self-check: this thread's keys, scanned now or through a snapshot.
        if op % 50 == 49 {
            let snap = db.snapshot();
            let scan = if rng.below(2) == 0 {
                snap.scan(lo.as_str()..hi.as_str())
            } else {
                db.scan(lo.as_str()..hi.as_str())
            };
            let got: Model = scan.unwrap().map(|item| item.unwrap()).collect();
            if got != mine {
                eprintln!("BUG: thread {t} op {op}: its scan disagrees with its writes");
                std::process::exit(2);
            }
        }
    }
}

/// One operation the child started, as the parent parsed it.
struct Op {
    key: String,
    /// `None` for a delete.
    value: Option<Vec<u8>>,
    acked: bool,
}

/// Runs one round: start the child, kill it after `kill_after`, and return
/// each thread's operations in order.
fn run_child(dir: &Path, round: u64, kill_after: Duration) -> Vec<Vec<Op>> {
    let mut proc = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD_DIR, dir)
        .env(CHILD_ROUND, round.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = proc.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            // The kill can cut the last line short; a short line is never
            // a complete `A`, so dropping it is safe.
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let mut ops: Vec<Vec<Op>> = (0..THREADS).map(|_| Vec::new()).collect();
    let handle = |line: String, ops: &mut Vec<Vec<Op>>| {
        let parts: Vec<&str> = line.split(' ').collect();
        match parts.as_slice() {
            ["S", t, op, key, kind] => {
                let (t, op): (usize, u64) = (t.parse().unwrap(), op.parse().unwrap());
                assert_eq!(ops[t].len() as u64, op, "thread {t} skipped an op");
                ops[t].push(Op {
                    key: key.to_string(),
                    value: (*kind == "P").then(|| value(round, t, op)),
                    acked: false,
                });
            }
            ["A", t, op] => {
                let (t, op): (usize, usize) = (t.parse().unwrap(), op.parse().unwrap());
                ops[t][op].acked = true;
            }
            // A line cut short by the kill, or test harness chatter.
            _ => {}
        }
    };

    // The clock starts once the child has opened (and recovered) the database.
    loop {
        let line = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("child never got ready");
        if line.ends_with("ready") {
            break;
        }
    }
    let deadline = Instant::now() + kill_after;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(left) {
            Ok(line) => handle(line, &mut ops),
            Err(mpsc::RecvTimeoutError::Timeout) => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let out = proc.wait_with_output().unwrap();
                panic!(
                    "round {round}: child exited on its own ({}):\n{}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
    }
    proc.kill().unwrap(); // SIGKILL: no destructors, no final sync
    proc.wait().unwrap();
    for line in rx {
        handle(line, &mut ops);
    }
    reader.join().unwrap();
    ops
}

/// Checks the recovered database against `model` (each key's value as of
/// the last round) plus this round's operations, then updates `model` to
/// what the database now holds.
fn check(dir: &Path, round: u64, ops: &[Vec<Op>], model: &mut HashMap<String, Option<Vec<u8>>>) {
    // What each key must hold, and the one other thing it may hold.
    let mut must: HashMap<String, Option<Vec<u8>>> = model.clone();
    let mut may: HashMap<String, Option<Vec<u8>>> = HashMap::new();
    for thread in ops {
        for (i, op) in thread.iter().enumerate() {
            if op.acked {
                must.insert(op.key.clone(), op.value.clone());
            } else {
                // Each thread waits for its acknowledgement before starting
                // the next operation, so only its last can be unacknowledged.
                assert_eq!(
                    i + 1,
                    thread.len(),
                    "round {round}: an op mid-stream wasn't acked"
                );
                may.insert(op.key.clone(), op.value.clone());
            }
        }
    }

    let db = Db::open_with(dir, options(round)).unwrap();
    let mut live = BTreeMap::new();
    for (k, want) in &must {
        let got = db.get(k.as_bytes()).unwrap();
        let ok = got == *want || may.get(k).is_some_and(|alt| got == *alt);
        assert!(
            ok,
            "round {round}: key {k} holds {:?}, expected {:?} (or {:?})",
            got.as_deref().map(String::from_utf8_lossy),
            want.as_deref().map(String::from_utf8_lossy),
            may.get(k)
                .map(|v| v.as_deref().map(String::from_utf8_lossy)),
        );
        if let Some(v) = &got {
            live.insert(k.clone().into_bytes(), v.clone());
        }
        model.insert(k.clone(), got);
    }
    // The scan sees exactly the keys the point reads found, and nothing else.
    let scanned: BTreeMap<Vec<u8>, Vec<u8>> = db.iter().unwrap().map(|i| i.unwrap()).collect();
    assert!(
        scanned == live,
        "round {round}: a full scan disagrees with point reads"
    );
}

/// Runs `rounds` crash rounds against one directory. Returns the totals.
fn crash_rounds(rounds: u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut model: HashMap<String, Option<Vec<u8>>> = (0..THREADS)
        .flat_map(|t| (0..KEYS_PER_THREAD).map(move |j| (key(t, j), None)))
        .collect();
    let mut rng = Rng::new(42);
    let (mut acked, mut unacked) = (0, 0);
    for round in 0..rounds {
        let kill_after = Duration::from_millis(20 + rng.below(280));
        let ops = run_child(dir.path(), round, kill_after);
        check(dir.path(), round, &ops, &mut model);
        acked += ops.iter().flatten().filter(|op| op.acked).count();
        unacked += ops.iter().flatten().filter(|op| !op.acked).count();
    }
    let db = Db::open_with(dir.path(), options(0)).unwrap();
    let st = db.stats();
    println!(
        "{rounds} rounds: {acked} acknowledged ops checked, {unacked} in flight at a kill; \
         now {} tables, files per level {:?}",
        st.tables, st.level_files
    );
    assert!(
        acked as u64 > rounds * 100,
        "too little work per round: {acked}"
    );
    assert!(
        st.level_files[1..].iter().any(|&n| n > 0),
        "never compacted"
    );
}

#[test]
fn kill_9_loses_no_acknowledged_write() {
    if let (Ok(dir), Ok(round)) = (std::env::var(CHILD_DIR), std::env::var(CHILD_ROUND)) {
        child(Path::new(&dir), round.parse().unwrap());
    }
    crash_rounds(6);
}

#[test]
#[ignore = "soak: about a minute in release mode"]
fn kill_9_soak() {
    let rounds = std::env::var("LSMKV_CRASH_ROUNDS").map_or(300, |s| s.parse().unwrap());
    crash_rounds(rounds);
}
