//! Crash harness (DESIGN.md D22): real `kill -9`s, round after round, against
//! one database directory.
//!
//! Each round, a child process (this same test binary, re-run with an
//! environment variable set) opens the database and runs 4 writer threads.
//! Each thread owns 2,000 keys and does random puts, deletes and atomic
//! batches of 2-4 writes on them, printing `S` before an operation and `A`
//! once it's acknowledged. The parent
//! `kill -9`s the child at a random moment, reopens the database itself, and
//! checks every key exactly:
//!
//! - it must hold the result of the last acknowledged operation on it,
//! - or, if the thread's one unacknowledged operation was on this key, the
//!   result of that operation (it may or may not have reached the log).
//!   A batch must be there whole or not at all: every one of its keys
//!   holding the new value, or every one holding the old.
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
//! Power cuts (losing the kernel's page cache too) need a fault-injecting
//! filesystem: `lazyfs_power_cuts` runs the same rounds on a LazyFS mount
//! and, after each kill, has LazyFS drop every byte that wasn't fsynced
//! (DESIGN.md D31). It's skipped unless `LSMKV_LAZYFS_DIR` (a directory on
//! the mount) and `LSMKV_LAZYFS_FIFO` (its fault FIFO) are set; see
//! `scripts/lazyfs.sh`. Only `Always` rounds run there: `Periodic` mode may
//! lose unsynced writes to a power cut by design.
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

use lsmkv::{Db, Options, SyncMode, WriteBatch};

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
        // 1 in 5 operations is an atomic batch of 2-4 distinct keys.
        let n = if rng.below(5) == 0 {
            2 + rng.below(3)
        } else {
            1
        };
        let mut keys: Vec<String> = Vec::new();
        while (keys.len() as u64) < n {
            let k = key(t, rng.below(KEYS_PER_THREAD as u64) as usize);
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        let v = value(round, t, op);
        let writes: Vec<(String, bool)> =
            keys.into_iter().map(|k| (k, rng.below(4) != 0)).collect();
        // `S <thread> <op> (<key> <P|D>)+ ;` -- the `;` marks a whole line.
        let mut line = format!("S {t} {op}");
        for (k, is_put) in &writes {
            line += &format!(" {k} {}", if *is_put { "P" } else { "D" });
        }
        print(line + " ;");
        let mut batch = WriteBatch::new();
        for (k, is_put) in &writes {
            if *is_put {
                batch.put(k.as_bytes(), &v);
                mine.insert(k.clone().into_bytes(), v.clone());
            } else {
                batch.delete(k.as_bytes());
                mine.remove(k.as_bytes());
            }
        }
        match writes.as_slice() {
            // Single writes go through put/delete, the common path.
            [(k, true)] => db.put(k.as_bytes(), &v).unwrap(),
            [(k, false)] => db.delete(k.as_bytes()).unwrap(),
            _ => db.write(batch).unwrap(),
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

/// One operation the child started, as the parent parsed it: one write, or
/// an atomic batch of several.
struct Op {
    /// (key, `Some(value)` for a put or `None` for a delete).
    writes: Vec<(String, Option<Vec<u8>>)>,
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
            ["S", t, op, writes @ .., ";"] if !writes.is_empty() && writes.len() % 2 == 0 => {
                let (t, op): (usize, u64) = (t.parse().unwrap(), op.parse().unwrap());
                assert_eq!(ops[t].len() as u64, op, "thread {t} skipped an op");
                let writes = writes
                    .chunks(2)
                    .map(|w| (w[0].to_string(), (w[1] == "P").then(|| value(round, t, op))))
                    .collect();
                ops[t].push(Op {
                    writes,
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
    // What each key must hold if the in-flight operations didn't land...
    let mut must: HashMap<String, Option<Vec<u8>>> = model.clone();
    // ...and those operations (at most one per thread).
    let mut in_flight: Vec<&Op> = Vec::new();
    for thread in ops {
        for (i, op) in thread.iter().enumerate() {
            if op.acked {
                for (k, v) in &op.writes {
                    must.insert(k.clone(), v.clone());
                }
            } else {
                // Each thread waits for its acknowledgement before starting
                // the next operation, so only its last can be unacknowledged.
                assert_eq!(
                    i + 1,
                    thread.len(),
                    "round {round}: an op mid-stream wasn't acked"
                );
                in_flight.push(op);
            }
        }
    }

    let db = Db::open_with(dir, options(round)).unwrap();
    let mut got: HashMap<String, Option<Vec<u8>>> = HashMap::new();
    for k in must.keys() {
        got.insert(k.clone(), db.get(k.as_bytes()).unwrap());
    }
    let show = |v: &Option<Vec<u8>>| {
        v.as_deref()
            .map(|v| String::from_utf8_lossy(v).into_owned())
    };
    // An in-flight operation landed entirely, or not at all.
    let mut covered = std::collections::HashSet::new();
    for op in &in_flight {
        let landed = op.writes.iter().all(|(k, v)| got[k] == *v);
        let absent = op.writes.iter().all(|(k, _)| got[k] == must[k]);
        assert!(
            landed || absent,
            "round {round}: in-flight operation half applied: {:?}",
            op.writes
                .iter()
                .map(|(k, v)| (k, show(v), show(&must[k]), show(&got[k])))
                .collect::<Vec<_>>()
        );
        covered.extend(op.writes.iter().map(|(k, _)| k.clone()));
    }
    // Every other key holds exactly its last acknowledged value.
    for (k, want) in &must {
        if !covered.contains(k) {
            assert!(
                got[k] == *want,
                "round {round}: key {k} holds {:?}, expected {:?}",
                show(&got[k]),
                show(want)
            );
        }
    }
    let live: BTreeMap<Vec<u8>, Vec<u8>> = got
        .iter()
        .filter_map(|(k, v)| Some((k.clone().into_bytes(), v.clone()?)))
        .collect();
    model.extend(got);
    // The scan sees exactly the keys the point reads found, and nothing else.
    let scanned: BTreeMap<Vec<u8>, Vec<u8>> = db.iter().unwrap().map(|i| i.unwrap()).collect();
    assert!(
        scanned == live,
        "round {round}: a full scan disagrees with point reads"
    );
}

/// Runs `rounds` crash rounds against one directory. Returns the totals.
fn crash_rounds(rounds: u64) {
    crash_rounds_in(tempfile::tempdir().unwrap(), rounds, None);
}

/// LazyFS's fault FIFO, and the FIFO it reports finished commands on.
struct PowerCut {
    fifo: String,
    /// Opened once and kept open: LazyFS holds the writing end for its whole
    /// life, so a reader that closed it between rounds would leave LazyFS
    /// writing into a pipe with no reader.
    done: std::io::BufReader<std::fs::File>,
}

impl PowerCut {
    fn open(fifo: String, done: &str) -> Self {
        let done = std::io::BufReader::new(std::fs::File::open(done).unwrap());
        Self { fifo, done }
    }

    /// Drops every byte the kernel would have lost in a power cut: LazyFS
    /// throws away all data that was written but never fsynced.
    fn now(&mut self) {
        std::fs::write(&self.fifo, "lazyfs::clear-cache\n").unwrap();
        let mut reply = String::new();
        self.done.read_line(&mut reply).unwrap();
        assert_eq!(reply.trim(), "finished::clear-cache");
    }
}

fn crash_rounds_in(dir: tempfile::TempDir, rounds: u64, mut power_cut: Option<PowerCut>) {
    let mut model: HashMap<String, Option<Vec<u8>>> = (0..THREADS)
        .flat_map(|t| (0..KEYS_PER_THREAD).map(move |j| (key(t, j), None)))
        .collect();
    let mut rng = Rng::new(42);
    let (mut acked, mut unacked) = (0, 0);
    for i in 0..rounds {
        // Power cuts run `Always` rounds only (the even ones).
        let round = if power_cut.is_some() { 2 * i } else { i };
        let kill_after = Duration::from_millis(20 + rng.below(280));
        let ops = run_child(dir.path(), round, kill_after);
        if let Some(cut) = &mut power_cut {
            cut.now();
        }
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

#[test]
#[ignore = "needs a LazyFS mount: scripts/lazyfs.sh"]
fn lazyfs_power_cuts() {
    let (Ok(dir), Ok(fifo)) = (
        std::env::var("LSMKV_LAZYFS_DIR"),
        std::env::var("LSMKV_LAZYFS_FIFO"),
    ) else {
        eprintln!("LSMKV_LAZYFS_DIR / LSMKV_LAZYFS_FIFO not set: skipped");
        return;
    };
    let done = std::env::var("LSMKV_LAZYFS_DONE").unwrap_or_else(|_| format!("{fifo}.completed"));
    let rounds = std::env::var("LSMKV_CRASH_ROUNDS").map_or(50, |s| s.parse().unwrap());
    crash_rounds_in(
        tempfile::tempdir_in(dir).unwrap(),
        rounds,
        Some(PowerCut::open(fifo, &done)),
    );
}
