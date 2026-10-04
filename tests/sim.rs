//! Deterministic simulation with power cuts and fsync failures (DESIGN.md D28).
//!
//! Each seed drives one database on a simulated disk (`SimFs`) through
//! several "epochs". An epoch opens the database, checks it, arms a fault,
//! runs random operations (puts, deletes, batches, transactions, flushes,
//! compactions, reads, WAL syncs) until one fails or the epoch ends, and then
//! cuts the power, kills the process (the page cache stays), or just closes.
//!
//! **The check after every reopen:** the database must hold the last state
//! known to be durable, plus some prefix of the operations after it, applied
//! in order. In `Always` mode every acknowledged write is durable, so the only
//! uncertain operation is the one in flight when the fault hit. In `Periodic`
//! mode, writes since the last `sync_wal` may be lost, but only from the end:
//! never a hole in the middle, and never half a batch.
//!
//! The engine runs with `inline_background` (no threads), and the simulated
//! disk is seeded, so a seed is a complete, exact replay: a failure prints
//! its seed, and `LSMKV_SIM_SEED=<n> cargo test --test sim -- --nocapture`
//! reruns just that one, printing every step.
//!
//! `cargo test` runs 100 seeds; `LSMKV_SIM_SEEDS=20000 cargo test --release --test sim` runs more.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use lsmkv::vfs::SimFs;
use lsmkv::{Db, Options, SyncMode, WriteBatch};

const DIR: &str = "/sim/db";
const EPOCHS: u64 = 6;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;
/// One write operation: a put (`Some`) or a delete (`None`) per key, applied
/// atomically.
type Op = Vec<(Vec<u8>, Option<Vec<u8>>)>;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0xD1B5_4A32_D192_ED03) | 1)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn apply(model: &mut Model, op: &Op) {
    for (k, v) in op {
        match v {
            Some(v) => model.insert(k.clone(), v.clone()),
            None => model.remove(k),
        };
    }
}

fn show(model: &Model) -> String {
    let items: Vec<String> = model
        .iter()
        .take(8)
        .map(|(k, v)| format!("{}={}B", String::from_utf8_lossy(k), v.len()))
        .collect();
    format!(
        "{} keys [{}{}]",
        model.len(),
        items.join(" "),
        if model.len() > 8 { " ..." } else { "" }
    )
}

/// What one seed did, compared across runs to check determinism.
#[derive(Debug, PartialEq, Eq)]
struct Summary {
    acked: u64,
    faults_hit: u64,
    power_cuts: u64,
    final_keys: usize,
    trace: u64,
}

fn options(sim: &SimFs, rng: &mut Rng, periodic: bool) -> Options {
    Options {
        memtable_size: 256 + rng.below(3000) as usize,
        l0_compaction_trigger: 2,
        l0_slowdown_trigger: 4,
        l0_stop_trigger: 6,
        level1_max_bytes: 2048 + rng.below(4096),
        level_size_multiplier: 3,
        target_file_size: 512 + rng.below(2048) as usize,
        bloom_bits_per_key: [0, 10][rng.below(2) as usize],
        block_cache_bytes: [0, 16 << 10][rng.below(2) as usize],
        sync_mode: if periodic {
            SyncMode::Periodic(Duration::from_secs(1))
        } else {
            SyncMode::Always
        },
        fs: Arc::new(sim.clone()),
        inline_background: true,
    }
}

fn random_op(rng: &mut Rng, step: u64) -> Op {
    let write = |rng: &mut Rng| {
        let key = format!("k{:03}", rng.below(150)).into_bytes();
        if rng.below(4) == 0 {
            (key, None)
        } else {
            // Unique per step, so an old value coming back is visible.
            let mut v = format!("s{step}.").into_bytes();
            v.resize(v.len() + rng.below(80) as usize, b'v');
            (key, Some(v))
        }
    };
    let n = if rng.below(5) == 0 {
        2 + rng.below(4)
    } else {
        1
    };
    (0..n).map(|_| write(rng)).collect()
}

/// Runs `op` through the database's API: a put, a delete, a batch, or a
/// transaction (single-threaded, so it never conflicts).
fn execute(db: &Db, rng: &mut Rng, op: &Op) -> lsmkv::Result<()> {
    match op.as_slice() {
        [(k, Some(v))] => db.put(k, v),
        [(k, None)] => db.delete(k),
        _ if rng.below(2) == 0 => {
            let mut b = WriteBatch::new();
            for (k, v) in op {
                match v {
                    Some(v) => b.put(k, v),
                    None => b.delete(k),
                };
            }
            db.write(b)
        }
        _ => {
            let mut tx = db.transaction();
            for (k, v) in op {
                match v {
                    Some(v) => tx.put(k, v),
                    None => tx.delete(k),
                }
            }
            tx.commit()
        }
    }
}

fn contents(db: &Db) -> lsmkv::Result<Model> {
    db.iter()?.collect()
}

/// One seed. `Err` describes the first violation.
fn run(seed: u64, verbose: bool) -> Result<Summary, String> {
    let sim = SimFs::new(seed);
    let mut rng = Rng::new(seed);
    let say = |msg: String| {
        if verbose {
            println!("{msg}");
        }
    };
    // What is known durable, the operations after it whose fate is unknown
    // (acknowledged-but-unsynced in Periodic mode, plus the one in flight),
    // and the full acknowledged state.
    let mut durable = Model::new();
    let mut unsure: Vec<Op> = Vec::new();
    let mut model = Model::new();
    let mut summary = Summary {
        acked: 0,
        faults_hit: 0,
        power_cuts: 0,
        final_keys: 0,
        trace: 0,
    };
    let mut step = 0u64;

    for epoch in 0..EPOCHS {
        let periodic = rng.below(3) == 0;
        let opts = options(&sim, &mut rng, periodic);
        let db =
            Db::open_with(DIR, opts).map_err(|e| format!("epoch {epoch}: reopen failed: {e}"))?;

        // Recovery check: durable state + some prefix of the unsure ops.
        let got = contents(&db).map_err(|e| format!("epoch {epoch}: scan after reopen: {e}"))?;
        let mut candidate = durable.clone();
        let mut matched = (candidate == got).then_some(0);
        for (i, op) in unsure.iter().enumerate() {
            apply(&mut candidate, op);
            if matched.is_none() && candidate == got {
                matched = Some(i + 1);
            }
        }
        if matched.is_none() && verbose {
            for k in durable
                .keys()
                .chain(got.keys())
                .collect::<std::collections::BTreeSet<_>>()
            {
                let (d, g) = (durable.get(k), got.get(k));
                if d != g {
                    let tag = |v: Option<&Vec<u8>>| {
                        v.map(|v| String::from_utf8_lossy(&v[..v.len().min(8)]).into_owned())
                    };
                    println!(
                        "    {}: durable {:?} recovered {:?}",
                        String::from_utf8_lossy(k),
                        tag(d),
                        tag(g)
                    );
                }
            }
            println!("    files after recovery: {:?}", sim.files());
        }
        let Some(kept) = matched else {
            return Err(format!(
                "epoch {epoch}: recovered state is not the durable state plus any prefix of the \
                 {} operations after it\n  durable:   {}\n  recovered: {}\n  all acked: {}",
                unsure.len(),
                show(&durable),
                show(&got),
                show(&model)
            ));
        };
        say(format!(
            "epoch {epoch}: reopened, kept {kept} of {} unsure ops; {}",
            unsure.len(),
            show(&got)
        ));
        durable = got.clone();
        model = got;
        unsure.clear();

        // Arm this epoch's fault.
        // 60% power cut, 10% process kill (the page cache survives), 20%
        // failed fsync, 10% nothing; each at a random point.
        let fault = rng.below(10);
        let cut_power = fault < 6;
        let kill = fault == 6;
        match fault {
            0..=6 => sim.crash_after(1 + rng.below(500)),
            7..=8 => sim.fail_sync_after(1 + rng.below(40)),
            _ => {}
        }
        say(format!(
            "  {} mode, fault {fault}",
            if periodic { "Periodic" } else { "Always" }
        ));

        for _ in 0..100 + rng.below(300) {
            step += 1;
            let kind = rng.below(100);
            let ok = match kind {
                0..=69 => {
                    let op = random_op(&mut rng, step);
                    match execute(&db, &mut rng, &op) {
                        Ok(()) => {
                            summary.acked += 1;
                            apply(&mut model, &op);
                            if periodic {
                                unsure.push(op);
                            } else {
                                apply(&mut durable, &op);
                            }
                            true
                        }
                        Err(e) => {
                            say(format!("  step {step}: write failed: {e}"));
                            unsure.push(op);
                            false
                        }
                    }
                }
                70..=74 => {
                    let r = db.flush();
                    say(format!("  step {step}: flush {r:?} files {:?}", sim.files()));
                    r.is_ok()
                }
                75..=76 => {
                    let r = db.compact_all();
                    say(format!("  step {step}: compact_all {r:?} files {:?}", sim.files()));
                    r.is_ok()
                }
                77..=81 if periodic => match db.sync_wal() {
                    Ok(()) => {
                        say(format!("  step {step}: sync_wal ok, {} keys durable, files {:?}", model.len(), sim.files()));
                        durable = model.clone();
                        unsure.clear();
                        true
                    }
                    Err(_) => false,
                },
                _ => match contents(&db) {
                    Ok(now) if now == model => true,
                    Ok(now) => {
                        return Err(format!(
                            "epoch {epoch} step {step}: a live scan disagrees with the acknowledged state\n  \
                             scan: {}\n  acked: {}",
                            show(&now),
                            show(&model)
                        ))
                    }
                    Err(_) => false,
                },
            };
            if !ok {
                summary.faults_hit += 1;
                break;
            }
        }

        if cut_power {
            sim.crash_now();
            drop(db);
            sim.power_cut();
            summary.power_cuts += 1;
            say("  power cut".into());
        } else if kill {
            // kill -9: no clean close, but the OS keeps what it was handed.
            sim.crash_now();
            drop(db);
            sim.process_restart();
            say("  process killed".into());
        } else {
            // A process exit: whatever reached the OS stays. A clean close
            // syncs the WAL in Periodic mode, unless the database was poisoned.
            drop(db);
            say("  closed".into());
        }
    }
    summary.final_keys = model.len();
    summary.trace = sim.stats().trace;
    Ok(summary)
}

fn seeds() -> Vec<u64> {
    if let Ok(one) = std::env::var("LSMKV_SIM_SEED") {
        return vec![one.parse().expect("LSMKV_SIM_SEED")];
    }
    let n = std::env::var("LSMKV_SIM_SEEDS").map_or(100, |s| s.parse().expect("LSMKV_SIM_SEEDS"));
    (0..n).collect()
}

#[test]
fn power_cuts_and_failed_fsyncs_lose_nothing_durable() {
    let verbose = std::env::var("LSMKV_SIM_SEED").is_ok();
    let (mut acked, mut faults, mut cuts) = (0, 0, 0);
    let seeds = seeds();
    for &seed in &seeds {
        match run(seed, verbose) {
            Ok(s) => {
                acked += s.acked;
                faults += s.faults_hit;
                cuts += s.power_cuts;
            }
            Err(e) => panic!(
                "seed {seed}: {e}\nreplay: LSMKV_SIM_SEED={seed} cargo test --test sim -- --nocapture"
            ),
        }
    }
    println!(
        "{} seeds: {acked} acknowledged writes, {cuts} power cuts, {faults} operations hit a fault",
        seeds.len()
    );
    assert!(
        faults > seeds.len() as u64 / 2,
        "faults rarely landed: {faults}"
    );
}

/// The same seed twice gives the same run: every I/O, in order (the disk
/// hashes each operation), and the same outcome.
#[test]
fn a_seed_replays_exactly() {
    for seed in [1, 2, 3, 42] {
        let a = run(seed, false).unwrap();
        let b = run(seed, false).unwrap();
        assert_eq!(a, b, "seed {seed} ran differently twice");
    }
    assert_ne!(run(1, false).unwrap().trace, run(2, false).unwrap().trace);
}
