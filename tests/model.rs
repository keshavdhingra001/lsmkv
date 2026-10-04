//! Model-based fuzz test (DESIGN.md D23): random sequences of every public
//! operation, run against the database and against a `BTreeMap`.
//!
//! The operations: put, delete, get and scan (random bounds), each now or
//! through a live snapshot, take a snapshot, drop one, atomic batches,
//! transactions (with direct writes racing them, so they must conflict
//! exactly when they share a key), flush, compact, and close + reopen (which
//! ends every snapshot). The options are random too: memtables
//! and tables small enough that a hundred operations run flushes and
//! compactions, either sync mode, filters and the cache on or off.
//!
//! On a failure, proptest shrinks the sequence to a minimal one that still
//! fails and saves it under `tests/model.proptest-regressions`, so the next
//! run tries it first. `PROPTEST_CASES=5000 cargo test --release --test model`
//! runs a longer search (the default is 256 cases).

use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Duration;

use lsmkv::{Db, Error, Options, Snapshot, SyncMode, WriteBatch};
use proptest::prelude::*;
use proptest::sample::Index;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    /// `None` reads now, `Some` through a live snapshot (if there is one).
    Get(Vec<u8>, Option<Index>),
    /// `None` scans now, `Some` through a live snapshot (if there is one).
    Scan(Bound<Vec<u8>>, Bound<Vec<u8>>, Option<Index>),
    Snapshot,
    DropSnapshot(Index),
    /// Puts (`Some`) and deletes (`None`) applied as one atomic batch.
    Batch(Vec<(Vec<u8>, Option<Vec<u8>>)>),
    /// A transaction writes `writes`; before it commits, `meanwhile` is
    /// written directly. It must conflict exactly when the two share a key.
    Txn {
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        meanwhile: Vec<Vec<u8>>,
    },
    Flush,
    CompactAll,
    Reopen,
}

/// Keys of 0-3 letters from "abc": few enough that operations keep hitting
/// the same keys, with prefixes and the empty key in the mix.
fn key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(prop::sample::select(b"abc".to_vec()), 0..4)
}

fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
    prop_oneof![
        key().prop_map(Bound::Included),
        key().prop_map(Bound::Excluded),
        Just(Bound::Unbounded),
    ]
}

fn write() -> impl Strategy<Value = (Vec<u8>, Option<Vec<u8>>)> {
    (
        key(),
        prop::option::weighted(0.75, prop::collection::vec(any::<u8>(), 0..32)),
    )
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        30 => (key(), prop::collection::vec(any::<u8>(), 0..64)).prop_map(|(k, v)| Op::Put(k, v)),
        10 => key().prop_map(Op::Delete),
        8 => (key(), any::<Option<Index>>()).prop_map(|(k, s)| Op::Get(k, s)),
        8 => (bound(), bound(), any::<Option<Index>>()).prop_map(|(a, b, s)| Op::Scan(a, b, s)),
        4 => Just(Op::Snapshot),
        3 => any::<Index>().prop_map(Op::DropSnapshot),
        2 => Just(Op::Flush),
        1 => Just(Op::CompactAll),
        1 => Just(Op::Reopen),
        4 => prop::collection::vec(write(), 0..6).prop_map(Op::Batch),
        3 => (prop::collection::vec(write(), 1..4), prop::collection::vec(key(), 0..3))
            .prop_map(|(writes, meanwhile)| Op::Txn { writes, meanwhile }),
    ]
}

fn options() -> impl Strategy<Value = Options> {
    (
        128usize..4096,
        256usize..4096,
        any::<bool>(),
        prop_oneof![Just(0usize), Just(10)],
        prop_oneof![Just(0usize), Just(64 << 10)],
    )
        .prop_map(|(memtable, table, always, bloom, cache)| Options {
            memtable_size: memtable,
            target_file_size: table,
            l0_compaction_trigger: 2,
            l0_slowdown_trigger: 4,
            l0_stop_trigger: 6,
            level1_max_bytes: 4096,
            level_size_multiplier: 3,
            bloom_bits_per_key: bloom,
            block_cache_bytes: cache,
            sync_mode: if always {
                SyncMode::Always
            } else {
                SyncMode::Periodic(Duration::from_millis(5))
            },
            ..Options::default()
        })
}

fn in_range(model: &Model, lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let range = (
        lo.as_ref().map(Vec::as_slice),
        hi.as_ref().map(Vec::as_slice),
    );
    model
        .iter()
        .filter(|(k, _)| std::ops::RangeBounds::contains(&range, k.as_slice()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn run(opts: Options, ops: Vec<Op>) -> Result<(), TestCaseError> {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let mut ops = ops.into_iter().peekable();
    // One pass per open: snapshots borrow the `Db`, so they all end before a
    // reopen, as they would in a real process restart.
    while ops.peek().is_some() {
        let db = Db::open_with(dir.path(), opts.clone()).unwrap();
        let all: Vec<_> = db.iter().unwrap().map(|i| i.unwrap()).collect();
        prop_assert_eq!(
            &all,
            &in_range(&model, &Bound::Unbounded, &Bound::Unbounded)
        );
        let mut snaps: Vec<(Snapshot, Model)> = Vec::new();
        for op in ops.by_ref() {
            match op {
                Op::Put(k, v) => {
                    db.put(&k, &v).unwrap();
                    model.insert(k, v);
                }
                Op::Delete(k) => {
                    db.delete(&k).unwrap();
                    model.remove(&k);
                }
                Op::Get(k, snap) => match snap.filter(|_| !snaps.is_empty()) {
                    Some(i) => {
                        let (s, then) = &snaps[i.index(snaps.len())];
                        prop_assert_eq!(s.get(&k).unwrap(), then.get(&k).cloned());
                    }
                    None => prop_assert_eq!(db.get(&k).unwrap(), model.get(&k).cloned()),
                },
                Op::Scan(lo, hi, snap) => {
                    let range = (
                        lo.as_ref().map(Vec::as_slice),
                        hi.as_ref().map(Vec::as_slice),
                    );
                    let (scan, want) = match snap.filter(|_| !snaps.is_empty()) {
                        Some(i) => {
                            let (s, then) = &snaps[i.index(snaps.len())];
                            (s.scan::<[u8]>(range).unwrap(), in_range(then, &lo, &hi))
                        }
                        None => (db.scan::<[u8]>(range).unwrap(), in_range(&model, &lo, &hi)),
                    };
                    let got: Vec<_> = scan.map(|i| i.unwrap()).collect();
                    prop_assert_eq!(got, want);
                }
                Op::Snapshot => snaps.push((db.snapshot(), model.clone())),
                Op::DropSnapshot(i) => {
                    if !snaps.is_empty() {
                        snaps.swap_remove(i.index(snaps.len()));
                    }
                }
                Op::Batch(writes) => {
                    let mut batch = WriteBatch::new();
                    for (k, v) in &writes {
                        match v {
                            Some(v) => batch.put(k, v),
                            None => batch.delete(k),
                        };
                    }
                    db.write(batch).unwrap();
                    for (k, v) in writes {
                        apply(&mut model, k, v);
                    }
                }
                Op::Txn { writes, meanwhile } => {
                    let mut tx = db.transaction();
                    for (k, v) in &writes {
                        // Reads see its own writes, layered over its snapshot.
                        let before = tx.get(k).unwrap();
                        match v {
                            Some(v) => tx.put(k, v),
                            None => tx.delete(k),
                        }
                        prop_assert_eq!(
                            tx.get(k).unwrap(),
                            v.clone(),
                            "own write, was {:?}",
                            before
                        );
                    }
                    for k in &meanwhile {
                        db.put(k, b"meanwhile").unwrap();
                        model.insert(k.clone(), b"meanwhile".to_vec());
                    }
                    let clash = writes.iter().any(|(k, _)| meanwhile.contains(k));
                    match tx.commit() {
                        Ok(()) => {
                            prop_assert!(!clash, "committed over a newer write");
                            for (k, v) in writes {
                                apply(&mut model, k, v);
                            }
                        }
                        Err(Error::Conflict(_)) => prop_assert!(clash, "a conflict with no clash"),
                        Err(e) => panic!("{e}"),
                    }
                }
                Op::Flush => db.flush().unwrap(),
                Op::CompactAll => db.compact_all().unwrap(),
                Op::Reopen => break,
            }
        }
        // Every live snapshot still sees exactly its moment.
        for (s, then) in &snaps {
            let got: Vec<_> = s.iter().unwrap().map(|i| i.unwrap()).collect();
            prop_assert_eq!(&got, &in_range(then, &Bound::Unbounded, &Bound::Unbounded));
        }
    }
    Ok(())
}

/// A put (`Some`) or delete (`None`) applied to the model.
fn apply(model: &mut Model, k: Vec<u8>, v: Option<Vec<u8>>) {
    match v {
        Some(v) => model.insert(k, v),
        None => model.remove(&k),
    };
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES").map_or(256, |s| s.parse().unwrap())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
        ..ProptestConfig::default()
    })]

    #[test]
    fn database_matches_a_model(opts in options(), ops in prop::collection::vec(op(), 1..300)) {
        run(opts, ops)?;
    }
}
