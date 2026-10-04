//! Atomic write batches and optimistic transactions (DESIGN.md D25, D26).
//!
//! A `WriteBatch` is several puts and deletes applied as one unit: they get
//! consecutive sequence numbers, go to the WAL as ONE record (so a crash keeps
//! all of them or none), and become visible to readers at the same instant
//! (a write group publishes its last sequence number once, at the end).
//!
//! A `Transaction` reads at a snapshot and buffers its writes. At commit, the
//! write group leader checks each key it writes: if any has a version newer
//! than the transaction's snapshot, someone else committed to it first, and
//! the commit fails with `Error::Conflict` (first committer wins). Otherwise
//! the writes commit as one batch. That's snapshot isolation: no lost
//! updates, but write skew is possible (see D26).

use std::collections::{BTreeMap, BTreeSet, HashSet};

use super::{Db, Snapshot, SuperVersion};
use crate::error::{Error, Result};
use crate::key::SeqNo;
use crate::wal::Record;

/// Puts and deletes that `Db::write` applies all at once, or not at all.
/// Later operations on the same key win, as if applied in order.
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    ops: Vec<Record>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> &mut Self {
        self.ops.push(Record::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self
    }

    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        self.ops.push(Record::Delete { key: key.to_vec() });
        self
    }

    /// Number of operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn clear(&mut self) {
        self.ops.clear();
    }
}

/// One entry in the writer queue: a batch, and for a transaction, the
/// snapshot its conflict check compares against.
pub(super) struct Pending {
    pub(super) ops: Vec<Record>,
    /// `Some(seq)` for a transaction: fail if any key in `ops`, or in
    /// `also_check`, has a version newer than `seq`.
    pub(super) read_seq: Option<SeqNo>,
    /// Keys a transaction read with `get_for_update` but didn't write.
    pub(super) also_check: Vec<Vec<u8>>,
}

impl Pending {
    pub(super) fn bytes(&self) -> usize {
        self.ops
            .iter()
            .map(|op| op.key().len() + op.value_len())
            .sum()
    }
}

/// The leader's conflict check for one write group, run with no lock held.
/// One verdict per entry: `Ok` to commit, or why it can't.
///
/// No other group can commit while this one is in flight, so `current` plus
/// the entries of this group accepted so far are every write that can be
/// newer than a transaction's snapshot. An entry earlier in the same group
/// counts too: it will be numbered after every snapshot that exists.
pub(super) fn check_conflicts(current: &SuperVersion, group: &[(u64, Pending)]) -> Vec<Result<()>> {
    let mut written: HashSet<&[u8]> = HashSet::new();
    let mut verdicts = Vec::with_capacity(group.len());
    for (_, pending) in group {
        let verdict = match pending.read_seq {
            None => Ok(()),
            Some(read_seq) => {
                let keys = pending.ops.iter().map(Record::key);
                keys.chain(pending.also_check.iter().map(Vec::as_slice))
                    .try_for_each(|key| {
                        let newer = written.contains(key)
                            || current.newest_seq(key)?.is_some_and(|seq| seq > read_seq);
                        if newer {
                            Err(Error::Conflict(format!(
                        "{:?} was written after this transaction's snapshot (seq {read_seq})",
                        String::from_utf8_lossy(key)
                    )))
                        } else {
                            Ok(())
                        }
                    })
            }
        };
        if verdict.is_ok() {
            written.extend(pending.ops.iter().map(Record::key));
        }
        verdicts.push(verdict);
    }
    verdicts
}

/// A read-write transaction with snapshot isolation (`Db::transaction`).
///
/// Reads see the database as of `Db::transaction`, plus this transaction's
/// own writes. Writes are buffered until `commit`, which applies all of them
/// atomically, or none if another writer committed to one of the same keys
/// after the snapshot. Dropping it without committing discards the writes.
pub struct Transaction<'a> {
    db: &'a Db,
    snapshot: Snapshot<'a>,
    /// Buffered writes by key: `Some(value)` for a put, `None` for a delete.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Keys read with `get_for_update`: checked at commit like written keys.
    locked: BTreeSet<Vec<u8>>,
}

impl Db {
    /// Writes every operation in `batch` atomically: a crash keeps all of
    /// them or none, and a reader sees all of them or none.
    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.commit(Pending {
            ops: batch.ops,
            read_seq: None,
            also_check: Vec::new(),
        })
    }

    /// Starts a transaction reading at a snapshot of the database as it is now.
    pub fn transaction(&self) -> Transaction<'_> {
        Transaction {
            db: self,
            snapshot: self.snapshot(),
            writes: BTreeMap::new(),
            locked: BTreeSet::new(),
        }
    }
}

impl Transaction<'_> {
    /// `key`'s value: this transaction's own write if it made one, otherwise
    /// the value as of its snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.writes.get(key) {
            Some(write) => Ok(write.clone()),
            None => self.snapshot.get(key),
        }
    }

    /// `get`, and also makes the commit fail if anyone else writes `key`
    /// after this transaction's snapshot, even if this transaction never
    /// writes it. Reading the keys a decision depends on this way prevents
    /// write skew on them (D26), like `SELECT ... FOR UPDATE` in SQL or
    /// RocksDB's `GetForUpdate`.
    pub fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.locked.insert(key.to_vec());
        self.get(key)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    /// The sequence number this transaction reads at.
    pub fn sequence(&self) -> SeqNo {
        self.snapshot.sequence()
    }

    /// Applies every buffered write atomically, or returns `Error::Conflict`
    /// (and applies nothing) if another writer committed to one of the same
    /// keys after this transaction started. A conflict is safe to retry with
    /// a new transaction. A transaction that wrote nothing and locked
    /// nothing always commits.
    pub fn commit(self) -> Result<()> {
        if self.writes.is_empty() && self.locked.is_empty() {
            return Ok(());
        }
        let also_check = self
            .locked
            .into_iter()
            .filter(|k| !self.writes.contains_key(k))
            .collect();
        let ops = self
            .writes
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => Record::Put { key, value },
                None => Record::Delete { key },
            })
            .collect();
        self.db.commit(Pending {
            ops,
            read_seq: Some(self.snapshot.sequence()),
            also_check,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Options;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    fn small() -> Options {
        Options {
            memtable_size: 4 << 10,
            ..Options::default()
        }
    }

    fn val(s: &str) -> Option<Vec<u8>> {
        Some(s.as_bytes().to_vec())
    }

    #[test]
    fn batch_applies_in_order_and_survives_reopen_and_flush() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"c", b"old").unwrap();
            let mut b = WriteBatch::new();
            b.put(b"a", b"1")
                .put(b"b", b"2")
                .delete(b"c")
                .put(b"a", b"3");
            assert_eq!(b.len(), 4);
            let before = db.stats().last_sequence;
            db.write(b).unwrap();
            // Four operations, four consecutive numbers.
            assert_eq!(db.stats().last_sequence, before + 4);
            assert_eq!(db.get(b"a").unwrap(), val("3"), "later op wins");
            assert_eq!(db.get(b"b").unwrap(), val("2"));
            assert_eq!(db.get(b"c").unwrap(), None);
            db.write(WriteBatch::new()).unwrap(); // empty: a no-op
            assert_eq!(db.stats().last_sequence, before + 4);
        }
        // Replayed from the WAL...
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), val("3"));
        assert_eq!(db.get(b"c").unwrap(), None);
        // ...and from a table.
        db.flush().unwrap();
        drop(db);
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), val("3"));
        assert_eq!(db.get(b"b").unwrap(), val("2"));
    }

    /// A writer sets all of k0..k9 to n in one batch, n = 1, 2, 3, ...;
    /// readers must never see a mix of two batches, through flushes too.
    #[test]
    fn readers_never_see_half_a_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), small()).unwrap();
        let done = AtomicBool::new(false);
        let checks = AtomicU64::new(0);
        thread::scope(|s| {
            for r in 0..3 {
                let (db, done, checks) = (&db, &done, &checks);
                s.spawn(move || {
                    while !done.load(Ordering::Acquire) {
                        let values: Vec<Option<Vec<u8>>> = if r == 0 {
                            // A scan is one point in time too.
                            let mut v: Vec<_> =
                                db.iter().unwrap().map(|i| Some(i.unwrap().1)).collect();
                            v.resize(10, None);
                            v
                        } else {
                            let snap = db.snapshot();
                            (0..10)
                                .map(|i| snap.get(format!("k{i}").as_bytes()).unwrap())
                                .collect()
                        };
                        assert!(
                            values.windows(2).all(|w| w[0] == w[1]),
                            "torn batch: {values:?}"
                        );
                        checks.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            for n in 1..=2000u32 {
                let mut b = WriteBatch::new();
                for i in 0..10 {
                    b.put(format!("k{i}").as_bytes(), &n.to_be_bytes());
                }
                db.write(b).unwrap();
            }
            done.store(true, Ordering::Release);
        });
        assert!(checks.load(Ordering::Relaxed) > 50);
        assert!(db.stats().tables > 0, "flushes ran meanwhile");
    }

    #[test]
    fn transaction_reads_its_own_writes_and_commits_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.put(b"a", b"0").unwrap();
        db.put(b"gone", b"x").unwrap();
        let mut tx = db.transaction();
        tx.put(b"a", b"1");
        tx.delete(b"gone");
        assert_eq!(tx.get(b"a").unwrap(), val("1"), "reads its own write");
        assert_eq!(tx.get(b"gone").unwrap(), None, "reads its own delete");
        // Nothing is visible until commit.
        assert_eq!(db.get(b"a").unwrap(), val("0"));
        tx.commit().unwrap();
        assert_eq!(db.get(b"a").unwrap(), val("1"));
        assert_eq!(db.get(b"gone").unwrap(), None);

        // A dropped transaction changes nothing.
        let mut tx = db.transaction();
        tx.put(b"a", b"never");
        drop(tx);
        assert_eq!(db.get(b"a").unwrap(), val("1"));
        // A read-only transaction always commits.
        let tx = db.transaction();
        db.put(b"a", b"2").unwrap();
        assert_eq!(tx.get(b"a").unwrap(), val("1"), "reads at its snapshot");
        tx.commit().unwrap();
    }

    /// The newer version that makes a commit fail can be anywhere: the
    /// memtable, a level-0 table, a deeper level; a value or a tombstone.
    #[test]
    fn a_write_after_the_snapshot_is_a_conflict_wherever_it_lives() {
        type Setup = fn(&Db);
        let cases: [(&str, Setup); 5] = [
            ("memtable put", |db| db.put(b"k", b"other").unwrap()),
            ("memtable delete", |db| db.delete(b"k").unwrap()),
            ("level-0 table", |db| {
                db.put(b"k", b"other").unwrap();
                db.flush().unwrap();
            }),
            ("bottom level", |db| {
                db.put(b"k", b"other").unwrap();
                db.compact_all().unwrap();
            }),
            ("another transaction", |db| {
                let mut t = db.transaction();
                t.put(b"k", b"other");
                t.commit().unwrap();
            }),
        ];
        for (name, write_after) in cases {
            let dir = tempfile::tempdir().unwrap();
            let db = Db::open(dir.path()).unwrap();
            db.put(b"k", b"base").unwrap();
            db.put(b"other-key", b"x").unwrap();
            db.compact_all().unwrap();

            let mut tx = db.transaction();
            tx.put(b"k", b"mine");
            tx.put(b"z", b"also mine");
            write_after(&db);
            let err = tx.commit().unwrap_err();
            assert!(matches!(err, Error::Conflict(_)), "{name}: {err:?}");
            assert_ne!(db.get(b"k").unwrap(), val("mine"), "{name}");
            assert_eq!(db.get(b"z").unwrap(), None, "{name}: nothing applied");
            // Nothing logged either: a reopen replays the WAL.
            drop(db);
            let db = Db::open(dir.path()).unwrap();
            assert_eq!(
                db.get(b"z").unwrap(),
                None,
                "{name}: a refused commit came back"
            );

            // Writes to other keys don't conflict, and a retry succeeds.
            let mut tx = db.transaction();
            tx.put(b"k", b"mine");
            db.put(b"other-key", b"y").unwrap();
            tx.commit().unwrap();
            assert_eq!(db.get(b"k").unwrap(), val("mine"), "{name}");
        }
    }

    /// Snapshot isolation checks writes against writes only, so two
    /// transactions that each read both keys and write a different one both
    /// commit ("write skew"). This test pins the documented behavior (D26).
    #[test]
    fn write_skew_is_allowed_under_snapshot_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.put(b"alice_on_call", b"yes").unwrap();
        db.put(b"bob_on_call", b"yes").unwrap();
        // Rule: at least one must stay on call. Each sees the other on call.
        let mut t1 = db.transaction();
        let mut t2 = db.transaction();
        assert_eq!(t1.get(b"bob_on_call").unwrap(), val("yes"));
        assert_eq!(t2.get(b"alice_on_call").unwrap(), val("yes"));
        t1.put(b"alice_on_call", b"no");
        t2.put(b"bob_on_call", b"no");
        t1.commit().unwrap();
        t2.commit().unwrap();
        assert_eq!(db.get(b"alice_on_call").unwrap(), val("no"));
        assert_eq!(db.get(b"bob_on_call").unwrap(), val("no"));
    }

    /// `get_for_update` puts the keys a decision rests on into the conflict
    /// check: the same on-call scenario now lets only one through. A
    /// transaction that only locked keys still fails if one changed.
    #[test]
    fn get_for_update_prevents_write_skew() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.put(b"alice_on_call", b"yes").unwrap();
        db.put(b"bob_on_call", b"yes").unwrap();
        let mut t1 = db.transaction();
        let mut t2 = db.transaction();
        assert_eq!(t1.get_for_update(b"bob_on_call").unwrap(), val("yes"));
        assert_eq!(t2.get_for_update(b"alice_on_call").unwrap(), val("yes"));
        t1.put(b"alice_on_call", b"no");
        t2.put(b"bob_on_call", b"no");
        t1.commit().unwrap();
        assert!(matches!(t2.commit(), Err(Error::Conflict(_))));
        assert_eq!(
            db.get(b"bob_on_call").unwrap(),
            val("yes"),
            "someone stays on call"
        );

        // Locked but not written: still checked (Redis WATCH + EXEC).
        let mut t = db.transaction();
        t.get_for_update(b"x").unwrap();
        db.put(b"x", b"changed").unwrap();
        assert!(matches!(t.commit(), Err(Error::Conflict(_))));
        let mut t = db.transaction();
        t.get_for_update(b"x").unwrap();
        t.commit().unwrap();
    }

    /// Staged: two transactions from the same snapshot, writing the same key,
    /// queue up behind a slow group and land in ONE group. The check must
    /// count the first one's write against the second: exactly one commits.
    #[test]
    fn conflicting_transactions_in_one_group_one_wins() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.put(b"k", b"0").unwrap();
        let mut t1 = db.transaction();
        let mut t2 = db.transaction();
        t1.put(b"k", b"t1");
        t2.put(b"k", b"t2");
        db.state().slow_wal = Duration::from_millis(150);
        let (r1, r2) = thread::scope(|s| {
            s.spawn(|| db.put(b"lead", b"x").unwrap());
            thread::sleep(Duration::from_millis(30));
            let a = s.spawn(|| t1.commit());
            thread::sleep(Duration::from_millis(30));
            let b = s.spawn(|| t2.commit());
            (a.join().unwrap(), b.join().unwrap())
        });
        assert!(r1.is_ok(), "the first in the queue wins: {r1:?}");
        assert!(matches!(r2, Err(Error::Conflict(_))), "{r2:?}");
        assert_eq!(db.get(b"k").unwrap(), val("t1"));
        assert_eq!(
            db.stats().write_groups,
            3,
            "base put, the lead, then one group of two"
        );
    }

    /// Bank transfers between 10 accounts from 4 threads, retrying on
    /// conflict. Money is never created or destroyed: every snapshot, and
    /// the end state, sum to the starting total.
    #[test]
    fn concurrent_transfers_conserve_money() {
        const ACCOUNTS: u64 = 10;
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), small()).unwrap();
        let acct = |i: u64| format!("acct{i}").into_bytes();
        let amount = |v: Option<Vec<u8>>| u64::from_be_bytes(v.unwrap().try_into().unwrap());
        for i in 0..ACCOUNTS {
            db.put(&acct(i), &100u64.to_be_bytes()).unwrap();
        }
        let total = |snap: &Snapshot| -> u64 {
            (0..ACCOUNTS)
                .map(|i| amount(snap.get(&acct(i)).unwrap()))
                .sum()
        };
        let conflicts = AtomicU64::new(0);
        let done = AtomicBool::new(false);
        thread::scope(|s| {
            let auditor = s.spawn(|| {
                let mut audits = 0;
                while !done.load(Ordering::Acquire) {
                    assert_eq!(
                        total(&db.snapshot()),
                        ACCOUNTS * 100,
                        "money appeared or vanished"
                    );
                    audits += 1;
                }
                audits
            });
            let workers: Vec<_> = (0..4u64)
                .map(|w| {
                    let (db, conflicts) = (&db, &conflicts);
                    s.spawn(move || {
                        let mut x = w * 7 + 1;
                        for _ in 0..300 {
                            x = x
                                .wrapping_mul(6364136223846793005)
                                .wrapping_add(1442695040888963407);
                            let (from, to) = ((x >> 33) % ACCOUNTS, (x >> 17) % ACCOUNTS);
                            if from == to {
                                continue;
                            }
                            loop {
                                let mut tx = db.transaction();
                                let a = amount(tx.get(&acct(from)).unwrap());
                                let b = amount(tx.get(&acct(to)).unwrap());
                                let n = a.min(5);
                                tx.put(&acct(from), &(a - n).to_be_bytes());
                                tx.put(&acct(to), &(b + n).to_be_bytes());
                                match tx.commit() {
                                    Ok(()) => break,
                                    Err(Error::Conflict(_)) => {
                                        conflicts.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(e) => panic!("{e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            for w in workers {
                w.join().unwrap();
            }
            done.store(true, Ordering::Release);
            assert!(auditor.join().unwrap() > 10);
        });
        assert_eq!(total(&db.snapshot()), ACCOUNTS * 100);
        assert!(
            conflicts.load(Ordering::Relaxed) > 0,
            "the test never raced"
        );
        drop(db);
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(total(&db.snapshot()), ACCOUNTS * 100, "after reopen");
    }
}
