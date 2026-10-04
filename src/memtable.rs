//! In-memory write buffer. Sorted, so it can be flushed straight into an SSTable.
//!
//! It holds every version written to it, keyed by (user key, seq) in the
//! internal key order (DESIGN.md D18): a key's newest version comes first.
//! Readers ask for a key "as of" a snapshot and get the newest version at or
//! below it.
//!
//! A lock-free skiplist (DESIGN.md D13): one writer at a time inserts (the
//! group commit leader) while any number of readers search it, and neither
//! waits for the other. Readers never see half of a group, because they read
//! at a snapshot taken before the group's numbers were published.

use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

use crate::key::{InternalKey, SeqNo};

/// What the memtable knows about a key.
///
/// A `Tombstone` is NOT the same as "missing": it means "this key was deleted,
/// so do not look in older SSTables". That difference matters once M3 lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Value(Vec<u8>),
    Tombstone,
}

/// One version in the memtable, borrowed from the skiplist.
pub type MemEntry<'a> = crossbeam_skiplist::map::Entry<'a, InternalKey, Entry>;

#[derive(Default)]
pub struct MemTable {
    map: SkipMap<InternalKey, Entry>,
    /// Rough bytes held, used to decide when to flush. Every version counts,
    /// since every version stays in the map until the flush.
    approx_size: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds version `seq` of `key`. Sequence numbers are unique, so this
    /// never replaces an existing version.
    pub fn put(&self, key: &[u8], seq: SeqNo, value: &[u8]) {
        self.insert(key, seq, Entry::Value(value.to_vec()));
    }

    /// Adds a tombstone at `seq`. It stays in the map so the delete still
    /// shadows older copies of the key that live in SSTables.
    pub fn delete(&self, key: &[u8], seq: SeqNo) {
        self.insert(key, seq, Entry::Tombstone);
    }

    fn insert(&self, key: &[u8], seq: SeqNo, entry: Entry) {
        let size = key.len() + 8 + entry_len(&entry);
        let key = InternalKey::new(key, seq);
        debug_assert!(!self.map.contains_key(&key), "sequence number {seq} reused");
        self.map.insert(key, entry);
        // Only the flush trigger reads it, and nothing is ordered by it.
        self.approx_size.fetch_add(size, Ordering::Relaxed);
    }

    /// The newest version of `key` at or below `snapshot`, with its seq.
    /// `None` = no such version here; `Some((_, Tombstone))` = deleted.
    pub fn get(&self, key: &[u8], snapshot: SeqNo) -> Option<(SeqNo, Entry)> {
        // (key, snapshot) sorts just before every version of `key` that the
        // snapshot may see, so the first entry from there on is the answer
        // if it belongs to `key` at all.
        let from = InternalKey::new(key, snapshot);
        let found = self.map.lower_bound(Bound::Included(&from))?;
        let k = found.key();
        (k.user_key == key).then(|| (k.seq, found.value().clone()))
    }

    pub fn approx_size(&self) -> usize {
        self.approx_size.load(Ordering::Relaxed)
    }

    /// Versions held (not distinct keys).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Every version in internal key order (what a flush consumes).
    pub fn iter(&self) -> impl Iterator<Item = MemEntry<'_>> {
        self.map.iter()
    }
}

fn entry_len(entry: &Entry) -> usize {
    match entry {
        Entry::Value(v) => v.len(),
        Entry::Tombstone => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::MAX_SEQ;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::thread;

    fn val(s: &str) -> Entry {
        Entry::Value(s.as_bytes().to_vec())
    }

    #[test]
    fn get_missing_is_none() {
        let m = MemTable::new();
        assert_eq!(m.get(b"nope", MAX_SEQ), None);
    }

    #[test]
    fn put_then_get() {
        let m = MemTable::new();
        m.put(b"a", 1, b"1");
        assert_eq!(m.get(b"a", MAX_SEQ), Some((1, val("1"))));
    }

    #[test]
    fn overwrite_keeps_both_versions_and_latest_wins() {
        let m = MemTable::new();
        m.put(b"a", 1, b"1");
        m.put(b"a", 2, b"2");
        assert_eq!(m.get(b"a", MAX_SEQ), Some((2, val("2"))));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn snapshot_sees_the_newest_version_at_or_below_it() {
        let m = MemTable::new();
        m.put(b"a", 3, b"three");
        m.put(b"a", 7, b"seven");
        m.delete(b"a", 9);
        m.put(b"b", 5, b"bee");
        assert_eq!(m.get(b"a", 2), None, "before the first version");
        assert_eq!(m.get(b"a", 3), Some((3, val("three"))));
        assert_eq!(m.get(b"a", 8), Some((7, val("seven"))));
        assert_eq!(m.get(b"a", 9), Some((9, Entry::Tombstone)));
        // A miss on "a" must not return the next key's version.
        assert_eq!(m.get(b"a\0", MAX_SEQ), None);
        assert_eq!(m.get(b"b", 4), None);
    }

    #[test]
    fn delete_leaves_tombstone() {
        let m = MemTable::new();
        m.put(b"a", 1, b"1");
        m.delete(b"a", 2);
        assert_eq!(m.get(b"a", MAX_SEQ), Some((2, Entry::Tombstone)));
    }

    #[test]
    fn delete_unknown_key_still_records_tombstone() {
        // The key may live in an older SSTable, so the tombstone must be kept.
        let m = MemTable::new();
        m.delete(b"ghost", 1);
        assert_eq!(m.get(b"ghost", MAX_SEQ), Some((1, Entry::Tombstone)));
    }

    #[test]
    fn iter_is_in_internal_key_order() {
        let m = MemTable::new();
        m.put(b"c", 1, b"x");
        m.put(b"a", 2, b"x");
        m.put(b"b", 3, b"x");
        m.put(b"a", 4, b"x");
        let keys: Vec<(Vec<u8>, SeqNo)> = m
            .iter()
            .map(|e| (e.key().user_key.clone(), e.key().seq))
            .collect();
        let want: [(&[u8], SeqNo); 4] = [(b"a", 4), (b"a", 2), (b"b", 3), (b"c", 1)];
        assert_eq!(keys, want.map(|(k, s)| (k.to_vec(), s)));
    }

    #[test]
    fn approx_size_grows() {
        let m = MemTable::new();
        assert_eq!(m.approx_size(), 0);
        m.put(b"key", 1, b"value");
        let after_put = m.approx_size();
        assert!(after_put >= 8);
        m.delete(b"key", 2);
        assert!(m.approx_size() > after_put);
    }

    /// One writer inserts versions 1..=N, version s going to key s % KEYS,
    /// and publishes each number after inserting it. Readers look keys up at
    /// the last published number, and must find exactly the newest version at
    /// or below it: never an older one (a lost insert) or a newer one.
    #[test]
    fn readers_see_exactly_their_snapshot_while_a_writer_inserts() {
        const N: u64 = 20_000;
        const KEYS: u64 = 4;
        let m = Arc::new(MemTable::new());
        let published = Arc::new(AtomicU64::new(0));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (m, published) = (Arc::clone(&m), Arc::clone(&published));
                thread::spawn(move || loop {
                    let snap = published.load(Ordering::Acquire);
                    for k in 0..KEYS {
                        // The largest s <= snap with s % KEYS == k (seq 0 is
                        // never assigned).
                        let want = if snap < k {
                            None
                        } else {
                            Some(snap - (snap - k) % KEYS).filter(|&s| s > 0)
                        };
                        let got = m.get(&k.to_le_bytes(), snap).map(|(seq, _)| seq);
                        assert_eq!(got, want, "key {k} at {snap}");
                    }
                    if snap == N {
                        return;
                    }
                })
            })
            .collect();
        for s in 1..=N {
            m.put(&(s % KEYS).to_le_bytes(), s, &s.to_le_bytes());
            published.store(s, Ordering::Release);
        }
        for r in readers {
            r.join().unwrap();
        }
    }
}
