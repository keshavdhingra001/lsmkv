//! In-memory write buffer. Sorted, so it can be flushed straight into an SSTable.
//!
//! It holds every version written to it, keyed by (user key, seq) in the
//! internal key order (DESIGN.md D18): a key's newest version comes first.
//! Readers ask for a key "as of" a snapshot and get the newest version at or
//! below it.

use std::collections::BTreeMap;
use std::ops::Bound;

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

#[derive(Debug, Default)]
pub struct MemTable {
    map: BTreeMap<InternalKey, Entry>,
    /// Rough bytes held, used to decide when to flush. Every version counts,
    /// since every version stays in the map until the flush.
    approx_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds version `seq` of `key`. Sequence numbers are unique, so this
    /// never replaces an existing version.
    pub fn put(&mut self, key: &[u8], seq: SeqNo, value: &[u8]) {
        self.insert(key, seq, Entry::Value(value.to_vec()));
    }

    /// Adds a tombstone at `seq`. It stays in the map so the delete still
    /// shadows older copies of the key that live in SSTables.
    pub fn delete(&mut self, key: &[u8], seq: SeqNo) {
        self.insert(key, seq, Entry::Tombstone);
    }

    fn insert(&mut self, key: &[u8], seq: SeqNo, entry: Entry) {
        self.approx_size += key.len() + 8 + entry_len(&entry);
        let old = self.map.insert(InternalKey::new(key, seq), entry);
        debug_assert!(old.is_none(), "sequence number {seq} reused");
    }

    /// The newest version of `key` at or below `snapshot`, with its seq.
    /// `None` = no such version here; `Some((_, Tombstone))` = deleted.
    pub fn get(&self, key: &[u8], snapshot: SeqNo) -> Option<(SeqNo, &Entry)> {
        // (key, snapshot) sorts just before every version of `key` that the
        // snapshot may see, so the first entry from there on is the answer
        // if it belongs to `key` at all.
        let from = InternalKey::new(key, snapshot);
        let (k, entry) = self
            .map
            .range((Bound::Included(from), Bound::Unbounded))
            .next()?;
        (k.user_key == key).then_some((k.seq, entry))
    }

    pub fn approx_size(&self) -> usize {
        self.approx_size
    }

    /// Versions held (not distinct keys).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Every version in internal key order (what a flush consumes).
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], SeqNo, &Entry)> {
        self.map
            .iter()
            .map(|(k, v)| (k.user_key.as_slice(), k.seq, v))
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
        let mut m = MemTable::new();
        m.put(b"a", 1, b"1");
        assert_eq!(m.get(b"a", MAX_SEQ), Some((1, &val("1"))));
    }

    #[test]
    fn overwrite_keeps_both_versions_and_latest_wins() {
        let mut m = MemTable::new();
        m.put(b"a", 1, b"1");
        m.put(b"a", 2, b"2");
        assert_eq!(m.get(b"a", MAX_SEQ), Some((2, &val("2"))));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn snapshot_sees_the_newest_version_at_or_below_it() {
        let mut m = MemTable::new();
        m.put(b"a", 3, b"three");
        m.put(b"a", 7, b"seven");
        m.delete(b"a", 9);
        m.put(b"b", 5, b"bee");
        assert_eq!(m.get(b"a", 2), None, "before the first version");
        assert_eq!(m.get(b"a", 3), Some((3, &val("three"))));
        assert_eq!(m.get(b"a", 8), Some((7, &val("seven"))));
        assert_eq!(m.get(b"a", 9), Some((9, &Entry::Tombstone)));
        // A miss on "a" must not return the next key's version.
        assert_eq!(m.get(b"a\0", MAX_SEQ), None);
        assert_eq!(m.get(b"b", 4), None);
    }

    #[test]
    fn delete_leaves_tombstone() {
        let mut m = MemTable::new();
        m.put(b"a", 1, b"1");
        m.delete(b"a", 2);
        assert_eq!(m.get(b"a", MAX_SEQ), Some((2, &Entry::Tombstone)));
    }

    #[test]
    fn delete_unknown_key_still_records_tombstone() {
        // The key may live in an older SSTable, so the tombstone must be kept.
        let mut m = MemTable::new();
        m.delete(b"ghost", 1);
        assert_eq!(m.get(b"ghost", MAX_SEQ), Some((1, &Entry::Tombstone)));
    }

    #[test]
    fn iter_is_in_internal_key_order() {
        let mut m = MemTable::new();
        m.put(b"c", 1, b"x");
        m.put(b"a", 2, b"x");
        m.put(b"b", 3, b"x");
        m.put(b"a", 4, b"x");
        let keys: Vec<(&[u8], SeqNo)> = m.iter().map(|(k, s, _)| (k, s)).collect();
        assert_eq!(keys, vec![(&b"a"[..], 4), (b"a", 2), (b"b", 3), (b"c", 1)]);
    }

    #[test]
    fn approx_size_grows() {
        let mut m = MemTable::new();
        assert_eq!(m.approx_size(), 0);
        m.put(b"key", 1, b"value");
        let after_put = m.approx_size();
        assert!(after_put >= 8);
        m.delete(b"key", 2);
        assert!(m.approx_size() > after_put);
    }
}
