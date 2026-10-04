//! In-memory write buffer. Sorted, so it can be flushed straight into an SSTable.
//!
//! M1 uses a `BTreeMap`. Swapping in a skiplist (for lock-free concurrent reads)
//! is a later milestone; keep the public API stable so `Db` doesn't care.

use std::collections::BTreeMap;

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
    map: BTreeMap<Vec<u8>, Entry>,
    /// Rough bytes written so far; used later to decide when to flush.
    /// Only ever grows (overwrites are not subtracted). That's fine for a flush
    /// trigger, and it's a tradeoff you should be able to explain.
    approx_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or overwrites `key`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.approx_size += key.len() + value.len();
        self.map.insert(key.to_vec(), Entry::Value(value.to_vec()));
    }

    /// Marks `key` as deleted. The key stays in the map as a tombstone so the
    /// delete still shadows older copies of the key that live in SSTables.
    pub fn delete(&mut self, key: &[u8]) {
        self.approx_size += key.len();
        self.map.insert(key.to_vec(), Entry::Tombstone);
    }

    /// `None` = memtable has never seen the key;
    /// `Some(Entry::Tombstone)` = key was deleted.
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(key)
    }

    pub fn approx_size(&self) -> usize {
        self.approx_size
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Entries in ascending key order (this is what a flush will consume).
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &Entry)> {
        self.map.iter().map(|(k, v)| (k.as_slice(), v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_missing_is_none() {
        let m = MemTable::new();
        assert_eq!(m.get(b"nope"), None);
    }

    #[test]
    fn put_then_get() {
        let mut m = MemTable::new();
        m.put(b"a", b"1");
        assert_eq!(m.get(b"a"), Some(&Entry::Value(b"1".to_vec())));
    }

    #[test]
    fn overwrite_keeps_latest() {
        let mut m = MemTable::new();
        m.put(b"a", b"1");
        m.put(b"a", b"2");
        assert_eq!(m.get(b"a"), Some(&Entry::Value(b"2".to_vec())));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn delete_leaves_tombstone() {
        let mut m = MemTable::new();
        m.put(b"a", b"1");
        m.delete(b"a");
        assert_eq!(m.get(b"a"), Some(&Entry::Tombstone));
    }

    #[test]
    fn delete_unknown_key_still_records_tombstone() {
        // The key may live in an older SSTable, so the tombstone must be kept.
        let mut m = MemTable::new();
        m.delete(b"ghost");
        assert_eq!(m.get(b"ghost"), Some(&Entry::Tombstone));
    }

    #[test]
    fn iter_is_sorted() {
        let mut m = MemTable::new();
        for k in ["c", "a", "b"] {
            m.put(k.as_bytes(), b"x");
        }
        let keys: Vec<&[u8]> = m.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn approx_size_grows() {
        let mut m = MemTable::new();
        assert_eq!(m.approx_size(), 0);
        m.put(b"key", b"value");
        let after_put = m.approx_size();
        assert!(after_put >= 8);
        m.delete(b"key");
        assert!(m.approx_size() > after_put);
    }
}
