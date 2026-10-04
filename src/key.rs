//! Sequence numbers and the internal key order (DESIGN.md D18).
//!
//! Every write gets the next sequence number, and every stored version of a
//! key carries the number of the write that made it. A reader picks a
//! snapshot number and sees, for each key, the newest version at or below it.
//!
//! Versions sort by user key ascending, then by sequence number DESCENDING,
//! so a key's newest version comes first and a lookup can stop at the first
//! version it's allowed to see.
//!
//! The pair is compared field by field, never as one byte string: appending
//! the sequence bytes to the key and comparing bytewise would sort `"a"` +
//! seq after `"ab"` + seq whenever the seq's first byte is above `b'b'`
//! (`"a"` must come first: it's a prefix of `"ab"`).

use std::cmp::Ordering;

/// A write's position in the database's history. 0 is never assigned, so it
/// means "before any write".
pub type SeqNo = u64;

/// Reading at this snapshot sees every version stored.
pub const MAX_SEQ: SeqNo = u64::MAX;

/// One version of a user key, as the memtable orders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: SeqNo,
}

impl InternalKey {
    pub fn new(user_key: &[u8], seq: SeqNo) -> Self {
        Self {
            user_key: user_key.to_vec(),
            seq,
        }
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.user_key, self.seq, &other.user_key, other.seq)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The internal key order: user key ascending, then seq descending.
pub fn compare(a_key: &[u8], a_seq: SeqNo, b_key: &[u8], b_seq: SeqNo) -> Ordering {
    a_key.cmp(b_key).then(b_seq.cmp(&a_seq))
}

/// Decides which versions flush and compaction may throw away: a version
/// can go once a newer version of the same key is visible to every reader
/// (its seq is at or below `oldest_snapshot`), since then no reader can ever
/// pick the older one. Feed it versions in internal key order.
///
/// This is LevelDB's rule. It keeps a few versions a precise per-snapshot
/// rule would drop (any version between two snapshots that's shadowed for
/// both of them), in exchange for tracking one number instead of a list.
pub struct Shadowed {
    oldest_snapshot: SeqNo,
    last_key: Option<Vec<u8>>,
    /// Seq of the previous (newer) version of `last_key`.
    newer_seq: SeqNo,
}

impl Shadowed {
    pub fn new(oldest_snapshot: SeqNo) -> Self {
        Self {
            oldest_snapshot,
            last_key: None,
            newer_seq: MAX_SEQ,
        }
    }

    /// True if no reader can see this version.
    pub fn check(&mut self, key: &[u8], seq: SeqNo) -> bool {
        if self.last_key.as_deref() != Some(key) {
            self.last_key = Some(key.to_vec());
            self.newer_seq = MAX_SEQ;
        }
        let hidden = self.newer_seq <= self.oldest_snapshot;
        self.newer_seq = seq;
        hidden
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newest_version_sorts_first_and_keys_compare_by_field() {
        let mut keys = [
            InternalKey::new(b"ab", 5),
            InternalKey::new(b"a", 1),
            InternalKey::new(b"a", 9),
            InternalKey::new(b"b", 2),
        ];
        keys.sort();
        let got: Vec<(&[u8], SeqNo)> = keys.iter().map(|k| (&k.user_key[..], k.seq)).collect();
        assert_eq!(got, vec![(&b"a"[..], 9), (b"a", 1), (b"ab", 5), (b"b", 2)]);
    }

    #[test]
    fn shadowed_keeps_what_some_reader_can_see() {
        // Versions of "k" at 9, 6, 3 (newest first), then "m" at 2.
        // Oldest snapshot 5: a reader at 5 sees k@3, so k@3 stays; nothing
        // below 3 exists. Versions 9 and 6 stay (readers above 5 see them).
        let mut s = Shadowed::new(5);
        assert!(!s.check(b"k", 9));
        assert!(!s.check(b"k", 6));
        assert!(!s.check(b"k", 3));
        assert!(!s.check(b"m", 2), "a new key starts fresh");

        // Oldest snapshot 7: everyone sees k@9 or k@6, so k@3 is hidden.
        let mut s = Shadowed::new(7);
        assert!(!s.check(b"k", 9));
        assert!(!s.check(b"k", 6));
        assert!(s.check(b"k", 3));

        // No snapshots (oldest = latest): only the newest version survives.
        let mut s = Shadowed::new(9);
        assert!(!s.check(b"k", 9));
        assert!(s.check(b"k", 6));
        assert!(s.check(b"k", 3));
    }
}
