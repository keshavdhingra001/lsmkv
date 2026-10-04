//! Range scans (DESIGN.md D19–D21).
//!
//! A scan merges every source a read might need, in internal key order
//! (user key ascending, then seq descending):
//!
//! - the memtable and the immutable memtable (`MemIter`),
//! - each level-0 table on its own, since level-0 tables overlap,
//! - one `LevelIter` per deeper level, which walks that level's tables in
//!   key order (they don't overlap), opening each only when it gets there.
//!
//! `MergeIter` does the k-way merge with a min-heap holding each source's
//! next entry. `DbIter` sits on top and turns versions into answers: for each
//! user key it takes the newest version the scan's snapshot can see, skips
//! the rest, and hides the key if that version is a tombstone.
//!
//! A scan holds `Arc`s of the memtables and tables it reads, not the state or
//! read-view lock. Those never change once built (a flush or compaction
//! installs new ones), and a table file compaction deletes stays readable
//! through the descriptor this scan holds open. So the scan sees one
//! consistent point in time without registering a snapshot (D21). The cost:
//! while it lives, it keeps those files' disk space and those memtables'
//! memory.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use super::{SuperVersion, Table};
use crate::error::Result;
use crate::key::{self, SeqNo};
use crate::memtable::{Entry, ScanEntry};
use crate::sstable::{SstIter, SstReader};

/// Anything that yields versions in internal key order.
pub(super) type Source = Box<dyn Iterator<Item = Result<ScanEntry>> + Send>;

/// A source's next entry, waiting in the heap.
struct Head {
    entry: ScanEntry,
    src: usize,
}

impl Ord for Head {
    /// Internal key order. Sequence numbers are unique, so two sources never
    /// hold the same version; the source index only makes the order total.
    fn cmp(&self, other: &Self) -> Ordering {
        let (a, b) = (&self.entry, &other.entry);
        key::compare(&a.0, a.1, &b.0, b.1).then(self.src.cmp(&other.src))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Head {}

/// A k-way merge of sorted sources into one sorted stream. Each `next` pops
/// the smallest head and refills it from the same source: O(log k) per
/// entry, with one entry per source in memory. After an error, it yields
/// nothing more.
pub(super) struct MergeIter {
    sources: Vec<Source>,
    /// `Reverse`, since `BinaryHeap` is a max-heap.
    heap: BinaryHeap<Reverse<Head>>,
    failed: bool,
}

impl MergeIter {
    pub(super) fn new(mut sources: Vec<Source>) -> Result<Self> {
        let mut heap = BinaryHeap::with_capacity(sources.len());
        for (src, source) in sources.iter_mut().enumerate() {
            if let Some(entry) = source.next().transpose()? {
                heap.push(Reverse(Head { entry, src }));
            }
        }
        Ok(Self {
            sources,
            heap,
            failed: false,
        })
    }
}

impl Iterator for MergeIter {
    type Item = Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let Reverse(Head { entry, src }) = self.heap.pop()?;
        match self.sources[src].next() {
            Some(Ok(next)) => self.heap.push(Reverse(Head { entry: next, src })),
            Some(Err(e)) => {
                self.failed = true;
                return Some(Err(e));
            }
            None => {}
        }
        Some(Ok(entry))
    }
}

/// One level >= 1: its tables that overlap the scan, in key order, read one
/// after another. Only the first can hold keys before the scan's start.
struct LevelIter {
    tables: VecDeque<Arc<SstReader>>,
    start: Option<Vec<u8>>,
    current: Option<SstIter>,
}

impl Iterator for LevelIter {
    type Item = Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.current.as_mut().and_then(Iterator::next) {
                return Some(item);
            }
            let table = self.tables.pop_front()?;
            match table.iter(self.start.take().as_deref()) {
                Ok(it) => self.current = Some(it),
                Err(e) => {
                    self.tables.clear();
                    return Some(Err(e));
                }
            }
        }
    }
}

/// The scan's key range, owned.
struct KeyRange {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl KeyRange {
    fn new<K: AsRef<[u8]> + ?Sized>(range: impl RangeBounds<K>) -> Self {
        Self {
            start: range.start_bound().map(|k| k.as_ref().to_vec()),
            end: range.end_bound().map(|k| k.as_ref().to_vec()),
        }
    }

    /// Where sources seek to: the start key, included or not (`DbIter`
    /// skips an excluded start key itself).
    fn seek(&self) -> Option<&[u8]> {
        match &self.start {
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
            Bound::Unbounded => None,
        }
    }

    fn before_start(&self, key: &[u8]) -> bool {
        match &self.start {
            Bound::Included(s) => key < s.as_slice(),
            Bound::Excluded(s) => key <= s.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn past_end(&self, key: &[u8]) -> bool {
        match &self.end {
            Bound::Included(e) => key > e.as_slice(),
            Bound::Excluded(e) => key >= e.as_slice(),
            Bound::Unbounded => false,
        }
    }

    /// Whether a table's keys can fall in the range.
    fn overlaps(&self, table: &Table) -> bool {
        let below = matches!(self.seek(), Some(s) if table.largest() < s);
        !below && !self.past_end(table.smallest())
    }
}

impl SuperVersion {
    /// One source per memtable and per level-0 table, and one per deeper
    /// level, all positioned at the range's start. Tables outside the range
    /// are left out, so a narrow scan reads few blocks.
    fn scan_sources(&self, range: &KeyRange) -> Result<Vec<Source>> {
        let seek = range.seek();
        let mut sources: Vec<Source> = Vec::new();
        for mem in std::iter::once(&self.mem).chain(&self.imm) {
            sources.push(Box::new(mem.iter_from(seek).map(Ok)));
        }
        for table in self.levels[0].iter().filter(|t| range.overlaps(t)) {
            sources.push(Box::new(table.reader.iter(seek)?));
        }
        for level in &self.levels[1..] {
            let tables: VecDeque<_> = level
                .iter()
                .filter(|t| range.overlaps(t))
                .map(|t| Arc::clone(&t.reader))
                .collect();
            if !tables.is_empty() {
                sources.push(Box::new(LevelIter {
                    tables,
                    start: seek.map(<[u8]>::to_vec),
                    current: None,
                }));
            }
        }
        Ok(sources)
    }
}

/// A forward range scan: `(key, value)` pairs in key order, as of one
/// snapshot (`Db::scan`, `Snapshot::scan`). Owns everything it reads, so it
/// borrows nothing and can be sent to another thread.
///
/// Every item is a `Result`: reading a block can fail (I/O, a bad checksum).
/// After an error the scan ends.
pub struct DbIter {
    merge: MergeIter,
    range: KeyRange,
    snapshot: SeqNo,
    /// The user key of the last version used (returned, or a tombstone that
    /// hid its key). Every later version of it is older, so it's skipped.
    last_key: Option<Vec<u8>>,
    done: bool,
}

impl DbIter {
    pub(super) fn new<K: AsRef<[u8]> + ?Sized>(
        current: &SuperVersion,
        snapshot: SeqNo,
        range: impl RangeBounds<K>,
    ) -> Result<Self> {
        let range = KeyRange::new(range);
        let merge = MergeIter::new(current.scan_sources(&range)?)?;
        Ok(Self {
            merge,
            range,
            snapshot,
            last_key: None,
            done: false,
        })
    }
}

impl Iterator for DbIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            let (key, seq, entry) = match self.merge.next() {
                Some(Ok(item)) => item,
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(e));
                }
                None => break,
            };
            if self.range.past_end(&key) {
                break;
            }
            // Written after the snapshot: invisible, but an older version
            // of the same key may still be visible.
            if seq > self.snapshot || self.range.before_start(&key) {
                continue;
            }
            if self.last_key.as_deref() == Some(key.as_slice()) {
                continue;
            }
            match entry {
                Entry::Value(v) => {
                    self.last_key = Some(key.clone());
                    return Some(Ok((key, v)));
                }
                Entry::Tombstone => self.last_key = Some(key),
            }
        }
        self.done = true;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn source(items: Vec<Result<ScanEntry>>) -> Source {
        Box::new(items.into_iter())
    }

    fn v(key: &str, seq: SeqNo) -> Result<ScanEntry> {
        Ok((key.as_bytes().to_vec(), seq, Entry::Value(vec![seq as u8])))
    }

    #[test]
    fn merge_yields_internal_key_order_across_sources() {
        let merge = MergeIter::new(vec![
            source(vec![v("a", 9), v("c", 2)]),
            source(vec![]),
            source(vec![v("a", 4), v("b", 7), v("c", 8)]),
            source(vec![v("a", 6), v("d", 1)]),
        ])
        .unwrap();
        let got: Vec<(String, SeqNo)> = merge
            .map(|e| {
                let (k, seq, _) = e.unwrap();
                (String::from_utf8(k).unwrap(), seq)
            })
            .collect();
        let want = [
            ("a", 9),
            ("a", 6),
            ("a", 4),
            ("b", 7),
            ("c", 8),
            ("c", 2),
            ("d", 1),
        ];
        let want: Vec<(String, SeqNo)> = want.iter().map(|&(k, s)| (k.into(), s)).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn merge_reports_a_failing_source_then_stops() {
        let bad = || Err(Error::Corruption("bad block".into()));
        let mut merge = MergeIter::new(vec![
            source(vec![v("a", 1), v("z", 1)]),
            source(vec![v("b", 1), bad(), v("c", 1)]),
        ])
        .unwrap();
        assert_eq!(merge.next().unwrap().unwrap().0, b"a");
        // Taking "b" refills its source, which fails.
        assert!(matches!(merge.next(), Some(Err(Error::Corruption(_)))));
        assert!(merge.next().is_none());
        // A source that fails on its first entry fails the constructor.
        assert!(MergeIter::new(vec![source(vec![bad()])]).is_err());
    }
}
