//! Data block: a run of sorted entries, the offsets of every 16th entry
//! (restart points), then a CRC32 of everything before it.
//!
//! ```text
//! +---------+-----+---------+---------------+-----+---------------+---------+-----------+
//! | entry 0 | ... | entry n | restart 0 u32 | ... | restart m u32 | m+1 u32 | crc32 u32 |
//! +---------+-----+---------+---------------+-----+---------------+---------+-----------+
//!
//! entry = [kind u8][seq u64][key_len u32][val_len u32][key][value]
//! ```
//!
//! - `kind`: 1 = value, 2 = tombstone (`val_len = 0`).
//! - `seq`: the version's sequence number. Entries are in internal key order
//!   (key ascending, then seq descending; DESIGN.md D18), so one key can
//!   have several entries, newest first.
//! - Restart points (format 5, DESIGN.md D30): the byte offset of entry 0,
//!   16, 32, ... A lookup binary-searches them, then scans at most 16
//!   entries, instead of scanning the whole block. Tables written before
//!   format 5 have blocks without the restart trailer; `Block` reads both.
//! - The CRC covers every byte before it, so one check validates the whole
//!   block (restarts included) before anything in it is trusted.

use crate::codec::read_u64;
use crate::codec::{len_u32, read_u32};
use crate::error::{Error, Result};
use crate::key::{self, SeqNo};
use crate::memtable::Entry;

pub const KIND_VALUE: u8 = 1;
pub const KIND_TOMBSTONE: u8 = 2;
pub(crate) const ENTRY_HEADER_LEN: usize = 1 + 8 + 4 + 4;
const CRC_LEN: usize = 4;
/// A restart point every this many entries.
pub const RESTART_INTERVAL: usize = 16;

/// Accumulates entries for one block. Reusable: `finish` resets it.
#[derive(Debug, Default)]
pub struct BlockBuilder {
    buf: Vec<u8>,
    /// Offsets of entries 0, 16, 32, ...
    restarts: Vec<u32>,
    count: usize,
    last_key: Vec<u8>,
    last_seq: SeqNo,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an entry. The caller (`SstWriter`) guarantees (key, seq) pairs
    /// arrive in strictly increasing internal key order.
    pub fn add(&mut self, key: &[u8], seq: SeqNo, entry: &Entry) -> Result<()> {
        let (kind, value): (u8, &[u8]) = match entry {
            Entry::Value(v) => (KIND_VALUE, v),
            Entry::Tombstone => (KIND_TOMBSTONE, &[]),
        };
        let key_len = len_u32(key, "key")?;
        let val_len = len_u32(value, "value")?;
        if self.count.is_multiple_of(RESTART_INTERVAL) {
            self.restarts.push(len_u32(&self.buf, "block")?);
        }

        self.buf.push(kind);
        self.buf.extend_from_slice(&seq.to_le_bytes());
        self.buf.extend_from_slice(&key_len.to_le_bytes());
        self.buf.extend_from_slice(&val_len.to_le_bytes());
        self.buf.extend_from_slice(key);
        self.buf.extend_from_slice(value);
        self.count += 1;

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.last_seq = seq;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Encoded size if the block were finished now (entries, restart
    /// trailer and CRC).
    pub fn size(&self) -> usize {
        self.buf.len() + 4 * self.restarts.len() + 4 + CRC_LEN
    }

    /// Largest (key, seq) in the block so far; it becomes the block's index key.
    pub fn last_key(&self) -> (&[u8], SeqNo) {
        (&self.last_key, self.last_seq)
    }

    /// Returns the encoded block and resets the builder for the next block.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        for r in &self.restarts {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        self.restarts.clear();
        self.count = 0;
        self.last_key.clear();
        out
    }
}

/// An entry borrowed from a block: `(key, seq, Some(value))`, or
/// `(key, seq, None)` for a tombstone.
pub type RawEntry<'a> = (&'a [u8], SeqNo, Option<&'a [u8]>);

/// A block whose checksum has been verified.
#[derive(Debug, Clone, Copy)]
pub struct Block<'a> {
    /// The entries alone.
    data: &'a [u8],
    /// Restart offsets, 4 bytes each; empty for a block from before format 5.
    restarts: &'a [u8],
}

impl<'a> Block<'a> {
    /// Verifies the trailing CRC, then the restart trailer if the table's
    /// format has one. Nothing in the block is read before the CRC passes.
    pub fn new(raw: &'a [u8], has_restarts: bool) -> Result<Self> {
        if raw.len() < CRC_LEN {
            return Err(Error::Corruption("block shorter than its checksum".into()));
        }
        let (body, crc) = raw.split_at(raw.len() - CRC_LEN);
        if crc32fast::hash(body) != read_u32(crc, 0) {
            return Err(Error::Corruption("block checksum mismatch".into()));
        }
        let block = split(body, has_restarts)?;
        if has_restarts {
            block.check_restarts()?;
        }
        Ok(block)
    }

    /// Wraps bytes that already passed `new` once (the block cache only holds
    /// verified blocks), skipping the CRC. Never use it on bytes from disk.
    pub(crate) fn from_verified(raw: &'a [u8], has_restarts: bool) -> Self {
        split(&raw[..raw.len() - CRC_LEN], has_restarts).expect("verified when it was read")
    }

    /// Entries in key order. Yields one `Err` and then stops if an entry is
    /// malformed (only possible with a writer bug, since the CRC passed).
    pub fn iter(&self) -> BlockIter<'a> {
        self.iter_at(0)
    }

    /// Entries from byte offset `pos` on, which must be an entry boundary
    /// (0, or a `BlockIter::position` from this block).
    pub(crate) fn iter_at(&self, pos: usize) -> BlockIter<'a> {
        BlockIter {
            data: self.data,
            pos,
        }
    }

    /// Byte offset of the first entry at or after (key, seq) in internal key
    /// order, or the end of the entries if there is none. Binary-searches
    /// the restart points for the last one before (key, seq), then scans on
    /// from there: at most `RESTART_INTERVAL` entries.
    pub(crate) fn seek(&self, key: &[u8], seq: SeqNo) -> Result<usize> {
        let before = |pos: usize| -> Result<bool> {
            let ((k, s, _), _) = parse_entry(&self.data[pos..])?;
            Ok(key::compare(k, s, key, seq).is_lt())
        };
        // Restarts [0, lo) start before the target; [hi, n) don't.
        let (mut lo, mut hi) = (0, self.restarts.len() / 4);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if before(self.restart(mid))? {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut it = self.iter_at(if lo == 0 { 0 } else { self.restart(lo - 1) });
        loop {
            let at = it.position();
            match it.next() {
                Some(Ok((k, s, _))) if key::compare(k, s, key, seq).is_lt() => {}
                Some(Err(e)) => return Err(e),
                _ => return Ok(at),
            }
        }
    }

    fn restart(&self, i: usize) -> usize {
        read_u32(self.restarts, 4 * i) as usize
    }

    /// Checks that the restarts are exactly the offsets of entries 0, 16,
    /// 32, ...: walks the entries once, when the block is read from disk.
    fn check_restarts(&self) -> Result<()> {
        let n = self.restarts.len() / 4;
        let mut it = self.iter();
        let mut i: usize = 0;
        loop {
            let at = it.position();
            if i.is_multiple_of(RESTART_INTERVAL) && at < self.data.len() {
                let k = i / RESTART_INTERVAL;
                if k >= n || self.restart(k) != at {
                    return Err(Error::Corruption(format!(
                        "block restarts: entry {i} at offset {at} is not restart {k}"
                    )));
                }
            }
            match it.next() {
                Some(item) => item?,
                None => break,
            };
            i += 1;
        }
        if n != i.div_ceil(RESTART_INTERVAL) {
            return Err(Error::Corruption(format!(
                "block restarts: {n} for {i} entries"
            )));
        }
        Ok(())
    }

    /// The newest version of `key` at or below `snapshot`. The first entry
    /// at or after (key, snapshot) in internal key order is that version if
    /// it belongs to `key`; if it belongs to a later key, there is none here.
    pub fn get(&self, key: &[u8], snapshot: SeqNo) -> Result<Option<Entry>> {
        Ok(self.get_versioned(key, snapshot)?.map(|(_, e)| e))
    }

    /// `get`, plus the found version's sequence number.
    pub fn get_versioned(&self, key: &[u8], snapshot: SeqNo) -> Result<Option<(SeqNo, Entry)>> {
        let pos = self.seek(key, snapshot)?;
        match self.iter_at(pos).next().transpose()? {
            Some((k, seq, v)) if k == key => {
                let entry = match v {
                    Some(v) => Entry::Value(v.to_vec()),
                    None => Entry::Tombstone,
                };
                Ok(Some((seq, entry)))
            }
            _ => Ok(None),
        }
    }
}

/// Splits a CRC-checked block body into its entries and restart offsets.
/// A count too big for the block is corruption (a writer bug, since the CRC
/// passed); `Block::check_restarts` checks the offsets themselves.
fn split(body: &[u8], has_restarts: bool) -> Result<Block<'_>> {
    if !has_restarts {
        return Ok(Block {
            data: body,
            restarts: &[],
        });
    }
    let bad = |what: &str| Error::Corruption(format!("block restarts: {what}"));
    if body.len() < 4 {
        return Err(bad("no count"));
    }
    let n = read_u32(body, body.len() - 4) as usize;
    let trailer = n
        .checked_mul(4)
        .and_then(|t| t.checked_add(4))
        .filter(|&t| t <= body.len())
        .ok_or_else(|| bad("count larger than the block"))?;
    let data = &body[..body.len() - trailer];
    let restarts = &body[data.len()..body.len() - 4];
    Ok(Block { data, restarts })
}

pub struct BlockIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl BlockIter<'_> {
    /// Byte offset of the next entry: lets a caller that can't hold the
    /// borrow (a table iterator) resume with `Block::iter_at`.
    pub(crate) fn position(&self) -> usize {
        self.pos
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = Result<RawEntry<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        let data: &'a [u8] = self.data;
        match parse_entry(&data[self.pos..]) {
            Ok((entry, len)) => {
                self.pos += len;
                Some(Ok(entry))
            }
            Err(e) => {
                self.pos = self.data.len();
                Some(Err(e))
            }
        }
    }
}

fn parse_entry(buf: &[u8]) -> Result<(RawEntry<'_>, usize)> {
    let bad = |what: &str| Error::Corruption(format!("block entry: {what}"));
    if buf.len() < ENTRY_HEADER_LEN {
        return Err(bad("truncated header"));
    }
    let kind = buf[0];
    let seq = read_u64(buf, 1);
    let key_len = read_u32(buf, 9) as usize;
    let val_len = read_u32(buf, 13) as usize;
    let total = ENTRY_HEADER_LEN
        .checked_add(key_len)
        .and_then(|n| n.checked_add(val_len))
        .ok_or_else(|| bad("length overflow"))?;
    if buf.len() < total {
        return Err(bad("truncated body"));
    }

    let key = &buf[ENTRY_HEADER_LEN..ENTRY_HEADER_LEN + key_len];
    let value = &buf[ENTRY_HEADER_LEN + key_len..total];
    let value = match kind {
        KIND_VALUE => Some(value),
        KIND_TOMBSTONE if val_len == 0 => None,
        _ => return Err(bad("unknown kind")),
    };
    Ok(((key, seq, value), total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::MAX_SEQ;
    use crate::test_util::val;

    fn build(entries: &[(&str, SeqNo, Entry)]) -> Vec<u8> {
        let mut b = BlockBuilder::new();
        for (k, seq, e) in entries {
            b.add(k.as_bytes(), *seq, e).unwrap();
        }
        b.finish()
    }

    #[test]
    fn roundtrip_values_tombstones_and_empty_values() {
        let raw = build(&[
            ("a", 1, val("1")),
            ("b", 2, Entry::Tombstone),
            ("c", 3, val("")),
        ]);
        let block = Block::new(&raw, true).unwrap();
        let got: Vec<RawEntry> = block.iter().map(|r| r.unwrap()).collect();
        assert_eq!(
            got,
            vec![
                (b"a".as_slice(), 1, Some(b"1".as_slice())),
                (b"b".as_slice(), 2, None),
                (b"c".as_slice(), 3, Some(b"".as_slice())),
            ]
        );
    }

    #[test]
    fn get_finds_present_and_misses_absent() {
        let raw = build(&[
            ("b", 1, val("2")),
            ("d", 2, Entry::Tombstone),
            ("f", 3, val("6")),
        ]);
        let block = Block::new(&raw, true).unwrap();
        assert_eq!(block.get(b"b", MAX_SEQ).unwrap(), Some(val("2")));
        assert_eq!(block.get(b"d", MAX_SEQ).unwrap(), Some(Entry::Tombstone));
        assert_eq!(block.get(b"f", MAX_SEQ).unwrap(), Some(val("6")));
        for missing in ["a", "c", "e", "g"] {
            let got = block.get(missing.as_bytes(), MAX_SEQ).unwrap();
            assert_eq!(got, None, "{missing}");
        }
    }

    #[test]
    fn get_picks_the_newest_version_the_snapshot_may_see() {
        let raw = build(&[
            ("a", 9, val("a9")),
            ("a", 5, Entry::Tombstone),
            ("a", 2, val("a2")),
            ("b", 4, val("b4")),
        ]);
        let block = Block::new(&raw, true).unwrap();
        assert_eq!(block.get(b"a", MAX_SEQ).unwrap(), Some(val("a9")));
        assert_eq!(block.get(b"a", 9).unwrap(), Some(val("a9")));
        assert_eq!(block.get(b"a", 8).unwrap(), Some(Entry::Tombstone));
        assert_eq!(block.get(b"a", 4).unwrap(), Some(val("a2")));
        assert_eq!(
            block.get(b"a", 1).unwrap(),
            None,
            "older than every version"
        );
        // Every version of "b" is newer than the snapshot: not b's next key.
        assert_eq!(block.get(b"b", 3).unwrap(), None);
    }

    #[test]
    fn size_matches_encoded_length() {
        let mut b = BlockBuilder::new();
        b.add(b"key", 1, &val("value")).unwrap();
        let predicted = b.size();
        assert_eq!(b.finish().len(), predicted);
        // One entry, one restart point, the restart count, the CRC.
        assert_eq!(predicted, ENTRY_HEADER_LEN + 3 + 5 + 4 + 4 + CRC_LEN);
    }

    #[test]
    fn finish_resets_builder() {
        let mut b = BlockBuilder::new();
        b.add(b"a", 3, &val("1")).unwrap();
        assert_eq!(b.last_key(), (&b"a"[..], 3));
        b.finish();
        assert!(b.is_empty());
        assert_eq!(b.last_key().0, b"");
        assert_eq!(b.size(), 4 + CRC_LEN);
    }

    #[test]
    fn any_flipped_byte_is_detected() {
        let raw = build(&[("a", 1, val("1")), ("b", 2, val("2"))]);
        for i in 0..raw.len() {
            let mut bad = raw.clone();
            bad[i] ^= 0x01;
            assert!(
                matches!(Block::new(&bad, true), Err(Error::Corruption(_))),
                "flip at byte {i} not detected"
            );
        }
    }

    #[test]
    fn too_short_is_corruption() {
        assert!(matches!(
            Block::new(&[1, 2], true),
            Err(Error::Corruption(_))
        ));
    }

    /// Versions of many keys, several per key, over many restart points.
    fn many() -> (Vec<u8>, Vec<crate::memtable::ScanEntry>) {
        let mut entries = Vec::new();
        for k in 0..100u32 {
            for v in (1..=k % 4 + 1).rev() {
                let e = if v % 3 == 0 {
                    Entry::Tombstone
                } else {
                    Entry::Value(format!("{k}.{v}").into_bytes())
                };
                entries.push((format!("k{k:03}").into_bytes(), (k * 10 + v) as SeqNo, e));
            }
        }
        let mut b = BlockBuilder::new();
        for (k, seq, e) in &entries {
            b.add(k, *seq, e).unwrap();
        }
        (b.finish(), entries)
    }

    /// What `seek` must return, found the slow way: the first entry at or
    /// after the target in internal key order.
    fn linear_seek(block: &Block, key: &[u8], seq: SeqNo) -> usize {
        let mut it = block.iter();
        loop {
            let at = it.position();
            match it.next() {
                Some(Ok((k, s, _))) if key::compare(k, s, key, seq).is_lt() => {}
                _ => return at,
            }
        }
    }

    #[test]
    fn seek_through_restart_points_matches_a_linear_scan() {
        let (raw, entries) = many();
        let block = Block::new(&raw, true).unwrap();
        assert!(block.restarts.len() / 4 > 10, "too few restart points");
        let mut targets: Vec<(Vec<u8>, SeqNo)> = vec![(b"".to_vec(), MAX_SEQ), (b"z".to_vec(), 0)];
        for (k, seq, _) in &entries {
            for s in [*seq + 1, *seq, *seq - 1, MAX_SEQ, 0] {
                targets.push((k.clone(), s));
            }
            let mut before = k.clone();
            before.pop();
            targets.push((before, MAX_SEQ)); // a prefix, between keys
        }
        for (k, s) in targets {
            assert_eq!(
                block.seek(&k, s).unwrap(),
                linear_seek(&block, &k, s),
                "{k:?} {s}"
            );
        }
    }

    #[test]
    fn a_block_without_restarts_still_reads() {
        // Format 4 and earlier: entries, then the CRC, no trailer.
        let (raw, entries) = many();
        let block = Block::new(&raw, true).unwrap();
        let mut old = block.data.to_vec();
        old.extend_from_slice(&crc32fast::hash(&old).to_le_bytes());
        let old_block = Block::new(&old, false).unwrap();
        let got: Vec<RawEntry> = old_block.iter().map(|r| r.unwrap()).collect();
        assert_eq!(got.len(), entries.len());
        for (k, seq, e) in &entries {
            assert_eq!(
                old_block.get_versioned(k, *seq).unwrap(),
                Some((*seq, e.clone()))
            );
            assert_eq!(
                old_block.seek(k, *seq).unwrap(),
                block.seek(k, *seq).unwrap()
            );
        }
    }

    /// A trailer that passes the CRC but is wrong (a writer bug) is reported,
    /// never trusted: a restart in the middle of an entry would be parsed
    /// as an entry.
    #[test]
    fn a_wrong_trailer_behind_a_good_crc_is_corruption() {
        let (raw, _) = many();
        let body = &raw[..raw.len() - CRC_LEN];
        let n = read_u32(body, body.len() - 4) as usize;
        let restart_at = body.len() - 4 - 4 * n;
        let reseal = |mut body: Vec<u8>| {
            let crc = crc32fast::hash(&body);
            body.extend_from_slice(&crc.to_le_bytes());
            body
        };
        let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
        let mut b = body.to_vec();
        b[restart_at + 4] += 1; // restart 1 off by a byte
        cases.push(("mid-entry restart", b));
        let mut b = body.to_vec();
        b[restart_at] = 1; // restart 0 isn't entry 0
        cases.push(("first restart not 0", b));
        let mut b = body.to_vec();
        let len = b.len();
        b[len - 4..].copy_from_slice(&((n - 1) as u32).to_le_bytes());
        cases.push(("count one short", b));
        let mut b = body.to_vec();
        b[len - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
        cases.push(("count beyond the block", b));
        for (what, b) in cases {
            let raw = reseal(b);
            assert!(
                matches!(Block::new(&raw, true), Err(Error::Corruption(_))),
                "{what} accepted"
            );
        }
    }
}
