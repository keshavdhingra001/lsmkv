//! Data block: a run of sorted entries followed by a CRC32 of those entries.
//!
//! ```text
//! +---------+---------+-----+---------+-----------+
//! | entry 0 | entry 1 | ... | entry n | crc32 u32 |
//! +---------+---------+-----+---------+-----------+
//!
//! entry = [kind u8][key_len u32][val_len u32][key][value]
//! ```
//!
//! - `kind`: 1 = value, 2 = tombstone (`val_len = 0`).
//! - The CRC covers every entry byte, so one check validates the whole block
//!   before any entry in it is trusted.

use crate::codec::{len_u32, read_u32};
use crate::error::{Error, Result};
use crate::memtable::Entry;

pub const KIND_VALUE: u8 = 1;
pub const KIND_TOMBSTONE: u8 = 2;
const ENTRY_HEADER_LEN: usize = 1 + 4 + 4;
const CRC_LEN: usize = 4;

/// Accumulates entries for one block. Reusable: `finish` resets it.
#[derive(Debug, Default)]
pub struct BlockBuilder {
    buf: Vec<u8>,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an entry. The caller (`SstWriter`) guarantees keys arrive in
    /// strictly increasing order.
    pub fn add(&mut self, key: &[u8], entry: &Entry) -> Result<()> {
        let (kind, value): (u8, &[u8]) = match entry {
            Entry::Value(v) => (KIND_VALUE, v),
            Entry::Tombstone => (KIND_TOMBSTONE, &[]),
        };
        let key_len = len_u32(key, "key")?;
        let val_len = len_u32(value, "value")?;

        self.buf.push(kind);
        self.buf.extend_from_slice(&key_len.to_le_bytes());
        self.buf.extend_from_slice(&val_len.to_le_bytes());
        self.buf.extend_from_slice(key);
        self.buf.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Encoded size if the block were finished now (entries + CRC).
    pub fn size(&self) -> usize {
        self.buf.len() + CRC_LEN
    }

    /// Largest key in the block so far; it becomes the block's index key.
    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Returns the encoded block and resets the builder for the next block.
    pub fn finish(&mut self) -> Vec<u8> {
        let crc = crc32fast::hash(&self.buf);
        let mut out = std::mem::take(&mut self.buf);
        out.extend_from_slice(&crc.to_le_bytes());
        self.last_key.clear();
        out
    }
}

/// An entry borrowed from a block: `(key, Some(value))`, or `(key, None)` for a tombstone.
pub type RawEntry<'a> = (&'a [u8], Option<&'a [u8]>);

/// A block whose checksum has been verified.
#[derive(Debug, Clone, Copy)]
pub struct Block<'a> {
    data: &'a [u8],
}

impl<'a> Block<'a> {
    /// Verifies the trailing CRC. Nothing in the block is read before this passes.
    pub fn new(raw: &'a [u8]) -> Result<Self> {
        if raw.len() < CRC_LEN {
            return Err(Error::Corruption("block shorter than its checksum".into()));
        }
        let (data, crc) = raw.split_at(raw.len() - CRC_LEN);
        if crc32fast::hash(data) != read_u32(crc, 0) {
            return Err(Error::Corruption("block checksum mismatch".into()));
        }
        Ok(Self { data })
    }

    /// Wraps bytes that already passed `new` once (the block cache only holds
    /// verified blocks), skipping the CRC. Never use it on bytes from disk.
    pub(crate) fn from_verified(raw: &'a [u8]) -> Self {
        Self {
            data: &raw[..raw.len() - CRC_LEN],
        }
    }

    /// Entries in key order. Yields one `Err` and then stops if an entry is
    /// malformed (only possible with a writer bug, since the CRC passed).
    pub fn iter(&self) -> BlockIter<'a> {
        BlockIter {
            data: self.data,
            pos: 0,
        }
    }

    /// Looks up `key`. Stops early once it passes `key`, since entries are sorted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        for item in self.iter() {
            let (k, v) = item?;
            match k.cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => {
                    return Ok(Some(match v {
                        Some(v) => Entry::Value(v.to_vec()),
                        None => Entry::Tombstone,
                    }))
                }
                std::cmp::Ordering::Greater => break,
            }
        }
        Ok(None)
    }
}

pub struct BlockIter<'a> {
    data: &'a [u8],
    pos: usize,
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
    let key_len = read_u32(buf, 1) as usize;
    let val_len = read_u32(buf, 5) as usize;
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
    Ok(((key, value), total))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(s: &str) -> Entry {
        Entry::Value(s.as_bytes().to_vec())
    }

    fn build(entries: &[(&str, Entry)]) -> Vec<u8> {
        let mut b = BlockBuilder::new();
        for (k, e) in entries {
            b.add(k.as_bytes(), e).unwrap();
        }
        b.finish()
    }

    #[test]
    fn roundtrip_values_tombstones_and_empty_values() {
        let raw = build(&[("a", val("1")), ("b", Entry::Tombstone), ("c", val(""))]);
        let block = Block::new(&raw).unwrap();
        let got: Vec<RawEntry> = block.iter().map(|r| r.unwrap()).collect();
        assert_eq!(
            got,
            vec![
                (b"a".as_slice(), Some(b"1".as_slice())),
                (b"b".as_slice(), None),
                (b"c".as_slice(), Some(b"".as_slice())),
            ]
        );
    }

    #[test]
    fn get_finds_present_and_misses_absent() {
        let raw = build(&[("b", val("2")), ("d", Entry::Tombstone), ("f", val("6"))]);
        let block = Block::new(&raw).unwrap();
        assert_eq!(block.get(b"b").unwrap(), Some(val("2")));
        assert_eq!(block.get(b"d").unwrap(), Some(Entry::Tombstone));
        assert_eq!(block.get(b"f").unwrap(), Some(val("6")));
        for missing in ["a", "c", "e", "g"] {
            assert_eq!(block.get(missing.as_bytes()).unwrap(), None, "{missing}");
        }
    }

    #[test]
    fn size_matches_encoded_length() {
        let mut b = BlockBuilder::new();
        b.add(b"key", &val("value")).unwrap();
        let predicted = b.size();
        assert_eq!(b.finish().len(), predicted);
        assert_eq!(predicted, ENTRY_HEADER_LEN + 3 + 5 + CRC_LEN);
    }

    #[test]
    fn finish_resets_builder() {
        let mut b = BlockBuilder::new();
        b.add(b"a", &val("1")).unwrap();
        assert_eq!(b.last_key(), b"a");
        b.finish();
        assert!(b.is_empty());
        assert_eq!(b.last_key(), b"");
        assert_eq!(b.size(), CRC_LEN);
    }

    #[test]
    fn any_flipped_byte_is_detected() {
        let raw = build(&[("a", val("1")), ("b", val("2"))]);
        for i in 0..raw.len() {
            let mut bad = raw.clone();
            bad[i] ^= 0x01;
            assert!(
                matches!(Block::new(&bad), Err(Error::Corruption(_))),
                "flip at byte {i} not detected"
            );
        }
    }

    #[test]
    fn too_short_is_corruption() {
        assert!(matches!(Block::new(&[1, 2]), Err(Error::Corruption(_))));
    }
}
