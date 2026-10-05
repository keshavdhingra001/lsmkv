//! Reads an SSTable: footer -> filter and index (both kept in memory) -> at
//! most one block per lookup.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::block::Block;
use super::filter::BloomFilter;
use super::{Footer, ReadContext, ReadStats, FOOTER_LEN};
use crate::codec::{read_u32, read_u64};
use crate::error::{Error, Result};
use crate::key::{self, SeqNo};
use crate::memtable::{Entry, ScanEntry};
use crate::vfs::{Fs, ReadableFile, RealFs};

#[derive(Debug)]
struct IndexEntry {
    last_key: Vec<u8>,
    last_seq: SeqNo,
    offset: u64,
    size: u32,
}

#[derive(Debug)]
pub struct SstReader {
    file: Box<dyn ReadableFile>,
    path: PathBuf,
    index: Vec<IndexEntry>,
    /// `None` for tables written without a filter: every lookup is a "maybe".
    filter: Option<BloomFilter>,
    footer: Footer,
    /// First key in the table (read from block 0 on open); `None` if empty.
    smallest: Option<Vec<u8>>,
    file_size: u64,
    /// The table's file number: its half of every block cache key.
    id: u64,
    ctx: Arc<ReadContext>,
}

impl SstReader {
    /// Opens a table with its own counters and no block cache (tests and tools).
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, 0, Arc::default())
    }

    /// Opens and validates the footer, filter and index. Data blocks are read
    /// lazily and checked by their own CRC when read from disk. `ctx` (cache
    /// and counters) is shared with the database's other tables; `id` must be
    /// unique among them, since it keys this table's blocks in the cache.
    pub fn open_with(path: &Path, id: u64, ctx: Arc<ReadContext>) -> Result<Self> {
        Self::open_in(&RealFs, path, id, ctx)
    }

    /// `open_with`, reading through `fs`.
    pub fn open_in(fs: &dyn Fs, path: &Path, id: u64, ctx: Arc<ReadContext>) -> Result<Self> {
        let corrupt = |what: String| Error::Corruption(format!("{}: {what}", path.display()));

        let file = fs.open_read(path)?;
        let file_len = file.size()?;
        if file_len < FOOTER_LEN as u64 {
            return Err(corrupt(format!(
                "file is {file_len} bytes, shorter than the footer"
            )));
        }

        let mut fbuf = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut fbuf, file_len - FOOTER_LEN as u64)?;
        let footer = Footer::decode(&fbuf).map_err(|e| corrupt(e.to_string()))?;

        // Regions must tile the file exactly: [blocks][filter][index][footer].
        let body_len = file_len - FOOTER_LEN as u64;
        let tiles = footer.filter_offset.checked_add(footer.filter_len)
            == Some(footer.index_offset)
            && footer.index_offset.checked_add(footer.index_len) == Some(body_len);
        if !tiles {
            return Err(corrupt(format!(
                "footer regions don't match file size {file_len}"
            )));
        }

        let mut ibuf = vec![0u8; footer.index_len as usize];
        file.read_exact_at(&mut ibuf, footer.index_offset)?;
        let index = decode_index(&ibuf, footer.filter_offset).map_err(corrupt)?;

        let filter = if footer.filter_len == 0 {
            None
        } else {
            let mut buf = vec![0u8; footer.filter_len as usize];
            file.read_exact_at(&mut buf, footer.filter_offset)?;
            Some(BloomFilter::decode(&buf).map_err(|e| corrupt(e.to_string()))?)
        };

        let mut reader = Self {
            file,
            path: path.to_path_buf(),
            index,
            filter,
            footer,
            smallest: None,
            file_size: file_len,
            id,
            ctx,
        };
        // The index only stores each block's LAST key, so the table's first
        // key costs one block read. Compaction needs it for overlap checks.
        if let Some(first) = reader.index.first() {
            let raw = reader.read_block(first)?;
            let block = Block::new(&raw, reader.footer.restarts)
                .map_err(|e| reader.block_error(e, first))?;
            let (key, _, _) = block
                .iter()
                .next()
                .ok_or_else(|| corrupt("first block is empty".into()))?
                .map_err(|e| reader.block_error(e, first))?;
            reader.smallest = Some(key.to_vec());
        }
        Ok(reader)
    }

    /// The newest version of `key` at or below `snapshot`. `None` = no such
    /// version in this table; `Some(Tombstone)` = deleted as of `snapshot`.
    pub fn get(&self, key: &[u8], snapshot: SeqNo) -> Result<Option<Entry>> {
        Ok(self.get_versioned(key, snapshot)?.map(|(_, e)| e))
    }

    /// `get`, plus the found version's sequence number (what a transaction's
    /// conflict check compares).
    pub fn get_versioned(&self, key: &[u8], snapshot: SeqNo) -> Result<Option<(SeqNo, Entry)>> {
        let Some(filter) = &self.filter else {
            return self.search(key, snapshot);
        };
        if !filter.may_contain(key) {
            ReadStats::bump(&self.ctx.stats.filter_negatives);
            return Ok(None);
        }
        let found = self.search(key, snapshot)?;
        if found.is_none() {
            ReadStats::bump(&self.ctx.stats.filter_false_positives);
        }
        Ok(found)
    }

    /// The lookup without the filter: index, then one block.
    fn search(&self, key: &[u8], snapshot: SeqNo) -> Result<Option<(SeqNo, Entry)>> {
        // First block whose last entry is at or after (key, snapshot): the
        // only block that can hold the first entry at or after it, which is
        // the version this lookup wants if it belongs to `key`.
        let i = self
            .index
            .partition_point(|e| key::compare(&e.last_key, e.last_seq, key, snapshot).is_lt());
        let Some(entry) = self.index.get(i) else {
            return Ok(None);
        };
        let raw = self.cached_block(entry)?;
        Block::from_verified(&raw, self.footer.restarts)
            .get_versioned(key, snapshot)
            .map_err(|e| self.block_error(e, entry))
    }

    /// A CRC-verified data block, from the cache if it's there, otherwise
    /// read from disk, verified, and cached. A block that fails its CRC is
    /// never cached, so every later read reports the corruption too.
    fn cached_block(&self, entry: &IndexEntry) -> Result<Arc<[u8]>> {
        let key = (self.id, entry.offset);
        let stats = &self.ctx.stats;
        if let Some(raw) = self.ctx.cache.get(key) {
            ReadStats::bump(&stats.cache_hits);
            return Ok(raw);
        }
        let raw = self.read_block(entry)?;
        ReadStats::bump(&stats.block_reads);
        Block::new(&raw, self.footer.restarts).map_err(|e| self.block_error(e, entry))?;
        let raw: Arc<[u8]> = raw.into();
        self.ctx.cache.insert(key, Arc::clone(&raw));
        Ok(raw)
    }

    /// An iterator over every version of every key from `start` on (all of
    /// them if `None`), in internal key order, one block in memory at a time.
    /// It owns an `Arc` of the reader, so it can outlive the caller's borrow.
    pub fn iter(self: &Arc<Self>, start: Option<&[u8]>) -> Result<SstIter> {
        Ok(SstIter {
            cursor: Cursor::seek(self, start)?,
            reader: Arc::clone(self),
        })
    }

    /// Every entry, read in one go. Checks the count against the footer, so
    /// tests use it to compare whole tables.
    pub fn entries(&self) -> Result<Vec<(Vec<u8>, SeqNo, Entry)>> {
        let mut out = Vec::with_capacity(self.footer.entry_count as usize);
        let mut cursor = Cursor::seek(self, None)?;
        while let Some(item) = cursor.next(self)? {
            out.push(item);
        }
        if out.len() as u64 != self.footer.entry_count {
            return Err(Error::Corruption(format!(
                "{}: footer says {} entries, blocks hold {}",
                self.path.display(),
                self.footer.entry_count,
                out.len()
            )));
        }
        Ok(out)
    }

    /// Block `i` for a scan or a compaction: from the cache if it's there,
    /// otherwise from disk and verified, but NOT added to the cache. A scan
    /// reads each block once, in order; caching them would evict the blocks
    /// that point reads keep coming back to (DESIGN.md D10, D20).
    fn scan_block(&self, i: usize) -> Result<Arc<[u8]>> {
        let entry = &self.index[i];
        if let Some(raw) = self.ctx.cache.get((self.id, entry.offset)) {
            return Ok(raw);
        }
        let raw = self.read_block(entry)?;
        Block::new(&raw, self.footer.restarts).map_err(|e| self.block_error(e, entry))?;
        Ok(raw.into())
    }

    /// Smallest key in the table, or `None` for an empty table.
    pub fn smallest_key(&self) -> Option<&[u8]> {
        self.smallest.as_deref()
    }

    /// Largest key in the table: the last block's index key.
    pub fn largest_key(&self) -> Option<&[u8]> {
        self.index.last().map(|e| e.last_key.as_slice())
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn entry_count(&self) -> u64 {
        self.footer.entry_count
    }

    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    pub fn has_filter(&self) -> bool {
        self.filter.is_some()
    }

    pub fn stats(&self) -> &ReadStats {
        &self.ctx.stats
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_block(&self, entry: &IndexEntry) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; entry.size as usize];
        self.file.read_exact_at(&mut buf, entry.offset)?;
        Ok(buf)
    }

    /// Adds the file and block offset to a block-level corruption message.
    fn block_error(&self, e: Error, entry: &IndexEntry) -> Error {
        match e {
            Error::Corruption(msg) => Error::Corruption(format!(
                "{}: block at offset {}: {msg}",
                self.path.display(),
                entry.offset
            )),
            other => other,
        }
    }
}

/// A table's entries from some key on, in internal key order (DESIGN.md D20).
/// Holds one block at a time. After an error it yields nothing more.
pub struct SstIter {
    reader: Arc<SstReader>,
    cursor: Cursor,
}

impl Iterator for SstIter {
    type Item = Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.next(&self.reader) {
            Ok(item) => item.map(Ok),
            Err(e) => {
                self.cursor.block = None;
                Some(Err(e))
            }
        }
    }
}

/// A position in a table: which block is loaded, and the byte offset of the
/// next entry in it. It doesn't own the reader, so each call is handed the
/// one it was created from (`SstIter` pairs them up).
struct Cursor {
    block_idx: usize,
    /// The loaded block (verified), or `None` once the table is exhausted.
    block: Option<Arc<[u8]>>,
    pos: usize,
}

impl Cursor {
    /// Positions at the first entry whose user key is >= `start`.
    fn seek(r: &SstReader, start: Option<&[u8]>) -> Result<Self> {
        // The first block whose last entry is at or after (start, MAX_SEQ),
        // the very first version of `start`, holds the first entry >= it.
        let block_idx = match start {
            Some(start) => r.index.partition_point(|e| {
                key::compare(&e.last_key, e.last_seq, start, key::MAX_SEQ).is_lt()
            }),
            None => 0,
        };
        let mut cursor = Self {
            block_idx,
            block: None,
            pos: 0,
        };
        cursor.load(r)?;
        // Skip the block's entries before `start`: a binary search over its
        // restart points, then a short scan.
        if let (Some(start), Some(raw)) = (start, &cursor.block) {
            cursor.pos = Block::from_verified(raw, r.footer.restarts)
                .seek(start, key::MAX_SEQ)
                .map_err(|e| r.block_error(e, &r.index[block_idx]))?;
        }
        Ok(cursor)
    }

    /// Loads block `block_idx` (or marks the cursor exhausted past the end).
    fn load(&mut self, r: &SstReader) -> Result<()> {
        self.pos = 0;
        self.block = None;
        if self.block_idx < r.index.len() {
            self.block = Some(r.scan_block(self.block_idx)?);
        }
        Ok(())
    }

    fn next(&mut self, r: &SstReader) -> Result<Option<ScanEntry>> {
        loop {
            let Some(raw) = &self.block else {
                return Ok(None);
            };
            let mut it = Block::from_verified(raw, r.footer.restarts).iter_at(self.pos);
            match it.next() {
                Some(Ok((k, seq, v))) => {
                    self.pos = it.position();
                    let v = match v {
                        Some(v) => Entry::Value(v.to_vec()),
                        None => Entry::Tombstone,
                    };
                    return Ok(Some((k.to_vec(), seq, v)));
                }
                Some(Err(e)) => return Err(r.block_error(e, &r.index[self.block_idx])),
                None => {
                    self.block_idx += 1;
                    self.load(r)?;
                }
            }
        }
    }
}

/// Parses and validates the index block. Blocks must be contiguous from
/// offset 0 up to `blocks_end`, and last entries strictly increasing in
/// internal key order.
fn decode_index(buf: &[u8], blocks_end: u64) -> std::result::Result<Vec<IndexEntry>, String> {
    if buf.len() < 4 {
        return Err("index shorter than its checksum".into());
    }
    let (data, crc) = buf.split_at(buf.len() - 4);
    if crc32fast::hash(data) != read_u32(crc, 0) {
        return Err("index checksum mismatch".into());
    }

    let mut index: Vec<IndexEntry> = Vec::new();
    let mut pos = 0;
    let mut expected_offset = 0u64;
    while pos < data.len() {
        let rest = &data[pos..];
        if rest.len() < 4 {
            return Err("index entry truncated".into());
        }
        let key_len = read_u32(rest, 0) as usize;
        let Some(need) = key_len.checked_add(4 + 8 + 8 + 4) else {
            return Err("index entry truncated".into());
        };
        if rest.len() < need {
            return Err("index entry truncated".into());
        }
        let last_key = rest[4..4 + key_len].to_vec();
        let last_seq = read_u64(rest, 4 + key_len);
        let offset = read_u64(rest, 4 + key_len + 8);
        let size = read_u32(rest, 4 + key_len + 16);

        if offset != expected_offset {
            return Err(format!("block at {offset}, expected {expected_offset}"));
        }
        if let Some(prev) = index.last() {
            if !key::compare(&last_key, last_seq, &prev.last_key, prev.last_seq).is_gt() {
                return Err("index keys not strictly increasing".into());
            }
        }
        expected_offset = offset + size as u64;
        index.push(IndexEntry {
            last_key,
            last_seq,
            offset,
            size,
        });
        pos += need;
    }
    if expected_offset != blocks_end {
        return Err(format!(
            "blocks end at {expected_offset}, filter starts at {blocks_end}"
        ));
    }
    Ok(index)
}
