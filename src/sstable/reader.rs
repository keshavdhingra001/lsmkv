//! Reads an SSTable: footer -> filter and index (both kept in memory) -> at
//! most one block per lookup.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::block::Block;
use super::filter::BloomFilter;
use super::{Footer, ReadContext, ReadStats, FOOTER_LEN};
use crate::codec::{read_u32, read_u64};
use crate::error::{Error, Result};
use crate::memtable::Entry;

#[derive(Debug)]
struct IndexEntry {
    last_key: Vec<u8>,
    offset: u64,
    size: u32,
}

#[derive(Debug)]
pub struct SstReader {
    file: File,
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
        let corrupt = |what: String| Error::Corruption(format!("{}: {what}", path.display()));

        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
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
            let block = Block::new(&raw).map_err(|e| reader.block_error(e, first))?;
            let (key, _) = block
                .iter()
                .next()
                .ok_or_else(|| corrupt("first block is empty".into()))?
                .map_err(|e| reader.block_error(e, first))?;
            reader.smallest = Some(key.to_vec());
        }
        Ok(reader)
    }

    /// `None` = key not in this table; `Some(Tombstone)` = deleted here.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        let Some(filter) = &self.filter else {
            return self.search(key);
        };
        if !filter.may_contain(key) {
            ReadStats::bump(&self.ctx.stats.filter_negatives);
            return Ok(None);
        }
        let found = self.search(key)?;
        if found.is_none() {
            ReadStats::bump(&self.ctx.stats.filter_false_positives);
        }
        Ok(found)
    }

    /// The lookup without the filter: index, then one block.
    fn search(&self, key: &[u8]) -> Result<Option<Entry>> {
        // First block whose last key >= key: the only block that can hold it.
        let i = self.index.partition_point(|e| e.last_key.as_slice() < key);
        let Some(entry) = self.index.get(i) else {
            return Ok(None);
        };
        let raw = self.cached_block(entry)?;
        Block::from_verified(&raw)
            .get(key)
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
        Block::new(&raw).map_err(|e| self.block_error(e, entry))?;
        let raw: Arc<[u8]> = raw.into();
        self.ctx.cache.insert(key, Arc::clone(&raw));
        Ok(raw)
    }

    /// Every entry in key order. Reads the whole table; used by tests now and
    /// by flush/compaction later (M9 replaces it with a streaming iterator).
    /// Bypasses the block cache: a compaction reads each block once, and
    /// caching them would evict the blocks that reads actually reuse.
    pub fn entries(&self) -> Result<Vec<(Vec<u8>, Entry)>> {
        let mut out = Vec::with_capacity(self.footer.entry_count as usize);
        for entry in &self.index {
            let raw = self.read_block(entry)?;
            let block = Block::new(&raw).map_err(|e| self.block_error(e, entry))?;
            for item in block.iter() {
                let (k, v) = item.map_err(|e| self.block_error(e, entry))?;
                let v = match v {
                    Some(v) => Entry::Value(v.to_vec()),
                    None => Entry::Tombstone,
                };
                out.push((k.to_vec(), v));
            }
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

/// Parses and validates the index block. Blocks must be contiguous from
/// offset 0 up to `blocks_end`, and last keys strictly increasing.
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
        let need = 4 + key_len + 8 + 4;
        if rest.len() < need {
            return Err("index entry truncated".into());
        }
        let last_key = rest[4..4 + key_len].to_vec();
        let offset = read_u64(rest, 4 + key_len);
        let size = read_u32(rest, 4 + key_len + 8);

        if offset != expected_offset {
            return Err(format!("block at {offset}, expected {expected_offset}"));
        }
        if let Some(prev) = index.last() {
            if last_key <= prev.last_key {
                return Err("index keys not strictly increasing".into());
            }
        }
        expected_offset = offset + size as u64;
        index.push(IndexEntry {
            last_key,
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
