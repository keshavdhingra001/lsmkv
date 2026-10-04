//! Sorted String Table: an immutable, sorted on-disk file of key -> entry.
//!
//! ```text
//! +--------------+-----+--------------+-------------+--------------+
//! | data block 0 | ... | filter block | index block | footer (52B) |
//! +--------------+-----+--------------+-------------+--------------+
//! ```
//!
//! - Data blocks: see `block.rs`.
//! - Filter block: a bloom filter over every distinct key (see `filter.rs`). Zero bytes
//!   means "no filter": tables written before M6, or with filters turned off.
//! - Index block: one entry per data block, then a CRC32 of the entries:
//!   `[key_len u32][last_key][last_seq u64][offset u64][size u32]`.
//!   (`last_key`, `last_seq`) is the block's last entry in internal key order
//!   (DESIGN.md D18), so a binary search over the index finds the only block
//!   that can hold the version a lookup wants.
//! - Footer: `[index_offset u64][index_len u64][filter_offset u64][filter_len u64]
//!   [entry_count u64][crc32 u32][magic u64]`. The CRC covers the five u64s.
//!   The footer is fixed size at a fixed place (end of file), so it's where a
//!   reader starts.
//!
//! See DESIGN.md D5 for the reasoning.

pub mod block;
pub mod cache;
pub mod filter;
mod reader;
mod writer;

pub use cache::BlockCache;
pub use reader::SstReader;
pub use writer::{SstWriter, WriterOptions};

use std::sync::atomic::{AtomicU64, Ordering};

use crate::codec::{read_u32, read_u64};
use crate::error::{Error, Result};

/// Target size of a data block. Blocks close once they reach it, so a block
/// can exceed it by at most one entry.
pub const DEFAULT_BLOCK_SIZE: usize = 4096;
pub const FOOTER_LEN: usize = 5 * 8 + 4 + 8;
/// "LSMKVSS2" read as a little-endian u64. The 2 is the format: sequence
/// numbers in every entry (M8). M3–M7 tables ended in "LSMKVSST".
pub const MAGIC: u64 = u64::from_le_bytes(*b"LSMKVSS2");

/// What every table a database has open shares: one block cache and one set
/// of counters. The default has the cache turned off.
#[derive(Debug)]
pub struct ReadContext {
    pub cache: BlockCache,
    pub stats: ReadStats,
}

impl ReadContext {
    pub fn new(cache_bytes: usize) -> Self {
        Self {
            cache: BlockCache::new(cache_bytes),
            stats: ReadStats::default(),
        }
    }
}

impl Default for ReadContext {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Counters shared by every table a database has open. Atomics, because
/// `SstReader::get` takes `&self` (and M8 will call it from many threads).
/// `Relaxed` is enough: each counter is independent, nothing is ordered by it.
#[derive(Debug, Default)]
pub struct ReadStats {
    /// Data blocks read from disk by `get` (block cache misses).
    pub block_reads: AtomicU64,
    /// Data blocks `get` found in the block cache.
    pub cache_hits: AtomicU64,
    /// Lookups where a table's filter said "definitely not here" (each one a
    /// block read saved).
    pub filter_negatives: AtomicU64,
    /// Lookups where the filter said "maybe" but the table didn't hold the key.
    pub filter_false_positives: AtomicU64,
}

impl ReadStats {
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Footer {
    index_offset: u64,
    index_len: u64,
    filter_offset: u64,
    filter_len: u64,
    entry_count: u64,
}

impl Footer {
    fn encode(&self) -> [u8; FOOTER_LEN] {
        let mut out = [0u8; FOOTER_LEN];
        let fields = [
            self.index_offset,
            self.index_len,
            self.filter_offset,
            self.filter_len,
            self.entry_count,
        ];
        for (i, f) in fields.iter().enumerate() {
            out[i * 8..i * 8 + 8].copy_from_slice(&f.to_le_bytes());
        }
        let crc = crc32fast::hash(&out[..40]);
        out[40..44].copy_from_slice(&crc.to_le_bytes());
        out[44..52].copy_from_slice(&MAGIC.to_le_bytes());
        out
    }

    fn decode(buf: &[u8; FOOTER_LEN]) -> Result<Self> {
        // Magic first: a wrong magic means "not an SSTable", a clearer error
        // than a checksum failure.
        if read_u64(buf, 44) != MAGIC {
            return Err(Error::Corruption(
                "bad magic number (not an sstable, or truncated)".into(),
            ));
        }
        if crc32fast::hash(&buf[..40]) != read_u32(buf, 40) {
            return Err(Error::Corruption("footer checksum mismatch".into()));
        }
        Ok(Self {
            index_offset: read_u64(buf, 0),
            index_len: read_u64(buf, 8),
            filter_offset: read_u64(buf, 16),
            filter_len: read_u64(buf, 24),
            entry_count: read_u64(buf, 32),
        })
    }
}

#[cfg(test)]
mod tests;
