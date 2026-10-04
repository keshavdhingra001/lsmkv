//! Builds an SSTable from entries supplied in strictly increasing internal key
//! order: key ascending, then seq descending (DESIGN.md D18).
//!
//! Crash safety: everything is written to `<path>.tmp`, fsynced, renamed to
//! `<path>`, and then the directory is fsynced. A crash at any point leaves
//! either no file at `<path>` or a complete one, never a half-written table.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::block::BlockBuilder;
use super::filter::{self, DEFAULT_BITS_PER_KEY};
use super::{Footer, DEFAULT_BLOCK_SIZE};
use crate::codec::len_u32;
use crate::error::{Error, Result};
use crate::key::{self, SeqNo};
use crate::memtable::Entry;
use crate::vfs::{Fs, RealFs, WritableFile};

#[derive(Debug, Clone, Copy)]
pub struct WriterOptions {
    /// Target data block size.
    pub block_size: usize,
    /// Bloom filter size per key; 0 writes no filter (an empty filter block,
    /// the same as tables written before filters existed).
    pub bloom_bits_per_key: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            bloom_bits_per_key: DEFAULT_BITS_PER_KEY,
        }
    }
}

pub struct SstWriter {
    fs: Arc<dyn Fs>,
    file: BufWriter<Box<dyn WritableFile>>,
    tmp_path: PathBuf,
    final_path: PathBuf,
    block: BlockBuilder,
    opts: WriterOptions,
    /// Encoded index entries, written out in `finish`.
    index: Vec<u8>,
    /// Bytes written so far, i.e. where the next block starts.
    offset: u64,
    last_key: Option<(Vec<u8>, SeqNo)>,
    /// Hash of every distinct key, for the bloom filter built in `finish`. 8 bytes per
    /// key, far less than keeping the keys themselves.
    key_hashes: Vec<u64>,
    entry_count: u64,
    finished: bool,
}

impl SstWriter {
    pub fn create(path: &Path) -> Result<Self> {
        Self::with_options(path, WriterOptions::default())
    }

    /// Like `create`, with a custom block size (tests use tiny blocks to get
    /// many of them).
    pub fn with_block_size(path: &Path, block_size: usize) -> Result<Self> {
        Self::with_options(
            path,
            WriterOptions {
                block_size,
                ..WriterOptions::default()
            },
        )
    }

    pub fn with_options(path: &Path, opts: WriterOptions) -> Result<Self> {
        Self::with_options_in(RealFs::shared(), path, opts)
    }

    /// `with_options`, writing through `fs`.
    pub fn with_options_in(fs: Arc<dyn Fs>, path: &Path, opts: WriterOptions) -> Result<Self> {
        if fs.exists(path) {
            return Err(Error::InvalidArgument(format!(
                "{} already exists; sstables are immutable",
                path.display()
            )));
        }
        let tmp_path = tmp_path_for(path);
        // A leftover .tmp can only be from a crashed writer, so overwrite it.
        let file = fs.create(&tmp_path)?;
        Ok(Self {
            fs,
            file: BufWriter::new(file),
            tmp_path,
            final_path: path.to_path_buf(),
            block: BlockBuilder::new(),
            opts,
            index: Vec::new(),
            offset: 0,
            last_key: None,
            key_hashes: Vec::new(),
            entry_count: 0,
            finished: false,
        })
    }

    /// Adds version `seq` of `key`. (key, seq) must be strictly increasing in
    /// internal key order (a memtable flush produces exactly that order).
    pub fn add(&mut self, key: &[u8], seq: SeqNo, entry: &Entry) -> Result<()> {
        let mut new_key = true;
        if let Some((last, last_seq)) = &self.last_key {
            if !key::compare(key, seq, last, *last_seq).is_gt() {
                return Err(Error::InvalidArgument(format!(
                    "entries must be strictly increasing: {:?}@{seq} after {:?}@{last_seq}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(last)
                )));
            }
            new_key = key != last.as_slice();
        }
        self.block.add(key, seq, entry)?;
        self.last_key = Some((key.to_vec(), seq));
        self.entry_count += 1;
        // The filter answers "might this table hold the key at all", so it
        // takes each key once, however many versions it has. Tombstones go
        // in too: a lookup must find them to learn the key is deleted, or it
        // would fall through to older tables.
        if self.opts.bloom_bits_per_key > 0 && new_key {
            self.key_hashes.push(filter::hash(key));
        }
        if self.block.size() >= self.opts.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Writes the pending block (if any) and records its index entry.
    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let (last_key, last_seq) = self.block.last_key();
        let last_key = last_key.to_vec();
        let data = self.block.finish();
        let size = len_u32(&data, "block")?;
        self.file.write_all(&data)?;

        self.index
            .extend_from_slice(&len_u32(&last_key, "key")?.to_le_bytes());
        self.index.extend_from_slice(&last_key);
        self.index.extend_from_slice(&last_seq.to_le_bytes());
        self.index.extend_from_slice(&self.offset.to_le_bytes());
        self.index.extend_from_slice(&size.to_le_bytes());

        self.offset += data.len() as u64;
        Ok(())
    }

    /// Writes the filter, index and footer, then atomically publishes the file.
    pub fn finish(mut self) -> Result<()> {
        self.flush_block()?;

        let filter_offset = self.offset;
        let mut filter_len = 0;
        if self.opts.bloom_bits_per_key > 0 {
            let f = filter::build(&self.key_hashes, self.opts.bloom_bits_per_key);
            self.file.write_all(&f)?;
            filter_len = f.len() as u64;
        }

        let index_offset = filter_offset + filter_len;
        let index_crc = crc32fast::hash(&self.index);
        self.file.write_all(&self.index)?;
        self.file.write_all(&index_crc.to_le_bytes())?;
        let index_len = self.index.len() as u64 + 4;

        let footer = Footer {
            index_offset,
            index_len,
            filter_offset,
            filter_len,
            entry_count: self.entry_count,
        };
        self.file.write_all(&footer.encode())?;

        // Order matters: data must be durable BEFORE the rename makes it
        // visible, and the rename must be durable before we report success.
        self.file.flush()?;
        self.file.get_mut().sync()?;
        self.fs.rename(&self.tmp_path, &self.final_path)?;
        self.fs
            .sync_dir(self.final_path.parent().unwrap_or(Path::new("")))?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for SstWriter {
    /// An abandoned writer (error or early return) removes its temp file.
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.fs.remove(&self.tmp_path);
        }
    }
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}
