//! Builds an SSTable from entries supplied in strictly increasing key order.
//!
//! Crash safety: everything is written to `<path>.tmp`, fsynced, renamed to
//! `<path>`, and then the directory is fsynced. A crash at any point leaves
//! either no file at `<path>` or a complete one, never a half-written table.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use super::block::BlockBuilder;
use super::{Footer, DEFAULT_BLOCK_SIZE};
use crate::codec::len_u32;
use crate::error::{Error, Result};
use crate::memtable::Entry;

pub struct SstWriter {
    file: BufWriter<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
    block: BlockBuilder,
    block_size: usize,
    /// Encoded index entries, written out in `finish`.
    index: Vec<u8>,
    /// Bytes written so far, i.e. where the next block starts.
    offset: u64,
    last_key: Option<Vec<u8>>,
    entry_count: u64,
    finished: bool,
}

impl SstWriter {
    pub fn create(path: &Path) -> Result<Self> {
        Self::with_block_size(path, DEFAULT_BLOCK_SIZE)
    }

    /// Like `create`, with a custom block size (tests use tiny blocks to get
    /// many of them).
    pub fn with_block_size(path: &Path, block_size: usize) -> Result<Self> {
        if path.exists() {
            return Err(Error::InvalidArgument(format!(
                "{} already exists; sstables are immutable",
                path.display()
            )));
        }
        let tmp_path = tmp_path_for(path);
        // A leftover .tmp can only be from a crashed writer, so overwrite it.
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        Ok(Self {
            file: BufWriter::new(file),
            tmp_path,
            final_path: path.to_path_buf(),
            block: BlockBuilder::new(),
            block_size,
            index: Vec::new(),
            offset: 0,
            last_key: None,
            entry_count: 0,
            finished: false,
        })
    }

    /// Adds one entry. Keys must be strictly increasing (a memtable flush
    /// produces exactly that order).
    pub fn add(&mut self, key: &[u8], entry: &Entry) -> Result<()> {
        if let Some(last) = &self.last_key {
            if key <= last.as_slice() {
                return Err(Error::InvalidArgument(format!(
                    "keys must be strictly increasing: {:?} after {:?}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(last)
                )));
            }
        }
        self.block.add(key, entry)?;
        self.last_key = Some(key.to_vec());
        self.entry_count += 1;
        if self.block.size() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Writes the pending block (if any) and records its index entry.
    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let last_key = self.block.last_key().to_vec();
        let data = self.block.finish();
        let size = len_u32(&data, "block")?;
        self.file.write_all(&data)?;

        self.index
            .extend_from_slice(&len_u32(&last_key, "key")?.to_le_bytes());
        self.index.extend_from_slice(&last_key);
        self.index.extend_from_slice(&self.offset.to_le_bytes());
        self.index.extend_from_slice(&size.to_le_bytes());

        self.offset += data.len() as u64;
        Ok(())
    }

    /// Writes the index and footer, then atomically publishes the file.
    pub fn finish(mut self) -> Result<()> {
        self.flush_block()?;

        // Filter block: empty until bloom filters land in M6.
        let filter_offset = self.offset;
        let filter_len = 0;

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
        self.file.get_ref().sync_all()?;
        fs::rename(&self.tmp_path, &self.final_path)?;
        sync_parent_dir(&self.final_path)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for SstWriter {
    /// An abandoned writer (error or early return) removes its temp file.
    fn drop(&mut self) {
        if !self.finished {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// fsync the directory so the rename (a directory entry change) survives a crash.
fn sync_parent_dir(path: &Path) -> Result<()> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}
