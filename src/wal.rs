//! Write-ahead log: every write hits disk here before the memtable, so a crash
//! can be recovered by replaying the log.
//!
//! On-disk record layout (all integers little-endian):
//!
//! ```text
//! +-----------+----------+--------------+--------------+---------+-----------+
//! | crc32 u32 | kind u8  | key_len u32  | val_len u32  | key ... | value ... |
//! +-----------+----------+--------------+--------------+---------+-----------+
//! ```
//!
//! - `crc32` covers every byte AFTER the crc field (kind through value).
//! - `kind`: 1 = Put, 2 = Delete (Delete has `val_len = 0`).
//! - Header is 13 bytes.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::error::Result;

pub const HEADER_LEN: usize = 4 + 1 + 4 + 4;
pub const KIND_PUT: u8 = 1;
pub const KIND_DELETE: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Result of replaying a log file.
#[derive(Debug, Default)]
pub struct Replay {
    pub records: Vec<Record>,
    /// Byte offset just past the last valid record. Anything after this is a
    /// torn or corrupt tail, and `Db::open` truncates the file back to here
    /// before appending again (otherwise new writes land after garbage and
    /// become unreachable on the next replay).
    pub valid_len: u64,
}

pub struct Wal {
    file: BufWriter<File>,
}

impl Wal {
    /// Opens (or creates) the log for appending.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    /// TODO(you, M2): encode `rec` in the layout above and write it to `self.file`.
    /// Use `crc32fast::Hasher`. Do NOT fsync here; that's `sync`'s job, which lets
    /// a caller batch several appends under one fsync later (group commit).
    pub fn append(&mut self, rec: &Record) -> Result<()> {
        let _ = rec;
        todo!("M2: Wal::append")
    }

    /// Pushes buffered bytes to the OS, then forces them to the disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }

    /// TODO(you, M2): read the whole file and decode records front to back.
    ///
    /// - Missing file => empty `Replay` (fresh database).
    /// - Stop (without an error) at the first record that is incomplete, has a
    ///   bad checksum, or has an unknown `kind`. A crash mid-append leaves
    ///   exactly that kind of torn tail.
    /// - Set `valid_len` to the offset just past the last good record.
    ///
    /// Design question to answer in DESIGN.md: should corruption in the MIDDLE
    /// of the log (good records after a bad one) be treated differently?
    pub fn replay(path: &Path) -> Result<Replay> {
        let _ = path;
        todo!("M2: Wal::replay")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn put(k: &str, v: &str) -> Record {
        Record::Put {
            key: k.into(),
            value: v.into(),
        }
    }

    fn write_all(path: &Path, recs: &[Record]) {
        let mut wal = Wal::open(path).unwrap();
        for r in recs {
            wal.append(r).unwrap();
        }
        wal.sync().unwrap();
    }

    #[test]
    fn missing_file_replays_empty() {
        let dir = tempfile::tempdir().unwrap();
        let r = Wal::replay(&dir.path().join("wal.log")).unwrap();
        assert!(r.records.is_empty());
        assert_eq!(r.valid_len, 0);
    }

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let recs = vec![
            put("a", "1"),
            Record::Delete { key: "b".into() },
            put("c", ""),
        ];
        write_all(&path, &recs);

        let r = Wal::replay(&path).unwrap();
        assert_eq!(r.records, recs);
        assert_eq!(r.valid_len, fs::metadata(&path).unwrap().len());
    }

    #[test]
    fn record_size_matches_layout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_all(&path, &[put("key", "value")]);
        let len = fs::metadata(&path).unwrap().len() as usize;
        assert_eq!(len, HEADER_LEN + 3 + 5);
    }

    #[test]
    fn torn_tail_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_all(&path, &[put("a", "1"), put("b", "2"), put("c", "3")]);

        // Simulate a crash in the middle of writing the last record.
        let full = fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 3).unwrap();

        let r = Wal::replay(&path).unwrap();
        assert_eq!(r.records, vec![put("a", "1"), put("b", "2")]);
        assert!(r.valid_len < full - 3);
    }

    #[test]
    fn bad_checksum_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_all(&path, &[put("a", "1"), put("b", "2")]);

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF; // flip a bit in the last record's value
        fs::write(&path, &bytes).unwrap();

        let r = Wal::replay(&path).unwrap();
        assert_eq!(r.records, vec![put("a", "1")]);
    }

    #[test]
    fn header_only_garbage_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        fs::write(&path, [0u8; 5]).unwrap(); // shorter than one header
        let r = Wal::replay(&path).unwrap();
        assert!(r.records.is_empty());
        assert_eq!(r.valid_len, 0);
    }
}
