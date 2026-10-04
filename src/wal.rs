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
use std::io::{BufWriter, ErrorKind, Write};
use std::path::Path;

use crate::error::{Error, Result};

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

    /// Encodes `rec` and writes it to the log buffer.
    ///
    /// Does NOT fsync; that's `sync`'s job, which lets a caller batch several
    /// appends under one fsync later (group commit).
    pub fn append(&mut self, rec: &Record) -> Result<()> {
        let (kind, key, value): (u8, &[u8], &[u8]) = match rec {
            Record::Put { key, value } => (KIND_PUT, key, value),
            Record::Delete { key } => (KIND_DELETE, key, &[]),
        };
        let key_len = len_u32(key, "key")?;
        let val_len = len_u32(value, "value")?;

        // Everything after the CRC field, built first so the CRC can cover it.
        let mut body = Vec::with_capacity(HEADER_LEN - 4 + key.len() + value.len());
        body.push(kind);
        body.extend_from_slice(&key_len.to_le_bytes());
        body.extend_from_slice(&val_len.to_le_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(value);

        let crc = crc32fast::hash(&body);
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        Ok(())
    }

    /// Pushes buffered bytes to the OS, then forces them to the disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }

    /// Reads the whole log and decodes records front to back.
    ///
    /// - Missing file => empty `Replay` (fresh database).
    /// - Stops (without an error) at the first record that is incomplete, has a
    ///   bad checksum, or has an unknown `kind`. A crash mid-append leaves
    ///   exactly that kind of torn tail. (Mid-log corruption: see DESIGN.md D2.)
    /// - `valid_len` is the offset just past the last good record.
    pub fn replay(path: &Path) -> Result<Replay> {
        let buf = match std::fs::read(path) {
            Ok(buf) => buf,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Replay::default()),
            Err(e) => return Err(e.into()),
        };

        let mut records = Vec::new();
        let mut pos = 0;
        while let Some((rec, len)) = decode(&buf[pos..]) {
            records.push(rec);
            pos += len;
        }
        Ok(Replay {
            records,
            valid_len: pos as u64,
        })
    }
}

/// Decodes one record from the front of `buf`.
/// Returns the record and its encoded length, or `None` if `buf` doesn't start
/// with a complete, valid record.
fn decode(buf: &[u8]) -> Option<(Record, usize)> {
    if buf.len() < HEADER_LEN {
        return None;
    }
    let crc = read_u32(buf, 0);
    let kind = buf[4];
    let key_len = read_u32(buf, 5) as usize;
    let val_len = read_u32(buf, 9) as usize;

    // Lengths come from disk and may be garbage, so guard the addition too.
    let total = HEADER_LEN.checked_add(key_len)?.checked_add(val_len)?;
    if buf.len() < total {
        return None;
    }
    if crc32fast::hash(&buf[4..total]) != crc {
        return None;
    }

    let key = buf[HEADER_LEN..HEADER_LEN + key_len].to_vec();
    let rec = match kind {
        KIND_PUT => Record::Put {
            key,
            value: buf[HEADER_LEN + key_len..total].to_vec(),
        },
        KIND_DELETE if val_len == 0 => Record::Delete { key },
        _ => return None,
    };
    Some((rec, total))
}

fn read_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().expect("4-byte slice"))
}

fn len_u32(bytes: &[u8], what: &str) -> Result<u32> {
    u32::try_from(bytes.len()).map_err(|_| {
        Error::Io(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("{what} too large: {} bytes (max {})", bytes.len(), u32::MAX),
        ))
    })
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

    /// Builds a record by hand with a correct CRC, so tests can reach the
    /// checks that come after the checksum.
    fn raw_record(kind: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut body = vec![kind];
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(&(value.len() as u32).to_le_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(value);
        let mut out = crc32fast::hash(&body).to_le_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn unknown_kind_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_all(&path, &[put("a", "1")]);
        let good_len = fs::metadata(&path).unwrap().len();

        let mut bytes = fs::read(&path).unwrap();
        bytes.extend(raw_record(9, b"x", b"y"));
        fs::write(&path, &bytes).unwrap();

        let r = Wal::replay(&path).unwrap();
        assert_eq!(r.records, vec![put("a", "1")]);
        assert_eq!(r.valid_len, good_len);
    }

    #[test]
    fn delete_with_value_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        fs::write(&path, raw_record(KIND_DELETE, b"k", b"oops")).unwrap();
        let r = Wal::replay(&path).unwrap();
        assert!(r.records.is_empty());
    }

    #[test]
    fn huge_lengths_do_not_panic() {
        // key_len = val_len = u32::MAX: must be treated as torn, not overflow
        // or try to allocate gigabytes.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut bytes = vec![0u8; 4];
        bytes.push(KIND_PUT);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        let r = Wal::replay(&path).unwrap();
        assert!(r.records.is_empty());
        assert_eq!(r.valid_len, 0);
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
