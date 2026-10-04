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

use crate::codec::{len_u32, read_u32};
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
    /// - A torn tail (what a crash mid-append leaves) is tolerated: replay stops
    ///   there and `valid_len` marks where the good data ends. A torn tail is an
    ///   incomplete last record, a bad last record, or an all-zero tail.
    /// - A complete bad record with more data after it can't come from a crash,
    ///   so it's reported as `Error::Corruption` instead (DESIGN.md D2).
    pub fn replay(path: &Path) -> Result<Replay> {
        let buf = match std::fs::read(path) {
            Ok(buf) => buf,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Replay::default()),
            Err(e) => return Err(e.into()),
        };

        let mut records = Vec::new();
        let mut pos = 0;
        loop {
            match decode(&buf[pos..]) {
                Decoded::Record(rec, len) => {
                    records.push(rec);
                    pos += len;
                }
                Decoded::Incomplete => break,
                Decoded::Bad(len) => {
                    let rest = &buf[pos + len..];
                    let is_tail = rest.is_empty() || buf[pos..].iter().all(|&b| b == 0);
                    if is_tail {
                        break;
                    }
                    return Err(Error::Corruption(format!(
                        "wal record at offset {pos} is invalid but {} bytes follow it",
                        rest.len()
                    )));
                }
            }
        }
        Ok(Replay {
            records,
            valid_len: pos as u64,
        })
    }
}

enum Decoded {
    /// A valid record and its encoded length.
    Record(Record, usize),
    /// Not enough bytes for the header, or for the length the header claims.
    Incomplete,
    /// Complete per its header, but the checksum or contents are wrong.
    /// Carries the encoded length so the caller can see what follows it.
    Bad(usize),
}

/// Decodes one record from the front of `buf`.
fn decode(buf: &[u8]) -> Decoded {
    if buf.len() < HEADER_LEN {
        return Decoded::Incomplete;
    }
    let crc = read_u32(buf, 0);
    let kind = buf[4];
    let key_len = read_u32(buf, 5) as usize;
    let val_len = read_u32(buf, 9) as usize;

    // Lengths come from disk and may be garbage, so guard the addition too.
    let Some(total) = HEADER_LEN
        .checked_add(key_len)
        .and_then(|n| n.checked_add(val_len))
    else {
        return Decoded::Incomplete;
    };
    if buf.len() < total {
        return Decoded::Incomplete;
    }
    if crc32fast::hash(&buf[4..total]) != crc {
        return Decoded::Bad(total);
    }

    let key = buf[HEADER_LEN..HEADER_LEN + key_len].to_vec();
    let rec = match kind {
        KIND_PUT => Record::Put {
            key,
            value: buf[HEADER_LEN + key_len..total].to_vec(),
        },
        KIND_DELETE if val_len == 0 => Record::Delete { key },
        _ => return Decoded::Bad(total),
    };
    Decoded::Record(rec, total)
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
    fn mid_log_corruption_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_all(&path, &[put("a", "1"), put("b", "2"), put("c", "3")]);

        // Flip the value byte of the MIDDLE record ("b" -> record 2 of 3).
        let rec_len = HEADER_LEN + 2;
        let mut bytes = fs::read(&path).unwrap();
        bytes[2 * rec_len - 1] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        match Wal::replay(&path) {
            Err(Error::Corruption(msg)) => assert!(msg.contains(&format!("offset {rec_len}"))),
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn zero_filled_tail_is_tolerated() {
        // Some filesystems extend the file with zeros on a crash.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_all(&path, &[put("a", "1"), put("b", "2")]);
        let good_len = fs::metadata(&path).unwrap().len();

        let mut bytes = fs::read(&path).unwrap();
        bytes.extend([0u8; 64]);
        fs::write(&path, &bytes).unwrap();

        let r = Wal::replay(&path).unwrap();
        assert_eq!(r.records, vec![put("a", "1"), put("b", "2")]);
        assert_eq!(r.valid_len, good_len);
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
