//! Write-ahead log: every write hits disk here before the memtable, so a crash
//! can be recovered by replaying the log.
//!
//! On-disk record layout (all integers little-endian):
//!
//! ```text
//! +-----------+---------+---------+-------------+-------------+---------+-----------+
//! | crc32 u32 | kind u8 | seq u64 | key_len u32 | val_len u32 | key ... | value ... |
//! +-----------+---------+---------+-------------+-------------+---------+-----------+
//! ```
//!
//! - `crc32` covers every byte AFTER the crc field (kind through value).
//! - `kind`: 1 = Put, 2 = Delete (Delete has `val_len = 0`), 3 = Batch.
//! - `seq`: the write's sequence number (DESIGN.md D18), so a replay rebuilds
//!   the memtable with the same versions it had.
//! - Header is 21 bytes.
//!
//! A batch (DESIGN.md D25) is ONE record, so one CRC covers all of it and a
//! torn tail drops the whole batch, never part of it. Its header reuses the
//! two length slots: `key_len` holds the number of operations and `val_len`
//! the payload length. `seq` is the first operation's number; the rest follow
//! consecutively. The payload is the operations back to back:
//!
//! ```text
//! op = [kind u8][key_len u32][val_len u32][key][value]     (kind 1 or 2)
//! ```

use std::io::{BufWriter, ErrorKind, Write};
use std::path::Path;

use crate::codec::{len_u32, read_u32, read_u64};
use crate::error::{Error, Result};
use crate::key::SeqNo;
use crate::vfs::{Fs, RealFs, SyncHandle, WritableFile};

pub const HEADER_LEN: usize = 4 + 1 + 8 + 4 + 4;
pub const KIND_PUT: u8 = 1;
pub const KIND_DELETE: u8 = 2;
pub const KIND_BATCH: u8 = 3;
/// Header of one operation inside a batch payload.
const OP_HEADER_LEN: usize = 1 + 4 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Record {
    pub fn key(&self) -> &[u8] {
        match self {
            Record::Put { key, .. } | Record::Delete { key } => key,
        }
    }

    pub fn value_len(&self) -> usize {
        match self {
            Record::Put { value, .. } => value.len(),
            Record::Delete { .. } => 0,
        }
    }
}

/// Result of replaying a log file.
#[derive(Debug, Default)]
pub struct Replay {
    /// Each record with its sequence number, in log order.
    pub records: Vec<(SeqNo, Record)>,
    /// Byte offset just past the last valid record. Anything after this is a
    /// torn or corrupt tail, and `Db::open` truncates the file back to here
    /// before appending again (otherwise new writes land after garbage and
    /// become unreachable on the next replay).
    pub valid_len: u64,
    /// The file's whole length, garbage included.
    pub file_len: u64,
}

pub struct Wal {
    file: BufWriter<Box<dyn WritableFile>>,
    /// Successful `sync` calls, so callers can report real fsyncs.
    syncs: u64,
}

impl Wal {
    /// Opens (or creates) the log for appending, on the real filesystem.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_in(&RealFs, path)
    }

    /// Opens (or creates) the log for appending, on `fs`.
    pub fn open_in(fs: &dyn Fs, path: &Path) -> Result<Self> {
        let file = fs.open_append(path)?;
        Ok(Self {
            file: BufWriter::new(file),
            syncs: 0,
        })
    }

    /// Encodes `rec` (the write numbered `seq`) and writes it to the log buffer.
    ///
    /// Does NOT fsync; that's `sync`'s job, which lets a caller batch several
    /// appends under one fsync later (group commit).
    pub fn append(&mut self, seq: SeqNo, rec: &Record) -> Result<()> {
        let (kind, key, value): (u8, &[u8], &[u8]) = match rec {
            Record::Put { key, value } => (KIND_PUT, key, value),
            Record::Delete { key } => (KIND_DELETE, key, &[]),
        };
        let key_len = len_u32(key, "key")?;
        let val_len = len_u32(value, "value")?;

        // Everything after the CRC field, built first so the CRC can cover it.
        let mut body = Vec::with_capacity(HEADER_LEN - 4 + key.len() + value.len());
        body.push(kind);
        body.extend_from_slice(&seq.to_le_bytes());
        body.extend_from_slice(&key_len.to_le_bytes());
        body.extend_from_slice(&val_len.to_le_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(value);

        let crc = crc32fast::hash(&body);
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        Ok(())
    }

    /// Logs `ops`, numbered `first_seq`, `first_seq + 1`, ..., as one unit:
    /// after a crash, replay returns all of them or none. A single operation
    /// is logged as a plain record (the format before batches existed).
    pub fn append_batch(&mut self, first_seq: SeqNo, ops: &[Record]) -> Result<()> {
        match ops {
            [] => Ok(()),
            [op] => self.append(first_seq, op),
            _ => {
                let mut payload = Vec::new();
                for op in ops {
                    let (kind, key, value): (u8, &[u8], &[u8]) = match op {
                        Record::Put { key, value } => (KIND_PUT, key, value),
                        Record::Delete { key } => (KIND_DELETE, key, &[]),
                    };
                    payload.push(kind);
                    payload.extend_from_slice(&len_u32(key, "key")?.to_le_bytes());
                    payload.extend_from_slice(&len_u32(value, "value")?.to_le_bytes());
                    payload.extend_from_slice(key);
                    payload.extend_from_slice(value);
                }
                let count = u32::try_from(ops.len())
                    .map_err(|_| Error::InvalidArgument("batch has over 4G operations".into()))?;
                let payload_len = len_u32(&payload, "batch")?;

                let mut body = Vec::with_capacity(HEADER_LEN - 4 + payload.len());
                body.push(KIND_BATCH);
                body.extend_from_slice(&first_seq.to_le_bytes());
                body.extend_from_slice(&count.to_le_bytes());
                body.extend_from_slice(&payload_len.to_le_bytes());
                body.extend_from_slice(&payload);
                let crc = crc32fast::hash(&body);
                self.file.write_all(&crc.to_le_bytes())?;
                self.file.write_all(&body)?;
                Ok(())
            }
        }
    }

    /// Pushes buffered bytes to the OS without waiting for the disk. They
    /// survive this process crashing, but not the machine losing power.
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    /// Pushes buffered bytes to the OS and returns a second handle to the
    /// same file. fsync acts on the file, not the handle, so a caller can
    /// fsync the clone without holding whatever lock guards this `Wal`.
    pub fn sync_handle(&mut self) -> Result<Box<dyn SyncHandle>> {
        self.file.flush()?;
        Ok(self.file.get_ref().sync_handle()?)
    }

    /// Pushes buffered bytes to the OS, then forces them to the disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync()?;
        self.syncs += 1;
        Ok(())
    }

    pub fn sync_count(&self) -> u64 {
        self.syncs
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
        Self::replay_in(&RealFs, path)
    }

    /// `replay`, on `fs`.
    pub fn replay_in(fs: &dyn Fs, path: &Path) -> Result<Replay> {
        let buf = match fs.read(path) {
            Ok(buf) => buf,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Replay::default()),
            Err(e) => return Err(e.into()),
        };

        let mut records = Vec::new();
        let mut pos = 0;
        loop {
            match decode(&buf[pos..]) {
                Decoded::Records(recs, len) => {
                    records.extend(recs);
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
            file_len: buf.len() as u64,
        })
    }
}

enum Decoded {
    /// A valid record (several operations, for a batch) and its encoded length.
    Records(Vec<(SeqNo, Record)>, usize),
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
    let seq = read_u64(buf, 5);
    let key_len = read_u32(buf, 13) as usize;
    let val_len = read_u32(buf, 17) as usize;

    // A batch's `key_len` slot is its operation count, not a length.
    let body_len = if kind == KIND_BATCH {
        Some(val_len)
    } else {
        key_len.checked_add(val_len)
    };
    // Lengths come from disk and may be garbage, so guard the addition too.
    let Some(total) = body_len.and_then(|n| n.checked_add(HEADER_LEN)) else {
        return Decoded::Incomplete;
    };
    if buf.len() < total {
        return Decoded::Incomplete;
    }
    if crc32fast::hash(&buf[4..total]) != crc {
        return Decoded::Bad(total);
    }

    let body = &buf[HEADER_LEN..total];
    if kind == KIND_BATCH {
        // `key_len` is the operation count, `val_len` the payload length.
        return match decode_batch(seq, key_len, body) {
            Some(recs) => Decoded::Records(recs, total),
            None => Decoded::Bad(total),
        };
    }
    match decode_op(kind, &body[..key_len], &body[key_len..]) {
        Some(rec) => Decoded::Records(vec![(seq, rec)], total),
        None => Decoded::Bad(total),
    }
}

fn decode_op(kind: u8, key: &[u8], value: &[u8]) -> Option<Record> {
    let key = key.to_vec();
    match kind {
        KIND_PUT => Some(Record::Put {
            key,
            value: value.to_vec(),
        }),
        KIND_DELETE if value.is_empty() => Some(Record::Delete { key }),
        _ => None,
    }
}

/// The operations of a batch payload whose CRC already passed. `None` if it
/// doesn't hold exactly `count` well-formed operations (a writer bug, since
/// the checksum matched).
fn decode_batch(
    first_seq: SeqNo,
    count: usize,
    mut payload: &[u8],
) -> Option<Vec<(SeqNo, Record)>> {
    if count == 0 {
        return None;
    }
    // `count` comes from disk: never allocate for more operations than the
    // payload has room for (each takes at least its header).
    let mut recs = Vec::with_capacity(count.min(payload.len() / OP_HEADER_LEN));
    for seq in (first_seq..).take(count) {
        if payload.len() < OP_HEADER_LEN {
            return None;
        }
        let kind = payload[0];
        let key_len = read_u32(payload, 1) as usize;
        let val_len = read_u32(payload, 5) as usize;
        let end = OP_HEADER_LEN.checked_add(key_len)?.checked_add(val_len)?;
        if payload.len() < end {
            return None;
        }
        let key = &payload[OP_HEADER_LEN..OP_HEADER_LEN + key_len];
        recs.push((
            seq,
            decode_op(kind, key, &payload[OP_HEADER_LEN + key_len..end])?,
        ));
        payload = &payload[end..];
    }
    payload.is_empty().then_some(recs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};

    fn put(k: &str, v: &str) -> Record {
        Record::Put {
            key: k.into(),
            value: v.into(),
        }
    }

    /// Logs `recs` numbered 1, 2, 3, ...
    fn write_all(path: &Path, recs: &[Record]) {
        let mut wal = Wal::open(path).unwrap();
        for (i, r) in recs.iter().enumerate() {
            wal.append(i as SeqNo + 1, r).unwrap();
        }
        wal.sync().unwrap();
    }

    /// What `write_all(recs)` replays as.
    fn numbered(recs: &[Record]) -> Vec<(SeqNo, Record)> {
        (1..).zip(recs.iter().cloned()).collect()
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
        assert_eq!(r.records, numbered(&recs));
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
        assert_eq!(r.records, numbered(&[put("a", "1"), put("b", "2")]));
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
        assert_eq!(r.records, numbered(&[put("a", "1")]));
    }

    /// Builds a record by hand with a correct CRC, so tests can reach the
    /// checks that come after the checksum.
    fn raw_record(kind: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut body = vec![kind];
        body.extend_from_slice(&7u64.to_le_bytes());
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
        assert_eq!(r.records, numbered(&[put("a", "1")]));
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
        bytes.extend_from_slice(&1u64.to_le_bytes());
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
        assert_eq!(r.records, numbered(&[put("a", "1"), put("b", "2")]));
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

    // ---- M13: batches ----

    fn del(k: &str) -> Record {
        Record::Delete { key: k.into() }
    }

    #[test]
    fn batches_roundtrip_with_consecutive_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let batch = [put("a", "1"), del("b"), put("c", "")];
        let mut wal = Wal::open(&path).unwrap();
        wal.append(1, &put("x", "0")).unwrap();
        wal.append_batch(2, &batch).unwrap();
        wal.append_batch(5, &[put("y", "9")]).unwrap(); // one op: a plain record
        wal.append_batch(6, &[]).unwrap(); // nothing at all
        wal.sync().unwrap();

        let r = Wal::replay(&path).unwrap();
        let want = vec![
            (1, put("x", "0")),
            (2, put("a", "1")),
            (3, del("b")),
            (4, put("c", "")),
            (5, put("y", "9")),
        ];
        assert_eq!(r.records, want);
        assert_eq!(r.valid_len, fs::metadata(&path).unwrap().len());
    }

    /// A crash can cut a batch anywhere. Replay must drop the whole batch,
    /// never return part of it.
    #[test]
    fn a_batch_torn_anywhere_disappears_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut wal = Wal::open(&path).unwrap();
        wal.append(1, &put("before", "x")).unwrap();
        wal.sync().unwrap();
        let before = fs::metadata(&path).unwrap().len() as usize;
        wal.append_batch(2, &[put("a", "1"), put("b", "2"), del("c")])
            .unwrap();
        wal.sync().unwrap();
        let full = fs::read(&path).unwrap();

        for cut in before..full.len() {
            fs::write(&path, &full[..cut]).unwrap();
            let r = Wal::replay(&path).unwrap();
            assert_eq!(r.records, vec![(1, put("before", "x"))], "cut at {cut}");
            assert_eq!(r.valid_len as usize, before, "cut at {cut}");
        }
        // Every flipped byte of a final batch drops it as a bad tail...
        for i in before..full.len() {
            let mut bytes = full.clone();
            bytes[i] ^= 0x40;
            fs::write(&path, &bytes).unwrap();
            let r = Wal::replay(&path).unwrap();
            assert_eq!(r.records.len(), 1, "flip at {i}");
        }
        // ...and with more records after it, it's corruption.
        let mut bytes = full.clone();
        bytes[before + 30] ^= 0x40;
        bytes.extend_from_slice(&full[..before]);
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(Wal::replay(&path), Err(Error::Corruption(_))));
    }

    /// A batch header and payload with a valid CRC but inconsistent contents
    /// (only a writer bug could produce one) is rejected, not misread.
    #[test]
    fn malformed_batches_are_rejected() {
        let op = |kind: u8, k: &[u8], v: &[u8]| {
            let mut o = vec![kind];
            o.extend_from_slice(&(k.len() as u32).to_le_bytes());
            o.extend_from_slice(&(v.len() as u32).to_le_bytes());
            o.extend_from_slice(k);
            o.extend_from_slice(v);
            o
        };
        let batch = |count: u32, payload: &[u8]| {
            let mut body = vec![KIND_BATCH];
            body.extend_from_slice(&1u64.to_le_bytes());
            body.extend_from_slice(&count.to_le_bytes());
            body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            body.extend_from_slice(payload);
            let mut rec = crc32fast::hash(&body).to_le_bytes().to_vec();
            rec.extend_from_slice(&body);
            rec
        };
        let two = [op(KIND_PUT, b"a", b"1"), op(KIND_DELETE, b"b", b"")].concat();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("count too high", batch(3, &two)),
            ("count too low", batch(1, &two)),
            ("count zero", batch(0, b"")),
            ("huge count", batch(u32::MAX, &two)),
            (
                "op runs past the payload",
                batch(1, &op(KIND_PUT, b"a", b"1")[..10]),
            ),
            (
                "delete with a value",
                batch(1, &op(KIND_DELETE, b"a", b"1")),
            ),
            ("nested batch kind", batch(1, &op(KIND_BATCH, b"a", b"1"))),
        ];
        for (why, rec) in cases {
            fs::write(&path, &rec).unwrap();
            let r = Wal::replay(&path).unwrap();
            assert!(r.records.is_empty(), "{why}");
            assert_eq!(r.valid_len, 0, "{why}");
        }
        fs::write(&path, batch(2, &two)).unwrap();
        assert_eq!(
            Wal::replay(&path).unwrap().records.len(),
            2,
            "the control case parses"
        );
    }
}
