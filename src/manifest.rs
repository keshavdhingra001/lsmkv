//! MANIFEST: an append-only log of edits that says which files are live.
//!
//! ```text
//! record = [crc32 u32][tag u8][value u64]      (13 bytes; CRC covers tag + value)
//! tag 4        = Format(v): always the first record
//! tag 0x10 + L = AddTable(id) at level L (0 <= L < MAX_LEVELS)
//! tag 2        = RemoveTable(id)
//! tag 3        = SetLogNumber(n)
//! tag 5        = SetLastSequence(n)
//! ```
//!
//! Putting the level in the tag keeps records fixed-size.
//!
//! - `Format(v)`: the on-disk format of the whole database: manifest, WALs
//!   and tables. Format 2 (M8) added sequence numbers to WAL records and table
//!   entries. Directories from before M8 have no format record and are
//!   refused: misreading an old WAL as format 2 would look like a torn tail,
//!   and truncating it would silently lose data.
//! - Replaying every edit in order rebuilds the current `Version`.
//! - `SetLogNumber(n)`: WALs numbered below `n` are already in tables, so
//!   they're obsolete; WALs numbered `n` and above are live and get replayed.
//! - `SetLastSequence(n)`: every write numbered `n` or below is in a table.
//!   A flush records it, because the flushed WAL (which held the numbers) is
//!   deleted; without it, numbering would restart below what tables hold.
//! - Same torn-tail rules as the WAL (DESIGN.md D2): an incomplete or bad final
//!   record, or an all-zero tail, is a crash artifact and is cut off. A bad
//!   record with more data after it is corruption.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::codec::{read_u32, read_u64};
use crate::error::{Error, Result};
use crate::fsutil::sync_dir;

pub const MANIFEST_FILE: &str = "MANIFEST";
const RECORD_LEN: usize = 4 + 1 + 8;
const TAG_REMOVE_TABLE: u8 = 2;
const TAG_SET_LOG_NUMBER: u8 = 3;
const TAG_FORMAT: u8 = 4;
const TAG_SET_LAST_SEQUENCE: u8 = 5;
const TAG_ADD_TABLE_BASE: u8 = 0x10;

/// The on-disk format this build reads and writes.
pub const FORMAT_VERSION: u64 = 2;

/// Levels 0..MAX_LEVELS. Level MAX_LEVELS - 1 is the bottom.
pub const MAX_LEVELS: usize = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edit {
    AddTable {
        id: u64,
        level: u8,
    },
    RemoveTable(u64),
    SetLogNumber(u64),
    SetLastSequence(u64),
    /// Written once, first, by `Manifest::open` on a fresh database.
    Format(u64),
}

/// The set of live files, as of the last manifest edit.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Version {
    /// Live table id -> level. Within level 0, higher id = newer data.
    pub tables: BTreeMap<u64, u8>,
    /// WALs numbered >= this are live. 0 = fresh database, no log yet.
    pub log_number: u64,
    /// Every write numbered at or below this is in a table.
    pub last_sequence: u64,
}

impl Version {
    /// Applies one edit, rejecting edits that can't happen in a valid history.
    pub fn apply(&mut self, edit: Edit) -> std::result::Result<(), String> {
        match edit {
            Edit::AddTable { id, level } => {
                if level as usize >= MAX_LEVELS {
                    return Err(format!(
                        "table {id} added at level {level}, max is {}",
                        MAX_LEVELS - 1
                    ));
                }
                if self.tables.insert(id, level).is_some() {
                    return Err(format!("table {id} added twice"));
                }
            }
            Edit::RemoveTable(id) => {
                if self.tables.remove(&id).is_none() {
                    return Err(format!("removing table {id}, which isn't live"));
                }
            }
            Edit::SetLogNumber(n) => {
                if n < self.log_number {
                    return Err(format!(
                        "log number went backwards: {} -> {n}",
                        self.log_number
                    ));
                }
                self.log_number = n;
            }
            Edit::SetLastSequence(n) => {
                if n < self.last_sequence {
                    return Err(format!(
                        "last sequence went backwards: {} -> {n}",
                        self.last_sequence
                    ));
                }
                self.last_sequence = n;
            }
            Edit::Format(v) => {
                if v != FORMAT_VERSION {
                    return Err(format!(
                        "format {v}, but this build reads format {FORMAT_VERSION} only"
                    ));
                }
            }
        }
        Ok(())
    }
}

pub struct Manifest {
    file: File,
    /// Bytes of valid records, used to undo a failed append.
    len: u64,
}

impl Manifest {
    /// Opens the manifest in `dir`, creating one for a fresh database, and
    /// returns the current `Version`.
    pub fn open(dir: &Path) -> Result<(Self, Version)> {
        let path = dir.join(MANIFEST_FILE);
        let buf = match fs::read(&path) {
            Ok(buf) => buf,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };

        let (version, valid_len) = replay(&buf)?;
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        if valid_len < buf.len() as u64 {
            // Cut the torn tail so new edits aren't appended after garbage.
            file.set_len(valid_len)?;
            file.sync_all()?;
        }
        let mut manifest = Self {
            file,
            len: valid_len,
        };
        if valid_len == 0 {
            // Fresh, or a crash before the first record was durable: either
            // way nothing was ever committed, so start the history now.
            manifest.append(&[Edit::Format(FORMAT_VERSION)])?;
            sync_dir(dir)?;
        }
        Ok((manifest, version))
    }

    /// Durably appends `edits` with one write and one fsync. On failure the
    /// file is cut back to its previous length, so a half-written edit can't
    /// end up in the middle of the log.
    pub fn append(&mut self, edits: &[Edit]) -> Result<()> {
        let mut buf = Vec::with_capacity(edits.len() * RECORD_LEN);
        for edit in edits {
            encode(*edit, &mut buf);
        }
        match self
            .file
            .write_all(&buf)
            .and_then(|()| self.file.sync_data())
        {
            Ok(()) => {
                self.len += buf.len() as u64;
                Ok(())
            }
            Err(e) => {
                let _ = self.file.set_len(self.len);
                Err(e.into())
            }
        }
    }
}

fn encode(edit: Edit, out: &mut Vec<u8>) {
    let (tag, value) = match edit {
        Edit::AddTable { id, level } => (TAG_ADD_TABLE_BASE + level, id),
        Edit::RemoveTable(id) => (TAG_REMOVE_TABLE, id),
        Edit::SetLogNumber(n) => (TAG_SET_LOG_NUMBER, n),
        Edit::SetLastSequence(n) => (TAG_SET_LAST_SEQUENCE, n),
        Edit::Format(v) => (TAG_FORMAT, v),
    };
    let mut body = [0u8; RECORD_LEN - 4];
    body[0] = tag;
    body[1..].copy_from_slice(&value.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
}

/// `None` = bad checksum or unknown tag.
fn decode(rec: &[u8]) -> Option<Edit> {
    if crc32fast::hash(&rec[4..RECORD_LEN]) != read_u32(rec, 0) {
        return None;
    }
    let value = read_u64(rec, 5);
    match rec[4] {
        tag if (TAG_ADD_TABLE_BASE..TAG_ADD_TABLE_BASE + MAX_LEVELS as u8).contains(&tag) => {
            Some(Edit::AddTable {
                id: value,
                level: tag - TAG_ADD_TABLE_BASE,
            })
        }
        TAG_REMOVE_TABLE => Some(Edit::RemoveTable(value)),
        TAG_SET_LOG_NUMBER => Some(Edit::SetLogNumber(value)),
        TAG_SET_LAST_SEQUENCE => Some(Edit::SetLastSequence(value)),
        TAG_FORMAT => Some(Edit::Format(value)),
        _ => None,
    }
}

/// Rebuilds the `Version` and returns it with the length of the valid prefix.
fn replay(buf: &[u8]) -> Result<(Version, u64)> {
    let mut version = Version::default();
    let mut pos = 0;
    while buf.len() - pos >= RECORD_LEN {
        let rest = &buf[pos..];
        match decode(&rest[..RECORD_LEN]) {
            Some(edit) => {
                let is_format = matches!(edit, Edit::Format(_));
                if (pos == 0) != is_format {
                    return Err(Error::Corruption(if pos == 0 {
                        "manifest has no format record: written by lsmkv before M8 \
                         (format 1), which this build can't read"
                            .into()
                    } else {
                        format!("manifest offset {pos}: format record after the first")
                    }));
                }
                version
                    .apply(edit)
                    .map_err(|msg| Error::Corruption(format!("manifest offset {pos}: {msg}")))?;
                pos += RECORD_LEN;
            }
            None => {
                let after = rest.len() - RECORD_LEN;
                if after == 0 || rest.iter().all(|&b| b == 0) {
                    break;
                }
                return Err(Error::Corruption(format!(
                    "manifest record at offset {pos} is invalid but {after} bytes follow it"
                )));
            }
        }
    }
    Ok((version, pos as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reopen(dir: &Path) -> Version {
        Manifest::open(dir).unwrap().1
    }

    fn add(id: u64) -> Edit {
        Edit::AddTable { id, level: 0 }
    }

    fn ids(v: &Version) -> Vec<u64> {
        v.tables.keys().copied().collect()
    }

    #[test]
    fn fresh_manifest_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let (_, v) = Manifest::open(dir.path()).unwrap();
        assert_eq!(v, Version::default());
        let bytes = fs::read(dir.path().join(MANIFEST_FILE)).unwrap();
        assert_eq!(decode(&bytes), Some(Edit::Format(FORMAT_VERSION)));
        assert_eq!(bytes.len(), RECORD_LEN);
        assert_eq!(reopen(dir.path()), Version::default());
    }

    #[test]
    fn last_sequence_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[add(2), Edit::SetLastSequence(40)]).unwrap();
            m.append(&[add(3), Edit::SetLastSequence(95)]).unwrap();
        }
        assert_eq!(reopen(dir.path()).last_sequence, 95);
    }

    #[test]
    fn empty_manifest_from_a_crash_at_creation_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(MANIFEST_FILE), b"").unwrap();
        assert_eq!(reopen(dir.path()), Version::default());
        // ...and it now has its format record.
        let (mut m, _) = Manifest::open(dir.path()).unwrap();
        m.append(&[add(1)]).unwrap();
        assert_eq!(ids(&reopen(dir.path())), vec![1]);
    }

    #[test]
    fn edits_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[Edit::SetLogNumber(1)]).unwrap();
            m.append(&[add(2), Edit::SetLogNumber(3)]).unwrap();
            m.append(&[add(4), Edit::SetLogNumber(5)]).unwrap();
            m.append(&[Edit::RemoveTable(2)]).unwrap();
        }
        let v = reopen(dir.path());
        assert_eq!(ids(&v), vec![4]);
        assert_eq!(v.log_number, 5);
    }

    #[test]
    fn torn_tail_is_cut_and_later_edits_survive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(MANIFEST_FILE);
        {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[add(1), add(2)]).unwrap();
        }
        // Crash halfway through the second record.
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 5)
            .unwrap();
        {
            let (mut m, v) = Manifest::open(dir.path()).unwrap();
            assert_eq!(ids(&v), vec![1]);
            m.append(&[add(7)]).unwrap();
        }
        assert_eq!(ids(&reopen(dir.path())), vec![1, 7]);
    }

    #[test]
    fn bad_last_record_and_zero_tail_are_tolerated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(MANIFEST_FILE);
        {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[add(1), add(2)]).unwrap();
        }
        let good = fs::read(&path).unwrap();

        let mut bad_last = good.clone();
        *bad_last.last_mut().unwrap() ^= 0xFF;
        fs::write(&path, &bad_last).unwrap();
        assert_eq!(ids(&reopen(dir.path())), vec![1]);

        let mut zero_tail = good.clone();
        zero_tail.extend([0u8; 40]);
        fs::write(&path, &zero_tail).unwrap();
        assert_eq!(ids(&reopen(dir.path())), vec![1, 2]);
    }

    #[test]
    fn mid_manifest_corruption_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(MANIFEST_FILE);
        {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[add(1), add(2), add(3)]).unwrap();
        }
        // Records: format, add(1), add(2), add(3).
        let mut bytes = fs::read(&path).unwrap();
        bytes[2 * RECORD_LEN + 6] ^= 0xFF; // inside add(2)
        fs::write(&path, &bytes).unwrap();
        match Manifest::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("offset 26"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.map(|(_, v)| v)),
        }
    }

    #[test]
    fn impossible_histories_are_corruption() {
        let cases: [&[Edit]; 6] = [
            &[add(1), add(1)],
            &[Edit::RemoveTable(9)],
            &[Edit::SetLogNumber(5), Edit::SetLogNumber(4)],
            &[Edit::SetLastSequence(5), Edit::SetLastSequence(4)],
            &[Edit::Format(FORMAT_VERSION)],
            &[Edit::Format(1)],
        ];
        for edits in cases {
            let dir = tempfile::tempdir().unwrap();
            {
                let (mut m, _) = Manifest::open(dir.path()).unwrap();
                m.append(edits).unwrap();
            }
            assert!(
                matches!(Manifest::open(dir.path()), Err(Error::Corruption(_))),
                "{edits:?}"
            );
        }
    }

    #[test]
    fn level_out_of_range_is_rejected() {
        let mut v = Version::default();
        let too_deep = MAX_LEVELS as u8;
        assert!(v
            .apply(Edit::AddTable {
                id: 1,
                level: too_deep
            })
            .is_err());
        assert!(v
            .apply(Edit::AddTable {
                id: 1,
                level: too_deep - 1
            })
            .is_ok());
    }

    #[test]
    fn levels_roundtrip_and_moves_work() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[add(1), Edit::AddTable { id: 2, level: 3 }])
                .unwrap();
            // A "trivial move": same id, new level, in one batch.
            m.append(&[Edit::RemoveTable(1), Edit::AddTable { id: 1, level: 1 }])
                .unwrap();
        }
        let v = reopen(dir.path());
        assert_eq!(v.tables, BTreeMap::from([(1, 1), (2, 3)]));
    }

    #[test]
    fn pre_m8_manifests_are_refused() {
        // What M4-M7 wrote first on a fresh database: SetLogNumber(1), tag 3.
        let dir = tempfile::tempdir().unwrap();
        let mut body = [0u8; RECORD_LEN - 4];
        body[0] = TAG_SET_LOG_NUMBER;
        body[1..].copy_from_slice(&1u64.to_le_bytes());
        let mut rec = crc32fast::hash(&body).to_le_bytes().to_vec();
        rec.extend_from_slice(&body);
        fs::write(dir.path().join(MANIFEST_FILE), &rec).unwrap();
        match Manifest::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("before M8"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.map(|(_, v)| v)),
        }
    }
}
