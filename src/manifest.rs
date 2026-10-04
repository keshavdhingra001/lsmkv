//! MANIFEST: an append-only log of edits that says which files are live.
//!
//! ```text
//! record = [crc32 u32][tag u8][value u64]      (13 bytes; CRC covers tag + value)
//! tag 0x10 + L = AddTable(id) at level L (0 <= L < MAX_LEVELS)
//! tag 2        = RemoveTable(id)
//! tag 3        = SetLogNumber(n)
//! tag 1        = AddTable(id) at level 0 (written by M4; still read)
//! ```
//!
//! Putting the level in the tag keeps records fixed-size and lets manifests
//! written before levels existed still open.
//!
//! - Replaying every edit in order rebuilds the current `Version`.
//! - `SetLogNumber(n)`: WALs numbered below `n` are already in tables, so
//!   they're obsolete; WALs numbered `n` and above are live and get replayed.
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
const TAG_ADD_TABLE_L0_LEGACY: u8 = 1;
const TAG_REMOVE_TABLE: u8 = 2;
const TAG_SET_LOG_NUMBER: u8 = 3;
const TAG_ADD_TABLE_BASE: u8 = 0x10;

/// Levels 0..MAX_LEVELS. Level MAX_LEVELS - 1 is the bottom.
pub const MAX_LEVELS: usize = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edit {
    AddTable { id: u64, level: u8 },
    RemoveTable(u64),
    SetLogNumber(u64),
}

/// The set of live files, as of the last manifest edit.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Version {
    /// Live table id -> level. Within level 0, higher id = newer data.
    pub tables: BTreeMap<u64, u8>,
    /// WALs numbered >= this are live. 0 = fresh database, no log yet.
    pub log_number: u64,
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
    /// Opens the manifest in `dir`, creating an empty one for a fresh
    /// database, and returns the current `Version`.
    pub fn open(dir: &Path) -> Result<(Self, Version)> {
        let path = dir.join(MANIFEST_FILE);
        let buf = match fs::read(&path) {
            Ok(buf) => buf,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let file = OpenOptions::new().create(true).append(true).open(&path)?;
                file.sync_all()?;
                sync_dir(dir)?;
                return Ok((Self { file, len: 0 }, Version::default()));
            }
            Err(e) => return Err(e.into()),
        };

        let (version, valid_len) = replay(&buf)?;
        let file = OpenOptions::new().append(true).open(&path)?;
        if valid_len < buf.len() as u64 {
            // Cut the torn tail so new edits aren't appended after garbage.
            file.set_len(valid_len)?;
            file.sync_all()?;
        }
        Ok((
            Self {
                file,
                len: valid_len,
            },
            version,
        ))
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
        TAG_ADD_TABLE_L0_LEGACY => Some(Edit::AddTable {
            id: value,
            level: 0,
        }),
        tag if (TAG_ADD_TABLE_BASE..TAG_ADD_TABLE_BASE + MAX_LEVELS as u8).contains(&tag) => {
            Some(Edit::AddTable {
                id: value,
                level: tag - TAG_ADD_TABLE_BASE,
            })
        }
        TAG_REMOVE_TABLE => Some(Edit::RemoveTable(value)),
        TAG_SET_LOG_NUMBER => Some(Edit::SetLogNumber(value)),
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
        assert!(dir.path().join(MANIFEST_FILE).exists());
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
        let mut bytes = fs::read(&path).unwrap();
        bytes[RECORD_LEN + 6] ^= 0xFF; // inside record 2 of 3
        fs::write(&path, &bytes).unwrap();
        match Manifest::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("offset 13"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.map(|(_, v)| v)),
        }
    }

    #[test]
    fn impossible_histories_are_corruption() {
        let cases: [&[Edit]; 3] = [
            &[add(1), add(1)],
            &[Edit::RemoveTable(9)],
            &[Edit::SetLogNumber(5), Edit::SetLogNumber(4)],
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
    fn m4_manifests_still_open_with_tables_at_level_0() {
        // Hand-encode a legacy tag-1 record, as M4 wrote it.
        let dir = tempfile::tempdir().unwrap();
        let mut body = [0u8; RECORD_LEN - 4];
        body[0] = TAG_ADD_TABLE_L0_LEGACY;
        body[1..].copy_from_slice(&42u64.to_le_bytes());
        let mut rec = crc32fast::hash(&body).to_le_bytes().to_vec();
        rec.extend_from_slice(&body);
        fs::write(dir.path().join(MANIFEST_FILE), &rec).unwrap();
        assert_eq!(reopen(dir.path()).tables, BTreeMap::from([(42, 0)]));
    }
}
