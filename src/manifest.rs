//! MANIFEST: an append-only log of edits that says which files are live.
//!
//! ```text
//! record = [crc32 u32][tag u8][value u64]      (13 bytes; CRC covers tag + value)
//! tag 4        = Format(v): the first record; later ones only upgrade
//! tag 0x10 + L = AddTable(id) at level L (0 <= L < MAX_LEVELS)
//! tag 2        = RemoveTable(id)
//! tag 3        = SetLogNumber(n)
//! tag 5        = SetLastSequence(n)
//! tag 6        = Group(n): the next n records are one commit (format 4)
//! ```
//!
//! Putting the level in the tag keeps records fixed-size.
//!
//! A commit of several edits (a flush: add the table, retire the WAL, record
//! the last sequence; a compaction: remove the inputs, add the outputs) is
//! written as a `Group(n)` header and then its n edits, in one write, and
//! replay applies a group whole or not at all (DESIGN.md D28). Each record has
//! its own CRC, so without the header a power cut that tears the write would
//! leave a valid-looking prefix of the commit: a compaction with its inputs
//! removed and its outputs never added. The simulation test found exactly
//! that; a `kill -9` can't tear a write, so nothing else could.
//!
//! - `Format(v)`: the on-disk format of the whole database: manifest, WALs
//!   and tables. Format 2 (M8) added sequence numbers to WAL records and table
//!   entries. Directories from before M8 have no format record and are
//!   refused: misreading an old WAL as format 2 would look like a torn tail,
//!   and truncating it would silently lose data. Format 3 (M13) added batch
//!   records to the WAL. A format-2 database is upgraded on open by appending
//!   `Format(3)`; a format-2 build then refuses the directory (it accepts no
//!   format record but the first), instead of misreading a batch record as a
//!   torn tail and cutting it off.
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

use std::io::Write;
use std::path::Path;

use crate::codec::{read_u32, read_u64};
use crate::error::{Error, Result};
use crate::vfs::{Fs, RealFs, WritableFile};

pub const MANIFEST_FILE: &str = "MANIFEST";
const RECORD_LEN: usize = 4 + 1 + 8;
const TAG_REMOVE_TABLE: u8 = 2;
const TAG_SET_LOG_NUMBER: u8 = 3;
const TAG_FORMAT: u8 = 4;
const TAG_SET_LAST_SEQUENCE: u8 = 5;
const TAG_GROUP: u8 = 6;
/// More edits than any commit makes; a bigger group count is corruption.
const MAX_GROUP: u64 = 1 << 16;
const TAG_ADD_TABLE_BASE: u8 = 0x10;

/// The on-disk format this build writes. It also reads (and upgrades)
/// everything from `OLDEST_FORMAT` on. 3 added WAL batches (M13); 4 added
/// manifest groups (M15).
pub const FORMAT_VERSION: u64 = 4;
pub const OLDEST_FORMAT: u64 = 2;

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
    /// Written first by `Manifest::open` on a fresh database, and again
    /// (with a higher number) when it upgrades an older one.
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
    /// The on-disk format, from the newest `Format` record.
    pub format: u64,
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
                if !(OLDEST_FORMAT..=FORMAT_VERSION).contains(&v) {
                    return Err(format!(
                        "format {v}, but this build reads formats {OLDEST_FORMAT} to {FORMAT_VERSION}"
                    ));
                }
                if v <= self.format {
                    return Err(format!("format went from {} to {v}", self.format));
                }
                self.format = v;
            }
        }
        Ok(())
    }
}

pub struct Manifest {
    file: Box<dyn WritableFile>,
    /// Bytes of valid records, used to undo a failed append.
    len: u64,
}

impl Manifest {
    /// Opens the manifest in `dir`, creating one for a fresh database, and
    /// returns the current `Version`.
    pub fn open(dir: &Path) -> Result<(Self, Version)> {
        Self::open_in(&RealFs, dir)
    }

    /// `open`, on `fs`.
    pub fn open_in(fs: &dyn Fs, dir: &Path) -> Result<(Self, Version)> {
        let path = dir.join(MANIFEST_FILE);
        let buf = match fs.read(&path) {
            Ok(buf) => buf,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };

        let (mut version, valid_len) = replay(&buf)?;
        let mut file = fs.open_append(&path)?;
        if valid_len < buf.len() as u64 {
            // Cut the torn tail so new edits aren't appended after garbage.
            file.set_len(valid_len)?;
            file.sync()?;
        }
        let mut manifest = Self {
            file,
            len: valid_len,
        };
        if valid_len == 0 {
            // Fresh, or a crash before the first record was durable: either
            // way nothing was ever committed, so start the history now.
            manifest.append(&[Edit::Format(FORMAT_VERSION)])?;
            fs.sync_dir(dir)?;
            version.format = FORMAT_VERSION;
        } else if version.format < FORMAT_VERSION {
            // Upgrade before anything new is written: from here on, an
            // older build refuses this directory.
            manifest.append(&[Edit::Format(FORMAT_VERSION)])?;
            version.format = FORMAT_VERSION;
        }
        Ok((manifest, version))
    }

    /// Durably appends `edits` with one write and one fsync. On failure the
    /// file is cut back to its previous length, so a half-written edit can't
    /// end up in the middle of the log.
    pub fn append(&mut self, edits: &[Edit]) -> Result<()> {
        let mut buf = Vec::with_capacity((edits.len() + 1) * RECORD_LEN);
        if edits.len() > 1 {
            encode_raw(TAG_GROUP, edits.len() as u64, &mut buf);
        }
        for edit in edits {
            encode(*edit, &mut buf);
        }
        match self.file.write_all(&buf).and_then(|()| self.file.sync()) {
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
    encode_raw(tag, value, out);
}

fn encode_raw(tag: u8, value: u64, out: &mut Vec<u8>) {
    let mut body = [0u8; RECORD_LEN - 4];
    body[0] = tag;
    body[1..].copy_from_slice(&value.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
}

/// `None` = bad checksum or unknown tag.
/// One record: an edit, or a group header.
enum Rec {
    Edit(Edit),
    Group(u64),
}

fn decode_rec(rec: &[u8]) -> Option<Rec> {
    if crc32fast::hash(&rec[4..RECORD_LEN]) != read_u32(rec, 0) {
        return None;
    }
    if rec[4] == TAG_GROUP {
        return Some(Rec::Group(read_u64(rec, 5)));
    }
    decode(rec).map(Rec::Edit)
}

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
        // The edits of the next commit, and how many bytes it takes; or the
        // offset (within `rest`) of a bad record.
        let parsed: std::result::Result<(Vec<Edit>, usize), usize> =
            match decode_rec(&rest[..RECORD_LEN]) {
                Some(Rec::Edit(edit)) => Ok((vec![edit], RECORD_LEN)),
                Some(Rec::Group(n)) if (2..=MAX_GROUP).contains(&n) => {
                    let len = (n as usize + 1) * RECORD_LEN;
                    if rest.len() < len {
                        // The commit was cut off: a torn tail.
                        break;
                    }
                    let mut edits = Vec::with_capacity(n as usize);
                    let mut bad = None;
                    for i in 1..=n as usize {
                        match decode_rec(&rest[i * RECORD_LEN..(i + 1) * RECORD_LEN]) {
                            Some(Rec::Edit(edit)) => edits.push(edit),
                            _ => {
                                bad = Some(i * RECORD_LEN);
                                break;
                            }
                        }
                    }
                    match bad {
                        None => Ok((edits, len)),
                        Some(at) => Err(at),
                    }
                }
                _ => Err(0),
            };
        let (edits, len) = match parsed {
            Ok(commit) => commit,
            Err(at) => {
                // A bad record is a torn tail if it's the last one, or if
                // everything from it on is zeros. Anything else is corruption.
                let from_bad = &rest[at..];
                if from_bad.len() <= RECORD_LEN || from_bad.iter().all(|&b| b == 0) {
                    break;
                }
                return Err(Error::Corruption(format!(
                    "manifest record at offset {} is invalid but {} bytes follow it",
                    pos + at,
                    from_bad.len() - RECORD_LEN
                )));
            }
        };
        if pos == 0 && !matches!(edits.as_slice(), [Edit::Format(_)]) {
            return Err(Error::Corruption(
                "manifest has no format record: written by lsmkv before M8 \
                 (format 1), which this build can't read"
                    .into(),
            ));
        }
        for edit in edits {
            version
                .apply(edit)
                .map_err(|msg| Error::Corruption(format!("manifest offset {pos}: {msg}")))?;
        }
        pos += len;
    }
    Ok((version, pos as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};

    fn reopen(dir: &Path) -> Version {
        Manifest::open(dir).unwrap().1
    }

    fn add(id: u64) -> Edit {
        Edit::AddTable { id, level: 0 }
    }

    fn ids(v: &Version) -> Vec<u64> {
        v.tables.keys().copied().collect()
    }

    /// A new database's version: nothing live yet, current format.
    fn fresh() -> Version {
        Version {
            format: FORMAT_VERSION,
            ..Version::default()
        }
    }

    #[test]
    fn fresh_manifest_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let (_, v) = Manifest::open(dir.path()).unwrap();
        assert_eq!(v, fresh());
        let bytes = fs::read(dir.path().join(MANIFEST_FILE)).unwrap();
        assert_eq!(decode(&bytes), Some(Edit::Format(FORMAT_VERSION)));
        assert_eq!(bytes.len(), RECORD_LEN);
        assert_eq!(reopen(dir.path()), fresh());
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
        assert_eq!(reopen(dir.path()), fresh());
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
            m.append(&[add(1)]).unwrap();
            m.append(&[add(2)]).unwrap();
        }
        // Crash halfway through the second commit.
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
            m.append(&[add(1)]).unwrap();
            m.append(&[add(2)]).unwrap();
        }
        let good = fs::read(&path).unwrap();

        // Two separate commits: a bad last record loses only the second.
        let mut bad_last = good.clone();
        *bad_last.last_mut().unwrap() ^= 0xFF;
        fs::write(&path, &bad_last).unwrap();
        assert_eq!(ids(&reopen(dir.path())), vec![1]);

        // One commit of both: a bad last record loses all of it.
        fs::write(&path, &good[..RECORD_LEN]).unwrap(); // just the format record
        {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[add(1), add(2)]).unwrap();
        }
        let mut one_commit = fs::read(&path).unwrap();
        *one_commit.last_mut().unwrap() ^= 0xFF;
        fs::write(&path, &one_commit).unwrap();
        assert_eq!(ids(&reopen(dir.path())), Vec::<u64>::new());
        fs::write(&path, &good).unwrap();

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
    fn format_2_is_upgraded_and_newer_formats_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        // A format-2 manifest, as M8–M12 wrote it.
        let mut old = Vec::new();
        encode(Edit::Format(2), &mut old);
        encode(add(7), &mut old);
        fs::write(&path, &old).unwrap();

        let (_, v) = Manifest::open(dir.path()).unwrap();
        assert_eq!((v.format, v.tables.len()), (FORMAT_VERSION, 1));
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..old.len()], &old[..], "history kept");
        assert_eq!(
            decode(&bytes[old.len()..]),
            Some(Edit::Format(FORMAT_VERSION))
        );
        // Reopening doesn't upgrade again.
        Manifest::open(dir.path()).unwrap();
        assert_eq!(fs::read(&path).unwrap().len(), bytes.len());

        // A format this build doesn't know is refused.
        let mut newer = Vec::new();
        encode(Edit::Format(FORMAT_VERSION + 1), &mut newer);
        fs::write(&path, &newer).unwrap();
        assert!(matches!(
            Manifest::open(dir.path()),
            Err(Error::Corruption(_))
        ));
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

    /// A power cut can tear a commit anywhere, and then the disk keeps a
    /// prefix of it. Replay must apply a multi-edit commit whole or not at
    /// all: a compaction commit cut after its RemoveTable edits but before
    /// its AddTable edits would drop data. (Found by the simulation test,
    /// D28: a kill -9 can't tear a write, so nothing else could.)
    #[test]
    fn a_commit_torn_anywhere_applies_whole_or_not_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(MANIFEST_FILE);
        let before = {
            let (mut m, _) = Manifest::open(dir.path()).unwrap();
            m.append(&[add(1), add(2), Edit::SetLogNumber(3)]).unwrap();
            drop(m);
            let (_, v) = Manifest::open(dir.path()).unwrap();
            v
        };
        let start = fs::read(&path).unwrap().len();
        // A compaction's commit: inputs out, outputs in.
        let commit = [
            Edit::RemoveTable(1),
            Edit::RemoveTable(2),
            add(4),
            add(5),
            Edit::SetLastSequence(9),
        ];
        let (mut m, _) = Manifest::open(dir.path()).unwrap();
        m.append(&commit).unwrap();
        drop(m);
        let full = fs::read(&path).unwrap();
        let after = {
            let mut v = before.clone();
            for e in commit {
                v.apply(e).unwrap();
            }
            v
        };
        for cut in start..=full.len() {
            fs::write(&path, &full[..cut]).unwrap();
            let v = reopen(dir.path());
            assert!(
                v == before || v == after,
                "cut at {cut} applied part of a commit: {v:?}"
            );
            if cut == full.len() {
                assert_eq!(v, after);
            }
        }
    }
}
