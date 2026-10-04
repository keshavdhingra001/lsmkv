//! Public database handle: ties together the WAL, memtable, SSTables and manifest.
//!
//! Files in the database directory:
//! - `MANIFEST`: which tables and which WALs are live (see `manifest.rs`).
//! - `NNNNNN.log`: write-ahead logs. Live if numbered >= the manifest's log number.
//! - `NNNNNN.sst`: SSTables. Live if listed in the manifest. Higher number = newer.
//!
//! Logs and tables share one number sequence, so "higher = newer" holds across
//! both. Anything else the engine owns (orphans from a crash mid-flush, `.tmp`
//! files) is deleted on open. Unrecognized files are left alone.

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::fsutil::sync_dir;
use crate::manifest::{Edit, Manifest, Version, MAX_LEVELS};
use crate::memtable::{Entry, MemTable};
use crate::sstable::{SstReader, SstWriter};
use crate::wal::{Record, Wal};

#[derive(Debug, Clone)]
pub struct Options {
    /// Flush the memtable to an SSTable once its approximate size reaches this.
    pub memtable_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size: 4 << 20,
        }
    }
}

/// Point-in-time numbers for the REPL and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    /// Total live tables across all levels.
    pub tables: usize,
    /// Tables per level, index = level.
    pub level_files: Vec<usize>,
    /// Bytes per level, index = level.
    pub level_bytes: Vec<u64>,
    pub log_number: u64,
}

/// A live table: its file number plus an open reader.
struct Table {
    id: u64,
    reader: SstReader,
}

impl Table {
    fn smallest(&self) -> &[u8] {
        self.reader.smallest_key().unwrap_or_default()
    }

    fn largest(&self) -> &[u8] {
        self.reader.largest_key().unwrap_or_default()
    }
}

pub struct Db {
    dir: PathBuf,
    opts: Options,
    memtable: MemTable,
    wal: Wal,
    wal_number: u64,
    manifest: Manifest,
    version: Version,
    /// Live tables by level. Level 0: newest first, ranges may overlap.
    /// Levels 1+: sorted by key, ranges never overlap.
    levels: Vec<Vec<Table>>,
    /// Next unused file number, for both logs and tables.
    next_file: u64,
    /// Set after a WAL or manifest write fails; see `Error::Poisoned`.
    poisoned: Option<String>,
    /// Test-only crash injection: the named failpoint returns an error.
    #[cfg(test)]
    fail_at: Option<&'static str>,
}

impl Db {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(dir, Options::default())
    }

    /// Opens the database in `dir`, creating it if needed, then recovers:
    /// 1. Replay the manifest to learn the live tables and log number.
    /// 2. Delete files the manifest says aren't live (leftovers from a crash).
    /// 3. Replay the live WALs, oldest first, into a fresh memtable.
    pub fn open_with(dir: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let (mut manifest, mut version) = Manifest::open(&dir)?;

        remove_obsolete_files(&dir, &version)?;
        let live_logs: Vec<u64> = list_files(&dir)?
            .into_iter()
            .filter_map(|(f, _)| match f {
                DbFile::Log(n) => Some(n),
                _ => None,
            })
            .collect();

        let mut next_file = 1 + version
            .tables
            .keys()
            .chain(&live_logs)
            .chain([&version.log_number])
            .copied()
            .max()
            .unwrap_or(0);

        let levels = open_levels(&dir, &version)?;

        let mut memtable = MemTable::new();
        for (i, &n) in live_logs.iter().enumerate() {
            let path = log_path(&dir, n);
            let replay = Wal::replay(&path)?;
            for rec in replay.records {
                apply(&mut memtable, rec);
            }
            // Only the newest log gets appended to, so only it needs its torn
            // tail cut off (new writes must not land after garbage).
            let is_active = i + 1 == live_logs.len();
            if is_active && fs::metadata(&path)?.len() > replay.valid_len {
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(replay.valid_len)?;
                f.sync_all()?;
            }
        }

        let wal_number = match live_logs.last() {
            Some(&n) => n,
            None if version.log_number > 0 => version.log_number,
            None => {
                // Fresh database: record its first log in the manifest.
                let n = next_file;
                next_file += 1;
                let edit = Edit::SetLogNumber(n);
                manifest.append(&[edit])?;
                version.apply(edit).map_err(Error::Corruption)?;
                n
            }
        };
        let wal = Wal::open(&log_path(&dir, wal_number))?;
        sync_dir(&dir)?;

        Ok(Self {
            dir,
            opts,
            memtable,
            wal,
            wal_number,
            manifest,
            version,
            levels,
            next_file,
            poisoned: None,
            #[cfg(test)]
            fail_at: None,
        })
    }

    /// Durably writes `key = value`. If this write fills the memtable, it also
    /// flushes. An `Err` from that flush still leaves the write itself durable
    /// in the WAL.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(Record::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(Record::Delete { key: key.to_vec() })
    }

    /// Newest data first: memtable, then every level-0 table (newest first),
    /// then at most one table per deeper level. The first hit wins, and a
    /// tombstone hit means "deleted": older data is not consulted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Some(Entry::Value(v)) => return Ok(Some(v.clone())),
            Some(Entry::Tombstone) => return Ok(None),
            None => {}
        }
        let level0 = self.levels[0].iter();
        let deeper = self.levels[1..]
            .iter()
            .filter_map(|level| table_for_key(level, key));
        for table in level0.chain(deeper) {
            match table.reader.get(key)? {
                Some(Entry::Value(v)) => return Ok(Some(v)),
                Some(Entry::Tombstone) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    /// Writes the memtable to a new SSTable and starts a fresh WAL.
    ///
    /// Steps, ordered so a crash after any of them loses nothing:
    /// 1. Write the table (tmp + fsync + rename). Unlisted, so a crash leaves an orphan.
    /// 2. Create the new WAL. Empty, so a crash leaves a harmless extra log.
    /// 3. **Commit point:** one manifest write adds the table and retires the
    ///    old WAL. Before this, recovery replays the old WAL; after it, the table
    ///    is live.
    /// 4. Switch in-memory state, then delete the now-obsolete WAL (best effort;
    ///    the next open retries).
    ///
    /// A failure before the commit point leaves the database usable; the
    /// orphans are cleaned up on the next open. A failure at or after it
    /// poisons the database (see `Error::Poisoned`).
    pub fn flush(&mut self) -> Result<()> {
        self.check_writable()?;
        if self.memtable.is_empty() {
            return Ok(());
        }
        let table_id = self.next_file;
        let log_id = table_id + 1;
        // Reserve both numbers up front so a failed flush never reuses one
        // that an orphan file on disk might still hold.
        self.next_file += 2;

        let table_path = table_path(&self.dir, table_id);
        let mut writer = SstWriter::create(&table_path)?;
        for (key, entry) in self.memtable.iter() {
            writer.add(key, entry)?;
        }
        writer.finish()?;
        let reader = SstReader::open(&table_path)?;
        self.failpoint("flush:after_table")?;

        let new_wal = Wal::open(&log_path(&self.dir, log_id))?;
        sync_dir(&self.dir)?;
        self.failpoint("flush:after_new_log")?;

        // Commit point. If the manifest write fails, it may still have reached
        // the disk, in which case the current WAL is now obsolete. Writing
        // more to it would lose data on the next open, so poison instead.
        let edits = [
            Edit::AddTable {
                id: table_id,
                level: 0,
            },
            Edit::SetLogNumber(log_id),
        ];
        let committed = self
            .failpoint("flush:manifest")
            .and_then(|()| self.manifest.append(&edits))
            .and_then(|()| self.failpoint("flush:after_manifest"));
        if let Err(e) = committed {
            return Err(self.poison(e));
        }
        for edit in edits {
            self.version.apply(edit).map_err(Error::Corruption)?;
        }

        self.levels[0].insert(
            0,
            Table {
                id: table_id,
                reader,
            },
        );
        self.memtable = MemTable::new();
        self.wal = new_wal;
        self.wal_number = log_id;
        let _ = remove_obsolete_files(&self.dir, &self.version);
        Ok(())
    }

    pub fn stats(&self) -> Stats {
        Stats {
            memtable_entries: self.memtable.len(),
            memtable_bytes: self.memtable.approx_size(),
            tables: self.levels.iter().map(Vec::len).sum(),
            level_files: self.levels.iter().map(Vec::len).collect(),
            level_bytes: self
                .levels
                .iter()
                .map(|l| l.iter().map(|t| t.reader.file_size()).sum())
                .collect(),
            log_number: self.wal_number,
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn write(&mut self, rec: Record) -> Result<()> {
        self.check_writable()?;
        // A failed append or fsync may leave a partial record in the log, and
        // later appends would land after it (mid-log corruption on reopen).
        if let Err(e) = self.wal.append(&rec).and_then(|()| self.wal.sync()) {
            return Err(self.poison(e));
        }
        apply(&mut self.memtable, rec);
        if self.memtable.approx_size() >= self.opts.memtable_size {
            self.flush()?;
        }
        Ok(())
    }

    fn check_writable(&self) -> Result<()> {
        match &self.poisoned {
            Some(why) => Err(Error::Poisoned(why.clone())),
            None => Ok(()),
        }
    }

    /// Marks the database read-only and passes the original error through.
    fn poison(&mut self, e: Error) -> Error {
        self.poisoned = Some(e.to_string());
        e
    }

    #[cfg(test)]
    fn failpoint(&self, name: &'static str) -> Result<()> {
        if self.fail_at == Some(name) {
            return Err(Error::Io(std::io::Error::other(format!(
                "failpoint {name}"
            ))));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn failpoint(&self, _name: &'static str) -> Result<()> {
        Ok(())
    }
}

fn apply(memtable: &mut MemTable, rec: Record) {
    match rec {
        Record::Put { key, value } => memtable.put(&key, &value),
        Record::Delete { key } => memtable.delete(&key),
    }
}

fn log_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("{n:06}.log"))
}

fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst"))
}

/// In a level >= 1 (sorted, non-overlapping), the only table that can hold `key`.
fn table_for_key<'a>(level: &'a [Table], key: &[u8]) -> Option<&'a Table> {
    let i = level.partition_point(|t| t.largest() < key);
    level.get(i).filter(|t| t.smallest() <= key)
}

/// Opens every live table and arranges them by level, checking that levels
/// 1+ are non-overlapping (the invariant `table_for_key` relies on).
fn open_levels(dir: &Path, version: &Version) -> Result<Vec<Vec<Table>>> {
    let mut levels: Vec<Vec<Table>> = (0..MAX_LEVELS).map(|_| Vec::new()).collect();
    for (&id, &level) in &version.tables {
        let reader = open_table(dir, id)?;
        levels[level as usize].push(Table { id, reader });
    }
    levels[0].sort_by_key(|t| std::cmp::Reverse(t.id));
    for (n, level) in levels.iter_mut().enumerate().skip(1) {
        if let Some(t) = level.iter().find(|t| t.reader.entry_count() == 0) {
            return Err(Error::Corruption(format!(
                "table {} at level {n} is empty",
                t.id
            )));
        }
        level.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        for pair in level.windows(2) {
            if pair[0].largest() >= pair[1].smallest() {
                return Err(Error::Corruption(format!(
                    "tables {} and {} overlap at level {n}",
                    pair[0].id, pair[1].id
                )));
            }
        }
    }
    Ok(levels)
}

fn open_table(dir: &Path, id: u64) -> Result<SstReader> {
    SstReader::open(&table_path(dir, id)).map_err(|e| match e {
        Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => Error::Corruption(format!(
            "manifest lists table {id}, but {id:06}.sst is missing"
        )),
        other => other,
    })
}

/// Files the engine owns, recognized by name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DbFile {
    Log(u64),
    Table(u64),
    Temp,
}

fn classify(name: &str) -> Option<DbFile> {
    if name.ends_with(".tmp") {
        return Some(DbFile::Temp);
    }
    if let Some(stem) = name.strip_suffix(".log") {
        return stem.parse().ok().map(DbFile::Log);
    }
    if let Some(stem) = name.strip_suffix(".sst") {
        return stem.parse().ok().map(DbFile::Table);
    }
    None
}

/// Engine-owned files in `dir`, sorted (logs by number, then tables, then temps).
fn list_files(dir: &Path) -> Result<Vec<(DbFile, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(kind) = entry.file_name().to_str().and_then(classify) {
            out.push((kind, entry.path()));
        }
    }
    out.sort();
    Ok(out)
}

/// Deletes WALs below the log number, tables not in the version, and temp files.
fn remove_obsolete_files(dir: &Path, version: &Version) -> Result<()> {
    let mut removed = false;
    for (kind, path) in list_files(dir)? {
        let obsolete = match kind {
            DbFile::Log(n) => n < version.log_number,
            DbFile::Table(id) => !version.tables.contains_key(&id),
            DbFile::Temp => true,
        };
        if obsolete {
            fs::remove_file(&path)?;
            removed = true;
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::Rng;
    use std::collections::BTreeMap;

    fn small() -> Options {
        Options {
            memtable_size: 1024,
        }
    }

    fn files(dir: &Path) -> Vec<DbFile> {
        list_files(dir)
            .unwrap()
            .into_iter()
            .map(|(f, _)| f)
            .collect()
    }

    fn only_log(dir: &Path) -> PathBuf {
        let logs: Vec<PathBuf> = list_files(dir)
            .unwrap()
            .into_iter()
            .filter(|(f, _)| matches!(f, DbFile::Log(_)))
            .map(|(_, p)| p)
            .collect();
        assert_eq!(logs.len(), 1, "expected exactly one log: {logs:?}");
        logs.into_iter().next().unwrap()
    }

    fn key(i: usize) -> Vec<u8> {
        format!("key{i:05}").into_bytes()
    }

    #[test]
    fn writes_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
            db.delete(b"a").unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn open_refuses_mid_log_corruption() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
        }
        let wal_path = only_log(dir.path());
        let mut bytes = fs::read(&wal_path).unwrap();
        bytes[crate::wal::HEADER_LEN] ^= 0xFF; // corrupt the first record's key
        fs::write(&wal_path, &bytes).unwrap();

        assert!(matches!(Db::open(dir.path()), Err(Error::Corruption(_))));
        // The log must be left untouched for a human to inspect.
        assert_eq!(fs::read(&wal_path).unwrap(), bytes);
    }

    #[test]
    fn writes_after_torn_tail_are_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
        }
        // Crash mid-write of "b".
        let wal_path = only_log(dir.path());
        let len = fs::metadata(&wal_path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap()
            .set_len(len - 2)
            .unwrap();

        {
            let mut db = Db::open(dir.path()).unwrap();
            assert_eq!(db.get(b"b").unwrap(), None);
            db.put(b"c", b"3").unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn fresh_db_layout() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(files(dir.path()), vec![DbFile::Log(1)]);
        assert!(dir.path().join(crate::manifest::MANIFEST_FILE).exists());
        assert_eq!(db.stats().log_number, 1);
    }

    #[test]
    fn automatic_flushes_keep_every_key_readable() {
        let dir = tempfile::tempdir().unwrap();
        let n = 2000;
        {
            let mut db = Db::open_with(dir.path(), small()).unwrap();
            for i in 0..n {
                db.put(&key(i), format!("v{i}").as_bytes()).unwrap();
            }
            assert!(db.stats().tables > 5, "{:?}", db.stats());
            for i in 0..n {
                assert_eq!(db.get(&key(i)).unwrap(), Some(format!("v{i}").into_bytes()));
            }
        }
        let db = Db::open_with(dir.path(), small()).unwrap();
        for i in 0..n {
            assert_eq!(db.get(&key(i)).unwrap(), Some(format!("v{i}").into_bytes()));
        }
        // Old logs are deleted after each flush.
        let logs = files(dir.path())
            .into_iter()
            .filter(|f| matches!(f, DbFile::Log(_)))
            .count();
        assert_eq!(logs, 1);
    }

    #[test]
    fn newer_data_shadows_older_tables() {
        let dir = tempfile::tempdir().unwrap();
        let check = |db: &Db| {
            assert_eq!(db.get(b"a").unwrap(), None, "a: tombstone in newer table");
            assert_eq!(db.get(b"b").unwrap(), None, "b: tombstone in memtable/log");
            assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()), "c: overwritten");
        };
        {
            let mut db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"1").unwrap();
            db.put(b"c", b"1").unwrap();
            db.flush().unwrap();
            db.put(b"c", b"3").unwrap();
            db.delete(b"a").unwrap();
            db.flush().unwrap();
            db.delete(b"b").unwrap(); // stays in the memtable
            assert_eq!(db.stats().tables, 2);
            check(&db);
        }
        check(&Db::open(dir.path()).unwrap());
    }

    #[test]
    fn flushing_an_empty_memtable_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        db.flush().unwrap();
        assert_eq!(db.stats().tables, 0);
        assert_eq!(files(dir.path()), vec![DbFile::Log(1)]);
    }

    #[test]
    fn open_removes_crash_leftovers_but_not_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.flush().unwrap(); // table 2, log 3; log 1 is obsolete
        }
        for junk in ["000001.log", "000950.sst", "000951.sst.tmp"] {
            fs::write(dir.path().join(junk), b"junk").unwrap();
        }
        fs::write(dir.path().join("notes.txt"), b"mine").unwrap();

        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(files(dir.path()), vec![DbFile::Log(3), DbFile::Table(2)]);
        assert!(dir.path().join("notes.txt").exists());
    }

    #[test]
    fn missing_live_table_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.flush().unwrap();
        }
        fs::remove_file(dir.path().join("000002.sst")).unwrap();
        match Db::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("000002.sst"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.err()),
        }
    }

    /// Writes a table file directly and registers it at `level`, bypassing
    /// flush/compaction, so tests can set up exact level layouts.
    fn place_table(dir: &Path, id: u64, level: u8, entries: &[(&str, Option<&str>)]) {
        let mut w = SstWriter::create(&table_path(dir, id)).unwrap();
        for (k, v) in entries {
            let e = match v {
                Some(v) => Entry::Value(v.as_bytes().to_vec()),
                None => Entry::Tombstone,
            };
            w.add(k.as_bytes(), &e).unwrap();
        }
        w.finish().unwrap();
        let (mut m, _) = Manifest::open(dir).unwrap();
        m.append(&[Edit::AddTable { id, level }]).unwrap();
    }

    #[test]
    fn reads_walk_levels_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        // Oldest data at the bottom; each level up overrides some keys.
        place_table(
            d,
            10,
            3,
            &[
                ("a", Some("L3")),
                ("b", Some("L3")),
                ("c", Some("L3")),
                ("z", Some("L3")),
            ],
        );
        place_table(d, 11, 2, &[("b", Some("L2")), ("d", Some("L2"))]);
        place_table(d, 12, 2, &[("m", None), ("n", Some("L2"))]);
        place_table(d, 13, 1, &[("c", None), ("m", Some("L1"))]);
        place_table(d, 14, 0, &[("a", Some("L0-old")), ("d", Some("L0-old"))]);
        place_table(d, 15, 0, &[("a", Some("L0-new"))]);

        let mut db = Db::open(d).unwrap();
        db.put(b"z", b"mem").unwrap();
        let get = |db: &Db, k: &str| {
            db.get(k.as_bytes())
                .unwrap()
                .map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(get(&db, "a").as_deref(), Some("L0-new"), "newest L0 wins");
        assert_eq!(get(&db, "b").as_deref(), Some("L2"), "L2 over L3");
        assert_eq!(get(&db, "c"), None, "L1 tombstone hides L3");
        assert_eq!(get(&db, "d").as_deref(), Some("L0-old"), "L0 over L2");
        assert_eq!(get(&db, "m").as_deref(), Some("L1"), "L1 over L2 tombstone");
        assert_eq!(get(&db, "n").as_deref(), Some("L2"));
        assert_eq!(get(&db, "z").as_deref(), Some("mem"), "memtable over all");
        assert_eq!(get(&db, "e"), None);
        assert_eq!(db.stats().level_files, vec![2, 1, 2, 1, 0, 0, 0]);
    }

    #[test]
    fn overlapping_tables_in_a_deep_level_are_corruption() {
        let dir = tempfile::tempdir().unwrap();
        place_table(dir.path(), 10, 1, &[("a", Some("1")), ("m", Some("1"))]);
        place_table(dir.path(), 11, 1, &[("k", Some("2")), ("z", Some("2"))]);
        match Db::open(dir.path()) {
            Err(Error::Corruption(msg)) => assert!(msg.contains("overlap at level 1"), "{msg}"),
            other => panic!("expected Corruption, got {:?}", other.err()),
        }
    }

    const FLUSH_FAILPOINTS: [&str; 4] = [
        "flush:after_table",
        "flush:after_new_log",
        "flush:manifest",
        "flush:after_manifest",
    ];

    /// Writes key(0), key(1), ... until a put fails (the flush hit the
    /// failpoint). Returns how many keys were written; the failing key's own
    /// WAL record is durable, so it counts too.
    fn write_until_failpoint(db: &mut Db, fp: &'static str) -> usize {
        db.fail_at = Some(fp);
        for i in 0..100_000 {
            if db.put(&key(i), &key(i)).is_err() {
                return i + 1;
            }
        }
        panic!("{fp} never triggered");
    }

    fn assert_keys(db: &Db, range: std::ops::Range<usize>, ctx: &str) {
        for i in range {
            assert_eq!(db.get(&key(i)).unwrap(), Some(key(i)), "{ctx}: key {i}");
        }
    }

    /// No temp files, and every table on disk is live.
    fn assert_no_orphans(dir: &Path, db: &Db, ctx: &str) {
        let on_disk = files(dir);
        assert!(!on_disk.contains(&DbFile::Temp), "{ctx}: {on_disk:?}");
        let tables = on_disk
            .iter()
            .filter(|f| matches!(f, DbFile::Table(_)))
            .count();
        assert_eq!(tables, db.stats().tables, "{ctx}: {on_disk:?}");
    }

    #[test]
    fn crash_at_every_flush_step_loses_nothing() {
        for fp in FLUSH_FAILPOINTS {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Db::open_with(dir.path(), small()).unwrap();
            db.put(b"before", b"flush").unwrap();
            db.flush().unwrap(); // so there's an older table and log in play
            let n = write_until_failpoint(&mut db, fp);
            drop(db); // the "crash": nothing after the failpoint runs

            let mut db = Db::open_with(dir.path(), small()).unwrap();
            assert_eq!(db.get(b"before").unwrap(), Some(b"flush".to_vec()), "{fp}");
            assert_keys(&db, 0..n, fp);
            assert_no_orphans(dir.path(), &db, fp);

            // The recovered database must be fully writable, with no file
            // number collisions, across more flushes and another reopen.
            for i in n..n + 300 {
                db.put(&key(i), &key(i)).unwrap();
            }
            db.flush().unwrap();
            drop(db);
            let db = Db::open_with(dir.path(), small()).unwrap();
            assert_keys(&db, 0..n + 300, &format!("{fp}, after more writes"));
            assert_no_orphans(dir.path(), &db, fp);
        }
    }

    #[test]
    fn failure_before_commit_is_retryable_without_reopen() {
        for fp in ["flush:after_table", "flush:after_new_log"] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Db::open_with(dir.path(), small()).unwrap();
            let n = write_until_failpoint(&mut db, fp);

            db.fail_at = None;
            db.flush().unwrap();
            for i in n..n + 300 {
                db.put(&key(i), &key(i)).unwrap();
            }
            assert_keys(&db, 0..n + 300, fp);
            drop(db);

            let db = Db::open_with(dir.path(), small()).unwrap();
            assert_keys(&db, 0..n + 300, &format!("{fp}, reopened"));
            assert_no_orphans(dir.path(), &db, fp);
        }
    }

    #[test]
    fn failure_at_commit_poisons_writes_but_not_reads() {
        for fp in ["flush:manifest", "flush:after_manifest"] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Db::open_with(dir.path(), small()).unwrap();
            let n = write_until_failpoint(&mut db, fp);
            db.fail_at = None;

            assert!(
                matches!(db.put(b"x", b"y"), Err(Error::Poisoned(_))),
                "{fp}"
            );
            assert!(matches!(db.delete(b"x"), Err(Error::Poisoned(_))), "{fp}");
            assert!(matches!(db.flush(), Err(Error::Poisoned(_))), "{fp}");
            assert_keys(&db, 0..n, &format!("{fp}, reads while poisoned"));
            drop(db);

            let mut db = Db::open_with(dir.path(), small()).unwrap();
            assert_keys(&db, 0..n, &format!("{fp}, reopened"));
            assert_eq!(db.get(b"x").unwrap(), None, "{fp}: refused write leaked");
            db.put(b"x", b"y").unwrap();
        }
    }

    /// Random puts, deletes, flushes, reads and reopens, checked against a
    /// BTreeMap after every read and at the end of each run.
    #[test]
    fn randomized_ops_match_a_btreemap() {
        for seed in 1..=12u64 {
            let mut rng = Rng::new(seed);
            let dir = tempfile::tempdir().unwrap();
            let opts = Options {
                memtable_size: 64 + rng.below(4000) as usize,
            };
            let mut db = Db::open_with(dir.path(), opts.clone()).unwrap();
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

            for _ in 0..2500 {
                let k = rng.key();
                match rng.below(100) {
                    0..=59 => {
                        let v = rng.value();
                        db.put(&k, &v).unwrap();
                        model.insert(k, v);
                    }
                    60..=84 => {
                        db.delete(&k).unwrap();
                        model.remove(&k);
                    }
                    85..=89 => db.flush().unwrap(),
                    90..=92 => {
                        drop(db);
                        db = Db::open_with(dir.path(), opts.clone()).unwrap();
                    }
                    _ => assert_eq!(db.get(&k).unwrap().as_ref(), model.get(&k), "seed {seed}"),
                }
            }
            drop(db);
            let db = Db::open_with(dir.path(), opts).unwrap();
            for (k, v) in &model {
                assert_eq!(
                    db.get(k).unwrap().as_ref(),
                    Some(v),
                    "seed {seed} key {k:?}"
                );
            }
            for _ in 0..500 {
                let k = rng.key();
                assert_eq!(
                    db.get(&k).unwrap().as_ref(),
                    model.get(&k),
                    "seed {seed} probe {k:?}"
                );
            }
        }
    }
}
