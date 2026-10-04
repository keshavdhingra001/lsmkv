//! Public database handle. Glue between the WAL and the memtable.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::memtable::{Entry, MemTable};
use crate::wal::{Record, Wal};

const WAL_FILE: &str = "wal.log";

pub struct Db {
    dir: PathBuf,
    memtable: MemTable,
    wal: Wal,
}

impl Db {
    /// Opens the database in `dir`, recovering any writes from the WAL.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let wal_path = dir.join(WAL_FILE);

        let replay = Wal::replay(&wal_path)?;
        let mut memtable = MemTable::new();
        for rec in replay.records {
            match rec {
                Record::Put { key, value } => memtable.put(&key, &value),
                Record::Delete { key } => memtable.delete(&key),
            }
        }

        // Cut off any torn tail so new appends are reachable on the next replay.
        if wal_path.exists() && std::fs::metadata(&wal_path)?.len() > replay.valid_len {
            let f = OpenOptions::new().write(true).open(&wal_path)?;
            f.set_len(replay.valid_len)?;
            f.sync_all()?;
        }

        let wal = Wal::open(&wal_path)?;
        Ok(Self { dir, memtable, wal })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wal.append(&Record::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })?;
        self.wal.sync()?;
        self.memtable.put(key, value);
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wal.append(&Record::Delete { key: key.to_vec() })?;
        self.wal.sync()?;
        self.memtable.delete(key);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(match self.memtable.get(key) {
            Some(Entry::Value(v)) => Some(v.clone()),
            Some(Entry::Tombstone) | None => None,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn writes_after_torn_tail_are_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join(WAL_FILE);
        {
            let mut db = Db::open(dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
        }
        // Crash mid-write of "b".
        let len = std::fs::metadata(&wal_path).unwrap().len();
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
}
