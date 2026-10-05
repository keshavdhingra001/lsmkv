//! Backward compatibility: a database written by the format-4 build (before
//! restart points, DESIGN.md D30), checked in under `tests/fixtures`, opens,
//! reads exactly what it held, and is upgraded in place.

use std::path::{Path, PathBuf};

use lsmkv::Db;

/// The fixture copied to a fresh directory (opening it writes to it).
fn copy_fixture() -> tempfile::TempDir {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format4-db");
    let dir = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        std::fs::copy(&path, dir.path().join(path.file_name().unwrap())).unwrap();
    }
    dir
}

fn expected() -> Vec<(Vec<u8>, Vec<u8>)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format4-db.expected");
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| {
            let (k, v) = l.split_once('=').unwrap();
            (k.as_bytes().to_vec(), v.as_bytes().to_vec())
        })
        .collect()
}

fn contents(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter().unwrap().map(Result::unwrap).collect()
}

/// The magic at the end of each table file: which block layout it uses.
fn table_magics(dir: &Path) -> Vec<String> {
    let mut tables: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sst"))
        .collect();
    tables.sort();
    tables
        .iter()
        .map(|p| {
            let bytes = std::fs::read(p).unwrap();
            String::from_utf8_lossy(&bytes[bytes.len() - 8..]).into_owned()
        })
        .collect()
}

#[test]
fn a_format_4_database_opens_reads_and_upgrades() {
    let dir = copy_fixture();
    let old = table_magics(dir.path());
    assert!(
        old.len() > 3 && old.iter().all(|m| m == "LSMKVSS2"),
        "{old:?}"
    );

    let db = Db::open(dir.path()).unwrap();
    let want = expected();
    assert_eq!(contents(&db), want, "read through format-4 tables and WAL");
    for (k, v) in want.iter().step_by(7) {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v));
    }
    assert_eq!(db.get(b"k9999").unwrap(), None);

    // New writes and a full compaction rewrite every table in format 5.
    db.put(b"after", b"upgrade").unwrap();
    db.compact_all().unwrap();
    drop(db);
    let new = table_magics(dir.path());
    assert!(
        !new.is_empty() && new.iter().all(|m| m == "LSMKVSS3"),
        "{new:?}"
    );

    let db = Db::open(dir.path()).unwrap();
    let mut want = want;
    want.push((b"after".to_vec(), b"upgrade".to_vec()));
    want.sort();
    assert_eq!(contents(&db), want, "after the rewrite");
}

/// Tables of both layouts in one database at once: new flushes are format 5
/// while the old tables are still there.
#[test]
fn old_and_new_tables_are_read_together() {
    let dir = copy_fixture();
    let db = Db::open(dir.path()).unwrap();
    db.put(b"k0001", b"newer").unwrap();
    db.flush().unwrap();
    let magics = table_magics(dir.path());
    assert!(magics.iter().any(|m| m == "LSMKVSS2"), "{magics:?}");
    assert!(magics.iter().any(|m| m == "LSMKVSS3"), "{magics:?}");
    assert_eq!(db.get(b"k0001").unwrap(), Some(b"newer".to_vec()));
    let want: Vec<_> = expected()
        .into_iter()
        .map(|(k, v)| {
            if k == b"k0001" {
                (k, b"newer".to_vec())
            } else {
                (k, v)
            }
        })
        .collect();
    assert_eq!(contents(&db), want);
}
