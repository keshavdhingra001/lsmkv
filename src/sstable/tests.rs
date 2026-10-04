//! SSTable writer + reader tests. Corruption and randomized tests: section 4.

use super::*;
use crate::memtable::Entry;
use std::path::{Path, PathBuf};

fn val(s: &str) -> Entry {
    Entry::Value(s.as_bytes().to_vec())
}

fn write_table(path: &Path, block_size: usize, entries: &[(Vec<u8>, Entry)]) {
    let mut w = SstWriter::with_block_size(path, block_size).unwrap();
    for (k, e) in entries {
        w.add(k, e).unwrap();
    }
    w.finish().unwrap();
}

fn numbered(n: usize) -> Vec<(Vec<u8>, Entry)> {
    (0..n)
        .map(|i| {
            let k = format!("key{i:06}").into_bytes();
            let e = if i % 7 == 0 {
                Entry::Tombstone
            } else {
                Entry::Value(format!("value-{i}").into_bytes())
            };
            (k, e)
        })
        .collect()
}

fn table_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("000001.sst")
}

#[test]
fn small_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = vec![
        (b"apple".to_vec(), val("red")),
        (b"banana".to_vec(), Entry::Tombstone),
        (b"cherry".to_vec(), val("")),
    ];
    write_table(&path, DEFAULT_BLOCK_SIZE, &entries);

    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.entry_count(), 3);
    assert_eq!(r.block_count(), 1);
    assert_eq!(r.get(b"apple").unwrap(), Some(val("red")));
    assert_eq!(r.get(b"banana").unwrap(), Some(Entry::Tombstone));
    assert_eq!(r.get(b"cherry").unwrap(), Some(val("")));
    assert_eq!(r.entries().unwrap(), entries);
}

#[test]
fn misses_before_between_and_after() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_table(
        &path,
        64,
        &[(b"b".to_vec(), val("1")), (b"d".to_vec(), val("2"))],
    );
    let r = SstReader::open(&path).unwrap();
    for missing in ["", "a", "c", "e", "zzz"] {
        assert_eq!(r.get(missing.as_bytes()).unwrap(), None, "{missing:?}");
    }
}

#[test]
fn many_blocks_every_key_found() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = numbered(2000);
    write_table(&path, 128, &entries);

    let r = SstReader::open(&path).unwrap();
    assert!(r.block_count() > 100, "only {} blocks", r.block_count());
    for (k, e) in &entries {
        assert_eq!(
            r.get(k).unwrap().as_ref(),
            Some(e),
            "{:?}",
            String::from_utf8_lossy(k)
        );
    }
    // Keys that sort between two existing keys, including across block edges.
    for i in 0..2000 {
        let probe = format!("key{i:06}x");
        assert_eq!(r.get(probe.as_bytes()).unwrap(), None);
    }
    assert_eq!(r.entries().unwrap(), entries);
}

#[test]
fn entry_larger_than_block_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let big = "x".repeat(10_000);
    let entries = vec![
        (b"a".to_vec(), val("small")),
        (b"b".to_vec(), val(&big)),
        (b"c".to_vec(), val("small")),
    ];
    write_table(&path, 64, &entries);
    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.get(b"b").unwrap(), Some(val(&big)));
    assert_eq!(r.entries().unwrap(), entries);
}

#[test]
fn empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_table(&path, DEFAULT_BLOCK_SIZE, &[]);
    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.entry_count(), 0);
    assert_eq!(r.block_count(), 0);
    assert_eq!(r.get(b"anything").unwrap(), None);
    assert!(r.entries().unwrap().is_empty());
}

#[test]
fn rejects_out_of_order_and_duplicate_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = SstWriter::create(&table_path(&dir)).unwrap();
    w.add(b"b", &val("1")).unwrap();
    assert!(matches!(
        w.add(b"a", &val("2")),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        w.add(b"b", &val("3")),
        Err(Error::InvalidArgument(_))
    ));
    w.add(b"c", &val("4")).unwrap();
}

#[test]
fn file_appears_only_after_finish() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let mut w = SstWriter::create(&path).unwrap();
    w.add(b"a", &val("1")).unwrap();
    assert!(!path.exists(), "final file visible before finish");
    w.finish().unwrap();
    assert!(path.exists());
    assert!(!dir.path().join("000001.sst.tmp").exists());
}

#[test]
fn abandoned_writer_leaves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    {
        let mut w = SstWriter::create(&path).unwrap();
        w.add(b"a", &val("1")).unwrap();
    } // dropped without finish
    assert!(!path.exists());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn refuses_to_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_table(&path, DEFAULT_BLOCK_SIZE, &[(b"a".to_vec(), val("1"))]);
    assert!(matches!(
        SstWriter::create(&path),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn footer_roundtrip_and_magic_at_end() {
    let f = Footer {
        index_offset: 1,
        index_len: 2,
        filter_offset: 3,
        filter_len: 4,
        entry_count: 5,
    };
    let bytes = f.encode();
    assert_eq!(&bytes[44..], b"LSMKVSST");
    assert_eq!(Footer::decode(&bytes).unwrap(), f);
}
