//! SSTable writer + reader tests. Corruption and randomized tests: section 4.

use super::*;
use crate::key::{SeqNo, MAX_SEQ};
use crate::memtable::Entry;
use crate::test_util::Rng;
use std::path::{Path, PathBuf};

fn val(s: &str) -> Entry {
    Entry::Value(s.as_bytes().to_vec())
}

/// Writes one version per key, all at seq 1.
fn write_table(path: &Path, block_size: usize, entries: &[(Vec<u8>, Entry)]) {
    let mut w = SstWriter::with_block_size(path, block_size).unwrap();
    for (k, e) in entries {
        w.add(k, 1, e).unwrap();
    }
    w.finish().unwrap();
}

/// What `entries()` returns for a table `write_table` wrote.
fn at_seq_1(entries: &[(Vec<u8>, Entry)]) -> Vec<(Vec<u8>, SeqNo, Entry)> {
    entries
        .iter()
        .map(|(k, e)| (k.clone(), 1, e.clone()))
        .collect()
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
    assert_eq!(r.get(b"apple", MAX_SEQ).unwrap(), Some(val("red")));
    assert_eq!(r.get(b"banana", MAX_SEQ).unwrap(), Some(Entry::Tombstone));
    assert_eq!(r.get(b"cherry", MAX_SEQ).unwrap(), Some(val("")));
    assert_eq!(r.entries().unwrap(), at_seq_1(&entries));
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
        assert_eq!(
            r.get(missing.as_bytes(), MAX_SEQ).unwrap(),
            None,
            "{missing:?}"
        );
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
            r.get(k, MAX_SEQ).unwrap().as_ref(),
            Some(e),
            "{:?}",
            String::from_utf8_lossy(k)
        );
    }
    // Keys that sort between two existing keys, including across block edges.
    for i in 0..2000 {
        let probe = format!("key{i:06}x");
        assert_eq!(r.get(probe.as_bytes(), MAX_SEQ).unwrap(), None);
    }
    assert_eq!(r.entries().unwrap(), at_seq_1(&entries));
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
    assert_eq!(r.get(b"b", MAX_SEQ).unwrap(), Some(val(&big)));
    assert_eq!(r.entries().unwrap(), at_seq_1(&entries));
}

#[test]
fn empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_table(&path, DEFAULT_BLOCK_SIZE, &[]);
    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.entry_count(), 0);
    assert_eq!(r.block_count(), 0);
    assert_eq!(r.get(b"anything", MAX_SEQ).unwrap(), None);
    assert!(r.entries().unwrap().is_empty());
}

#[test]
fn rejects_out_of_order_and_duplicate_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = SstWriter::create(&table_path(&dir)).unwrap();
    w.add(b"b", 5, &val("1")).unwrap();
    for (key, seq) in [("a", 9), ("b", 5), ("b", 6)] {
        assert!(
            matches!(
                w.add(key.as_bytes(), seq, &val("x")),
                Err(Error::InvalidArgument(_))
            ),
            "{key}@{seq} after b@5"
        );
    }
    // An older version of the same key, or any later key, is fine.
    w.add(b"b", 4, &val("2")).unwrap();
    w.add(b"c", 9, &val("3")).unwrap();
}

#[test]
fn file_appears_only_after_finish() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let mut w = SstWriter::create(&path).unwrap();
    w.add(b"a", 1, &val("1")).unwrap();
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
        w.add(b"a", 1, &val("1")).unwrap();
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
    assert_eq!(&bytes[44..], b"LSMKVSS2");
    assert_eq!(Footer::decode(&bytes).unwrap(), f);
}

// ---- Section 4: corruption and randomized tests ----

fn footer_of(bytes: &[u8]) -> Footer {
    let tail: &[u8; FOOTER_LEN] = bytes[bytes.len() - FOOTER_LEN..].try_into().unwrap();
    Footer::decode(tail).unwrap()
}

fn expect_corruption<T: std::fmt::Debug>(r: Result<T>, why: &str) {
    assert!(matches!(r, Err(Error::Corruption(_))), "{why}: got {r:?}");
}

#[test]
fn flipped_data_byte_fails_only_that_block() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = numbered(200);
    write_table(&path, 128, &entries);

    let good = std::fs::read(&path).unwrap();
    // A byte just before the last block's 4-byte CRC trailer, i.e. inside the
    // last data block (the filter block follows it).
    let last_block_byte = footer_of(&good).filter_offset as usize - 5;
    let mut bytes = good.clone();
    bytes[last_block_byte] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();

    // Index, footer and block 0 are intact, so open succeeds...
    let r = SstReader::open(&path).unwrap();
    // ...but reading the last block is caught by its CRC,
    match r.get(&entries.last().unwrap().0, MAX_SEQ) {
        Err(Error::Corruption(msg)) => assert!(msg.contains("block at offset"), "{msg}"),
        other => panic!("expected Corruption, got {other:?}"),
    }
    expect_corruption(r.entries(), "full scan over bad block");
    // ...while keys in other blocks are still readable.
    let (k, e) = &entries[0];
    assert_eq!(r.get(k, MAX_SEQ).unwrap().as_ref(), Some(e));

    // Block 0 is read on open (for the smallest key), so damage there
    // is caught immediately.
    let mut bytes = good;
    bytes[5] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
    expect_corruption(SstReader::open(&path), "block 0 damage at open");
}

#[test]
fn every_metadata_byte_flip_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_table(&path, 64, &numbered(50));
    let good = std::fs::read(&path).unwrap();
    // Filter, index and footer: everything after the data blocks.
    let meta_start = footer_of(&good).filter_offset as usize;
    assert!(footer_of(&good).filter_len > 0);

    for i in meta_start..good.len() {
        let mut bad = good.clone();
        bad[i] ^= 0x01;
        std::fs::write(&path, &bad).unwrap();
        expect_corruption(SstReader::open(&path), &format!("flip at byte {i}"));
    }
}

#[test]
fn truncation_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_table(&path, 64, &numbered(50));
    let good = std::fs::read(&path).unwrap();

    let cuts = [
        1,
        2,
        10,
        FOOTER_LEN - 1,
        FOOTER_LEN,
        FOOTER_LEN + 1,
        good.len() / 2,
        good.len() - 1,
    ];
    for cut in cuts {
        std::fs::write(&path, &good[..good.len() - cut]).unwrap();
        expect_corruption(SstReader::open(&path), &format!("cut {cut} bytes"));
    }
}

#[test]
fn appended_garbage_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_table(&path, 64, &numbered(10));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(b"trailing junk");
    std::fs::write(&path, &bytes).unwrap();
    expect_corruption(SstReader::open(&path), "appended bytes");
}

#[test]
fn non_sstable_files_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);

    std::fs::write(&path, b"").unwrap();
    expect_corruption(SstReader::open(&path), "empty file");

    std::fs::write(
        &path,
        "this is a text file, definitely not an sstable!!".repeat(3),
    )
    .unwrap();
    match SstReader::open(&path) {
        Err(Error::Corruption(msg)) => assert!(msg.contains("magic"), "{msg}"),
        other => panic!("expected bad magic, got {other:?}"),
    }
}

/// Random tables holding several versions per key, checked against a model
/// at random snapshots.
#[test]
fn randomized_tables_match_a_btreemap() {
    use std::cmp::Reverse;
    let dir = tempfile::tempdir().unwrap();
    for seed in 1..=40u64 {
        let mut rng = Rng::new(seed);
        // Internal key order: key ascending, then seq descending.
        let mut model = std::collections::BTreeMap::new();
        let n = rng.below(1500);
        for seq in 1..=n {
            let e = if rng.below(5) == 0 {
                Entry::Tombstone
            } else {
                Entry::Value(rng.value())
            };
            model.insert((rng.key(), Reverse(seq)), e);
        }
        let entries: Vec<(Vec<u8>, SeqNo, Entry)> = model
            .iter()
            .map(|((k, Reverse(seq)), e)| (k.clone(), *seq, e.clone()))
            .collect();
        let block_size = 32 + rng.below(1024) as usize;
        let path = dir.path().join(format!("{seed}.sst"));
        let mut w = SstWriter::with_block_size(&path, block_size).unwrap();
        for (k, seq, e) in &entries {
            w.add(k, *seq, e).unwrap();
        }
        w.finish().unwrap();

        let r = SstReader::open(&path).unwrap();
        assert_eq!(r.entries().unwrap(), entries, "seed {seed}");
        for _ in 0..500 {
            let probe = rng.key();
            let snapshot = rng.below(n + 2);
            let want = model
                .range((probe.clone(), Reverse(snapshot))..)
                .next()
                .filter(|((k, _), _)| *k == probe)
                .map(|(_, e)| e);
            assert_eq!(
                r.get(&probe, snapshot).unwrap().as_ref(),
                want,
                "seed {seed} probe {probe:?} at {snapshot}"
            );
        }
    }
}

#[test]
fn key_range_and_size_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = numbered(500);
    write_table(&path, 128, &entries);
    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.smallest_key(), Some(entries[0].0.as_slice()));
    assert_eq!(r.largest_key(), Some(entries[499].0.as_slice()));
    assert_eq!(r.file_size(), std::fs::metadata(&path).unwrap().len());

    let empty = dir.path().join("empty.sst");
    write_table(&empty, 128, &[]);
    let r = SstReader::open(&empty).unwrap();
    assert_eq!((r.smallest_key(), r.largest_key()), (None, None));
}

// ---- M6: bloom filters ----

fn write_with_bloom(path: &Path, bits_per_key: usize, entries: &[(Vec<u8>, Entry)]) {
    let opts = WriterOptions {
        block_size: 128,
        bloom_bits_per_key: bits_per_key,
    };
    let mut w = SstWriter::with_options(path, opts).unwrap();
    for (k, e) in entries {
        w.add(k, 1, e).unwrap();
    }
    w.finish().unwrap();
}

fn count(c: &std::sync::atomic::AtomicU64) -> u64 {
    ReadStats::get(c)
}

#[test]
fn filter_skips_block_reads_for_missing_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = numbered(2000);
    write_with_bloom(&path, 10, &entries);
    let r = SstReader::open(&path).unwrap();
    assert!(r.has_filter());

    // Present keys (tombstones included) always get through the filter.
    for (k, e) in &entries {
        assert_eq!(r.get(k, MAX_SEQ).unwrap().as_ref(), Some(e));
    }
    let s = r.stats();
    assert_eq!(count(&s.block_reads), 2000);
    assert_eq!(count(&s.filter_negatives), 0);
    assert_eq!(count(&s.filter_false_positives), 0);

    // Missing keys inside the table's key range: without a filter, each one
    // costs a block read.
    let misses = 2000;
    for i in 0..misses {
        assert_eq!(
            r.get(format!("key{i:06}x").as_bytes(), MAX_SEQ).unwrap(),
            None
        );
    }
    let fp = count(&s.filter_false_positives);
    assert_eq!(count(&s.filter_negatives) + fp, misses);
    assert_eq!(count(&s.block_reads), 2000 + fp);
    assert!(fp < misses / 50, "{fp} false positives in {misses}");
}

#[test]
fn table_without_filter_reads_correctly() {
    // bits_per_key = 0 writes the same empty filter block as pre-M6 tables.
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = numbered(300);
    write_with_bloom(&path, 0, &entries);
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(footer_of(&bytes).filter_len, 0);

    let r = SstReader::open(&path).unwrap();
    assert!(!r.has_filter());
    for (k, e) in &entries {
        assert_eq!(r.get(k, MAX_SEQ).unwrap().as_ref(), Some(e));
    }
    assert_eq!(r.get(b"key000000x", MAX_SEQ).unwrap(), None);
    // No filter, so no filter verdicts, and the miss cost a block read.
    assert_eq!(count(&r.stats().filter_negatives), 0);
    assert_eq!(count(&r.stats().filter_false_positives), 0);
    assert_eq!(count(&r.stats().block_reads), 301);
}

#[test]
fn filter_block_sits_between_blocks_and_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    write_with_bloom(&path, 10, &numbered(1000));
    let f = footer_of(&std::fs::read(&path).unwrap());
    // 1000 keys * 10 bits = 1250 bytes of bits, plus k and the CRC.
    assert_eq!(f.filter_len, 1250 + 1 + 4);
    assert_eq!(f.filter_offset + f.filter_len, f.index_offset);
}

// ---- M6: block cache ----

fn cached_ctx() -> std::sync::Arc<ReadContext> {
    std::sync::Arc::new(ReadContext::new(1 << 20))
}

#[test]
fn repeat_reads_hit_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = numbered(500);
    write_table(&path, 128, &entries);
    let r = SstReader::open_with(&path, 1, cached_ctx()).unwrap();

    for _ in 0..3 {
        for (k, e) in &entries {
            assert_eq!(r.get(k, MAX_SEQ).unwrap().as_ref(), Some(e));
        }
    }
    let s = r.stats();
    // Each block comes off the disk once; every other access is a hit.
    let blocks = r.block_count() as u64;
    assert_eq!(count(&s.block_reads), blocks);
    assert_eq!(count(&s.cache_hits), 3 * 500 - blocks);
}

#[test]
fn cache_keys_include_the_table_id() {
    // Two tables with identical layouts (same keys, same-length values), so
    // every block sits at the same offset in both. Only the id tells them apart.
    let dir = tempfile::tempdir().unwrap();
    let ctx = cached_ctx();
    let mut readers = Vec::new();
    for (id, v) in [(1, "AAAA"), (2, "BBBB")] {
        let path = dir.path().join(format!("{id}.sst"));
        let entries: Vec<_> = (0..100)
            .map(|i| (format!("k{i:03}").into_bytes(), val(v)))
            .collect();
        write_table(&path, 128, &entries);
        readers.push(SstReader::open_with(&path, id, std::sync::Arc::clone(&ctx)).unwrap());
    }
    for _ in 0..2 {
        assert_eq!(readers[0].get(b"k050", MAX_SEQ).unwrap(), Some(val("AAAA")));
        assert_eq!(readers[1].get(b"k050", MAX_SEQ).unwrap(), Some(val("BBBB")));
    }
    assert_eq!(count(&ctx.stats.block_reads), 2);
    assert_eq!(count(&ctx.stats.cache_hits), 2);
}

#[test]
fn corrupt_block_is_never_cached() {
    let dir = tempfile::tempdir().unwrap();
    let path = table_path(&dir);
    let entries = numbered(200);
    write_table(&path, 128, &entries);
    let mut bytes = std::fs::read(&path).unwrap();
    let last_block_byte = footer_of(&bytes).filter_offset as usize - 5;
    bytes[last_block_byte] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();

    let ctx = cached_ctx();
    let r = SstReader::open_with(&path, 1, std::sync::Arc::clone(&ctx)).unwrap();
    let last = &entries.last().unwrap().0;
    for _ in 0..2 {
        expect_corruption(r.get(last, MAX_SEQ), "bad block, read again");
    }
    // Both reads went to the disk, and nothing bad was kept.
    assert_eq!(count(&ctx.stats.block_reads), 2);
    assert!(ctx.cache.is_empty());
}
