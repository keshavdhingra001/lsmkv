use super::*;
use crate::sstable::SstWriter;
use crate::test_util::Rng;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::ops::Bound;

fn small() -> Options {
    Options {
        memtable_size: 1024,
        ..Options::default()
    }
}

/// Small enough that a few thousand writes reach levels 2-4.
fn tiny() -> Options {
    Options {
        memtable_size: 512,
        l0_compaction_trigger: 2,
        // Close behind, so writes get slowed and stalled for real.
        l0_slowdown_trigger: 4,
        l0_stop_trigger: 6,
        level1_max_bytes: 4096,
        level_size_multiplier: 3,
        target_file_size: 1024,
        ..Options::default()
    }
}

fn files(dir: &Path) -> Vec<DbFile> {
    list_files(&RealFs, dir)
        .unwrap()
        .into_iter()
        .map(|(f, _)| f)
        .collect()
}

fn only_log(dir: &Path) -> PathBuf {
    let logs: Vec<PathBuf> = list_files(&RealFs, dir)
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
        let db = Db::open(dir.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.delete(b"a").unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn sequence_numbers_survive_replay_and_flush() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        for i in 0..3 {
            db.put(b"a", format!("v{i}").as_bytes()).unwrap();
        }
        assert_eq!(db.stats().last_sequence, 3);
    }
    // From the WAL alone.
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.stats().last_sequence, 3);
    assert_eq!(db.get(b"a").unwrap(), Some(b"v2".to_vec()));

    // After a flush the WAL that held 1..=3 is gone; the manifest has to
    // remember them, or numbering restarts below what the table holds and
    // reads at the restarted numbers can't see the table's versions.
    db.flush().unwrap();
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.stats().last_sequence, 3);
    assert_eq!(db.get(b"a").unwrap(), Some(b"v2".to_vec()));
    db.put(b"a", b"v3").unwrap();
    assert_eq!(db.stats().last_sequence, 4);
    db.compact_all().unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn flush_keeps_only_versions_a_reader_can_see() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    for i in 0..5 {
        db.put(b"a", format!("v{i}").as_bytes()).unwrap();
    }
    db.delete(b"b").unwrap();
    db.put(b"b", b"back").unwrap();
    db.delete(b"c").unwrap();
    assert_eq!(
        db.stats().memtable_entries,
        8,
        "every version is kept in memory"
    );
    db.flush().unwrap();
    let st = db.state();
    let entries = st.current.levels[0][0].reader.entries().unwrap();
    let kept: Vec<(&[u8], SeqNo)> = entries.iter().map(|(k, s, _)| (&k[..], *s)).collect();
    // Newest of a and b; c's tombstone stays (older tables may hold c).
    assert_eq!(kept, vec![(&b"a"[..], 5), (b"b", 7), (b"c", 8)]);
}

#[test]
fn open_refuses_mid_log_corruption() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
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
        let db = Db::open(dir.path()).unwrap();
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
        let db = Db::open(dir.path()).unwrap();
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
        let db = Db::open_with(dir.path(), small()).unwrap();
        for i in 0..n {
            db.put(&key(i), format!("v{i}").as_bytes()).unwrap();
        }
        let st = db.stats();
        // ~32 flushes: level 0 kept under its trigger by compaction into L1.
        assert!(st.level_files[0] < 4 && st.level_files[1] > 0, "{st:?}");
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
        let db = Db::open(dir.path()).unwrap();
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
    let db = Db::open(dir.path()).unwrap();
    db.flush().unwrap();
    assert_eq!(db.stats().tables, 0);
    assert_eq!(files(dir.path()), vec![DbFile::Log(1)]);
}

#[test]
fn open_removes_crash_leftovers_but_not_foreign_files() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap(); // log 2, then table 3; log 1 is obsolete
    }
    for junk in ["000001.log", "000950.sst", "000951.sst.tmp"] {
        fs::write(dir.path().join(junk), b"junk").unwrap();
    }
    fs::write(dir.path().join("notes.txt"), b"mine").unwrap();

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(files(dir.path()), vec![DbFile::Log(2), DbFile::Table(3)]);
    assert!(dir.path().join("notes.txt").exists());
}

#[test]
fn missing_live_table_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap();
    }
    fs::remove_file(dir.path().join("000003.sst")).unwrap();
    match Db::open(dir.path()) {
        Err(Error::Corruption(msg)) => assert!(msg.contains("000003.sst"), "{msg}"),
        other => panic!("expected Corruption, got {:?}", other.err()),
    }
}

/// Writes a table file directly and registers it at `level`, bypassing
/// flush/compaction, so tests can set up exact level layouts. Its
/// entries are numbered `id`, so place newer data with higher ids.
fn place_table(dir: &Path, id: u64, level: u8, entries: &[(&str, Option<&str>)]) {
    let mut w = SstWriter::create(&table_path(dir, id)).unwrap();
    for (k, v) in entries {
        let e = match v {
            Some(v) => Entry::Value(v.as_bytes().to_vec()),
            None => Entry::Tombstone,
        };
        w.add(k.as_bytes(), id, &e).unwrap();
    }
    w.finish().unwrap();
    let (mut m, _) = Manifest::open(dir).unwrap();
    m.append(&[Edit::AddTable { id, level }, Edit::SetLastSequence(id)])
        .unwrap();
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

    let db = Db::open(d).unwrap();
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

/// Entries stored across all live tables (all versions, plus tombstones).
fn stored_entries(db: &Db) -> u64 {
    db.state()
        .current
        .levels
        .iter()
        .flatten()
        .map(|t| t.reader.entry_count())
        .sum()
}

#[test]
fn compaction_bounds_level0_and_fills_deep_levels() {
    let dir = tempfile::tempdir().unwrap();
    let n = 4000;
    {
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        let stop = tiny().l0_stop_trigger;
        for i in 0..n {
            db.put(&key(i), &key(i)).unwrap();
            // Compaction runs in the background, so level 0 may grow
            // past its trigger, but the stop trigger bounds it.
            let l0 = db.stats().level_files[0];
            assert!(l0 <= stop, "L0 at {l0} after put {i}");
        }
        db.flush().unwrap();
        let st = db.stats();
        assert!(
            st.level_files[0] < 2,
            "L0 over trigger once settled: {st:?}"
        );
        assert!(st.level_files[3] > 0, "never reached L3: {st:?}");
        assert!(st.write_amplification() > 1.0, "{st:?}");
        assert_keys(&db, 0..n, "before reopen");
    }
    // Reopen re-validates that levels 1+ don't overlap.
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    assert_keys(&db, 0..n, "after reopen");
}

#[test]
fn overwritten_versions_are_garbage_collected() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    for round in 0..40 {
        for i in 0..100 {
            db.put(&key(i), format!("r{round}").as_bytes()).unwrap();
        }
    }
    // 4000 versions were written; compaction keeps few stale ones.
    assert!(stored_entries(&db) < 800, "{} stored", stored_entries(&db));
    db.compact_all().unwrap();
    assert_eq!(stored_entries(&db), 100);
    for i in 0..100 {
        assert_eq!(db.get(&key(i)).unwrap(), Some(b"r39".to_vec()));
    }
}

#[test]
fn compact_all_drops_deleted_data_entirely() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    for i in 0..1000 {
        db.put(&key(i), &key(i)).unwrap();
    }
    for i in 0..1000 {
        db.delete(&key(i)).unwrap();
    }
    db.compact_all().unwrap();
    assert_eq!(stored_entries(&db), 0, "{:?}", db.stats());
    assert_eq!(db.stats().tables, 0);
    assert_eq!(db.get(&key(5)).unwrap(), None);
    drop(db);
    let tables = files(dir.path())
        .into_iter()
        .filter(|f| matches!(f, DbFile::Table(_)))
        .count();
    assert_eq!(tables, 0, "deleted tables left on disk");
}

#[test]
fn tombstone_survives_while_older_data_is_deeper() {
    let dir = tempfile::tempdir().unwrap();
    place_table(dir.path(), 10, 3, &[("a", Some("old"))]);
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    db.delete(b"a").unwrap();
    db.flush().unwrap();
    // Spans "a", so the two level-0 tables overlap and merge (not move, D29).
    db.put(b"0", b"1").unwrap();
    db.put(b"b", b"1").unwrap();
    db.flush().unwrap(); // L0 hits the trigger (2): L0 -> L1
    assert_eq!(db.stats().level_files[..2], [0, 1], "{:?}", db.stats());
    // Dropping the tombstone in L1 would resurrect "old" from L3.
    assert_eq!(db.get(b"a").unwrap(), None);
    drop(db);
    assert_eq!(
        Db::open_with(dir.path(), tiny())
            .unwrap()
            .get(b"a")
            .unwrap(),
        None
    );
}

#[test]
fn lone_table_moves_down_without_rewriting() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    // Flush, then 6 trivial moves to the bottom, then the one rewrite
    // `compact_all` does at the bottom (D18).
    db.compact_all().unwrap();
    let st = db.stats();
    assert_eq!(st.level_files, vec![0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(
        st.compaction_bytes, st.flush_bytes,
        "a trivial move rewrote data"
    );
    drop(db);
    assert_eq!(
        Db::open(dir.path()).unwrap().get(b"a").unwrap(),
        Some(b"1".to_vec())
    );
}

/// Every step of a memtable switch and its background flush that can fail.
const FLUSH_FAILPOINTS: [&str; 4] = [
    "switch:new_log",
    "flush:after_table",
    "flush:manifest",
    "flush:after_manifest",
];

/// Writes key(0), key(1), ... until a put fails (the switch or the
/// background flush hit the failpoint and poisoned the database). Returns
/// how many puts succeeded: a refused put was never logged.
fn write_until_failpoint(db: &mut Db, fp: &'static str) -> usize {
    db.state().fail_at = Some(fp);
    for i in 0..100_000 {
        if db.put(&key(i), &key(i)).is_err() {
            return i;
        }
    }
    panic!("{fp} never triggered");
}

fn assert_keys(db: &Db, range: std::ops::Range<usize>, ctx: &str) {
    for i in range {
        assert_eq!(db.get(&key(i)).unwrap(), Some(key(i)), "{ctx}: key {i}");
    }
}

/// No temp files, and every table on disk is live. Waits for the
/// background thread to go idle first: a job that finishes between listing
/// the files and counting the live tables would make them disagree.
fn assert_no_orphans(dir: &Path, db: &Db, ctx: &str) {
    db.wait_for_background(db.lock()).unwrap();
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

        let db = Db::open_with(dir.path(), small()).unwrap();
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
fn crash_at_every_compaction_step_loses_nothing() {
    for fp in [
        "compact:after_tables",
        "compact:manifest",
        "compact:after_manifest",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        // Let a few compactions succeed first, so deeper levels exist.
        // Rewriting one of the first 50 keys after each new one makes every
        // level-0 table overlap the others, so compactions merge (sequential
        // keys alone would only move tables, D29, and never reach `fp`).
        for i in 0..500 {
            db.put(&key(i), &key(i)).unwrap();
            db.put(&key(i % 50), &key(i % 50)).unwrap();
        }
        assert!(db.stats().level_files[2] > 0, "{fp}: {:?}", db.stats());
        let start = 500;
        db.state().fail_at = Some(fp);
        let mut n = start;
        while db.put(&key(n), &key(n)).is_ok() {
            n += 1;
            if db.put(&key(n % 50), &key(n % 50)).is_err() {
                break;
            }
        }
        // n puts were acknowledged; the failing one was refused unlogged.
        drop(db);

        let db = Db::open_with(dir.path(), tiny()).unwrap();
        assert_keys(&db, 0..n, fp);
        assert_no_orphans(dir.path(), &db, &format!("{fp}, reopened"));

        for i in n..n + 1000 {
            db.put(&key(i), &key(i)).unwrap();
        }
        db.compact_all().unwrap();
        drop(db);
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        assert_keys(&db, 0..n + 1000, &format!("{fp}, after more writes"));
        assert_no_orphans(dir.path(), &db, fp);
    }
}

#[test]
fn background_failure_poisons_writes_but_not_reads() {
    // Before the commit too: a background job has no caller to hand a
    // retryable error to (DESIGN.md D15).
    for fp in FLUSH_FAILPOINTS {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open_with(dir.path(), small()).unwrap();
        let n = write_until_failpoint(&mut db, fp);
        db.state().fail_at = None;

        assert!(
            matches!(db.put(b"x", b"y"), Err(Error::Poisoned(_))),
            "{fp}"
        );
        assert!(matches!(db.delete(b"x"), Err(Error::Poisoned(_))), "{fp}");
        assert!(matches!(db.flush(), Err(Error::Poisoned(_))), "{fp}");
        assert_keys(&db, 0..n, &format!("{fp}, reads while poisoned"));
        drop(db);

        let db = Db::open_with(dir.path(), small()).unwrap();
        assert_keys(&db, 0..n, &format!("{fp}, reopened"));
        assert_eq!(db.get(b"x").unwrap(), None, "{fp}: refused write leaked");
        db.put(b"x", b"y").unwrap();
    }
}

/// Eight level-0 tables that each span the whole key range, so every
/// lookup must consult all of them. Returns the database, with the key
/// numbers 0..n spread over the tables.
fn eight_overlapping_tables(dir: &Path, bloom_bits_per_key: usize, n: usize) -> Db {
    let opts = Options {
        l0_compaction_trigger: 100,
        l0_slowdown_trigger: 100,
        l0_stop_trigger: 100,
        bloom_bits_per_key,
        // Cache off, so every block a lookup needs is a disk read.
        block_cache_bytes: 0,
        ..Options::default()
    };
    let db = Db::open_with(dir, opts).unwrap();
    for round in 0..8 {
        for i in (round..n).step_by(8) {
            db.put(format!("key{i:06}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(db.stats().level_files[0], 8);
    db
}

#[test]
fn bloom_filters_cut_block_reads_for_missing_keys() {
    let n = 4000;
    let mut reads = Vec::new();
    for bits in [0, 10] {
        let dir = tempfile::tempdir().unwrap();
        let db = eight_overlapping_tables(dir.path(), bits, n);
        let before = db.stats();
        // Between two real keys. Stopping 8 short of n keeps each probe
        // below every table's last key, so the index alone can't rule it
        // out: without a filter, each table costs one block read.
        let misses = n - 8;
        for i in 0..misses {
            assert_eq!(db.get(format!("key{i:06}x").as_bytes()).unwrap(), None);
        }
        let after = db.stats();
        let block_reads = after.block_reads - before.block_reads;
        let negatives = after.filter_negatives - before.filter_negatives;
        let fps = after.filter_false_positives - before.filter_false_positives;
        let lookups = 8 * misses as u64;
        println!(
            "bits/key {bits:2}: {block_reads} block reads for {misses} missing keys \
             ({negatives} filter negatives, {fps} false positives = {:.2}%)",
            100.0 * fps as f64 / lookups as f64
        );
        if bits == 0 {
            assert_eq!(block_reads, lookups, "one read per table");
            assert_eq!(negatives + fps, 0);
        } else {
            assert_eq!(negatives + fps, lookups, "every table consulted its filter");
            assert_eq!(block_reads, fps, "only false positives read a block");
        }
        reads.push(block_reads);
    }
    assert!(
        reads[1] * 50 < reads[0],
        "filters saved too little: {reads:?}"
    );
}

#[test]
fn deleted_key_in_a_filtered_table_shadows_older_tables() {
    // The tombstone must pass the newer table's filter; if tombstones were
    // left out of filters, the lookup would fall through to the old value.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), small()).unwrap();
    db.put(b"k", b"old").unwrap();
    db.flush().unwrap();
    db.delete(b"k").unwrap();
    db.flush().unwrap();
    assert_eq!(db.stats().level_files[0], 2);
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn tables_with_and_without_filters_coexist() {
    let dir = tempfile::tempdir().unwrap();
    let no_filters = Options {
        bloom_bits_per_key: 0,
        block_cache_bytes: 0,
        l0_compaction_trigger: 100,
        l0_slowdown_trigger: 100,
        l0_stop_trigger: 100,
        ..small()
    };
    let db = Db::open_with(dir.path(), no_filters.clone()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    drop(db);

    let with_filters = Options {
        bloom_bits_per_key: 10,
        ..no_filters
    };
    let db = Db::open_with(dir.path(), with_filters).unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    let filtered: Vec<bool> = db.state().current.levels[0]
        .iter()
        .map(|t| t.reader.has_filter())
        .collect();
    assert_eq!(filtered, [true, false], "newest first");

    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    let before = db.stats();
    // "0" sorts before every key, so the index alone can't rule it out.
    assert_eq!(db.get(b"0").unwrap(), None);
    let after = db.stats();
    // The filtered table rules it out; the old one has to read a block.
    assert_eq!(after.filter_negatives - before.filter_negatives, 1);
    assert_eq!(after.block_reads - before.block_reads, 1);
}

/// Skewed reads (90% of lookups hit the first 5% of keys), with the cache
/// off and then on.
#[test]
fn hot_keys_are_served_from_the_cache() {
    let n = 20_000;
    let value = [b'v'; 100];
    let mut results = Vec::new();
    for cache in [0, 256 << 10] {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            block_cache_bytes: cache,
            ..Options::default()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        for i in 0..n {
            db.put(format!("key{i:06}").as_bytes(), &value).unwrap();
        }
        db.compact_all().unwrap();

        let mut rng = Rng::new(7);
        let before = db.stats();
        let reads = 20_000;
        for _ in 0..reads {
            let i = if rng.below(10) < 9 {
                rng.below(n / 20)
            } else {
                rng.below(n)
            };
            let k = format!("key{i:06}");
            assert_eq!(db.get(k.as_bytes()).unwrap().as_deref(), Some(&value[..]));
        }
        let after = db.stats();
        let disk = after.block_reads - before.block_reads;
        let hits = after.cache_hits - before.cache_hits;
        println!(
            "cache {:>3} KiB: {disk} disk reads, {hits} hits ({:.1}% hit rate) for {reads} gets",
            cache >> 10,
            100.0 * hits as f64 / reads as f64
        );
        assert_eq!(disk + hits, reads, "one block per get, from somewhere");
        assert!(after.cache_bytes <= cache);
        results.push(disk);
    }
    assert_eq!(results[0], 20_000);
    assert!(
        results[1] * 5 < results[0],
        "cache saved too little: {results:?}"
    );
}

// ---- M7: group commit and sync modes ----

#[test]
fn db_is_send_and_sync() {
    fn shareable<T: Send + Sync>() {}
    shareable::<Db>();
}

#[test]
fn concurrent_writers_share_fsyncs() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    db.state().slow_wal = Duration::from_millis(2);
    let (threads, per) = (8, 40);
    thread::scope(|s| {
        for t in 0..threads {
            let db = &db;
            s.spawn(move || {
                for i in 0..per {
                    db.put(format!("t{t}-{i}").as_bytes(), b"v").unwrap();
                }
            });
        }
    });
    let st = db.stats();
    println!(
        "{} writes from {threads} threads in {} groups ({} fsyncs)",
        st.writes, st.write_groups, st.wal_syncs
    );
    assert_eq!(st.writes, threads * per);
    assert_eq!(st.wal_syncs, st.write_groups, "one fsync per group");
    assert!(
        st.write_groups * 3 < st.writes,
        "writers didn't share fsyncs"
    );

    drop(db);
    let db = Db::open(dir.path()).unwrap();
    for t in 0..threads {
        for i in 0..per {
            assert!(db.get(format!("t{t}-{i}").as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn failed_group_sync_fails_every_writer_in_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    db.state().slow_wal = Duration::from_millis(100);
    let pause = || thread::sleep(Duration::from_millis(20));
    let results: Vec<Result<()>> = thread::scope(|s| {
        // The first writer leads alone and holds the WAL for 100 ms...
        let first = s.spawn(|| db.put(b"first", b"1"));
        pause();
        // ...while seven more queue up behind it as one group...
        let rest: Vec<_> = (0..7)
            .map(|i| {
                let db = &db;
                s.spawn(move || db.put(format!("k{i}").as_bytes(), b"v"))
            })
            .collect();
        pause();
        // ...whose fsync will fail. (The first group already passed this
        // failpoint before it released the state lock.)
        db.state().fail_at = Some("wal:sync");
        std::iter::once(first)
            .chain(rest)
            .map(|h| h.join().unwrap())
            .collect()
    });

    assert!(
        results[0].is_ok(),
        "the first group synced: {:?}",
        results[0]
    );
    let failed = &results[1..];
    assert!(
        failed.iter().all(Result::is_err),
        "acked without an fsync: {failed:?}"
    );
    let followers = failed
        .iter()
        .filter(|r| matches!(r, Err(Error::Poisoned(_))))
        .count();
    assert_eq!(
        followers, 6,
        "the leader gets the I/O error, followers Poisoned"
    );
    assert_eq!(db.stats().write_groups, 1);
    assert!(matches!(db.put(b"x", b"y"), Err(Error::Poisoned(_))));
    assert_eq!(db.get(b"first").unwrap(), Some(b"1".to_vec()));

    drop(db);
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"first").unwrap(), Some(b"1".to_vec()));
    db.put(b"x", b"y").unwrap();
}

/// Writers race each other and explicit flushes (with automatic flushes
/// and compactions too). Two checks after a reopen: every acknowledged
/// unique key is there, and every shared key reads exactly as before the
/// close, which needs the WAL and memtable to agree on write order.
#[test]
fn concurrent_writes_and_flushes_survive_reopen_identically() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    db.state().slow_wal = Duration::from_micros(300);
    let threads = 6;
    thread::scope(|s| {
        for t in 0..threads {
            let db = &db;
            s.spawn(move || {
                let mut rng = Rng::new(t + 1);
                for i in 0..300 {
                    db.put(format!("u{t}-{i}").as_bytes(), b"unique").unwrap();
                    let shared = format!("s{}", rng.below(20));
                    if rng.below(5) == 0 {
                        db.delete(shared.as_bytes()).unwrap();
                    } else {
                        db.put(shared.as_bytes(), format!("{t}-{i}").as_bytes())
                            .unwrap();
                    }
                    if i % 50 == 25 {
                        db.flush().unwrap();
                    }
                }
            });
        }
    });
    let shared: Vec<_> = (0..20)
        .map(|k| db.get(format!("s{k}").as_bytes()).unwrap())
        .collect();
    drop(db);

    let db = Db::open_with(dir.path(), tiny()).unwrap();
    for t in 0..threads {
        for i in 0..300 {
            let k = format!("u{t}-{i}");
            assert!(db.get(k.as_bytes()).unwrap().is_some(), "lost {k}");
        }
    }
    for (k, before) in shared.iter().enumerate() {
        let after = db.get(format!("s{k}").as_bytes()).unwrap();
        assert_eq!(&after, before, "s{k} changed across reopen");
    }
}

/// For the staged tests below: the leader holds the WAL for 150 ms, and
/// each step waits 30 ms so the previous one has reached its position.
const SLOW: Duration = Duration::from_millis(150);
fn step() {
    thread::sleep(Duration::from_millis(30));
}

#[test]
fn flush_waits_for_the_group_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    db.put(b"before", b"1").unwrap();
    db.state().slow_wal = SLOW;
    thread::scope(|s| {
        // "a" is being logged into the current WAL...
        s.spawn(|| db.put(b"a", b"1").unwrap());
        step();
        // ...so this flush must wait. If it went ahead, it would retire
        // that WAL, and "a" would then land in a memtable whose log
        // doesn't hold it.
        db.flush().unwrap();
    });
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"before").unwrap(), Some(b"1".to_vec()));
    assert_eq!(
        db.get(b"a").unwrap(),
        Some(b"1".to_vec()),
        "acked write lost"
    );
}

#[test]
fn group_applies_in_queue_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    db.state().slow_wal = SLOW;
    thread::scope(|s| {
        s.spawn(|| db.put(b"lead", b"x").unwrap());
        step();
        // Two writes to one key queue up, in this order, into one group.
        s.spawn(|| db.put(b"k", b"first").unwrap());
        step();
        s.spawn(|| db.put(b"k", b"second").unwrap());
    });
    assert_eq!(db.stats().write_groups, 2);
    // Later in the queue = later in the WAL = the value a replay ends on.
    // The memtable must agree, or a reopen changes what readers see.
    assert_eq!(db.get(b"k").unwrap(), Some(b"second".to_vec()));
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"second".to_vec()));
}

#[test]
fn writers_queued_behind_a_failed_group_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    {
        let mut st = db.state();
        st.slow_wal = SLOW;
        st.fail_at = Some("wal:sync");
    }
    let results: Vec<Result<()>> = thread::scope(|s| {
        let first = s.spawn(|| db.put(b"first", b"1"));
        step();
        let rest: Vec<_> = (0..3)
            .map(|i| {
                let db = &db;
                s.spawn(move || db.put(format!("k{i}").as_bytes(), b"v"))
            })
            .collect();
        step();
        // The disk "recovers" before the queued group leads. It must
        // still be refused: the first group may have left a partial
        // record at the end of the log, and appending after it would
        // turn a torn tail into mid-log corruption (D2).
        db.state().fail_at = None;
        std::iter::once(first)
            .chain(rest)
            .map(|h| h.join().unwrap())
            .collect()
    });
    assert!(matches!(results[0], Err(Error::Io(_))), "{:?}", results[0]);
    for r in &results[1..] {
        assert!(matches!(r, Err(Error::Poisoned(_))), "{r:?}");
    }
    assert_eq!(db.stats().write_groups, 0);
}

fn periodic(every: Duration) -> Options {
    Options {
        sync_mode: SyncMode::Periodic(every),
        ..Options::default()
    }
}

/// Waits up to 5 s for `cond`, for tests that depend on a background thread.
fn eventually(mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        thread::sleep(Duration::from_millis(5));
    }
    false
}

#[test]
fn periodic_mode_acks_once_the_os_has_the_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), periodic(Duration::from_secs(3600))).unwrap();
    for i in 0..3 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    assert_eq!(db.stats().wal_syncs, 0, "no fsync yet");
    // Not fsynced, but already written to the OS: another reader of the
    // file (here, a replay) sees every acknowledged record.
    let replay = Wal::replay(&only_log(dir.path())).unwrap();
    assert_eq!(replay.records.len(), 3);

    // Dropping must stop the thread now, not after its hour-long wait.
    let start = std::time::Instant::now();
    drop(db);
    assert!(start.elapsed() < Duration::from_secs(2));
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k2").unwrap(), Some(b"v".to_vec()));
}

/// Regression test for a lost wakeup: `Drop` signalled "stop" before the
/// new sync thread was waiting, and the thread then slept a full interval
/// (an hour here), hanging the drop. Opening and dropping at once, many
/// times, makes that ordering near-certain to occur.
#[test]
fn drop_right_after_open_does_not_wait_out_the_interval() {
    let dir = tempfile::tempdir().unwrap();
    let start = std::time::Instant::now();
    for _ in 0..200 {
        drop(Db::open_with(dir.path(), periodic(Duration::from_secs(3600))).unwrap());
    }
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[test]
fn periodic_thread_syncs_in_the_background() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), periodic(Duration::from_millis(5))).unwrap();
    db.put(b"k", b"v").unwrap();
    assert!(eventually(|| db.stats().wal_syncs >= 2));
    assert_eq!(db.stats().write_groups, 1);
}

/// The periodic fsync must not wait behind a long flush or compaction,
/// or the "lose at most one interval" bound stretches under load.
#[test]
fn periodic_sync_never_waits_for_the_state_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), periodic(Duration::from_millis(5))).unwrap();
    db.put(b"k", b"v").unwrap();
    let syncs = || db.shared.periodic_syncs.load(Ordering::Relaxed);
    let st = db.state(); // stands in for a 300 ms compaction
    let before = syncs();
    thread::sleep(Duration::from_millis(300));
    assert!(
        syncs() >= before + 5,
        "{} syncs in 300 ms",
        syncs() - before
    );
    drop(st);
}

#[test]
fn failed_periodic_sync_poisons() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), periodic(Duration::from_millis(5))).unwrap();
    db.put(b"k", b"v").unwrap();
    db.shared.fail_periodic_sync.store(true, Ordering::Relaxed);
    assert!(eventually(|| matches!(
        db.put(b"k2", b"v"),
        Err(Error::Poisoned(_))
    )));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

/// Random puts, deletes, flushes, reads and reopens, checked against a
/// BTreeMap after every read and at the end of each run.
#[test]
fn randomized_ops_match_a_btreemap() {
    for seed in 1..=12u64 {
        let mut rng = Rng::new(seed);
        let dir = tempfile::tempdir().unwrap();
        // Filter size changes on every reopen, so one database mixes
        // tables with no filter, weak filters and normal ones.
        let bloom_sizes = [0, 1, 4, 10];
        // Close together, so slowdowns and stalls happen too.
        let l0_compaction_trigger = 2 + rng.below(4) as usize;
        let l0_slowdown_trigger = l0_compaction_trigger + rng.below(3) as usize;
        let mut opts = Options {
            memtable_size: 64 + rng.below(4000) as usize,
            l0_compaction_trigger,
            l0_slowdown_trigger,
            l0_stop_trigger: l0_slowdown_trigger + rng.below(3) as usize,
            level1_max_bytes: 512 + rng.below(8192),
            level_size_multiplier: 2 + rng.below(9),
            target_file_size: 256 + rng.below(4096) as usize,
            bloom_bits_per_key: bloom_sizes[rng.below(4) as usize],
            // Off, tiny (constant eviction) or roomy.
            block_cache_bytes: [0, 300, 4096, 1 << 20][rng.below(4) as usize],
            sync_mode: if rng.below(2) == 0 {
                SyncMode::Always
            } else {
                SyncMode::Periodic(Duration::from_millis(1))
            },
            ..Options::default()
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
                    opts.bloom_bits_per_key = bloom_sizes[rng.below(4) as usize];
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

// ---- M8: reads without the state lock ----

#[test]
fn reads_never_wait_for_the_state_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path()).unwrap());
    db.put(b"in-table", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"in-memtable", b"2").unwrap();

    // Stand-in for a long flush or compaction: hold the state lock.
    let st = db.state();
    let reader = {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            (
                db.get(b"in-table").unwrap(),
                db.get(b"in-memtable").unwrap(),
                db.get(b"missing").unwrap(),
            )
        })
    };
    let finished = eventually(|| reader.is_finished());
    drop(st);
    assert!(finished, "a read waited for the state lock");
    let got = reader.join().unwrap();
    assert_eq!(got, (Some(b"1".to_vec()), Some(b"2".to_vec()), None));
}

#[test]
fn an_old_super_version_stays_readable_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), small()).unwrap();
    for i in 0..200 {
        db.put(&key(i), b"old").unwrap();
    }
    db.flush().unwrap();
    // A reader that grabbed the view before the compaction...
    let (old, snapshot) = db.shared.read_view();
    let old_ids: Vec<u64> = old.levels.iter().flatten().map(|t| t.id).collect();
    for i in 0..200 {
        db.put(&key(i), b"new").unwrap();
    }
    db.compact_all().unwrap();
    // ...whose table files the compaction has since deleted...
    for id in &old_ids {
        assert!(
            !table_path(dir.path(), *id).exists(),
            "table {id} not deleted"
        );
    }
    // ...still reads its point in time, through the descriptors it holds.
    for i in (0..200).step_by(7) {
        assert_eq!(old.get(&key(i), snapshot).unwrap(), Some(b"old".to_vec()));
        assert_eq!(db.get(&key(i)).unwrap(), Some(b"new".to_vec()));
    }
}

/// One writer counts a key up from 0 while flushes and compactions run.
/// Readers check two things on every read: the value never goes
/// backwards, and it's never below the last value acknowledged before the
/// read started. A reader that saw a memtable swapped out before its
/// table was in place, or a stale view, would break one of them.
#[test]
fn readers_see_every_acknowledged_write_across_flushes_and_compactions() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open_with(dir.path(), tiny()).unwrap());
    let acked = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    db.put(b"counter", &0u64.to_be_bytes()).unwrap();

    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (db, acked, done) = (Arc::clone(&db), Arc::clone(&acked), Arc::clone(&done));
            thread::spawn(move || {
                let mut last = 0;
                let mut reads = 0u64;
                while !done.load(Ordering::Acquire) {
                    let floor = acked.load(Ordering::Acquire);
                    let v = db.get(b"counter").unwrap().expect("counter vanished");
                    let v = u64::from_be_bytes(v.try_into().unwrap());
                    assert!(v >= floor, "read {v} after {floor} was acknowledged");
                    assert!(v >= last, "went backwards: {last} then {v}");
                    last = v;
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    let n = 3000u64;
    for i in 1..=n {
        db.put(b"counter", &i.to_be_bytes()).unwrap();
        // Filler, so the memtable fills and flushes and compactions run.
        db.put(&key(i as usize % 500), &[b'x'; 40]).unwrap();
        acked.store(i, Ordering::Release);
    }
    done.store(true, Ordering::Release);
    let reads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
    let st = db.stats();
    assert!(st.compaction_bytes > 0 && st.flush_bytes > 0, "{st:?}");
    assert!(reads > 1000, "only {reads} reads");
}

// ---- M8: background flush and compaction ----

/// The longest single call to `op`, over `n` calls.
fn slowest(n: usize, mut op: impl FnMut(usize)) -> Duration {
    (0..n)
        .map(|i| {
            let start = std::time::Instant::now();
            op(i);
            start.elapsed()
        })
        .max()
        .unwrap_or_default()
}

#[test]
fn writes_and_reads_continue_while_a_flush_runs() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size: 16 << 10,
        ..Options::default()
    };
    let db = Db::open_with(dir.path(), opts).unwrap();
    // Long enough that the second memtable fills while the first is still
    // flushing, even on a loaded machine (400 ms wasn't, under parallel tests).
    db.state().slow_background = Duration::from_secs(2);
    // Fill the memtable until the switch: it's now immutable, and the
    // background thread is (slowly) flushing it. Watched through the read
    // view, which (unlike `stats`) doesn't take the state lock.
    let flushing = |db: &Db| db.shared.read_view().0.imm.is_some();
    let mut n = 0;
    while !flushing(&db) {
        db.put(&key(n), &key(n)).unwrap();
        n += 1;
    }
    // Writes into the fresh memtable and reads of the flushing one (and
    // of the new one) don't wait for the flush.
    let put_max = slowest(50, |i| db.put(&key(n + i), &key(n + i)).unwrap());
    let get_max = slowest(n + 50, |i| {
        assert_eq!(db.get(&key(i)).unwrap(), Some(key(i)))
    });
    assert!(
        put_max < Duration::from_millis(100),
        "a put took {put_max:?}"
    );
    assert!(
        get_max < Duration::from_millis(100),
        "a get took {get_max:?}"
    );
    assert!(flushing(&db), "flush finished too soon to tell");
    assert_eq!(db.stats().write_stalls, 0);

    // Filling the second memtable before the first is flushed must wait.
    n += 50;
    while db.stats().write_stalls == 0 {
        db.put(&key(n), &key(n)).unwrap();
        n += 1;
    }
    let st = db.stats();
    assert!(st.stall_micros > 50_000, "{st:?}");
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    assert_keys(&db, 0..n, "reopened");
}

#[test]
fn writes_and_reads_continue_while_a_compaction_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), small()).unwrap();
    db.state().pause_compactions = true;
    for round in 0..4 {
        for i in 0..50 {
            db.put(&key(i), format!("r{round}").as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(db.stats().level_files[0], 4);
    {
        let mut st = db.state();
        st.pause_compactions = false;
        st.slow_background = Duration::from_millis(400);
    }
    db.shared.bg_work.notify_one();
    assert!(eventually(|| db.state().bg_busy));

    // Fewer bytes than a memtable holds, so no switch is needed.
    let put_max = slowest(20, |i| db.put(&key(100 + i), b"new").unwrap());
    let get_max = slowest(50, |i| {
        assert_eq!(db.get(&key(i)).unwrap(), Some(b"r3".to_vec()));
    });
    assert!(db.state().bg_busy, "compaction finished too soon to tell");
    assert!(
        put_max < Duration::from_millis(100),
        "a put took {put_max:?}"
    );
    assert!(
        get_max < Duration::from_millis(100),
        "a get took {get_max:?}"
    );

    db.flush().unwrap(); // waits for the compaction too
    let st = db.stats();
    // The 4 old tables went down; the one new table is the 20 puts.
    assert_eq!(st.level_files[0], 1, "{st:?}");
    assert!(st.compaction_bytes > 0, "{st:?}");
    assert_eq!(db.get(&key(7)).unwrap(), Some(b"r3".to_vec()));
}

#[test]
fn deep_level_0_slows_then_stops_writes_until_compaction_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size: 1024,
        l0_compaction_trigger: 2,
        l0_slowdown_trigger: 3,
        l0_stop_trigger: 4,
        ..Options::default()
    };
    let db = Arc::new(Db::open_with(dir.path(), opts).unwrap());
    db.state().pause_compactions = true;
    let written = Arc::new(AtomicU64::new(0));
    let writer = {
        let (db, written) = (Arc::clone(&db), Arc::clone(&written));
        thread::spawn(move || {
            for i in 0..2000 {
                db.put(&key(i), &[b'v'; 50]).unwrap();
                written.fetch_add(1, Ordering::Relaxed);
            }
        })
    };
    // Level 0 fills to the stop trigger. Writes go on into the memtable
    // until it's full; then the switch it needs waits, and they stop.
    let stopped = eventually(|| {
        let before = written.load(Ordering::Relaxed);
        thread::sleep(Duration::from_millis(100));
        before == written.load(Ordering::Relaxed)
    });
    assert!(stopped && !writer.is_finished(), "writes never stopped");
    assert_eq!(db.stats().level_files[0], 4);
    assert!(db.stats().write_slowdowns > 0, "{:?}", db.stats());

    // Compaction drains level 0, and the writer finishes.
    db.state().pause_compactions = false;
    db.shared.bg_work.notify_one();
    writer.join().unwrap();
    db.flush().unwrap();
    let st = db.stats();
    assert!(st.write_stalls > 0, "{st:?}");
    assert!(st.level_files[0] < 2, "{st:?}");
    assert_eq!(db.get(&key(1999)).unwrap(), Some(vec![b'v'; 50]));
}

#[test]
fn level_0_triggers_must_be_ordered() {
    let dir = tempfile::tempdir().unwrap();
    for (compaction, slowdown, stop) in [(4, 3, 12), (4, 8, 7)] {
        let opts = Options {
            l0_compaction_trigger: compaction,
            l0_slowdown_trigger: slowdown,
            l0_stop_trigger: stop,
            ..Options::default()
        };
        assert!(matches!(
            Db::open_with(dir.path(), opts),
            Err(Error::InvalidArgument(_))
        ));
    }
}

#[test]
fn switch_syncs_the_old_log_even_in_periodic_mode() {
    // An hour-long interval: no periodic fsync will happen in this test.
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size: 1024,
        ..periodic(Duration::from_secs(3600))
    };
    let db = Db::open_with(dir.path(), opts).unwrap();
    let mut n = 0;
    while db.stats().immutable_entries == 0 && db.stats().flush_bytes == 0 {
        db.put(&key(n), &key(n)).unwrap();
        n += 1;
    }
    // The one fsync is the switch sealing the old log, which still holds
    // every acknowledged write until its table commits. Without it, a
    // power cut before the flush finished could lose more than the
    // interval allows.
    assert_eq!(db.stats().wal_syncs, 1, "{:?}", db.stats());
}

// ---- M8: snapshots ----

#[test]
fn snapshot_reads_a_point_in_time_through_flush_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    for i in 0..100 {
        db.put(&key(i), b"old").unwrap();
    }
    let snap = db.snapshot();
    assert_eq!(snap.sequence(), 100);
    for i in 0..100 {
        if i % 2 == 0 {
            db.put(&key(i), b"new").unwrap();
        } else {
            db.delete(&key(i)).unwrap();
        }
    }
    db.put(b"born-later", b"x").unwrap();
    db.compact_all().unwrap();

    for i in 0..100 {
        assert_eq!(snap.get(&key(i)).unwrap(), Some(b"old".to_vec()), "{i}");
        let now = (i % 2 == 0).then(|| b"new".to_vec());
        assert_eq!(db.get(&key(i)).unwrap(), now, "{i}");
    }
    assert_eq!(snap.get(b"born-later").unwrap(), None);
    let st = db.stats();
    assert_eq!((st.snapshots, st.oldest_snapshot), (1, Some(100)));
    // Both versions of every key are still stored, for the snapshot.
    let kept = stored_entries(&db);
    assert!(kept >= 200, "{kept}");

    // Once it's gone, compaction may drop what only it could see:
    // the old values, and the tombstones with the keys they deleted.
    drop(snap);
    assert_eq!(db.stats().snapshots, 0);
    db.compact_all().unwrap();
    assert_eq!(stored_entries(&db), 51, "50 new values and born-later");
}

/// Section 1's surviving mutation: a compaction may split its output
/// only between user keys. Snapshots keep many big versions of one key
/// alive, more than one output table holds, so a split would put that
/// key in two tables of one level: reads would miss versions, and a
/// reopen would refuse the overlapping tables.
#[test]
fn one_keys_versions_never_span_two_tables() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        let mut snaps = Vec::new();
        for v in 0..20u8 {
            db.put(b"hot", &[v; 200]).unwrap();
            db.put(&key(v as usize), b"filler").unwrap();
            snaps.push(db.snapshot());
        }
        db.compact_all().unwrap();
        let st = db.stats();
        assert!(st.level_files[6] > 1, "too few tables to split: {st:?}");
        for (v, snap) in snaps.iter().enumerate() {
            assert_eq!(snap.get(b"hot").unwrap(), Some(vec![v as u8; 200]));
        }
    }
    Db::open_with(dir.path(), tiny()).expect("levels overlap after reopen");
}

/// A writer sets a = i, then b = i, for i = 1, 2, ... So in any
/// consistent view, b <= a. Readers read a, then b, through one
/// snapshot; separate gets could see a newer b than a.
#[test]
fn reads_through_one_snapshot_agree_under_concurrent_writes() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open_with(dir.path(), tiny()).unwrap());
    let done = Arc::new(AtomicBool::new(false));
    let num = |v: Option<Vec<u8>>| v.map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap()));
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (db, done) = (Arc::clone(&db), Arc::clone(&done));
            thread::spawn(move || {
                let mut checks = 0u64;
                while !done.load(Ordering::Acquire) {
                    let snap = db.snapshot();
                    let a = num(snap.get(b"a").unwrap());
                    thread::yield_now(); // let the writer get ahead
                    let b = num(snap.get(b"b").unwrap());
                    assert!(b <= a, "b = {b} but a = {a} in one snapshot");
                    checks += 1;
                }
                checks
            })
        })
        .collect();
    for i in 1..=3000u64 {
        db.put(b"a", &i.to_be_bytes()).unwrap();
        db.put(b"b", &i.to_be_bytes()).unwrap();
    }
    done.store(true, Ordering::Release);
    let checks: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
    assert!(checks > 100, "only {checks} checks");
    assert!(db.stats().compaction_bytes > 0, "{:?}", db.stats());
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Random writes, snapshots taken and dropped, flushes and compactions;
/// every live snapshot is checked against a copy of the model taken with it.
#[test]
fn randomized_snapshots_match_a_model() {
    for seed in 1..=10u64 {
        let mut rng = Rng::new(seed);
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size: 256 + rng.below(2048) as usize,
            target_file_size: 256 + rng.below(2048) as usize,
            ..tiny()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        // Each live snapshot, with the model as it was when it was taken.
        let mut snaps: Vec<(Snapshot, Model)> = Vec::new();
        for step in 0..3000 {
            let k = rng.key();
            match rng.below(100) {
                0..=49 => {
                    let v = rng.value();
                    db.put(&k, &v).unwrap();
                    model.insert(k, v);
                }
                50..=64 => {
                    db.delete(&k).unwrap();
                    model.remove(&k);
                }
                65..=69 => snaps.push((db.snapshot(), model.clone())),
                70..=73 if !snaps.is_empty() => {
                    let i = rng.below(snaps.len() as u64) as usize;
                    snaps.swap_remove(i);
                }
                74 => db.flush().unwrap(),
                75 => db.compact_all().unwrap(),
                _ if !snaps.is_empty() => {
                    let i = rng.below(snaps.len() as u64) as usize;
                    let (snap, then) = &snaps[i];
                    assert_eq!(
                        snap.get(&k).unwrap().as_ref(),
                        then.get(&k),
                        "seed {seed} step {step} key {k:?} at {}",
                        snap.sequence()
                    );
                }
                _ => assert_eq!(db.get(&k).unwrap().as_ref(), model.get(&k)),
            }
        }
        db.compact_all().unwrap();
        for (snap, then) in &snaps {
            for (k, v) in then {
                assert_eq!(snap.get(k).unwrap().as_ref(), Some(v), "seed {seed}");
            }
        }
    }
}

// ---- M15: recovery makes what it replays durable ----

/// A write that's only in the page cache when the process dies (here an
/// acknowledged `Periodic` write) is replayed by the next open and
/// served from then on. So it must be durable from then on too: a power
/// cut right after the reopen must not take it back.
#[test]
fn recovered_writes_survive_a_later_power_cut() {
    use crate::vfs::SimFs;
    let sim = SimFs::new(1);
    let opts = Options {
        sync_mode: SyncMode::Periodic(Duration::from_secs(3600)),
        fs: Arc::new(sim.clone()),
        inline_background: true,
        ..Options::default()
    };
    let dir = Path::new("/sim/db");
    let db = Db::open_with(dir, opts.clone()).unwrap();
    db.put(b"k", b"acknowledged, not yet synced").unwrap();
    sim.crash_now(); // kill -9: no final sync...
    drop(db);
    sim.process_restart(); // ...but the page cache survives.

    let db = Db::open_with(dir, opts.clone()).unwrap();
    assert!(
        db.get(b"k").unwrap().is_some(),
        "replayed from the page cache"
    );
    sim.crash_now();
    drop(db);
    sim.power_cut();
    let db = Db::open_with(dir, opts).unwrap();
    assert!(
        db.get(b"k").unwrap().is_some(),
        "a write the reopened database served was lost by a power cut"
    );
}

// ---- M9: range scans ----

type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

fn collect(it: DbIter) -> Pairs {
    it.map(|item| item.unwrap()).collect()
}

fn pairs<K: AsRef<[u8]> + ?Sized>(model: &Model, range: impl RangeBounds<K>) -> Pairs {
    let range = (
        range.start_bound().map(|k| k.as_ref()),
        range.end_bound().map(|k| k.as_ref()),
    );
    model
        .iter()
        .filter(|(k, _)| range.contains(k.as_slice()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Versions spread over every kind of source: L1+ tables, level-0
/// tables, the immutable memtable is covered by the concurrent tests.
#[test]
fn scan_merges_every_source_and_honors_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), small()).unwrap();
    let mut model = Model::new();
    let mut put = |db: &Db, k: &str, v: &str| {
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
        model.insert(k.as_bytes().to_vec(), v.as_bytes().to_vec());
    };
    for i in 0..40 {
        put(&db, &format!("k{i:02}"), "deep");
    }
    db.compact_all().unwrap();
    for i in (0..40).step_by(3) {
        put(&db, &format!("k{i:02}"), "l0");
    }
    db.flush().unwrap();
    for i in (0..40).step_by(5) {
        put(&db, &format!("k{i:02}"), "mem");
    }
    for k in ["k07", "k09", "k10"] {
        db.delete(k.as_bytes()).unwrap();
        model.remove(k.as_bytes());
    }
    let (current, _) = db.shared.read_view();
    assert!(!current.levels[0].is_empty() && !current.mem.is_empty());
    assert!(current.levels[1..].iter().any(|l| !l.is_empty()));

    let b = |s: &'static str| s.as_bytes();
    assert_eq!(collect(db.iter().unwrap()), pairs::<[u8]>(&model, ..));
    assert_eq!(
        collect(db.scan(b("k05")..b("k12")).unwrap()),
        pairs(&model, b("k05")..b("k12"))
    );
    assert_eq!(
        collect(db.scan(b("k05")..=b("k12")).unwrap()),
        pairs(&model, b("k05")..=b("k12"))
    );
    assert_eq!(
        collect(db.scan(b("k3")..).unwrap()),
        pairs(&model, b("k3")..)
    );
    assert_eq!(
        collect(db.scan(..b("k02")).unwrap()),
        pairs(&model, ..b("k02"))
    );
    let excl = (Bound::Excluded(b("k05")), Bound::Included(b("k08")));
    assert_eq!(
        collect(db.scan::<[u8]>(excl).unwrap()),
        pairs::<[u8]>(&model, excl)
    );
    // A deleted start key, ranges with nothing in them, and start > end.
    assert_eq!(
        collect(db.scan(b("k09")..b("k11")).unwrap()),
        pairs(&model, b("k09")..b("k11"))
    );
    assert!(collect(db.scan(b("x")..).unwrap()).is_empty());
    assert!(collect(db.scan(b("k20")..b("k10")).unwrap()).is_empty());
    assert!(collect(db.scan(b("k20")..b("k20")).unwrap()).is_empty());
}

#[test]
fn scan_is_one_point_in_time() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    for i in 0..300 {
        db.put(format!("k{i:03}").as_bytes(), b"before").unwrap();
    }
    let mut it = db.iter().unwrap();
    let first = it.next().unwrap().unwrap();
    // After the scan starts: overwrite, delete and add keys, then push
    // everything down so the tables the scan is reading are deleted.
    for i in 0..300 {
        match i % 3 {
            0 => db.put(format!("k{i:03}").as_bytes(), b"after").unwrap(),
            1 => db.delete(format!("k{i:03}").as_bytes()).unwrap(),
            _ => db.put(format!("k{i:03}x").as_bytes(), b"new").unwrap(),
        }
    }
    db.compact_all().unwrap();
    let rest = collect(it);
    let mut seen = vec![first];
    seen.extend(rest);
    let want: Pairs = (0..300)
        .map(|i| (format!("k{i:03}").into_bytes(), b"before".to_vec()))
        .collect();
    assert_eq!(seen, want);
    // A new scan sees the new state: 100 overwritten, 100 deleted, 100
    // untouched and 100 added.
    let now = collect(db.iter().unwrap());
    let count = |v: &[u8]| now.iter().filter(|(_, x)| x == v).count();
    assert_eq!(
        (count(b"after"), count(b"before"), count(b"new")),
        (100, 100, 100)
    );
}

#[test]
fn scan_keeps_reading_tables_compaction_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), small()).unwrap();
    for i in 0..200 {
        db.put(format!("k{i:03}").as_bytes(), &[b'v'; 50]).unwrap();
    }
    db.flush().unwrap();
    let sst = |dir: &Path| -> HashSet<PathBuf> {
        fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "sst"))
            .collect()
    };
    let before = sst(dir.path());
    let it = db.iter().unwrap();
    for i in 0..200 {
        db.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    db.compact_all().unwrap();
    assert!(
        sst(dir.path()).is_disjoint(&before),
        "the old tables are gone"
    );
    let got = collect(it);
    assert_eq!(got.len(), 200);
    assert!(got.iter().all(|(_, v)| v == &[b'v'; 50]));
}

#[test]
fn snapshot_scan_sees_the_snapshot_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    for i in 0..100 {
        db.put(format!("k{i:02}").as_bytes(), b"old").unwrap();
    }
    let snap = db.snapshot();
    for i in 0..100 {
        if i % 2 == 0 {
            db.delete(format!("k{i:02}").as_bytes()).unwrap();
        } else {
            db.put(format!("k{i:02}").as_bytes(), b"new").unwrap();
        }
    }
    db.compact_all().unwrap();
    let got = collect(snap.scan(&b"k10"[..]..&b"k20"[..]).unwrap());
    let want: Pairs = (10..20)
        .map(|i| (format!("k{i:02}").into_bytes(), b"old".to_vec()))
        .collect();
    assert_eq!(got, want);
    let now = collect(db.scan(&b"k10"[..]..&b"k20"[..]).unwrap());
    assert_eq!(now.len(), 5);
    assert!(now.iter().all(|(_, v)| v == b"new"));
}

/// Staged: a flush is held in place, so the immutable memtable is one
/// of the scan's sources, overlapping the new memtable's keys.
#[test]
fn scan_reads_the_immutable_memtable_during_a_flush() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size: 16 << 10,
        ..Options::default()
    };
    let db = Db::open_with(dir.path(), opts).unwrap();
    db.state().slow_background = Duration::from_millis(400);
    let flushing = |db: &Db| db.shared.read_view().0.imm.is_some();
    let mut n = 0;
    while !flushing(&db) {
        db.put(&key(n), b"imm").unwrap();
        n += 1;
    }
    // Overwrite every other key in the fresh memtable.
    for i in (0..n).step_by(2) {
        db.put(&key(i), b"mem").unwrap();
    }
    let got = collect(db.iter().unwrap());
    assert!(flushing(&db), "flush finished too soon to tell");
    let want: Pairs = (0..n)
        .map(|i| {
            (
                key(i),
                if i % 2 == 0 {
                    b"mem".to_vec()
                } else {
                    b"imm".to_vec()
                },
            )
        })
        .collect();
    assert_eq!(got, want);
}

/// A scan only opens tables whose key range overlaps it: with a table
/// outside the range damaged on disk, a narrow scan still succeeds.
#[test]
fn narrow_scan_skips_tables_outside_its_range() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        target_file_size: 16 << 10,
        block_cache_bytes: 0,
        ..small()
    };
    let db = Db::open_with(dir.path(), opts).unwrap();
    for i in 0..4000 {
        db.put(format!("k{i:04}").as_bytes(), &[b'v'; 20]).unwrap();
    }
    db.compact_all().unwrap();
    let (current, _) = db.shared.read_view();
    let level = current.levels.iter().rfind(|l| !l.is_empty()).unwrap();
    assert!(level.len() > 4, "{} tables", level.len());
    // Damage block 0 of every table except the first. Open already read
    // it (for the smallest key) but kept nothing, and the cache is off,
    // so any scan that opens one of these tables reads the bad bytes.
    // (Damage further in would go unnoticed by a scan that only opens
    // the next table to peek at its first key.)
    for t in &level[1..] {
        let path = table_path(dir.path(), t.id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[20] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
    }
    let first = &level[0];
    let (lo, hi) = (first.smallest().to_vec(), first.largest().to_vec());
    let got = collect(db.scan(&lo[..]..=&hi[..]).unwrap());
    assert_eq!(got.len() as u64, first.reader.entry_count());
    // A scan that reaches the damaged tables reports it.
    let err = db.iter().unwrap().find_map(|item| item.err());
    assert!(matches!(err, Some(Error::Corruption(_))), "{err:?}");
}

/// While a writer adds k0000, k0001, ... in order, with flushes and
/// compactions running, every scan must see a gap-free prefix: it's one
/// point in time, so it can't see a write without every earlier one.
#[test]
fn concurrent_scans_see_a_prefix_of_ordered_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open_with(dir.path(), tiny()).unwrap());
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let scanners: Vec<_> = (0..3)
        .map(|_| {
            let (db, done) = (Arc::clone(&db), Arc::clone(&done));
            thread::spawn(move || {
                let mut scans = 0;
                while !done.load(Ordering::Acquire) {
                    let keys: Vec<Vec<u8>> = collect(db.iter().unwrap())
                        .into_iter()
                        .map(|(k, _)| k)
                        .collect();
                    for (i, k) in keys.iter().enumerate() {
                        assert_eq!(k, format!("k{i:04}").as_bytes(), "gap in a scan");
                    }
                    scans += 1;
                }
                scans
            })
        })
        .collect();
    for i in 0..3000 {
        db.put(format!("k{i:04}").as_bytes(), &[b'v'; 16]).unwrap();
    }
    done.store(true, Ordering::Release);
    let scans: u64 = scanners.into_iter().map(|s| s.join().unwrap()).sum();
    assert!(scans > 10, "only {scans} scans");
    // Sequential keys: tables moved down (D29) while the scans ran.
    assert!(db.stats().level_files[1..].iter().sum::<usize>() > 0);
}

/// Random writes, snapshots, flushes and compactions; scans with random
/// bounds, at now or at a random live snapshot, checked against a model.
#[test]
fn randomized_scans_match_a_model() {
    for seed in 1..=10u64 {
        let mut rng = Rng::new(seed);
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size: 256 + rng.below(2048) as usize,
            target_file_size: 256 + rng.below(2048) as usize,
            ..tiny()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        let mut model = Model::new();
        let mut snaps: Vec<(Snapshot, Model)> = Vec::new();
        let bound = |rng: &mut Rng| match rng.below(3) {
            0 => Bound::Included(rng.key()),
            1 => Bound::Excluded(rng.key()),
            _ => Bound::Unbounded,
        };
        for step in 0..3000 {
            let k = rng.key();
            match rng.below(100) {
                0..=54 => {
                    let v = rng.value();
                    db.put(&k, &v).unwrap();
                    model.insert(k, v);
                }
                55..=69 => {
                    db.delete(&k).unwrap();
                    model.remove(&k);
                }
                70..=72 => snaps.push((db.snapshot(), model.clone())),
                73..=75 if !snaps.is_empty() => {
                    let i = rng.below(snaps.len() as u64) as usize;
                    snaps.swap_remove(i);
                }
                76 => db.flush().unwrap(),
                77 => db.compact_all().unwrap(),
                _ => {
                    let (lo, hi) = (bound(&mut rng), bound(&mut rng));
                    let range = (
                        lo.as_ref().map(Vec::as_slice),
                        hi.as_ref().map(Vec::as_slice),
                    );
                    let (got, want) = if !snaps.is_empty() && rng.below(2) == 0 {
                        let (snap, then) = &snaps[rng.below(snaps.len() as u64) as usize];
                        (
                            collect(snap.scan::<[u8]>(range).unwrap()),
                            pairs::<[u8]>(then, range),
                        )
                    } else {
                        (
                            collect(db.scan::<[u8]>(range).unwrap()),
                            pairs::<[u8]>(&model, range),
                        )
                    };
                    assert_eq!(got, want, "seed {seed} step {step} range {range:?}");
                }
            }
        }
    }
}

// ---- M17: level-0 trivial moves (D29) ----

/// Sequential keys: every flush makes a level-0 table past all the others,
/// so level 0 moves down whole, by manifest edits, and nothing is rewritten.
#[test]
fn sequential_level0_tables_move_down_without_rewriting() {
    let dir = tempfile::tempdir().unwrap();
    let n = 3000;
    {
        let db = Db::open_with(dir.path(), tiny()).unwrap();
        for i in 0..n {
            db.put(&key(i), &key(i)).unwrap();
        }
        db.flush().unwrap();
        let st = db.stats();
        assert_eq!(
            st.compaction_bytes, 0,
            "a sequential load was rewritten: {st:?}"
        );
        assert!(st.level_files[1..].iter().sum::<usize>() > 2, "{st:?}");
        assert_keys(&db, 0..n, "before reopen");
    }
    // Reopen re-checks that the moved tables don't overlap in their levels.
    let db = Db::open_with(dir.path(), tiny()).unwrap();
    assert_keys(&db, 0..n, "after reopen");
}

/// Flushes each batch of puts into its own level-0 table (compactions held
/// back), then lets the one level-0 compaction run and waits for it. Returns
/// the database and whether that compaction rewrote data (merged) rather
/// than moving tables. Stops there: `compact_all` would rewrite the bottom
/// level and hide a bad move.
fn compact_level0(dir: &Path, batches: &[&[(&str, &str)]]) -> (Db, bool) {
    let opts = Options {
        l0_compaction_trigger: batches.len(),
        ..Options::default()
    };
    let db = Db::open_with(dir, opts).unwrap();
    db.state().pause_compactions = true;
    for batch in batches {
        for (k, v) in *batch {
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(db.stats().level_files[0], batches.len());
    db.state().pause_compactions = false;
    db.shared.bg_work.notify_one();
    db.flush().unwrap(); // waits until the background thread is idle
    let st = db.stats();
    assert_eq!(st.level_files[0], 0, "{st:?}");
    (db, st.compaction_bytes > 0)
}

/// Reopening re-checks that no level 1+ holds overlapping or empty tables.
fn reopen_and_get(dir: &Path, key: &str) -> Option<Vec<u8>> {
    Db::open(dir).unwrap().get(key.as_bytes()).unwrap()
}

/// Level-0 tables that share even one user key must merge, never move:
/// moving both would put two tables holding that key into one level.
#[test]
fn level0_tables_sharing_a_key_are_merged() {
    let dir = tempfile::tempdir().unwrap();
    // The second table touches the first only at its edge, "m".
    let (db, merged) = compact_level0(
        dir.path(),
        &[&[("a", "old"), ("m", "x")], &[("m", "new"), ("z", "y")]],
    );
    assert!(merged, "edge-sharing tables were moved");
    assert_eq!(db.stats().level_files[1], 1);
    drop(db);
    assert_eq!(reopen_and_get(dir.path(), "m"), Some(b"new".to_vec()));
}

/// Disjoint level-0 tables still merge when level 1 overlaps them.
#[test]
fn level0_tables_overlapping_level1_are_merged() {
    let dir = tempfile::tempdir().unwrap();
    place_table(dir.path(), 10, 1, &[("a", Some("1")), ("z", Some("1"))]);
    let (db, merged) = compact_level0(dir.path(), &[&[("c", "2")], &[("m", "2")]]);
    assert!(merged, "moved into an overlapping level 1");
    assert_eq!(db.stats().level_files[1], 1);
    drop(db);
    assert_eq!(reopen_and_get(dir.path(), "m"), Some(b"2".to_vec()));

    // A level-1 table in the gap between them, touching neither, still
    // makes this a merge: it lies in the range the compaction covers.
    let dir = tempfile::tempdir().unwrap();
    place_table(dir.path(), 10, 1, &[("e", Some("1")), ("f", Some("1"))]);
    let (db, merged) = compact_level0(dir.path(), &[&[("c", "2")], &[("m", "2")]]);
    assert!(merged);
    assert_eq!(db.stats().level_files[1], 1);
    drop(db);
    assert_eq!(reopen_and_get(dir.path(), "e"), Some(b"1".to_vec()));
}

/// Disjoint level-0 tables with nothing below them move without a rewrite,
/// and keep their contents.
#[test]
fn disjoint_level0_tables_move() {
    let dir = tempfile::tempdir().unwrap();
    let (db, merged) = compact_level0(dir.path(), &[&[("m", "2"), ("n", "2")], &[("a", "1")]]);
    assert!(!merged, "disjoint tables were rewritten");
    assert_eq!(db.stats().level_files[1], 2);
    drop(db);
    assert_eq!(reopen_and_get(dir.path(), "a"), Some(b"1".to_vec()));
}

/// An empty level-0 table is merged away, never moved: levels 1+ refuse
/// empty tables on open.
#[test]
fn an_empty_level0_table_is_not_moved() {
    let dir = tempfile::tempdir().unwrap();
    place_table(dir.path(), 10, 0, &[]);
    place_table(dir.path(), 11, 0, &[("a", Some("1"))]);
    let opts = Options {
        l0_compaction_trigger: 2,
        ..Options::default()
    };
    let db = Db::open_with(dir.path(), opts).unwrap();
    db.flush().unwrap();
    assert_eq!(db.stats().level_files[0], 0);
    drop(db);
    assert_eq!(reopen_and_get(dir.path(), "a"), Some(b"1".to_vec()));
}
