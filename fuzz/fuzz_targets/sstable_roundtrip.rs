//! Arbitrary entries and block sizes: a table written by `SstWriter` must
//! read back exactly (every point read, every seek, the full scan).
#![no_main]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use lsmkv::key::{SeqNo, MAX_SEQ};
use lsmkv::memtable::Entry;
use lsmkv::sstable::{SstReader, SstWriter, WriterOptions};
use lsmkv::vfs::SimFs;

#[derive(Debug, Arbitrary)]
struct Input {
    block_size: u16,
    bloom_bits: u8,
    /// (key, value or tombstone); a key may repeat, as versions.
    entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    probes: Vec<Vec<u8>>,
}

fuzz_target!(|input: Input| {
    // Number every write; keep (key, seq) -> entry, in internal key order
    // (key ascending, seq descending).
    let mut model: BTreeMap<(Vec<u8>, std::cmp::Reverse<SeqNo>), Entry> = BTreeMap::new();
    for (i, (key, value)) in input.entries.into_iter().enumerate() {
        let entry = value.map_or(Entry::Tombstone, Entry::Value);
        model.insert((key, std::cmp::Reverse(i as SeqNo + 1)), entry);
    }

    let fs = Arc::new(SimFs::new(0));
    let path = Path::new("/db/000001.sst");
    let opts = WriterOptions {
        block_size: input.block_size as usize % 8192 + 1,
        bloom_bits_per_key: input.bloom_bits as usize % 20,
    };
    let mut w = SstWriter::with_options_in(fs.clone(), path, opts).unwrap();
    for ((key, seq), entry) in &model {
        w.add(key, seq.0, entry).unwrap();
    }
    w.finish().unwrap();

    let r = Arc::new(SstReader::open_in(&*fs, path, 1, Arc::default()).unwrap());
    let expected: Vec<_> = model
        .iter()
        .map(|((k, s), e)| (k.clone(), s.0, e.clone()))
        .collect();
    assert_eq!(r.entries().unwrap(), expected);
    let scanned: Vec<_> = r.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(scanned, expected);

    // Point reads: the newest version of each key, at the newest snapshot
    // and at one just below it.
    let keys = expected.iter().map(|e| e.0.clone()).chain(input.probes);
    for key in keys {
        let newest = expected.iter().find(|e| e.0 == key);
        assert_eq!(
            r.get(&key, MAX_SEQ).unwrap(),
            newest.map(|e| e.2.clone()),
            "get {key:?}"
        );
        if let Some(&(_, seq, _)) = newest {
            let older = expected.iter().find(|e| e.0 == key && e.1 < seq);
            assert_eq!(
                r.get(&key, seq - 1).unwrap(),
                older.map(|e| e.2.clone()),
                "get {key:?} below {seq}"
            );
        }
        // A seek starts at the first entry with a key >= the probe.
        let from: Vec<_> = r.iter(Some(&key)).unwrap().map(Result::unwrap).collect();
        let want: Vec<_> = expected.iter().filter(|e| e.0 >= key).cloned().collect();
        assert_eq!(from, want, "seek {key:?}");
    }
});
