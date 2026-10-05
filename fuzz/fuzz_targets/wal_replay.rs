//! Any bytes as a WAL: replay must never panic or over-allocate, and what
//! it does accept must survive being logged again unchanged.
#![no_main]

use std::io::Write;
use std::path::Path;

use libfuzzer_sys::fuzz_target;
use lsmkv::vfs::{Fs, SimFs};
use lsmkv::wal::Wal;

fuzz_target!(|data: &[u8]| {
    let fs = SimFs::new(0);
    let path = Path::new("/f/000001.log");
    let mut f = fs.create(path).unwrap();
    f.write_all(data).unwrap();
    drop(f);

    let Ok(replay) = Wal::replay_in(&fs, path) else {
        return;
    };
    assert!(replay.valid_len <= replay.file_len);
    assert_eq!(replay.file_len, data.len() as u64);

    // Re-log what was accepted: it must replay to exactly the same records.
    let again = Path::new("/f/000002.log");
    let mut wal = Wal::open_in(&fs, again).unwrap();
    for (seq, rec) in &replay.records {
        wal.append(*seq, rec).unwrap();
    }
    wal.sync().unwrap();
    assert_eq!(Wal::replay_in(&fs, again).unwrap().records, replay.records);
});
