//! Any bytes as an SSTable: opening, point reads and a full scan must
//! return results or errors, never panic or allocate without bound.
#![no_main]

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use lsmkv::key::MAX_SEQ;
use lsmkv::sstable::SstReader;
use lsmkv::vfs::{Fs, SimFs};

fuzz_target!(|data: &[u8]| {
    let fs = SimFs::new(0);
    let path = Path::new("/db/000001.sst");
    let mut f = fs.create(path).unwrap();
    f.write_all(data).unwrap();
    drop(f);

    let Ok(reader) = SstReader::open_in(&fs, path, 1, Arc::default()) else {
        return;
    };
    let reader = Arc::new(reader);
    let _ = reader.get(b"k", MAX_SEQ);
    let _ = reader.entries();
    if let Ok(iter) = reader.iter(Some(b"a")) {
        for item in iter {
            if item.is_err() {
                break;
            }
        }
    }
});
