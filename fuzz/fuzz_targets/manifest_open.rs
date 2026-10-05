//! Any bytes as a MANIFEST: opening must never panic, and a manifest that
//! opens must open again to the same version (opening is idempotent).
#![no_main]

use std::io::Write;
use std::path::Path;

use libfuzzer_sys::fuzz_target;
use lsmkv::manifest::{Manifest, MANIFEST_FILE};
use lsmkv::vfs::{Fs, SimFs};

fuzz_target!(|data: &[u8]| {
    let fs = SimFs::new(0);
    let dir = Path::new("/db");
    let mut f = fs.create(&dir.join(MANIFEST_FILE)).unwrap();
    f.write_all(data).unwrap();
    drop(f);

    let Ok((m, version)) = Manifest::open_in(&fs, dir) else {
        return;
    };
    drop(m);
    let (_, again) = Manifest::open_in(&fs, dir).expect("an opened manifest reopens");
    assert_eq!(again, version);
});
