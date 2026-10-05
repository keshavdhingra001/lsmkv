//! Any bytes from a client: the parser must never panic, never claim more
//! bytes than it was given, and must parse the same command when the input
//! arrives one byte at a time (pipelining across reads).
#![no_main]

use libfuzzer_sys::fuzz_target;
use lsmkv::resp::parse;

fuzz_target!(|data: &[u8]| {
    let whole = parse(data);
    if let Ok(Some((_, used))) = &whole {
        assert!(*used > 0 && *used <= data.len());
    }
    // Fed incrementally, it needs more bytes until the same answer appears.
    for end in 0..data.len() {
        match parse(&data[..end]) {
            Ok(None) => {}
            Ok(Some((cmd, used))) => {
                assert_eq!(whole, Ok(Some((cmd, used))), "prefix {end}");
                return;
            }
            // An error is final: more bytes can't make it valid.
            Err(e) => {
                assert!(whole.is_err(), "prefix {end} failed ({e}) but whole parsed");
                return;
            }
        }
    }
});
