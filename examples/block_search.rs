//! Point lookups inside one data block, with restart points (format 5,
//! DESIGN.md D30) and without (formats 2-4: a scan from the block's start).
//! Same entries, same machine, so the difference is the search alone.
//!
//! `cargo run --release --example block_search`

use std::hint::black_box;
use std::time::Instant;

use lsmkv::key::MAX_SEQ;
use lsmkv::memtable::Entry;
use lsmkv::sstable::block::{Block, BlockBuilder};

/// A block of about `block_size` bytes holding 16-byte keys and `value_len`-byte values.
fn build(block_size: usize, value_len: usize) -> (Vec<u8>, Vec<Vec<u8>>) {
    let mut b = BlockBuilder::new();
    let mut keys = Vec::new();
    let mut i = 0u64;
    while b.size() < block_size {
        let key = format!("{i:016}").into_bytes();
        b.add(&key, i + 1, &Entry::Value(vec![b'v'; value_len]))
            .unwrap();
        keys.push(key);
        i += 1;
    }
    (b.finish(), keys)
}

fn per_lookup_ns(block: &Block, keys: &[Vec<u8>]) -> f64 {
    let rounds = 2_000_000 / keys.len().max(1);
    let start = Instant::now();
    for _ in 0..rounds {
        for k in keys {
            black_box(block.get(black_box(k), MAX_SEQ).unwrap());
        }
    }
    start.elapsed().as_nanos() as f64 / (rounds * keys.len()) as f64
}

fn main() {
    println!(
        "| block | value | entries | scan (formats 2-4) | restart points (format 5) | speedup |"
    );
    println!("|---:|---:|---:|---:|---:|---:|");
    for (block_size, value_len) in [(4096, 100), (4096, 16), (16384, 100), (65536, 100)] {
        let (raw, keys) = build(block_size, value_len);
        let with = Block::new(&raw, true).unwrap();
        // The same entries in the old layout: no trailer, one CRC.
        let mut old = with.iter().fold(Vec::new(), |mut out, e| {
            let (k, seq, v) = e.unwrap();
            let mut b = BlockBuilder::new();
            b.add(k, seq, &Entry::Value(v.unwrap().to_vec())).unwrap();
            let one = b.finish();
            out.extend_from_slice(&one[..one.len() - 12]); // entry only
            out
        });
        old.extend_from_slice(&crc32fast::hash(&old).to_le_bytes());
        let without = Block::new(&old, false).unwrap();
        let (a, b) = (per_lookup_ns(&without, &keys), per_lookup_ns(&with, &keys));
        println!(
            "| {} KiB | {value_len} B | {} | {a:.0} ns | {b:.0} ns | {:.1}x |",
            block_size / 1024,
            keys.len(),
            a / b
        );
    }
}
