//! Bloom filter: one per SSTable, so a lookup for a key the table doesn't hold
//! usually skips the table without reading a data block.
//!
//! ```text
//! +---------------------+------+-----------+
//! | bit array (m/8 B)   | k u8 | crc32 u32 |
//! +---------------------+------+-----------+
//! ```
//!
//! - A key sets `k` bits, chosen from one 64-bit hash by double hashing
//!   (Kirsch–Mitzenmacher): probe `i` is bit `(h1 + i * h2) mod m`.
//! - `k` is stored in the filter, so tables written with different settings
//!   stay readable. The CRC covers the bits and `k`.
//! - The answer is "definitely not here" or "maybe here". False negatives are
//!   impossible: every bit an added key set is still set.
//!
//! See DESIGN.md D9 for the false-positive math.

use crate::codec::read_u32;
use crate::error::{Error, Result};

/// LevelDB's default: about 1% false positives.
pub const DEFAULT_BITS_PER_KEY: usize = 10;
/// Tiny filters have a high false-positive rate whatever the key count.
const MIN_BITS: usize = 64;
/// More probes than this never pays off (and caps a corrupt `k`).
const MAX_PROBES: u8 = 30;
const TRAILER_LEN: usize = 1 + 4;

/// FNV-1a over the key, then the MurmurHash3 64-bit finalizer.
///
/// Filters are persisted, so this must never change: a different hash would
/// make every saved filter answer "not here" for keys it holds. That's why
/// `std`'s `DefaultHasher` is not used: its algorithm is unspecified and may
/// change between Rust releases. `hash_is_stable` pins the output.
///
/// FNV-1a alone mixes the last bytes of a key poorly into the high bits; the
/// finalizer spreads every input bit across all 64 output bits, which double
/// hashing needs because it uses both halves.
pub fn hash(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Optimal probe count is `bits_per_key * ln 2` (10 bits -> 6.93 -> 7).
fn probes_for(bits_per_key: usize) -> u8 {
    let k = (bits_per_key * 69 + 50) / 100;
    k.clamp(1, MAX_PROBES as usize) as u8
}

/// The `k` bit positions for a key's hash, in a bit array of `m` bits.
fn positions(h: u64, k: u8, m: u64) -> impl Iterator<Item = u64> {
    let h1 = h & 0xffff_ffff;
    let h2 = h >> 32;
    // h1, h2 < 2^32 and i < 2^5, so this can't overflow a u64.
    (0..k as u64).map(move |i| (h1 + i * h2) % m)
}

/// Builds an encoded filter from the hashes of every key in a table.
pub fn build(hashes: &[u64], bits_per_key: usize) -> Vec<u8> {
    let bits = (hashes.len() * bits_per_key).max(MIN_BITS);
    let bytes = bits.div_ceil(8);
    let m = bytes as u64 * 8;
    let k = probes_for(bits_per_key);

    let mut out = vec![0u8; bytes];
    for &h in hashes {
        for bit in positions(h, k, m) {
            out[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }
    out.push(k);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// A decoded, checksum-verified filter.
#[derive(Debug)]
pub struct BloomFilter {
    bits: Vec<u8>,
    k: u8,
}

impl BloomFilter {
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let bad = |what: &str| Error::Corruption(format!("bloom filter: {what}"));
        if buf.len() < TRAILER_LEN + 1 {
            return Err(bad("too short"));
        }
        let (body, crc) = buf.split_at(buf.len() - 4);
        if crc32fast::hash(body) != read_u32(crc, 0) {
            return Err(bad("checksum mismatch"));
        }
        let (bits, k) = body.split_at(body.len() - 1);
        let k = k[0];
        if k == 0 || k > MAX_PROBES {
            return Err(bad(&format!("probe count {k} out of range")));
        }
        Ok(Self {
            bits: bits.to_vec(),
            k,
        })
    }

    /// `false` = the key was definitely never added. `true` = maybe.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let m = self.bits.len() as u64 * 8;
        positions(hash(key), self.k, m)
            .all(|bit| self.bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_for(keys: &[Vec<u8>], bits_per_key: usize) -> BloomFilter {
        let hashes: Vec<u64> = keys.iter().map(|k| hash(k)).collect();
        BloomFilter::decode(&build(&hashes, bits_per_key)).unwrap()
    }

    fn keys(prefix: &str, n: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("{prefix}{i}").into_bytes())
            .collect()
    }

    #[test]
    fn hash_is_stable() {
        // Pinned outputs. If this fails, every filter already on disk is now
        // wrong: do not "fix" the constants, fix the hash.
        assert_eq!(hash(b""), 0xefd0_1f60_ba99_2926);
        assert_eq!(hash(b"a"), 0x82a2_a958_a9be_ce5b);
        assert_eq!(hash(b"key000123"), 0x6bdf_8c18_4853_d46c);
    }

    #[test]
    fn probe_count_follows_ln2() {
        assert_eq!(probes_for(10), 7);
        assert_eq!(probes_for(1), 1);
        assert_eq!(probes_for(0), 1);
        assert_eq!(probes_for(1000), MAX_PROBES);
    }

    #[test]
    fn no_false_negatives() {
        for bits_per_key in [1, 4, 10, 20] {
            let added = keys("key", 5000);
            let f = filter_for(&added, bits_per_key);
            for k in &added {
                assert!(f.may_contain(k), "{bits_per_key} bits/key lost {k:?}");
            }
        }
    }

    #[test]
    fn false_positive_rate_matches_theory() {
        // Theory for 10 bits/key, k = 7: (1 - e^(-7/10))^7 = 0.82%.
        let f = filter_for(&keys("present", 10_000), 10);
        let probes = 100_000;
        let hits = keys("absent", probes)
            .iter()
            .filter(|k| f.may_contain(k))
            .count();
        let rate = hits as f64 / probes as f64;
        println!("false-positive rate at 10 bits/key: {:.3}%", rate * 100.0);
        assert!(rate < 0.015, "false-positive rate {rate}");
    }

    #[test]
    fn empty_filter_rejects_everything() {
        let f = filter_for(&[], 10);
        assert!(!f.may_contain(b""));
        assert!(!f.may_contain(b"anything"));
    }

    #[test]
    fn size_is_bits_per_key_times_keys() {
        let hashes: Vec<u64> = (0..1000)
            .map(|i| hash(&[i as u8, (i >> 8) as u8]))
            .collect();
        assert_eq!(build(&hashes, 10).len(), 1250 + TRAILER_LEN);
        assert_eq!(build(&[], 10).len(), MIN_BITS / 8 + TRAILER_LEN);
    }

    #[test]
    fn any_flipped_byte_is_detected() {
        let raw = build(
            &keys("k", 20).iter().map(|k| hash(k)).collect::<Vec<_>>(),
            10,
        );
        for i in 0..raw.len() {
            let mut bad = raw.clone();
            bad[i] ^= 0x01;
            assert!(
                matches!(BloomFilter::decode(&bad), Err(Error::Corruption(_))),
                "flip at byte {i} not detected"
            );
        }
    }

    #[test]
    fn bad_probe_count_and_short_input_are_corruption() {
        for k in [0u8, MAX_PROBES + 1] {
            let mut raw = vec![0u8; 8];
            raw.push(k);
            let crc = crc32fast::hash(&raw);
            raw.extend_from_slice(&crc.to_le_bytes());
            assert!(
                matches!(BloomFilter::decode(&raw), Err(Error::Corruption(_))),
                "k = {k}"
            );
        }
        assert!(matches!(
            BloomFilter::decode(&[1, 2, 3, 4, 5]),
            Err(Error::Corruption(_))
        ));
    }
}
