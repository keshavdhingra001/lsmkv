//! Helpers shared by tests across modules.

/// xorshift64: tiny deterministic PRNG, so failures are reproducible by seed.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        // Spread small seeds out; xorshift must never start at 0.
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub(crate) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// Short keys over a 4-letter alphabet: lots of shared prefixes, empty
    /// keys and near-misses, which is where off-by-one bugs live.
    pub(crate) fn key(&mut self) -> Vec<u8> {
        let len = self.below(9);
        (0..len).map(|_| b'a' + self.below(4) as u8).collect()
    }

    pub(crate) fn value(&mut self) -> Vec<u8> {
        let len = self.below(65);
        (0..len).map(|_| self.next() as u8).collect()
    }
}
