//! LRU block cache, shared by every table a database has open.
//!
//! - Key: `(table id, block offset)`. File numbers are never reused (D6), so a
//!   key can never point at a different table's block.
//! - Value: a data block that already passed its CRC check, as `Arc<[u8]>`.
//!   A lookup clones the `Arc` and releases the lock right away; if the block
//!   is evicted while a reader still uses it, the bytes live until that reader
//!   drops its `Arc`.
//! - Capacity is in bytes, not entries: blocks vary in size (one large value
//!   makes one large block).
//!
//! Classic O(1) LRU: a hash map from key to slot, plus a doubly linked list of
//! slots in recency order. The list lives in a `Vec` and links by index
//! instead of by pointer, which keeps it in safe Rust. Freed slots are reused.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type CacheKey = (u64, u64);

/// "No slot": the end of the list in either direction.
const NIL: usize = usize::MAX;

#[derive(Debug)]
pub struct BlockCache {
    capacity: usize,
    /// `get` takes `&self` (reads share the database), and a hit must still
    /// reorder the list, so the state sits behind a lock.
    inner: Mutex<Lru>,
}

#[derive(Debug, Default)]
struct Lru {
    used: usize,
    map: HashMap<CacheKey, usize>,
    slots: Vec<Slot>,
    /// Slots of evicted entries, reused before `slots` grows.
    free: Vec<usize>,
    /// Most recently used.
    head: usize,
    /// Least recently used: the next eviction.
    tail: usize,
}

#[derive(Debug)]
struct Slot {
    key: CacheKey,
    block: Arc<[u8]>,
    prev: usize,
    next: usize,
}

impl BlockCache {
    /// A capacity of 0 turns the cache off: nothing is ever stored.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Mutex::new(Lru {
                head: NIL,
                tail: NIL,
                ..Lru::default()
            }),
        }
    }

    pub fn get(&self, key: CacheKey) -> Option<Arc<[u8]>> {
        let mut lru = self.lock();
        let i = *lru.map.get(&key)?;
        lru.unlink(i);
        lru.push_front(i);
        Some(Arc::clone(&lru.slots[i].block))
    }

    /// Adds (or replaces) a block as the most recently used, then evicts from
    /// the cold end until the total fits. A block bigger than the whole cache
    /// is not stored: it would evict everything and then itself.
    pub fn insert(&self, key: CacheKey, block: Arc<[u8]>) {
        if block.len() > self.capacity {
            return;
        }
        let mut lru = self.lock();
        if let Some(&i) = lru.map.get(&key) {
            lru.used = lru.used - lru.slots[i].block.len() + block.len();
            lru.slots[i].block = block;
            lru.unlink(i);
            lru.push_front(i);
        } else {
            lru.used += block.len();
            let slot = Slot {
                key,
                block,
                prev: NIL,
                next: NIL,
            };
            let i = match lru.free.pop() {
                Some(i) => {
                    lru.slots[i] = slot;
                    i
                }
                None => {
                    lru.slots.push(slot);
                    lru.slots.len() - 1
                }
            };
            lru.map.insert(key, i);
            lru.push_front(i);
        }
        while lru.used > self.capacity {
            lru.evict_tail();
        }
    }

    /// Bytes of block data currently cached.
    pub fn used(&self) -> usize {
        self.lock().used
    }

    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Lru> {
        // Poisoned only if a thread panicked mid-update, which would leave the
        // list in an unknown state. A wrong list could serve the wrong block,
        // so fail loudly instead of carrying on.
        self.inner.lock().expect("block cache lock poisoned")
    }
}

impl Lru {
    /// Detaches slot `i` from the list, patching its neighbours (or the
    /// head/tail if it was at an end).
    fn unlink(&mut self, i: usize) {
        let (prev, next) = (self.slots[i].prev, self.slots[i].next);
        match prev {
            NIL => self.head = next,
            p => self.slots[p].next = next,
        }
        match next {
            NIL => self.tail = prev,
            n => self.slots[n].prev = prev,
        }
        self.slots[i].prev = NIL;
        self.slots[i].next = NIL;
    }

    fn push_front(&mut self, i: usize) {
        self.slots[i].prev = NIL;
        self.slots[i].next = self.head;
        match self.head {
            NIL => self.tail = i,
            h => self.slots[h].prev = i,
        }
        self.head = i;
    }

    fn evict_tail(&mut self) {
        let i = self.tail;
        debug_assert_ne!(i, NIL, "over capacity with an empty list");
        let key = self.slots[i].key;
        // A stale tail (a link bug) would point at a freed, empty slot, and
        // "evicting" it frees nothing, so `insert`'s loop would spin forever.
        debug_assert_eq!(self.map.get(&key), Some(&i), "tail is not a live slot");
        self.unlink(i);
        self.map.remove(&key);
        self.used -= self.slots[i].block.len();
        // Drop our reference now rather than when the slot is reused.
        self.slots[i].block = Arc::from(&[][..]);
        self.free.push(i);
    }

    /// Keys from most to least recently used (tests only).
    #[cfg(test)]
    fn order(&self) -> Vec<CacheKey> {
        let mut out = Vec::new();
        let mut i = self.head;
        while i != NIL {
            // A link bug can make the list loop; fail instead of hanging.
            assert!(out.len() < self.map.len(), "cycle in the LRU list");
            out.push(self.slots[i].key);
            i = self.slots[i].next;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::Rng;

    fn block(len: usize, fill: u8) -> Arc<[u8]> {
        vec![fill; len].into()
    }

    fn order(c: &BlockCache) -> Vec<CacheKey> {
        c.lock().order()
    }

    #[test]
    fn hit_and_miss() {
        let c = BlockCache::new(100);
        assert!(c.get((1, 0)).is_none());
        c.insert((1, 0), block(10, 7));
        assert_eq!(&*c.get((1, 0)).unwrap(), &[7; 10][..]);
        // Same offset, other table: a different key.
        assert!(c.get((2, 0)).is_none());
    }

    #[test]
    fn evicts_least_recently_used_not_oldest() {
        let c = BlockCache::new(30);
        c.insert((1, 0), block(10, 1));
        c.insert((1, 1), block(10, 2));
        c.insert((1, 2), block(10, 3));
        // Touch the oldest, so the next insert evicts (1, 1) instead.
        c.get((1, 0)).unwrap();
        c.insert((1, 3), block(10, 4));
        assert!(c.get((1, 1)).is_none());
        assert_eq!(order(&c), [(1, 3), (1, 0), (1, 2)]);
        assert_eq!(c.used(), 30);
    }

    #[test]
    fn capacity_is_in_bytes() {
        let c = BlockCache::new(100);
        for i in 0..9 {
            c.insert((1, i), block(10, 0));
        }
        // 90 + 50 = 140 bytes: the four coldest 10-byte blocks go, which
        // lands exactly on the capacity (allowed: the limit is inclusive).
        c.insert((2, 0), block(50, 0));
        assert_eq!(c.used(), 100);
        assert_eq!(c.len(), 6);
        assert_eq!(order(&c), [(2, 0), (1, 8), (1, 7), (1, 6), (1, 5), (1, 4)]);
    }

    #[test]
    fn oversized_block_and_zero_capacity_store_nothing() {
        let c = BlockCache::new(10);
        c.insert((1, 0), block(5, 0));
        c.insert((1, 1), block(11, 0));
        assert_eq!(order(&c), [(1, 0)], "an oversized block evicts nothing");

        let off = BlockCache::new(0);
        off.insert((1, 0), block(1, 0));
        assert!(off.is_empty());
    }

    #[test]
    fn reinsert_replaces_and_recharges() {
        let c = BlockCache::new(100);
        c.insert((1, 0), block(10, 1));
        c.insert((1, 1), block(10, 2));
        c.insert((1, 0), block(40, 3));
        assert_eq!(c.used(), 50);
        assert_eq!(c.len(), 2);
        assert_eq!(order(&c), [(1, 0), (1, 1)]);
        assert_eq!(&*c.get((1, 0)).unwrap(), &[3; 40][..]);
    }

    #[test]
    fn evicted_block_stays_valid_for_its_holder() {
        let c = BlockCache::new(10);
        c.insert((1, 0), block(10, 9));
        let held = c.get((1, 0)).unwrap();
        c.insert((1, 1), block(10, 0));
        assert!(c.get((1, 0)).is_none());
        assert_eq!(&*held, &[9; 10][..]);
    }

    /// Random gets and inserts against a deliberately naive model: a Vec in
    /// recency order with linear scans. Catches link-patching bugs that the
    /// hand-written cases miss (unlinking the head, the tail, a lone slot,
    /// reusing a freed slot...).
    #[test]
    fn randomized_matches_a_simple_model() {
        for seed in 1..=30u64 {
            let mut rng = Rng::new(seed);
            let capacity = 1 + rng.below(200) as usize;
            let c = BlockCache::new(capacity);
            let mut model: Vec<(CacheKey, Arc<[u8]>)> = Vec::new();
            for step in 0..3000 {
                let key = (rng.below(3), rng.below(8));
                if rng.below(2) == 0 {
                    let got = c.get(key);
                    let pos = model.iter().position(|(k, _)| *k == key);
                    assert_eq!(got.is_some(), pos.is_some(), "seed {seed} step {step}");
                    if let Some(p) = pos {
                        let e = model.remove(p);
                        assert_eq!(got.unwrap(), e.1, "seed {seed} step {step}");
                        model.insert(0, e);
                    }
                } else {
                    let b = block(rng.below(60) as usize, rng.next() as u8);
                    c.insert(key, Arc::clone(&b));
                    if b.len() <= capacity {
                        model.retain(|(k, _)| *k != key);
                        model.insert(0, (key, b));
                        while model.iter().map(|(_, b)| b.len()).sum::<usize>() > capacity {
                            model.pop();
                        }
                    }
                }
                let want: Vec<CacheKey> = model.iter().map(|(k, _)| *k).collect();
                assert_eq!(order(&c), want, "seed {seed} step {step}");
                assert_eq!(c.used(), model.iter().map(|(_, b)| b.len()).sum::<usize>());
            }
        }
    }

    #[test]
    fn shared_across_threads() {
        let c = BlockCache::new(4096);
        std::thread::scope(|s| {
            for t in 0..4u64 {
                let c = &c;
                s.spawn(move || {
                    for i in 0..2000u64 {
                        let key = (t, i % 64);
                        match c.get(key) {
                            Some(b) => assert_eq!(b[0], t as u8),
                            None => c.insert(key, block(32, t as u8)),
                        }
                    }
                });
            }
        });
        assert!(c.used() <= 4096);
        assert_eq!(c.used(), c.len() * 32);
    }
}
