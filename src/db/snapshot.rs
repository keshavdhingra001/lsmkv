//! Point-in-time reads (DESIGN.md D18).
//!
//! A snapshot is only a sequence number: reads through it see, for each key,
//! the newest version at or below that number. It stays valid because flush
//! and compaction never drop a version a registered snapshot could see
//! (`State::oldest_snapshot`), and dropping the handle unregisters it, so
//! those versions can then be garbage collected.
//!
//! Taking and dropping a snapshot uses the read-view lock only, never the
//! state lock, so like `get` it never waits for writes, flushes or
//! compactions.

use std::sync::Arc;

use super::{lock, Db};
use crate::error::Result;
use crate::key::SeqNo;

/// A consistent view of the database as of `Db::snapshot`: every write
/// acknowledged before that call, and none started after it. Reads of
/// several keys through one snapshot agree with each other, unlike separate
/// `Db::get` calls, between which other writes can land.
///
/// Hold it only as long as needed: while it lives, the versions it can see
/// are kept, even ones every other reader has moved past.
pub struct Snapshot<'a> {
    db: &'a Db,
    seq: SeqNo,
}

impl Db {
    /// Takes a snapshot of the database as it is now.
    pub fn snapshot(&self) -> Snapshot<'_> {
        let mut view = lock(&self.shared.view);
        let seq = view.last_seq;
        *view.snapshots.entry(seq).or_insert(0) += 1;
        Snapshot { db: self, seq }
    }
}

impl Snapshot<'_> {
    /// `key`'s value as of this snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // The current tables, not the ones at snapshot time: they still hold
        // every version this snapshot can see, since compaction keeps those.
        let current = Arc::clone(&lock(&self.db.shared.view).current);
        current.get(key, self.seq)
    }

    /// The sequence number this snapshot reads at: the last write it sees.
    pub fn sequence(&self) -> SeqNo {
        self.seq
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        let mut view = lock(&self.db.shared.view);
        let count = view
            .snapshots
            .get_mut(&self.seq)
            .expect("a live snapshot is registered");
        *count -= 1;
        if *count == 0 {
            view.snapshots.remove(&self.seq);
        }
    }
}
