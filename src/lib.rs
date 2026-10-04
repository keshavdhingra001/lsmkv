//! lsmkv: an LSM-tree key-value storage engine.
//!
//! Write path: `Db::put` -> WAL append + fsync -> memtable insert
//!             -> (memtable full) flush to a new SSTable + manifest edit.
//! Read path:  `Db::get` -> memtable -> SSTables newest to oldest; first hit wins.

mod codec;
pub mod db;
pub mod error;
mod fsutil;
pub mod key;
pub mod manifest;
pub mod memtable;
pub mod sstable;
#[cfg(test)]
mod test_util;
pub mod wal;

pub use db::{Db, Options, Snapshot, Stats, SyncMode};
pub use error::{Error, Result};
