//! lsmkv: an LSM-tree key-value storage engine.
//!
//! Write path: `Db::put` -> WAL append + fsync -> memtable insert
//!             -> (memtable full) flush to a new SSTable + manifest edit.
//! Read path:  `Db::get` -> memtable -> SSTables newest to oldest; first hit wins.
//! Scans:      `Db::scan` -> a k-way merge of every source, newest version per key.

// Compiles and runs the README's Rust example as a doc test.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

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

pub use db::{Db, DbIter, Options, Snapshot, Stats, SyncMode};
pub use error::{Error, Result};
