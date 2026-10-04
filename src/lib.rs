//! lsmkv: an LSM-tree key-value storage engine.
//!
//! Write path: `Db::put` -> WAL append + fsync -> memtable insert.
//! Read path:  `Db::get` -> memtable (SSTables are wired in at M4).

mod codec;
pub mod db;
pub mod error;
mod fsutil;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;

pub use db::Db;
pub use error::{Error, Result};
