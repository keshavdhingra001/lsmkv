//! lsmkv: an LSM-tree key-value storage engine.
//!
//! Write path: `Db::put` -> WAL append + fsync -> memtable insert.
//! Read path:  `Db::get` -> memtable (SSTables come in milestone M3).

pub mod db;
pub mod error;
pub mod memtable;
pub mod wal;

pub use db::Db;
pub use error::{Error, Result};
