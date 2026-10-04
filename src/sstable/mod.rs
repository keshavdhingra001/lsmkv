//! Sorted String Table: an immutable, sorted on-disk file of key -> entry.
//!
//! ```text
//! +--------------+--------------+-----+--------------+-------------+-------------+
//! | data block 0 | data block 1 | ... | filter block | index block | footer (48B)|
//! +--------------+--------------+-----+--------------+-------------+-------------+
//! ```
//!
//! See `block.rs` for the data block layout and DESIGN.md D5 for the reasoning.

pub mod block;
