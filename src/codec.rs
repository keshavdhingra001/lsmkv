//! Little-endian integer helpers shared by the WAL and SSTable formats.

use crate::error::{Error, Result};

/// Reads a little-endian `u32` at `at`. Caller guarantees `buf` is long enough.
pub(crate) fn read_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().expect("4-byte slice"))
}

/// Length of `bytes` as a `u32`, or an error if it doesn't fit (> 4 GiB).
pub(crate) fn len_u32(bytes: &[u8], what: &str) -> Result<u32> {
    u32::try_from(bytes.len()).map_err(|_| {
        Error::InvalidArgument(format!(
            "{what} too large: {} bytes (max {})",
            bytes.len(),
            u32::MAX
        ))
    })
}
