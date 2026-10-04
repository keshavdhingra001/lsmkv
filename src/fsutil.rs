//! Filesystem durability helpers.

use std::fs::File;
use std::path::Path;

use crate::error::Result;

/// fsyncs a directory, which makes file creations, renames and deletions
/// inside it durable. fsyncing a file only covers the file's contents, not
/// its directory entry.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    File::open(dir)?.sync_all()?;
    Ok(())
}
