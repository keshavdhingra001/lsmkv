#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// An earlier WAL or manifest write failed, so the on-disk state is
    /// uncertain. Writes are refused until the database is reopened, which
    /// recovers from what actually reached the disk.
    #[error("database is read-only after an earlier write failure: {0}")]
    Poisoned(String),

    /// A transaction's commit lost to a write committed after its snapshot.
    /// Nothing was applied; retrying with a new transaction is safe.
    #[error("transaction conflict: {0}")]
    Conflict(String),
}

pub type Result<T> = std::result::Result<T, Error>;
