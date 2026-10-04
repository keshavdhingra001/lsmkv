# lsmkv

An LSM-tree key-value storage engine written from scratch in Rust: write-ahead log,
memtable, SSTables, compaction and crash recovery.

> Work in progress. See [CHECKPOINT.md](CHECKPOINT.md) for status and [DESIGN.md](DESIGN.md) for design decisions.

## Try it

```bash
cargo test
cargo run -- ./data
```

```
> put user:1 alice
OK
> get user:1
alice
> flush
OK
> stats
memtable: 0 entries, ~0 bytes | tables: 1 | log: 000003.log
> del user:1
OK
```

## What's built so far

- **Write-ahead log** with per-record CRC32. Torn tails from a crash are cut off; corruption mid-log refuses to open.
- **Memtable** (sorted) with tombstones.
- **SSTables**: 4 KiB checksummed blocks, an in-memory block index, and a checksummed footer. Published atomically (tmp + fsync + rename + dir fsync).
- **Flush and recovery**: the memtable flushes to an SSTable at 4 MiB. A manifest records live files, with a single commit point per flush. Crash leftovers are cleaned up on open.
- **Failure handling**: a failed WAL or manifest write makes the database read-only until it's reopened (the fsyncgate lesson).
- **Tests**: 59, including crash injection at every flush step, every-byte corruption checks, and randomized operations checked against a `BTreeMap`.
