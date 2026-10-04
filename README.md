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
> del user:1
OK
```
