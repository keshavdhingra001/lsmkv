# Checkpoint log

Single source of truth for "where are we". Update at the end of every session.

## Roadmap

### Tier 1: Baseline
- [x] **M0** Scaffold: crate layout, error type, Db glue, REPL, tests for M1/M2 *(Claude)*
- [ ] **M1** Memtable: `put` / `delete` / `get` with tombstones *(you)*
- [ ] **M2** WAL: `append` / `replay`, CRC32 per record, torn-tail handling *(you)*
- [ ] **M3** SSTable writer + reader: data blocks, index block, footer *(you, Claude writes tests)*
- [ ] **M4** Flush memtable -> SSTable at size threshold; read path checks memtable then SSTables newest-first; manifest *(you)*
- [ ] **Gate:** Tier 1 done -> make the GitHub repo public

### Tier 2: Strong (target)
- [ ] **M5** Compaction (decide: leveled vs size-tiered), drop tombstones safely
- [ ] **M6** Bloom filters per SSTable + LRU block cache
- [ ] **M7** Durability modes: per-write fsync / group commit / periodic; measure each
- [ ] **M8** Concurrency: snapshot reads that never block the writer
- [ ] **M9** Range scans: merging iterator across memtable + levels
- [ ] **M10** Crash-injection harness (random kill -9, verify no acked write lost) + model-based fuzz test
- [ ] **M11** Benchmarks (ops/sec, p50/p99, write amplification) vs RocksDB
- [ ] **M12** DESIGN.md complete, README with results

### Tier 3: Stretch (pick 2–3 later)
MVCC, atomic batches, RESP server, compression, deterministic simulation testing, Raft.

## Current status

**2026-10-04: M0 done.** Scaffold committed. Nothing compiled yet (Rust not installed at scaffold time).

**Next step:** install Rust, then run `cargo test`. Every M1/M2 test should fail with `todo!()` panics.
Implement `src/memtable.rs` (M1) until `cargo test memtable` passes, then `src/wal.rs` (M2).

## Blockers / open decisions
- [ ] Rust toolchain not installed (`sudo pacman -S rustup && rustup default stable`)
- [x] GitHub: private repo https://github.com/keshavdhingra001/lsmkv (HTTPS remote, pushed M0)
- [ ] Confirm git email `keshavdhingra007@gmail.com` is on the GitHub account
- [ ] DESIGN.md: how to treat mid-log WAL corruption (see `Wal::replay` doc comment)
