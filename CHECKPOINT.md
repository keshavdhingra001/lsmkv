# Checkpoint log

Single source of truth for "where are we". Update at the end of every session.

## Roadmap

### Tier 1: Baseline
- [x] **M0** Scaffold: crate layout, error type, Db glue, REPL, tests for M1/M2 *(Claude)*
- [x] **M1** Memtable: `put` / `delete` / `get` with tombstones *(Claude; 7/7 tests pass; owner review pending)*
- [x] **M2** WAL: `append` / `replay`, CRC32 per record, torn-tail handling *(Claude; 18/18 tests + manual kill -9 recovery check; owner review pending)*
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

**2026-10-04: Mode switched.** Claude now writes each milestone; owner studies and answers questions before the next one (see CLAUDE.md).
M1 done: 7/7 memtable tests pass, clippy clean. Rust 1.99.0 installed (via TUNA mirror; Fastly route from the hotspot is slow).

**2026-10-04: M2 done.** WAL append/replay written; all 18 tests pass; REPL survives `kill -9` (verified by hand).
The owner asked to keep building, so M2 went ahead before the M1 answers came in.

**Next step:** owner answers the M1 + M2 questions, then decides D2 (mid-log corruption). After that, M3 (SSTable) needs a design consult first (block size, index format, footer).

## M1 review questions (owner answers)
1. Why does `delete` insert a tombstone instead of `map.remove(key)`? What breaks once SSTables exist?
2. Why `BTreeMap` and not `HashMap`?
3. `approx_size` never shrinks on overwrite. When does that flush early, and why is it acceptable?

## M2 review questions (owner answers)
1. Why is the CRC stored first, and why does it cover the length fields, not just key + value?
2. Why does `append` not fsync? What would group commit change about latency vs throughput?
3. Why must `Db::open` truncate the torn tail before appending? (Hint: the `writes_after_torn_tail_are_not_lost` test)
4. `decode` uses `checked_add` on lengths read from disk. What happens without it on a corrupted file?

## Blockers / open decisions
- [x] Rust 1.99.0 installed
- [x] GitHub: private repo https://github.com/keshavdhingra001/lsmkv (SSH remote, key ~/.ssh/id_ed25519)
- [x] Git email links to GitHub account keshavdhingra001
- [ ] D2: mid-log WAL corruption: Tier 1 default is stop silently; owner to confirm or choose fail-loud
