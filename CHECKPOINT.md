# Checkpoint log

Single source of truth for "where are we". Update at the end of every session.

## Roadmap

### Tier 1: Baseline
- [x] **M0** Scaffold: crate layout, error type, Db glue, REPL, tests for M1/M2 *(Claude)*
- [x] **M1** Memtable: `put` / `delete` / `get` with tombstones *(Claude; 7/7 tests pass; owner review pending)*
- [x] **M2** WAL: `append` / `replay`, CRC32 per record, torn-tail handling *(Claude; 18/18 tests + manual kill -9 recovery check; owner review pending)*
- [x] **M3** SSTable writer + reader: data blocks, index block, footer *(Claude; 43/43 tests incl. corruption + randomized; owner review pending)*
- [x] **M4** Flush memtable -> SSTable at size threshold; read path checks memtable then SSTables newest-first; manifest *(Claude; 59/59 tests incl. crash injection at every flush step; owner review pending)*
- [ ] **Gate:** Tier 1 done -> make the GitHub repo public *(Tier 1 complete 2026-10-04; owner said not yet)*

### Tier 2: Strong (target)
- [x] **M5** Compaction: leveled, tombstones dropped safely *(Claude; 71/71 tests incl. compaction crash injection; owner review pending)*
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

**2026-10-04: M5 done.** Built in 4 sections: manifest levels + table key ranges, level read path, compaction, crash tests + REPL.
71/71 tests, clippy clean. Mutation-checked: always dropping tombstones, compacting only the newest L0 table, the oldest version winning the merge, and deleting inputs before the commit were all caught.
The owner chose to keep the repo private for now (2026-10-04).

**Next step:**
1. Owner works through the review questions (M1-M5, below).
2. M6 (bloom filters + block cache) needs a design consult: bits per key, filter granularity (per table vs per block), cache size and eviction.

## M1 review questions (owner answers)
1. Why does `delete` insert a tombstone instead of `map.remove(key)`? What breaks once SSTables exist?
2. Why `BTreeMap` and not `HashMap`?
3. `approx_size` never shrinks on overwrite. When does that flush early, and why is it acceptable?

## M2 review questions (owner answers)
1. Why is the CRC stored first, and why does it cover the length fields, not just key + value?
2. Why does `append` not fsync? What would group commit change about latency vs throughput?
3. Why must `Db::open` truncate the torn tail before appending? (Hint: the `writes_after_torn_tail_are_not_lost` test)
4. `decode` uses `checked_add` on lengths read from disk. What happens without it on a corrupted file?

## M3 review questions (owner answers)
1. The index stores each block's *last* key, not its first. Walk through `get("m")` when block 0 ends at "k" and block 1 ends at "p". Why does `partition_point(|e| e.last_key < key)` find the right block?
2. Why fsync the file *before* the rename, and the directory *after*? What could a crash leave behind if either fsync were skipped?
3. Why does the footer need its own CRC when the index and blocks already have one?
4. Why are tombstones written into SSTables at all? When can one finally be dropped? (Preview of M5 compaction.)
5. `get` takes `&self` and uses `read_exact_at` instead of seek + read. Why does that matter for concurrency?

## M4 review questions (owner answers)
1. Walk through the 4 flush steps. For a crash right after each step, what's on disk, and what does recovery do?
2. Why can there be TWO live WALs after a crash, and why must recovery replay both, oldest first?
3. Why does `get` stop at the first tombstone instead of continuing to older tables?
4. What is fsyncgate, and why does a failed manifest fsync *poison* the database instead of just returning an error?
5. Why do logs and tables share one file-number counter?

## M5 review questions (owner answers)
1. Why does a level-0 compaction take ALL level-0 tables, while level n takes just one?
2. A tombstone for "a" is being compacted into L2. When is it safe to drop, and what goes wrong if it's dropped too early?
3. Leveled vs size-tiered: which has higher write amplification, which has worse reads, and why?
4. What is a trivial move, and why is it safe?
5. Compaction deletes its input files only after the manifest commit. What happens on reopen if it deleted them first and then crashed?
6. Write amplification: what does `stats` report after a big write workload, and where do the extra bytes come from?

## Blockers / open decisions
- [x] Rust 1.99.0 installed
- [x] GitHub: private repo https://github.com/keshavdhingra001/lsmkv (SSH remote, key ~/.ssh/id_ed25519)
- [x] Git email links to GitHub account keshavdhingra001
- [x] D2: mid-log WAL corruption fails loud (owner approved 2026-10-04)
