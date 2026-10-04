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
- [x] **M6** Bloom filters per SSTable + LRU block cache *(Claude; 97/97 tests incl. measured filter/cache wins; owner review pending)*
- [x] **M7** Durability modes: per-write fsync / group commit / periodic; measure each *(Claude; 109 unit tests + kill -9 test + benchmark; owner review pending)*
- [ ] **M8** Concurrency: snapshot reads that never block the writer
- [ ] **M9** Range scans: merging iterator across memtable + levels
- [ ] **M10** Crash-injection harness (random kill -9, verify no acked write lost) + model-based fuzz test
- [ ] **M11** Benchmarks (ops/sec, p50/p99, write amplification) vs RocksDB
- [ ] **M12** DESIGN.md complete, README with results

### Tier 3: Stretch (pick 2–3 later)
MVCC, atomic batches, RESP server, compression, deterministic simulation testing, Raft.

## ▶ RESUME HERE (handover 2026-10-04, after M7)

**State:** M0–M7 are done. 109 unit tests + the kill -9 integration test pass, clippy clean, everything pushed to `main`
at https://github.com/keshavdhingra001/lsmkv (private; owner said keep it private "not yet").

**Next action:** M8 (concurrency, the read side). It needs a design consult before any code. Propose these as a table with a recommendation, then wait for "go":
- **Reads without the state lock:** today `get` holds the state lock for its whole lookup, including disk reads. RocksDB's "SuperVersion" approach: an `Arc` snapshot of (memtable, immutable memtables, table levels), grabbed under a brief lock, then read with no lock held.
- **Immutable memtable + background flush:** swap a full memtable out instead of flushing inline, so writers don't stall (D11's max latency is 140–570 ms). Reads must then check the active memtable, then the immutable ones, then the tables.
- **Background compaction thread**, plus L0 write slowdown/stop triggers (LevelDB: 8 / 12 files), since compaction can now fall behind.
- **Memtable structure:** while it's being written it must be readable without the state lock: a concurrent skiplist (`crossbeam-skiplist`) vs a `BTreeMap` behind an `RwLock`.
- **Snapshots (`db.snapshot()`, point-in-time reads):** these need sequence numbers inside keys, a format change to the memtable, WAL and SSTables (internal key = user key + seq + kind). Decide: do it in M8, or keep M8 to "reads never block writers" and push MVCC to Tier 3.
- **Writer wakeups:** per-writer condition variables instead of `notify_all` (D11: `Periodic` throughput falls from 335k to 220k ops/s as threads go 1 to 16).
- **Proof:** max write latency during flushes, and reader throughput under a concurrent write load, before and after.

**Working agreement (see CLAUDE.md):** Claude writes each milestone in sections, tests it (including
mutation checks that the tests can fail), commits `M<n>: ...` and pushes, explains it, and asks review questions.
Consult the owner on design decisions before coding, and record them in DESIGN.md as D12+.

**Environment gotchas:**
- Pushing over SSH from Claude's shell: `SSH_AUTH_SOCK=$(ls ~/.ssh/agent/s.* | head -1) git push`.
  If that fails, the owner must run `ssh-add ~/.ssh/id_ed25519` (the key has a passphrase).
- Rust 1.99.0 is installed. If rustup is slow (the hotspot's route to the Fastly CDN), prefix the command with
  `RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup`. crates.io downloads work fine.
- Mutation checks: a mutation can turn a test into an infinite loop. Run them under `timeout 120 cargo test`, and check for an abort (no "test result" line) as well as FAILED.
- `/tmp` is tmpfs: fine for tests, but run benchmarks on the real disk (`target/bench-*`, the default for `examples/durability.rs`).
- `ptrace` is restricted (gdb/eu-stack can't attach), so to find a hung test, look for libtest's "has been running for over 60 seconds".
- Don't `pkill -f <pattern>` where the pattern appears in the same command line: it kills the shell running it.

**Code map:** `src/wal.rs` (log), `src/memtable.rs`, `src/sstable/{block,writer,reader,filter,cache,mod}.rs`,
`src/manifest.rs`, `src/db.rs` (`Db` handle + writer queue + periodic sync thread; `State` = open/recovery/flush/read path/poisoning; most tests),
`src/db/compaction.rs`, `src/codec.rs`, `src/fsutil.rs`, `src/test_util.rs` (Rng), `src/main.rs` (REPL),
`tests/kill9.rs` (real-process crash test), `examples/durability.rs` (benchmark).

## Current status

**2026-10-04: M0 done.** Scaffold committed. Nothing compiled yet (Rust not installed at scaffold time).

**2026-10-04: Mode switched.** Claude now writes each milestone; owner studies and answers questions before the next one (see CLAUDE.md).
M1 done: 7/7 memtable tests pass, clippy clean. Rust 1.99.0 installed (via TUNA mirror; Fastly route from the hotspot is slow).

**2026-10-04: M5 done.** Built in 4 sections: manifest levels + table key ranges, level read path, compaction, crash tests + REPL.
71/71 tests, clippy clean. Mutation-checked: always dropping tombstones, compacting only the newest L0 table, the oldest version winning the merge, and deleting inputs before the commit were all caught.
The owner chose to keep the repo private for now (2026-10-04).

**2026-10-04: M6 done.** The owner approved the proposal as-is ("go"). Built in 3 sections: the filter (`filter.rs`), filters written into and checked by tables, and the shared LRU block cache (`cache.rs`).
97/97 tests, clippy clean. Measured: filters cut block reads for missing keys from 31,936 to 279 (114x) at a 0.87% false-positive rate; a 256 KiB cache served 91% of skewed reads. Mutation-checked: 15 planted bugs, all caught (see D9, D10).
The owner went ahead without answering the M1–M5 review questions (CLAUDE.md says to answer them before the next milestone).

**2026-10-04: M7 done.** The owner approved the proposal ("go"). Built as: (1+2) writer queue + group commit + `SyncMode::{Always, Periodic}`, merged because the code is intertwined; (3) the kill -9 test, the benchmark, and moving the periodic fsync off both locks.
Found and fixed along the way: a lost-wakeup hang in the periodic thread's shutdown, the periodic interval stretching to 177 ms during compactions, and the sync thread blocking at startup on the state lock.
Measured: group commit gives 16 writers 5–9.5x single-writer throughput (about 10 writes per fsync); `Periodic` runs about 335k writes/s vs 1.3k for `Always`. Max latency of 140–570 ms comes from inline flush/compaction (M8). Mutation-checked: 11 planted bugs, all caught (see D11).

**Next step:**
1. Owner works through the review questions (M1–M7, below).
2. M8 (read-side concurrency) needs a design consult; see RESUME HERE.

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

## M6 review questions (owner answers)
1. Why is the filter checked *before* the index, and why can't it ever cause a wrong answer? What's the only cost of a false positive?
2. Derive the 0.82% false-positive rate for 10 bits/key and k = 7. What happens to the rate with k = 1, and with k = 20?
3. Why would using `std`'s `DefaultHasher` for the filter be a data-loss bug, not just a performance bug?
4. Why must tombstones go into the filter? Walk through a `get` that returns a deleted value if they don't.
5. Why is the cache key `(table id, offset)` and not just `offset`, and why does it matter that file numbers are never reused?
6. Why does `get` on the cache need a `Mutex` even though it only "reads"? Why does the cache hand out `Arc<[u8]>` instead of `&[u8]`?
7. Why do compaction reads skip the cache? What would happen to the hit rate if they didn't?

## M7 review questions (owner answers)
1. Walk through one write group: who becomes the leader, what it does with and without the state lock, and how each follower learns its result.
2. Why does the leader release the state lock during the fsync? What would happen to throughput if it didn't?
3. Why must `flush` wait until no group is in flight? Describe the exact interleaving that loses an acknowledged write if it doesn't.
4. Why must the memtable apply a group in the same order as the WAL? What would a reader see before and after a reopen otherwise?
5. `Periodic` mode doesn't fsync before acking, yet `kill -9` loses nothing. Why? What failure *does* lose data, and how much?
6. What is a lost wakeup, and why does `wait_timeout_while` fix the one we hit in the periodic thread?
7. The periodic thread fsyncs a *cloned* file handle without holding the WAL lock. Why is that still correct?
8. In the benchmark, why do 16 `Always` writers get ~10x the throughput of 1, while 16 `Periodic` writers get *less* than 1?

## Blockers / open decisions
- [x] Rust 1.99.0 installed
- [x] GitHub: private repo https://github.com/keshavdhingra001/lsmkv (SSH remote, key ~/.ssh/id_ed25519)
- [x] Git email links to GitHub account keshavdhingra001
- [x] D2: mid-log WAL corruption fails loud (owner approved 2026-10-04)
