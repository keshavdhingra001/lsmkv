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
- [x] **M8** Concurrency: snapshot reads that never block the writer *(Claude; 129 unit tests + kill -9 test + before/after benchmark vs M7; owner review pending)*
- [x] **M9** Range scans: merging iterator across memtable + levels *(Claude; 145 unit tests; 20 mutants, 19 caught + 1 equivalent; owner review pending)*
- [x] **M10** Crash-injection harness (random kill -9, verify no acked write lost) + model-based fuzz test *(Claude; 300-round soak: 1.02M acked ops, none lost; 5,000 fuzz cases; harness and fuzzer each mutation-checked; owner review pending)*
- [x] **M11** Benchmarks (ops/sec, p50/p99, write amplification) vs RocksDB *(Claude; `bench/` crate, RocksDB 11.8; writes even, lsmkv faster on single-threaded reads (unprofiled), RocksDB better compaction and tails)*
- [x] **M12** DESIGN.md complete, README with results *(Claude; README rewrite with mermaid diagram + all results, DESIGN index + final pass, `examples/demo.rs`)*

### Tier 3: Stretch (pick 2–3 later)
MVCC, atomic batches, RESP server, compression, deterministic simulation testing, Raft.

## ▶ RESUME HERE (handover 2026-10-05, after M12: Tier 2 complete)

**State:** M0–M12 are done. All of Tier 2 is built, measured and documented. 145 unit tests + the kill -9 harness + the proptest model test + 2 doc tests pass, clippy clean, everything pushed to `main` at https://github.com/keshavdhingra001/lsmkv (still **private**).

**Open with the owner:**
1. **Make the repo public?** The Tier 1 gate said "not yet"; ask again now that Tier 2 is done. Don't do it without an explicit yes.
2. **The review questions (M1–M11, 71 in all) are unanswered.** They need to be able to defend this in interviews. Suggested order: M8, M7, M9, M10, then the rest. Run them as a quiz, one at a time, and record the answers here.
3. Tier 3 is optional. If wanted, the cheapest high-signal items: atomic write batches (sequence numbers already exist), level-0 trivial moves (fixes `fillseq` write amp 2.28 vs 1.00), a RESP server.

**Commands:**
- Everything: `cargo fmt && cargo clippy --all-targets && cargo test` (about 10 s).
- Demo: `cargo run --release --example demo` (a few seconds; its output is in the README).
- Soak: `cargo test --release --test kill9 -- --ignored` (300 rounds, ~90 s). Fuzz: `PROPTEST_CASES=5000 cargo test --release --test model` (~40 s).
- Benchmarks: `examples/{durability,concurrency,scan}.rs`; vs RocksDB: `cd bench && cargo run --release` (the first build compiles RocksDB, ~7 min; **never run `cargo clippy` in `bench/`**, it recompiles the C++ again).
- REPL: `cargo run -- ./data` (`scan [from [to]]`, `snap`/`sget`/`sscan`/`unsnap`, `stats`).

**Environment gotchas:**
- Pushing: `SSH_AUTH_SOCK=$(ls ~/.ssh/agent/s.* | head -1) git push`. If that fails, the owner runs `ssh-add ~/.ssh/id_ed25519`.
- Other sessions (lob, ember) run cargo on this machine. Cargo's package-cache lock is shared, so their `cargo add`/`fetch` can block our builds for minutes, and their builds skew benchmarks. The scratchpad mutation runner builds with `cargo test --no-run` first so lock waits aren't scored; check `uptime` before benchmarking.
- Mutation checks: run under `timeout`, `touch` restored files, clean `/tmp/.tmp*` lsmkv dirs after killed tests, and close the "diagnose-crash" sessions that aborting test binaries open. If a session dies mid-run, look for `*.bak` files in `src/` and restore them.
- Don't `pkill -f <pattern>` where the pattern appears in the same command line: it kills the shell running it (happened again in M11).
- `/tmp` is tmpfs: benchmark on the real disk (`target/`). No `perf`, `ptrace` restricted.

**Code map:** `src/wal.rs` (log), `src/key.rs` (sequence numbers, internal key order, `Shadowed` garbage rule), `src/memtable.rs` (skiplist + `MemIter`),
`src/sstable/{block,writer,reader,filter,cache,mod}.rs` (reader has `SstIter`), `src/manifest.rs`,
`src/db.rs` (`Db`, writer queue, `SuperVersion`, recovery, most tests), `src/db/background.rs` (flush), `src/db/compaction.rs`,
`src/db/iter.rs` (`MergeIter`, `LevelIter`, `DbIter`), `src/db/snapshot.rs`, `src/main.rs` (REPL),
`tests/kill9.rs` (crash harness), `tests/model.rs` (proptest), `examples/{demo,durability,concurrency,scan}.rs`, `bench/` (vs RocksDB).

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

**2026-10-04: M8 done.** The owner approved the whole proposal ("go"). Built in 5 sections, each committed and pushed:
(1) sequence numbers in every version, with a format version (D18); (2) lock-free reads: skiplist memtable + SuperVersion read view (D12, D13);
(3) immutable memtable, background flush + compaction thread, L0 slowdown/stop, background errors poison (D14–D16); (4) `Db::snapshot()` (D18);
(5) per-writer condition variables (D17) and the before/after benchmark.
Measured against M7 on the same machine: 4 readers 439k → 0.93–1.11M reads/s; with a writer running, read max 120–130 ms → under 7 ms, and the writer 21–171k → 153–390k writes/s.
D17 cut context switches 20–30% but not throughput: a negative result, documented (the bottleneck is the serial leader path).
Found along the way: a `stats()` self-deadlock (two guards in one struct literal); `compact_all` never compacting the bottom level, so versions snapshots had pinned there stayed after the snapshot was released; a staged test that probed through the state lock and so only caught a lock-holding mutant indirectly.
Mutation-checked: 38 planted bugs, all caught in the end (two survived at first and got new tests; see D14–D18).
Semantics changed: any background failure poisons (M4's "retryable before commit" is gone), and pre-M8 data directories are refused (format 2).

**2026-10-04: M9 done.** The owner approved the combined M9–M12 proposal in one "go". Built in 3 sections: (1) streaming table iterator + memtable range iterator; (2) `MergeIter` (heap k-way merge), `DbIter`, `Db::scan`/`iter`, `Snapshot::scan`/`iter`, REPL `scan`/`sscan`; (3) compaction switched to the streaming merge.
Measured: compaction peak RSS 265 MiB → 24 MiB over a 129 MiB rewrite; full scan 5.8M keys/s, seek + 100 keys 76k/s.
Corrected the handover note: an open scan does NOT need to register a snapshot (it holds an immutable SuperVersion; see D21).
Found along the way: `impl RangeBounds<[u8]>` doesn't accept `&[u8]` ranges (std's impl needs sized T), so the API takes `RangeBounds<K: AsRef<[u8]>>`; a mutation survivor showed the narrow-scan test damaged bytes the scan never reads.
Environment: another session's `cargo add` held the shared package-cache lock for minutes, so the mutation runner now builds with `cargo test --no-run` before the timed run (a lock wait can't be scored as "caught").

**2026-10-04: M10 done.** `tests/kill9.rs` rewritten as a multi-round crash harness (one directory, random kill times, both sync modes, an exact checker, the child self-checking with scans). `tests/model.rs`: a proptest model test over every public operation, including reopen. proptest is the only new dependency (dev only).
Measured: a 300-round soak checked 1,022,073 acknowledged operations with none lost; 5,000 fuzz cases passed.
Mutation-checked separately: the harness alone catches 3 of 5 crash-only bugs (the other 2 can't show under `kill -9` and are caught by unit tests); the fuzzer alone catches 7 of 7 planted bugs from earlier milestones after a gap it exposed (no snapshot `get`s) was fixed.
Harness bug found: libtest's unterminated `test <name> ... ` line swallowed the child's first output line.

**2026-10-05: M11 and M12 done; Tier 2 complete.** M11: `bench/` crate vs RocksDB 11.8 with matched settings. After fairness fixes (stats level, static levels, native builds): writes even, RocksDB better on compaction and tail latency, lsmkv faster on single-threaded point reads (unprofiled). M12: README rewrite (mermaid architecture, every result table, test strategy, limits, real demo output, the snippet doc-tested), DESIGN.md decision index + "Not done", `examples/demo.rs`.
Mistakes on the way: a non-terminating `shuffled` helper hung a benchmark for 44 minutes (fixed in both copies); running clippy in `bench/` recompiled RocksDB.

**Next step:** see RESUME HERE (repo visibility, the review-question quiz, optional Tier 3).

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

## M8 review questions (owner answers)
1. A `get` runs while a flush installs its table. What does the reader hold, and why can't it miss a key that's moving from the immutable memtable into the table?
2. Why are `last_seq` and the SuperVersion read under one lock? Describe the interleaving that misses an acknowledged write if they were two atomics loaded in the wrong order.
3. Compaction deletes a table file that a reader is still searching. Why is that safe on Linux, and what would break on Windows?
4. Why is the sequence number compared as a separate field instead of appended to the key bytes? Give two keys that sort wrong the other way.
5. Why must a flush write `SetLastSequence` to the manifest? What goes wrong after a reopen without it?
6. A snapshot at 100 is the oldest. Key k has versions 150, 120, 90 and 40. Which can compaction drop, and why? And if the oldest snapshot is 130?
7. Why may a compaction output table end only between user keys, never inside one key's versions?
8. Why does the memtable switch fsync the old WAL, even in `Periodic` mode? Exactly what could a power cut lose without it?
9. Why have both a slowdown and a stop trigger? Why must compaction <= slowdown <= stop hold?
10. Why does a background failure poison the database instead of returning an error? What did that change from M4?
11. Per-writer condition variables cut context switches 20–30% but left throughput flat. What is the bottleneck, and how does RocksDB attack it?
12. Why did `stats()` deadlock when it called `lock(&self.view)` twice inside one struct literal?

## M9 review questions (owner answers)
1. Walk through `db.scan("b".."d")` with "b" in an L0 table, a newer "b" in the memtable, and a tombstone for "c" in L2 over a "c" in L3. Which sources exist, what does the heap pop in order, and what does `DbIter` return?
2. Why does each level-0 table get its own source, while a whole deeper level is one `LevelIter`?
3. An open scan holds no snapshot registration. Why can't compaction drop a version it's about to read? What does it cost to keep a scan open for an hour?
4. Why does the memtable iterator copy 64 entries at a time instead of holding a skiplist iterator? What happens to a write that lands between two refills?
5. Why do scans read from the block cache but never add to it? What would a full scan do to `get`'s hit rate otherwise?
6. Compaction's peak memory went from 265 MiB to 24 MiB. What was holding the memory before, and what bounds it now?
7. `DbIter` skips versions with `seq > snapshot` *without* marking the key as done. Why would marking it done be a bug?
8. Why can't `DbIter` be a lending iterator handing out `&[u8]`, and what does the owned version cost?

## M10 review questions (owner answers)
1. The crash checker allows each key either its last acknowledged value or the in-flight operation's result. Why can at most one operation per thread be in flight, and why does each value name the operation that wrote it?
2. Why does every round reuse the same directory instead of a fresh one? Name a bug only that catches.
3. `kill -9` couldn't catch a torn-tail bug. Why can't a process kill tear a WAL write, and what kind of failure can?
4. What does proptest's shrinking give you that a seeded random loop doesn't? What does the regressions file do?
5. Why do the fuzz test's keys come from 3 letters instead of random bytes?
6. A planted bug survived the fuzzer until snapshot `get`s were added. What does that say about how much a model test can be trusted, and how did the mutation run expose it?
7. How would you test power-loss durability for real? What would LazyFS or dm-log-writes let you check that `kill -9` can't?

## M11 review questions (owner answers)
1. Why does RocksDB get write amplification 1.00 on `fillseq` and lsmkv 2.28? What would you change in lsmkv's level-0 compaction to match it?
2. The first run showed lsmkv ahead almost everywhere. What did you check before believing it, and which settings changed?
3. Why measure p99 and p99.9 and not just the mean? Which row shows why?
4. Why is `readmissing` so much faster than `readrandom` for both engines?
5. With 4 readers and a writer, read throughput is equal but RocksDB has the better p99. What in lsmkv's write path could cause read tail latency?
6. Why is `fillrandom` with fsync per write about 1,000 ops/s for both? What would make it faster without giving up durability?

## Blockers / open decisions
- [x] Rust 1.99.0 installed
- [x] GitHub: private repo https://github.com/keshavdhingra001/lsmkv (SSH remote, key ~/.ssh/id_ed25519)
- [x] Git email links to GitHub account keshavdhingra001
- [x] D2: mid-log WAL corruption fails loud (owner approved 2026-10-04)
