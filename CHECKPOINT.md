# Checkpoint log

Single source of truth for "where are we". Update at the end of every session.

## Roadmap

### Tier 1: Baseline
- [x] **M0** Scaffold: crate layout, error type, Db glue, REPL, tests for M1/M2
- [x] **M1** Memtable: `put` / `delete` / `get` with tombstones *(7/7 tests pass; owner review pending)*
- [x] **M2** WAL: `append` / `replay`, CRC32 per record, torn-tail handling *(18/18 tests + manual kill -9 recovery check; owner review pending)*
- [x] **M3** SSTable writer + reader: data blocks, index block, footer *(43/43 tests incl. corruption + randomized; owner review pending)*
- [x] **M4** Flush memtable -> SSTable at size threshold; read path checks memtable then SSTables newest-first; manifest *(59/59 tests incl. crash injection at every flush step; owner review pending)*
- [x] **Gate:** make the GitHub repo public *(owner approved 2026-10-05, after Tier 2)*

### Tier 2: Strong (target)
- [x] **M5** Compaction: leveled, tombstones dropped safely *(71/71 tests incl. compaction crash injection; owner review pending)*
- [x] **M6** Bloom filters per SSTable + LRU block cache *(97/97 tests incl. measured filter/cache wins; owner review pending)*
- [x] **M7** Durability modes: per-write fsync / group commit / periodic; measure each *(109 unit tests + kill -9 test + benchmark; owner review pending)*
- [x] **M8** Concurrency: snapshot reads that never block the writer *(129 unit tests + kill -9 test + before/after benchmark vs M7; owner review pending)*
- [x] **M9** Range scans: merging iterator across memtable + levels *(145 unit tests; 20 mutants, 19 caught + 1 equivalent; owner review pending)*
- [x] **M10** Crash-injection harness (random kill -9, verify no acked write lost) + model-based fuzz test *(300-round soak: 1.02M acked ops, none lost; 5,000 fuzz cases; harness and fuzzer each mutation-checked; owner review pending)*
- [x] **M11** Benchmarks (ops/sec, p50/p99, write amplification) vs RocksDB *(`bench/` crate, RocksDB 11.8; writes even, lsmkv faster on single-threaded reads (unprofiled), RocksDB better compaction and tails)*
- [x] **M12** DESIGN.md complete, README with results *(README rewrite with mermaid diagram + all results, DESIGN index + final pass, `examples/demo.rs`)*

### Tier 3: Stretch (owner approved M13–M15 on 2026-10-05)
- [x] **M13** Atomic write batches + optimistic transactions (snapshot isolation, `get_for_update`) *(15 mutants, all caught)*
- [x] **M14** Redis-protocol (RESP) server: `redis-cli` talks to lsmkv *(`lsmkv-server`, 11 mutants all caught)*
- [x] **M15** Deterministic simulation + power-loss testing (simulated disk behind an `Fs` trait) *(found and fixed a real torn-manifest-commit bug; format 4)*
Not planned: compression, column families, Raft.

### Tier 4: Hardening (owner asked for it on 2026-10-05, approved D29 and D30 the same day)
- [x] **M16** Testing infrastructure: GitHub Actions CI, libFuzzer targets for every parser, the planted-bug runner in the repo, LazyFS power cuts, the real `redis-cli` *(D31; 27 planted bugs, all caught; 300 LazyFS power cuts, 138k acked writes, none lost)*
- [x] **M17** Trivial moves out of level 0 *(D29; `fillseq` write amplification 2.28 → 1.17)*
- [x] **M18** Restart points in data blocks, format 5 *(D30; in-block lookups 1.2x–9.3x faster; format-4 databases still open)*

## ▶ RESUME HERE (2026-10-06, after M18: Tier 4 done and merged)

**State:** M0–M18 are done and merged to `main` (PR #1: cleanup pass; PR #2: seed 2793 fix + M16–M18). Nothing is in flight.
No milestone is planned after M18: the next step is the owner's study, not more building.
196 tests pass (`cargo test`, about 70 s), clippy clean, CI green.

**What the owner does next: study, then merge.**
1. **The review-question quiz** is the priority: 117 questions, M1–M18, below. Ask them one at a time, have the owner answer in their own words, correct and explain, and record each answer and a short verdict under its question. Suggested order (what interviewers probe most): M2 (WAL), M4 (flush/recovery), M7 (group commit), M8 (concurrency/snapshots), M15 (power loss + the bugs it found), M13 (transactions), M16 (how it's tested), then the rest.
2. For each milestone, it helps to open the code alongside: the "Code map" below says where each piece lives, and DESIGN.md has a decision index (D1–D31).
3. The headline stories to be able to tell, start to finish:
   - **The three bugs the simulation found** (D28): the torn manifest commit (seed 44, the `Group(n)` fix), recovery serving un-fsynced WAL data (seed 14676, fsync on replay), and recovery acting on an un-fsynced manifest commit (seed 2793, fsync the manifest on open). Why `kill -9` can't find any of them.
   - **Three ways to test crashes** (D22, D28, D31): `kill -9` (real binary, kernel keeps the data), `SimFs` (deterministic, models the disk), LazyFS (real kernel, drops unsynced data). What each can and can't catch.
   - **The single-bit magic** (D30): "LSMKVSS3" vs "LSMKVSS2", and why the footer CRC now covers the magic.
   - **Compaction memory** 265 → 24 MiB (D20). **RocksDB comparison**, why the first run wasn't trusted (D24), and `fillseq` 2.28 → 1.17 (D29).

**Commands:**
- Everything: `cargo fmt && cargo clippy --all-targets && cargo test`.
- Demo: `cargo run --release --example demo`. REPL: `cargo run -- ./data`. Server: `cargo run --release --bin lsmkv-server -- --dir ./data`.
- Crash soak: `cargo test --release --test kill9 -- --ignored` (300 rounds; `LSMKV_CRASH_ROUNDS=10000` for more).
- Simulation: `LSMKV_SIM_SEEDS=20000 cargo test --release --test sim`; replay one seed: `LSMKV_SIM_SEED=44 cargo test --test sim -- --nocapture`.
- Fuzz: `PROPTEST_CASES=5000 cargo test --release --test model`.
- Benchmarks: `examples/{durability,concurrency,scan,server_bench,block_search}.rs`; vs RocksDB: `cd bench && cargo run --release` (never `cargo clippy` in `bench/`: it recompiles RocksDB's C++, ~7 min).
- Planted bugs: `python3 scripts/mutate.py` (all, ~30 min), `python3 scripts/mutate.py D30` (by name), `--control` (the same tests unmutated; must all pass). Logs in `target/mutants/`.
- Power cuts on LazyFS: `scripts/lazyfs.sh 300` (needs `fuse3 libfuse3-dev cmake g++`).
- Fuzzing: `cargo install cargo-fuzz`, then `cd fuzz && cargo +nightly fuzz run <target>` (`cargo fuzz list`).

**Code map:** `src/wal.rs` (log, batch records), `src/key.rs` (sequence numbers, internal key order), `src/memtable.rs` (skiplist + `MemIter`),
`src/sstable/{block,writer,reader,filter,cache,mod}.rs` (blocks with restart points), `src/manifest.rs` (edit log, `Group(n)` commits, format 5), `src/vfs.rs` (`Fs`, `RealFs`, `SimFs`),
`src/db.rs` (`Db`, writer queue, `SuperVersion`, recovery, inline mode), `src/db/background.rs` (flush, `run_one`), `src/db/compaction.rs`,
`src/db/iter.rs` (`MergeIter`, `DbIter`), `src/db/batch.rs` (`WriteBatch`, `Transaction`, conflict check), `src/db/snapshot.rs`,
`src/resp.rs` + `src/server.rs` + `src/bin/lsmkv-server.rs` (Redis protocol), `src/main.rs` (REPL),
`tests/{kill9,sim,model,server,compat}.rs` (+ `tests/fixtures/format4-db`), `examples/{demo,durability,concurrency,scan,server_bench,block_search}.rs`, `bench/` (vs RocksDB),
`fuzz/` (libFuzzer), `scripts/{mutate.py,mutants.toml,lazyfs.sh}`, `.github/workflows/ci.yml`.

## Current status

**2026-10-04: M0 done.** Scaffold committed. Nothing compiled yet (Rust not installed at scaffold time).

M1 done: 7/7 memtable tests pass, clippy clean. Rust 1.99.0 installed.

**2026-10-04: M5 done.** Built in 4 sections: manifest levels + table key ranges, level read path, compaction, crash tests + REPL.
71/71 tests, clippy clean. Mutation-checked: always dropping tombstones, compacting only the newest L0 table, the oldest version winning the merge, and deleting inputs before the commit were all caught.
The owner chose to keep the repo private for now (2026-10-04).

**2026-10-04: M6 done.** The owner approved the proposal as-is ("go"). Built in 3 sections: the filter (`filter.rs`), filters written into and checked by tables, and the shared LRU block cache (`cache.rs`).
97/97 tests, clippy clean. Measured: filters cut block reads for missing keys from 31,936 to 279 (114x) at a 0.87% false-positive rate; a 256 KiB cache served 91% of skewed reads. Mutation-checked: 15 planted bugs, all caught (see D9, D10).
The owner went ahead without answering the M1–M5 review questions.

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
Corrected the M9 plan: an open scan does NOT need to register a snapshot (it holds an immutable SuperVersion; see D21).
Found along the way: `impl RangeBounds<[u8]>` doesn't accept `&[u8]` ranges (std's impl needs sized T), so the API takes `RangeBounds<K: AsRef<[u8]>>`; a mutation survivor showed the narrow-scan test damaged bytes the scan never reads.

**2026-10-04: M10 done.** `tests/kill9.rs` rewritten as a multi-round crash harness (one directory, random kill times, both sync modes, an exact checker, the child self-checking with scans). `tests/model.rs`: a proptest model test over every public operation, including reopen. proptest is the only new dependency (dev only).
Measured: a 300-round soak checked 1,022,073 acknowledged operations with none lost; 5,000 fuzz cases passed.
Mutation-checked separately: the harness alone catches 3 of 5 crash-only bugs (the other 2 can't show under `kill -9` and are caught by unit tests); the fuzzer alone catches 7 of 7 planted bugs from earlier milestones after a gap it exposed (no snapshot `get`s) was fixed.
Harness bug found: libtest's unterminated `test <name> ... ` line swallowed the child's first output line.

**2026-10-05: M11 and M12 done; Tier 2 complete.** M11: `bench/` crate vs RocksDB 11.8 with matched settings. After fairness fixes (stats level, static levels, native builds): writes even, RocksDB better on compaction and tail latency, lsmkv faster on single-threaded point reads (unprofiled). M12: README rewrite (mermaid architecture, every result table, test strategy, limits, real demo output, the snippet doc-tested), DESIGN.md decision index + "Not done", `examples/demo.rs`.
Mistakes on the way: a non-terminating `shuffled` helper hung a benchmark for 44 minutes (fixed in both copies); running clippy in `bench/` recompiled RocksDB.

**2026-10-05: M13–M15 done (Tier 3).** The owner approved one combined consult ("go"). M13: atomic batches (one WAL record, format 3), optimistic transactions with `get_for_update` (15 mutants caught). M14: `lsmkv-server`, RESP2 with MULTI/EXEC/WATCH on transactions (11 mutants caught). M15: every file operation through an `Fs` trait, `SimFs` with power cuts and fsync failures, inline background mode, a deterministic simulation test. **It found a real bug: a power cut could tear a multi-edit manifest commit and lose data; fixed with `Group(n)` commits (format 4).** Planted durability bugs: simulation 9/10, kill -9 0/10.
Mistakes on the way: a `/tmp` cleanup deleted a live soak's database (not an engine bug); the first inline mode flushed eagerly and masked two planted bugs; a recursive glob was exponential (replaced); the first pipelining test was too small to split commands across reads.

A 20,000-seed release run found a second real bug (seed 14676): recovery replayed WALs without fsyncing them, so a later failed fsync or power cut could take back writes the reopened database had already served. Fixed (recovery fsyncs replayed WALs), with a unit test and a process-kill fault in the simulation.
**Left running (detached):** a 20,000-seed simulation rerun on the final code (`target/sim20k-final.log`), and the 10,000-round kill -9 soak started in M13 (`target/soak10k.log`, built from M13-era code). Check both first thing. If they passed, update the README's long-run numbers (it currently cites the 300-round soak and "20,000-seed release runs"), and rerun the demo to refresh the README's demo output (it gained a transactions step). If either failed, replay it (`LSMKV_SIM_SEED=<n>`) before anything else.

**2026-10-05: Cleanup pass (no behavior or format change).** Removed duplication: one WAL record writer (`Wal::write_record`) for plain and batch records, `Record::size`; one manifest record decoder (the CRC was checked twice per record); `SuperVersion::newest` behind both `get` and `newest_seq`; `Shared::read_view`, `Pending::writes`, `State::new_file_number`, `Table::open`, `bytes(level)`; `ENTRY_HEADER_LEN` instead of a literal 17; a shared `test_util::val`. `db.rs` tests moved to `src/db/tests.rs` (like `sstable/tests.rs`). 184 tests pass; simulation seeds 0–2792 pass (see below).
Simulation sweep (`LSMKV_SIM_SEEDS=20000`, release) on this machine: **seed 2186** failed because a failed-fsync fault armed for an epoch that ended in a clean close stayed armed into the next reopen. That's a harness bug, fixed with `SimFs::disarm` on clean close. **Seed 2793 was a real durability bug:** a flush's manifest commit was only in the page cache when the process died; the next open replayed it, deleted the WAL it retired, and never fsynced the manifest, so a later power cut lost the commit *and* the WAL. Fixed: `Manifest::open` fsyncs what it replayed (D28, `a_replayed_commit_survives_a_later_power_cut`). All 20,000 seeds pass now.
Commit history rewritten (owner approved 2026-10-05) to drop AI co-author trailers; new commits carry none.

**2026-10-05: M16–M18 (Tier 4) built while the owner studies.** The owner asked for the project to be finished and approved D29 and D30 up front.
M16: CI on every push (short soaks) and nightly (long); five libFuzzer targets, ~10M runs each, no crashes; `scripts/mutate.py` with a 27-bug catalog, all caught, plus a control mode; LazyFS power cuts (300 rounds, 138k acked writes, none lost; ack-before-fsync caught in round 1); the real `redis-cli` in tests.
M17: level-0 trivial moves (`fillseq` write amp 2.28 → 1.17). M18: restart points, format 5 (in-block lookups 1.2x on 4 KiB blocks of 100-byte values, 9.3x on 64 KiB blocks); format-4 databases still open (`tests/compat.rs`).
Found along the way: CI's second run caught a flaky orphan check (a compaction finishing between listing files and counting tables); "LSMKVSS3" and "LSMKVSS2" differ in one bit, so the footer CRC now covers the magic; the first D29 tests used `compact_all`, which repaired bad moves before anything looked (all planted bugs survived them); the mutation runner's control mode showed `writes_and_reads_continue_while_a_flush_runs` was timing-sensitive under load (400 ms → 2 s margin); LazyFS's completion FIFO must stay open; GitHub archive downloads are blocked here, so LazyFS's `spdlog` is fetched with git.

**2026-10-05/06: session record (what the owner asked for, and what was done).**
- *Commit history:* all 49 commits carried `Co-Authored-By: Claude`. The owner asked for them gone and approved a rewrite: history was rewritten (messages only; trees and dates identical) and `main` force-pushed. From then on, no AI attribution anywhere (rule in CLAUDE.md).
- *Working logs:* CHECKPOINT.md's internal environment notes (SSH agent paths, other local sessions' names, scratchpad scripts) and the per-milestone "(Claude; ...)" tags were removed, at the owner's choice ("trim it").
- *Cleanup pass* (PR #1, merged by the owner), then *seed 2793* root-caused and fixed, then *Tier 4* (M16–M18) built at the owner's request while they study; the owner approved D29 and D30 up front and said "do your recommendation" for the PR, so Claude opened PR #2, waited for green CI, removed an auto-added attribution footer from its description, and merged it (2026-10-05).
- *The owner's level question:* the project reads as senior-level (L5) work in scope and depth, but an interview rates what the owner can defend; the 117 review questions are the gap that matters most. Study topics and the Rust concepts to know were given in chat; they follow the review-question order below.

**Next step:** see RESUME HERE: the quiz in a new chat.

## Study guide (given to the owner 2026-10-05)

**Storage-engine topics, in interview priority order** (DESIGN entry, then the code, then that milestone's questions):
1. LSM-tree basics: sequential writes instead of in-place updates; write/read/space amplification; vs B-trees.
2. WAL and durability (M2, D2–D4): CRCs, torn tails, truncation on recovery, what fsync guarantees, fsyncgate. `wal.rs`
3. Flush, recovery, manifest (M4, D6–D7): the commit point, crash at every step, poisoning, file numbers. `db.rs` `open_with`, `manifest.rs`, `db/background.rs`
4. Group commit (M7, D11): leader/followers, lock released during fsync, the sync modes. `Db::commit`
5. Concurrency and snapshots (M8, D12–D18): sequence numbers, SuperVersion, lock-free reads, what compaction may drop. `key.rs`, `db/snapshot.rs`
6. Power loss and the simulation (M15, D28): seeds 44, 14676 and 2793 end to end; why `kill -9` can't find them. `vfs.rs`, `tests/sim.rs`
7. Transactions (M13, D25–D26): atomic batches, optimistic concurrency, snapshot isolation, write skew, `get_for_update`.
8. Compaction (M5, D8, D29): leveled vs size-tiered, safe tombstone drop, trivial moves.
9. SSTables, bloom filters, block cache (M3, M6, D30): layout, false-positive math, LRU, restart points.
10. Range scans (M9): heap k-way merge; why an open scan needs no registered snapshot.
11. Benchmarking (M11, D24): p99 vs mean; why the first RocksDB run wasn't trusted.
12. Redis server (M14): RESP, pipelining, thread-per-connection vs async.
Background reading: *Designing Data-Intensive Applications* ch. 3 and 7; LevelDB/RocksDB wikis (group commit); the PostgreSQL "fsyncgate" write-ups (2018).

**Rust concepts the code relies on:** ownership and borrowing (`&[u8]` vs `Vec<u8>`, moving into threads); lifetimes (`Snapshot<'a>`, passing `MutexGuard<'a, State>` in and out of functions); `Arc`/`Mutex`/`Condvar` (`wait_timeout_while`, lost wakeups, lock order, poisoning); atomics and `Ordering::Relaxed`; `Send`/`Sync`; trait objects vs generics (`Box<dyn Fs>`, `impl RangeBounds<K>` with `K: ?Sized`); implementing `Iterator`, `Ord` (heap + `Reverse`) and `Drop`; errors (`thiserror`, `?`, `From`); patterns (`let … else`, slice patterns); `#[cfg(test)]` failpoints; iterator adapters and `FnMut`; `thread::spawn` vs `thread::scope`; Unix I/O (`read_exact_at`, `BufWriter`, `set_len`); proptest.

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

## M13 review questions (owner answers)
1. Why is a whole batch ONE WAL record instead of one record per operation? What exactly does a torn tail do to each design?
2. A reader takes a snapshot while a 10-key batch is being applied. Why can it never see 5 of the 10 keys updated?
3. Why did batches force a format bump, and what would an M12 binary have done to a WAL containing a batch?
4. Walk through `tx.commit()`: where does the conflict check run, which lock is held, and why can't a write sneak in between the check and the commit?
5. Two transactions from the same snapshot both write key `k` and land in the same write group. How does the check catch the second one?
6. What is write skew? Give the on-call example, and say how `get_for_update` prevents it.
7. Optimistic vs pessimistic concurrency control: when does each win, and why did lsmkv choose optimistic?
8. The batch decoder aborted the process on `count = u32::MAX`. Why, and what's the general rule it broke?

## M14 review questions (owner answers)
1. What's RESP? Encode `SET k v` as a client sends it, and the reply.
2. What is pipelining, and why must the server keep the bytes of a command that's cut off at the end of a read?
3. Why thread-per-connection instead of async? At what point would you switch?
4. How does `WATCH` + `MULTI` + `EXEC` map onto lsmkv's transactions? What does `EXEC` return when a watched key changed?
5. Why does `INCR` need a transaction with a retry loop? What would two clients incrementing at once lose without one?
6. Why is a recursive glob matcher a denial-of-service risk, and how does the iterative one avoid it?
7. Why does the server bind to 127.0.0.1 by default? What's missing before it could face a network?
8. `MGET` here reads all keys at one snapshot. Does Redis promise that? Why is it free in lsmkv?

## M15 review questions (owner answers)
1. What can a power cut lose that a `kill -9` can't? Name three things a power cut can do to files.
2. What does `SimFs` keep after a power cut, file by file and name by name? Why is "a random prefix of the unsynced bytes" a good model of a torn write?
3. Walk through the bug seed 44 found: what was written, what survived the power cut, what did replay do with it, and why did 300 kill -9 rounds never hit it?
4. How does the `Group(n)` header make a manifest commit atomic? Why didn't per-record CRCs already do that?
5. Why must every file creation be followed by a directory fsync? What happens to an fsynced file whose directory wasn't?
6. What makes a simulation run deterministic, and how is that checked? What's still not deterministic about the real engine?
7. Why did flushing right after each memtable switch hide two planted bugs? What does that teach about test schedules?
8. What's the `Periodic`-mode guarantee the simulation checks ("a prefix, never a hole"), and why does a log-structured design give it naturally?
9. Seed 2793: walk through how a manifest commit that was never fsynced ends up losing a WAL. Why must `Manifest::open` fsync even when it found no torn tail, and why couldn't a `kill -9` alone (no power cut) ever show this?

## M16 review questions (owner answers)
1. CI runs short soaks on every push and long ones nightly. Why not run the 20,000-seed simulation on every push? What does a nightly failure tell you that a push failure doesn't?
2. A fuzz target that only feeds random bytes to `SstReader::open` rarely gets past the footer CRC. Why, and what does `sstable_roundtrip` check that `sstable_read` can't?
3. The RESP fuzz target feeds the input one byte at a time and requires the same answer as the whole input. What bug does that property catch, and why does it matter for pipelining?
4. What is mutation testing? Why does the runner build before it starts the timer, and why must a mutant that doesn't compile count as a failure of the catalog, not as "caught"?
5. LazyFS caught "ack before fsync" in the first round; `kill -9` never can. Explain exactly what LazyFS drops on `clear-cache`, and why that's a power cut but `kill -9` isn't.
6. Why does the LazyFS harness run only `Always` rounds? What property could you check for `Periodic` mode instead?
7. The harness hung the first time because it read LazyFS's completion FIFO to end-of-file. Why does that never end, and why must the FIFO stay open between rounds?

## M17 review questions (owner answers)
1. Why does a level-0 compaction normally take every level-0 table? Under what three conditions can they all move down without being rewritten instead?
2. Two level-0 tables are [a..m] and [m..z]. Why can't they both move to level 1, even though they only touch at "m"? What would a later `get("m")` do?
3. Why must an empty table never move to level 1? Can a flush even produce one?
4. A level-1 table [e..f] sits between level-0 tables [c] and [m]. It overlaps neither, so why is this still a merge?
5. `fillseq` write amplification went from 2.28 to 1.17, not 1.00. Where do the remaining rewrites come from?
6. The first version of the tests ran `compact_all` and every planted bug survived. Why? What's the general lesson about where a test looks?

## M18 review questions (owner answers)
1. Walk through `get("k0057")` in a block with restart points: what does the binary search compare, where does the scan start, and at most how many entries does it read?
2. Why does `seek` start from restart `lo - 1` and not `lo`?
3. The CRC already covers the restart trailer. Why also check that every restart is exactly the offset of entry 16k? What would a restart pointing mid-entry do?
4. Why is that check done when a block is read from disk but not on a cache hit?
5. "LSMKVSS3" and "LSMKVSS2" differ by one bit. Walk through what a single flipped bit did before the footer CRC covered the magic.
6. How does a format-5 build read a format-4 table, and why does the database format go to 5 even though old tables stay readable?
7. Restart points give 1.2x on 4 KiB blocks of 100-byte values but 9.3x on 64 KiB blocks. Why does the gain grow with block size?
8. Why no prefix compression? What would it change about `RawEntry` and the iterators?

## Blockers / open decisions
- [x] Rust 1.99.0 installed
- [x] GitHub repo https://github.com/keshavdhingra001/lsmkv
- [x] Git email links to GitHub account keshavdhingra001
- [x] D2: mid-log WAL corruption fails loud (owner approved 2026-10-04)
- [x] Simulation seed 2793: manifest not fsynced on recovery (fixed 2026-10-05)
