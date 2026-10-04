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
- [ ] **M9** Range scans: merging iterator across memtable + levels
- [ ] **M10** Crash-injection harness (random kill -9, verify no acked write lost) + model-based fuzz test
- [ ] **M11** Benchmarks (ops/sec, p50/p99, write amplification) vs RocksDB
- [ ] **M12** DESIGN.md complete, README with results

### Tier 3: Stretch (pick 2–3 later)
MVCC, atomic batches, RESP server, compression, deterministic simulation testing, Raft.

## ▶ RESUME HERE (handover 2026-10-04, after M8)

**State:** M0–M8 are done. 129 unit tests + the kill -9 integration test pass, clippy clean, everything pushed to `main`
at https://github.com/keshavdhingra001/lsmkv (private; owner said keep it private "not yet").

**Progress:** 9 of 13 milestones (M0–M8). By effort, about 70%: M8 was the biggest; M9–M11 are medium, M12 is small.

**Open with the owner (not blocking, but raise them):**
- 52 review questions (M1–M8, below) are unanswered. CLAUDE.md says to answer them before the next milestone; the owner chose to build through to the end first. Offer the quiz at the end, after the demo (M8 and M7 first): they need to be able to defend this in interviews.
- The repo stays private until the owner says otherwise.

**Commands:**
- Everything: `cargo fmt && cargo clippy --all-targets && cargo test` (about 4 s; includes `tests/kill9.rs`).
- Benchmarks (release, on the real disk under `target/`):
  - `cargo run --release --example concurrency`: readers + one writer (about 15 s).
  - `cargo run --release --example durability -- [dir] [secs] [Always|Periodic] [threads]`: write modes (about 20 s for all rows).
  - Before/after against an old commit: `git archive <commit> | tar -x -C target/old`, copy the example in, build with its own `CARGO_TARGET_DIR`. Both examples use only API that M7 had.
- REPL: `cargo run -- ./data`. `stats` shows writes/groups/fsyncs, backpressure, snapshots, reads, cache and bloom counters; `snap` / `sget k` / `unsnap` try a snapshot.

**Owner's plan for the next session (2026-10-04):** take the project from here to finished, M9 through M12, and end with a demo run. To keep it moving:
1. **Open with ONE design consult covering M9–M12** (a table per milestone, each with a recommendation), and get a single "go". After that, build straight through, milestone by milestone. Stop again only if a measurement or bug contradicts an approved decision.
2. Keep the usual rhythm per milestone: sections, tests, mutation checks (with the runner's cleanup and `touch` fixes), commit `M<n>: ...`, push, DESIGN.md entries (D19+), and review questions in this file.
3. Finish with the demo (below), then ask the owner about making the repo public.

**M9: range scans. Decisions to propose:**
- **API:** `db.scan(range)` and `snapshot.scan(range)` returning an iterator of `(key, value)`. Forward only, or reverse too? Owned or borrowed results?
- **Merging iterator:** a heap (k-way merge) over the memtable range, the immutable memtable, each L0 table, and one "level iterator" per deeper level (it walks that level's tables in order). Newest source wins per user key; hide versions above the snapshot, shadowed versions and tombstones.
- **Streaming table iterator:** `SstReader::entries()` loads a whole table. Replace it with a block-at-a-time iterator (through the cache or not?). Decide whether compaction switches to the same streaming merge: it currently builds a `BTreeMap` of all inputs, i.e. the whole bottom level in memory during `compact_all`.
- **Lifetime:** the iterator holds an `Arc<SuperVersion>` (tables stay open, as for `get`) plus a snapshot number. An open iterator must pin versions like a `Snapshot`, or compaction could drop what it's about to read.
- **Skiplist range:** `crossbeam_skiplist::SkipMap::range` over internal keys.
- **Proof:** a model-based randomized test (scans at random snapshots vs a `BTreeMap`), plus scan throughput. Add `scan` to the REPL.

**M10: crash harness and fuzzing. Decisions to propose:**
- **Crash loop:** extend `tests/kill9.rs` into many rounds. Random kill times, both sync modes, small memtables (flushes and compactions mid-flight), snapshots and deletes in the mix. After each crash, check that every acknowledged write is present with its latest acknowledged value (unacknowledged writes may or may not be there). Run time: a short version in `cargo test`, and a long one as `#[ignore]` (`cargo test -- --ignored`)?
- **Model-based fuzz:** random op sequences (put/delete/get/scan/snapshot/flush/compact/reopen) against a model. Seeds with no new dependency, or `proptest` (shrinking)? `cargo-fuzz` needs nightly; skip or optional?
- **Power loss** (LazyFS / dm-log-writes) stays out of scope unless cheap; say so in DESIGN.md.

**M11: benchmarks vs RocksDB. Decisions to propose:**
- **How:** the `rocksdb` crate (it builds librocksdb from C++: check for clang/libclang and cmake first, and expect a slow first build), or RocksDB's own `db_bench` if installable.
- **Workloads,** in db_bench terms: `fillseq`, `fillrandom`, `readrandom`, `readwhilewriting`, and a range scan. Same settings on both sides: 4 MiB memtable, 10-bit bloom filters, no compression, matched WAL sync modes.
- **Metrics:** ops/sec, p50/p99 latency, write amplification (ours from `Stats`, RocksDB's from its compaction statistics). Fold the existing `durability` and `concurrency` examples in, or keep them.
- **Honesty:** report where RocksDB wins and why (compression off, its memtable/arena, its compaction tuning).

**M12: documentation and the demo. Decisions to propose:**
- **README:** what it is, architecture diagram, the design decisions in brief (pointing to DESIGN.md), results tables (M7 durability, M8 concurrency, M11 vs RocksDB), how to run the tests and benchmarks, and known limits.
- **DESIGN.md:** a final pass (consistent structure, the Tier 3 list as "not done").
- **Demo run:** a scripted, repeatable demo, e.g. `examples/demo.rs` or `demo.sh`:
  1. Write a batch.
  2. `kill -9` the writer mid-run, reopen, and show every acknowledged write recovered.
  3. A snapshot read surviving overwrites and a compaction.
  4. A range scan.
  5. `stats` (levels, write amplification, bloom/cache hits).
  6. A short benchmark.
  Then run it live in the chat, and show the REPL by hand. Decide whether to record it (asciinema or a GIF) for the README.

**Working agreement (see CLAUDE.md):** Claude writes each milestone in sections, tests it (including
mutation checks that the tests can fail), commits `M<n>: ...` and pushes, explains it, and asks review questions.
Consult the owner on design decisions before coding, and record them in DESIGN.md as D19+.

**Environment gotchas:**
- Pushing over SSH from Claude's shell: `SSH_AUTH_SOCK=$(ls ~/.ssh/agent/s.* | head -1) git push`.
  If that fails, the owner must run `ssh-add ~/.ssh/id_ed25519` (the key has a passphrase).
- Rust 1.99.0 is installed. If rustup is slow (the hotspot's route to the Fastly CDN), prefix the command with
  `RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup`. crates.io downloads work fine.
- Mutation checks: a mutation can turn a test into an infinite loop. Run them under `timeout 120 cargo test`, and check for an abort (no "test result" line) as well as FAILED.
- **After restoring a mutated file, `touch` it.** A backup made before the mutant's build carries an older mtime, so cargo treats the restored source as unchanged and keeps running the *mutant* binary. In M8 this showed up as "every concurrent-writer test hangs" right after the D17 mutation run; the engine was fine. The scratchpad runner (`mutate.py`) now calls `os.utime` after each restore.
- **A killed test leaves its tempdirs in `/tmp` (tmpfs).** A mutant that loops on flushing wrote 1.3 GB each and filled `/tmp` (7.7 GB) in M8. After killing tests, delete `/tmp/.tmp*` dirs that hold lsmkv files (a `MANIFEST`, `.log`, `.sst`); the scratchpad mutation runner now does this after every mutant.
- **Aborting test binaries trigger Omarchy's crash notifier,** which opens a separate "diagnose-crash" Claude session per SIGABRT of `lsmkv-*`. During mutation runs those are expected; tell the owner they can close them.
- `/tmp` is tmpfs: fine for tests, but run benchmarks on the real disk (`target/bench-*`, the default for both examples).
- `ptrace` is restricted (gdb/eu-stack can't attach) and there's no `perf` or `/usr/bin/time`. To find a hung test, look for libtest's "has been running for over 60 seconds", or run tests one at a time under `timeout`. For context switches and CPU time, use Python's `resource.getrusage(RUSAGE_CHILDREN)` around a child process.
- Don't `pkill -f <pattern>` where the pattern appears in the same command line: it kills the shell running it.

**Code map:** `src/wal.rs` (log), `src/key.rs` (sequence numbers, internal key order, `Shadowed` garbage rule), `src/memtable.rs` (skiplist),
`src/sstable/{block,writer,reader,filter,cache,mod}.rs`, `src/manifest.rs` (format record, last sequence),
`src/db.rs` (`Db` handle, writer queue with per-writer condvars, `make_room` + memtable switch, `SuperVersion` + `ReadView`, recovery, most tests),
`src/db/background.rs` (background thread, flush job), `src/db/compaction.rs` (pick / start / run unlocked / finish),
`src/db/snapshot.rs` (`Snapshot`), `src/codec.rs`, `src/fsutil.rs`, `src/test_util.rs` (Rng), `src/main.rs` (REPL),
`tests/kill9.rs` (real-process crash test), `examples/{durability,concurrency}.rs` (benchmarks).

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

**Next step:**
1. Owner works through the review questions (M1–M8, below).
2. M9 (range scans) needs a design consult; see RESUME HERE.

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

## Blockers / open decisions
- [x] Rust 1.99.0 installed
- [x] GitHub: private repo https://github.com/keshavdhingra001/lsmkv (SSH remote, key ~/.ssh/id_ed25519)
- [x] Git email links to GitHub account keshavdhingra001
- [x] D2: mid-log WAL corruption fails loud (owner approved 2026-10-04)
