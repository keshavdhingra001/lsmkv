# lsmkv design

Living document. Every non-obvious decision gets a short entry: **what**, **alternatives**, **why**.

## Architecture

```text
put/delete ─> writer queue ─> leader: WAL append + one fsync per group ─> memtable (lock-free skiplist)
                                                                             │ full: switch (new WAL)
                                                                             v
                          background thread: flush immutable memtable ─> L0 tables ─> compaction ─> L1 … L6
get / scan / snapshot ─> copy Arc<SuperVersion> (memtable, immutable memtable, levels) ─> read with no lock
                         get:  memtables, then L0 newest first, then one table per level (bloom -> index -> block)
                         scan: k-way merge of every source, newest visible version per key
```

The README has the overview and the results. The rest of this file is the decisions, in the order they were made.
Later milestones changed some early decisions; those entries say so, and point at the one that replaced them.

## Decision index

| # | Decision | Milestone |
|---|---|---|
| D1 | WAL record format: CRC32 over each record | M2 |
| D2 | A torn WAL tail is cut off; corruption mid-log refuses to open | M2 |
| D3 | Memtable: `BTreeMap` (replaced by D13) | M1 |
| D4 | WAL replay reads the whole log at once | M2 |
| D5 | SSTable: 4 KiB CRC'd blocks, filter, index of last keys, footer | M3 |
| D6 | Flush, manifest (edit log) and recovery; one commit point | M4 |
| D7 | A failed WAL or manifest write poisons the database (fsyncgate) | M4 |
| D8 | Leveled compaction, LevelDB-style; tombstones dropped only at the safe level | M5 |
| D9 | Bloom filters, 10 bits per key, double hashing | M6 |
| D10 | Shared LRU block cache; compactions and scans don't fill it | M6 |
| D11 | Sync modes `Always` / `Periodic`, group commit | M7 |
| D12 | Reads without the state lock: an immutable SuperVersion | M8 |
| D13 | Memtable: crossbeam lock-free skiplist | M8 |
| D14 | Immutable memtable, background flush | M8 |
| D15 | Background compaction in three phases; background errors poison | M8 |
| D16 | Level-0 slowdown and stop triggers | M8 |
| D17 | One condition variable per writer | M8 |
| D18 | Sequence numbers, internal key order, snapshots | M8 |
| D19 | Scan API: forward, owned pairs, one point in time | M9 |
| D20 | Heap merge over streaming sources; compaction streams too | M9 |
| D21 | An open scan pins files, not a sequence number | M9 |
| D22 | Crash harness: many `kill -9`s, one directory, an exact checker | M10 |
| D23 | Model-based fuzzing with proptest | M10 |
| D24 | Benchmark methodology vs RocksDB | M11 |
| D25 | Atomic write batches: one WAL record each; format 3 | M13 |
| D26 | Optimistic transactions: snapshot isolation, first committer wins | M13 |
| D27 | Redis-protocol server: RESP2, thread per connection | M14 |
| D28 | Deterministic simulation: `Fs` trait, simulated disk, power cuts; atomic manifest commits (format 4) | M15 |
| D29 | Trivial moves out of level 0: disjoint level-0 tables move down by manifest edit | M17 |
| D30 | Restart points in data blocks: binary search inside a block (format 5) | M18 |
| D31 | Testing infrastructure: CI, libFuzzer, a committed mutation runner, LazyFS power cuts, the real `redis-cli` | M16 |

## Decisions

### D1: WAL record format
- **What:** `[crc32][kind][key_len][val_len][key][value]`, little-endian, CRC over everything after the CRC field.
- **Alternatives:** LevelDB-style 32 KiB blocks with fragmented records; no checksum.
- **Why:** Simplest format that still detects torn and corrupt writes. Revisit block framing if replay speed matters.

### D2: Torn-tail handling
- **What:** Replay stops at the first invalid record. `Db::open` truncates the file to the last valid offset before appending.
- **Why:** Without truncation, new writes land after garbage and are lost on the next replay (see the `writes_after_torn_tail_are_not_lost` test).
- **Mid-log corruption (decided 2026-10-04: fail loud):** a complete record with a bad checksum or contents *followed by more data* returns `Error::Corruption` with the offset, and `Db::open` refuses to start, leaving the log untouched.
  A crash can only tear the *last* record, so data after a bad record means disk corruption, not a crash. This mirrors RocksDB's default `kTolerateCorruptedTailRecords`.
- **Zero-filled tails are tolerated:** some filesystems extend a file with zeros on a crash, which would otherwise look like corruption.
- **Known limit:** a corrupted *length* field that points past EOF looks like a torn tail, so it's tolerated silently. Fixing that needs a header checksum or LevelDB-style fixed-size blocks (Tier 3).
- **In-process write failures:** these are handled by poisoning (D7), so a partial record is never followed by more appends.
- **Group commit (M7, D11):** a group's records are appended together and synced once, so a crash can tear several of them at once. The tail rule still holds: they're all at the end of the log, and none of them was acknowledged before the fsync.

### D3: Memtable structure
- **What:** `BTreeMap<Vec<u8>, Entry>` for now.
- **Later:** A skiplist for concurrent reads (M8).
- **Superseded by D13 (M8):** the memtable is now a crossbeam `SkipMap` keyed by (user key, sequence number).

### D4: Replay reads the whole log into memory
- **What:** `Wal::replay` does one `fs::read` and decodes from the buffer.
- **Why:** The log is bounded by the memtable size (it gets rotated once flushes exist in M4), so it's small. A single read is simpler than a streaming reader.
- **Guard:** Lengths read from disk are untrusted, so `checked_add` and a bounds check run before slicing, so a garbage `u32::MAX` length can't overflow or panic.

### D5: SSTable format (approved 2026-10-04)
- **Layout:** `[data blocks][filter block][index block][footer]`. Full byte layout is in `src/sstable/mod.rs` and `block.rs`.
- **Data blocks:** about 4 KiB target (one OS page, one disk read), entries `[kind][key_len][val_len][key][value]`, then a CRC32 trailer. (Since M8, each entry also carries its sequence number and the index stores each block's last (key, seq); see D18.) Tombstones are stored, because they must shadow older tables.
- **Index:** one entry per block, holding the block's *last* key, offset and size. It's kept in memory, so `get` is a binary search (`partition_point`) plus exactly one block read. A per-key index was rejected because it's about the size of the data itself.
- **Filter block:** reserved and empty in M3, so M6 bloom filters dropped in without a format change (see D9).
- **Footer (52 B):** five u64 fields, a CRC32 and an 8-byte magic `LSMKVSST`. This is a change from the proposed 48 B: the footer is the root of trust (every offset comes from it), so it gets its own CRC.
- **Validation on open:** magic, then footer CRC, then the regions must tile the file exactly, then the index CRC, then blocks must be contiguous from 0 with strictly increasing keys. No offset is trusted before it's checked. Data blocks are verified by their CRC on every read.
- **Atomic publish:** write `<name>.tmp`, fsync the file, rename it, fsync the directory. A crash leaves either no table or a complete one. An abandoned writer deletes its temp file (`Drop`).
- **Immutable:** the writer refuses to overwrite an existing table.
- **Deferred (Tier 3):** prefix compression and restart points within blocks, plus block compression.
- **Platform:** reads use `FileExt::read_exact_at` (pread), so `get(&self)` needs no `&mut` or seek state, which matters for concurrent reads in M8. It's Unix-only.

### D6: Flush, manifest and recovery (approved 2026-10-04)
- **Flush threshold:** `Options::memtable_size`, default 4 MiB (same as LevelDB). Flushing is synchronous: the `put` that crosses the threshold does the flush. Background flushing waits for M8.
- **File numbers:** logs and tables share one counter (`000001.log`, `000002.sst`, ...), so higher always means newer. Both numbers a flush needs are reserved up front, so a failed flush never reuses a number an orphan file still holds.
- **Manifest:** an append-only log of 13-byte checksummed edits: `AddTable`, `RemoveTable` (for M5) and `SetLogNumber`. Replaying it rebuilds the live set. Impossible histories (adding the same table twice, removing an unknown table, the log number going backwards) are corruption. It's never rewritten yet; it grows by about 26 B per flush, and compacting it is Tier 3.
- **Flush order:**
  1. Write the table (tmp + fsync + rename).
  2. Create the new WAL and fsync the directory.
  3. **Commit point:** one manifest write of `AddTable` + `SetLogNumber`.
  4. Switch in-memory state and delete obsolete logs (best effort).
  - A crash before step 3 leaves orphans, and the old WAL still holds the data. A crash after it leaves the table live, and the old WAL is ignored.
- **Recovery:** replay the manifest, then delete obsolete logs (number < log number), unlisted tables and `*.tmp` files, then replay *all* live logs oldest-first. Two live logs exist after a crash between steps 2 and 3. Only the newest log gets its torn tail cut, because it's the only one appended to. Unrecognized files are never touched.
- **Read path:** memtable, then tables newest to oldest. The first hit wins, and a tombstone means "not found".
- **Verified by:** failpoints after each step plus a simulated crash, then reopen. Every write is present, there are no orphans, and the database keeps working. Two planted bugs (no poisoning, and replaying only the newest log) were caught.

### D7: Poison on write failure (fsyncgate)
- **What:** if a WAL append/fsync or the manifest commit fails, the database becomes read-only (`Error::Poisoned`). Reads still work. Reopening recovers from whatever actually reached the disk.
- **Why:** after a failed fsync, you can't know what's on disk (PostgreSQL's 2018 "fsyncgate"). If the manifest commit actually landed, the current WAL is obsolete, and writing more to it would lose those writes on reopen. A failed WAL append can also leave a partial record, and later appends after it would look like mid-log corruption.
- **Not poisoned:** failures before the commit point (writing the table, creating the log). State is still consistent, and a retry works; the orphans are cleaned up on the next open.
- **Known gaps:** there's no `LOCK` file yet, so two processes could open the same directory. Tier 2 adds an `flock`.

### D8: Leveled compaction (approved 2026-10-04)
- **Why leveled:** levels 1 to 6 are each sorted runs of non-overlapping tables, and each level's size limit is `level_size_multiplier` (10) times the one above (L1 = 10 MiB). A point read checks every level-0 table, then at most one table per deeper level. The space overhead is about 1.1x.
  - **The alternative, size-tiered (Cassandra's default):** it merges similar-sized runs, so it writes less (lower write amplification) but reads more (many overlapping runs) and can need 2x space during a merge. Leveled suits read-heavy workloads, size-tiered suits write-heavy ones.
- **Triggers:**
  - Level 0 at `l0_compaction_trigger` (4) tables.
  - Level n by `bytes / limit`. The highest score of at least 1.0 runs first. Compaction loops until nothing is over its limit, and the bottom level (6) never compacts.
- **Inputs:**
  - **Level 0: all of its tables.** They overlap, and moving only the newer ones down would leave older versions above them in level 0, where reads check first (stale reads). A mutation test confirmed this.
  - **Level n: one table**, chosen round-robin by `compact_pointer` so the whole key space gets compacted over time.
  - Plus every overlapping table in level n+1.
- **Merge:** inputs are read newest first (level 0 by id descending, then level n, then level n+1); the first version of a key wins. It's currently done in memory (about 26 MiB worst case with the defaults). M9's streaming merge iterator will replace it.
- **Tombstones:** dropped only if no table in a level *deeper than the output level* covers the key. Dropping earlier would resurrect an older value from below (`tombstone_survives_while_older_data_is_deeper`).
- **Output:** split into tables at `target_file_size` (2 MiB), so a later compaction of level n+1 can pick small units.
- **Trivial move:** a single input table with nothing overlapping in level n+1 moves by a manifest edit alone (`RemoveTable` + `AddTable` with the same id), with no I/O.
- **Commit:** one manifest write (`RemoveTable` for the inputs, `AddTable` for the outputs). This is the same commit step as flush, with the same poisoning. Input files are deleted only after it. A crash earlier leaves orphan outputs, which the next open deletes.
- **Manifest format:** the level is encoded in the tag (`0x10 + level`), so records stay 13 bytes. M4's tag-1 records still read as level 0.
- **Not done yet:** compaction runs synchronously inside the write that triggers it (M8 moves it to the background). There's no grandparent-overlap limit on outputs (LevelDB's `max_grandparent_overlap`) and no L0 write stalls; both matter only once compaction is in the background.
- **Verified by:**
  - Crash injection at `compact:after_tables`, `compact:manifest` and `compact:after_manifest`.
  - Randomized operations with randomized compaction settings, checked against a `BTreeMap`.
  - Mutation checks, each caught by the tests: always dropping tombstones, compacting only the newest level-0 table, letting the oldest version win the merge, and deleting the inputs before the commit.

### D9: Bloom filters (approved 2026-10-04)
- **What:** one filter per SSTable over every key (tombstones included), in the filter block the format reserved in M3. Loaded and CRC-checked on open and kept in memory beside the index. `get` asks the filter first; "definitely not here" skips the table without a block read.
- **Why per table, not per block:** the filter is only worth having when it saves a block read. One filter per table rules a table out with a single in-memory check; per-block filters (LevelDB's original layout) need the index search first and cost more metadata. RocksDB moved to per-table ("full") filters for the same reason.
- **Size:** `bloom_bits_per_key` = 10, so k = round(10 · ln 2) = 7 probes.
  - False-positive rate: p = (1 − e^(−kn/m))^k = (1 − e^(−0.7))^7 ≈ **0.82%**.
  - The best k for a given m/n is (m/n) · ln 2; more probes then set too many bits, fewer leave too few to check.
  - Memory: 1.25 bytes per key (about 1.25 MB per million keys).
  - Minimum 64 bits, so tiny tables don't get a degenerate filter.
- **Hash:** FNV-1a, then the MurmurHash3 64-bit finalizer. It's written by hand on purpose: `std`'s `DefaultHasher` is unspecified and may change between Rust releases, which would make every saved filter say "not here" for keys it holds (silent data loss on reads). `hash_is_stable` pins three outputs, checked against an independent Python implementation. The finalizer is there because FNV-1a alone mixes a key's last bytes poorly into the high 32 bits, and double hashing uses both halves.
- **Probes:** double hashing (Kirsch–Mitzenmacher, 2006): probe i is bit (h1 + i · h2) mod m, with h1 and h2 the two 32-bit halves of one 64-bit hash. One hash per lookup, with the same asymptotic false-positive rate as k independent hashes.
- **Format:** `[bits][k u8][crc32]`. Storing k makes old tables readable after the setting changes. A bad CRC or k outside 1..30 is corruption (fails loud, like a bad block).
- **Tombstones are in the filter:** a lookup must find a tombstone to learn the key is deleted. If the filter skipped it, the read would fall through to an older table and return the deleted value.
- **Compatibility:** `filter_len = 0` means "no filter, always maybe". That covers M5-era tables and `bloom_bits_per_key = 0`, and tables with and without filters mix freely in one database.
- **Measured** (`bloom_filters_cut_block_reads_for_missing_keys`, 8 overlapping L0 tables, 3,992 missing keys inside every table's range, cache off):
  - No filters: 31,936 block reads.
  - 10 bits/key: **279 block reads (114x fewer)**, with a 0.87% false-positive rate (theory: 0.82%).
  - Unit test (10,000 keys, 100,000 probes): 0.811%.
- **Verified by:** mutation checks, each caught: probes collapsing to one bit (h2 = 0), `any` instead of `all`, a changed hash, a wrong byte index when building, tombstones left out of the filter, the reader ignoring the filter, and the writer hashing the wrong bytes.

### D10: Block cache (approved 2026-10-04)
- **What:** one LRU cache shared by every table, `block_cache_bytes` = 8 MiB by default (0 turns it off). Key = `(table id, block offset)`. Value = a CRC-verified data block as `Arc<[u8]>`.
- **Why the key is safe:** file numbers are never reused (D6), so a key can't point at another table's block, even after compaction deletes a table. A trivial move keeps the id, but the bytes don't change, so its cached blocks stay valid. Blocks of deleted tables are not evicted eagerly; they're cold and age out (LevelDB does the same).
- **Structure:** the textbook O(1) LRU: a `HashMap` from key to slot, plus a doubly linked list in recency order. The list lives in a `Vec` and links by index rather than pointer, so it's safe Rust with no `unsafe` and no `Rc<RefCell>` web; freed slots are reused.
- **Capacity in bytes, not entries:** blocks vary in size (one large value makes one large block). A block bigger than the whole cache is not stored.
- **Concurrency:** `get` takes `&self`, but a hit must reorder the list, so the state sits behind a `Mutex` (interior mutability). The lock is held only to find the block and relink it; the caller gets a cloned `Arc` and reads with the lock released. An evicted block a reader still holds stays alive until that `Arc` drops. Counters are `AtomicU64` with `Relaxed` ordering (independent counts; nothing synchronizes through them).
  - **Later (M8):** one lock is a contention point under many reader threads. RocksDB shards the cache by key hash into 2^n independently locked LRUs.
- **CRC:** verified on the way from disk into the cache, not on every hit. A block that fails is never cached, so every read of it keeps reporting the corruption.
- **Compaction bypasses the cache** (LevelDB's `fill_cache = false`): it reads every block exactly once, and caching them would evict the blocks real reads reuse.
- **Not cached:** index and filter blocks. They stay in memory for every open table, so their memory grows with the table count, outside the cache's limit. RocksDB's `cache_index_and_filter_blocks` puts them under the cache budget; it's worth it once the data is far bigger than RAM.
- **Measured** (`hot_keys_are_served_from_the_cache`, 20,000 keys with 100-byte values, 20,000 gets with 90% of them on the first 5% of keys):
  - Cache off: 20,000 disk reads.
  - 256 KiB cache: **1,797 disk reads, a 91% hit rate**.
- **Verified by:**
  - A randomized test against a naive `Vec` model, checking full recency order and byte count after every step.
  - A 4-thread test.
  - Mutation checks, each caught: dropping the table id from the key, FIFO instead of LRU, evicting the most recent entry, a stale tail pointer (now a clean failure via a debug assertion instead of an infinite eviction loop), and caching before the CRC check.

### D11: Durability modes and group commit (approved 2026-10-04)
- **API:** `put`, `delete`, `get`, `flush` and `compact_all` take `&self`, and `Db` is `Send + Sync`, so threads share it as `Arc<Db>`. Internally, the M6 engine became a private `State` behind one `Mutex`, and `Db` is a thin handle that adds the writer queue.
- **Modes** (`Options::sync_mode`, set for the whole database):
  - `Always` (default): fsync the WAL before acknowledging. Survives power loss.
  - `Periodic(d)`: acknowledge once the bytes reach the OS (`write`, no fsync); a background thread fsyncs every `d`. It survives a process crash (`kill -9`: the kernel already holds the bytes) but not a power cut or kernel crash, which can lose up to about `d` of acknowledged writes. This is Cassandra's `commitlog_sync: periodic`.
  - Per-write `sync` flags (LevelDB's `WriteOptions::sync`) were rejected for now: one setting is easier to reason about and to measure.
- **Group commit** (LevelDB's `DBImpl::Write` design):
  - Each write joins a FIFO queue under the state lock and waits on a `Condvar`.
  - The writer at the front, when no group is in flight, becomes the **leader**. It takes queued records up to 1 MiB, sets `writing`, and **releases the state lock**. Then it appends them all, does one fsync (or one flush to the OS in `Periodic` mode), and re-takes the lock.
  - It then applies the group to the memtable in queue order (the WAL's order, so a replay rebuilds exactly this memtable), posts each follower's result, and wakes everyone.
  - Writers that arrive during the fsync queue up and form the next group, so the more writers wait, the bigger the groups get, and fsync cost is shared automatically.
  - The 1 MiB cap keeps one writer's latency from stretching behind an unbounded group.
- **Why the state lock is released during I/O:** that's the whole point. Holding it would serialize writers on the fsync just like before, and block readers too.
- **WAL lock:** the WAL has its own `Mutex`.
  - Lock order is state, then WAL; nothing takes the state lock while holding the WAL, so they can't deadlock.
  - Flush and compaction (`exclusive()`) wait until no group is in flight before switching the WAL. Otherwise a leader's records would go into a log the manifest just retired, and they'd be lost on reopen even though they were acknowledged (`flush_waits_for_the_group_in_flight`).
- **Failures:**
  - If a group's append or fsync fails, the leader gets the I/O error, every follower gets `Poisoned`, and nothing in the group is acknowledged.
  - Writers already queued behind it are refused too. The failed group may have left a partial record at the log's end, and appending after it would turn a torn tail into mid-log corruption (D2).
  - A failed periodic fsync also poisons the database. Before the poison, writes in that window were already acknowledged; that's the trade-off `Periodic` makes.
- **Periodic thread:**
  - It waits with `wait_timeout_while`, which checks the stop flag before sleeping. A plain `wait_timeout` lost the wakeup when `Drop` signalled before the thread was waiting, and the drop then hung for a full interval: a real bug, found by a hung test run and pinned by `drop_right_after_open_does_not_wait_out_the_interval`.
  - It never takes the state lock on its normal path (an atomic counter instead), so a long flush or compaction can't delay it. A probe showed the interval stretching from 100 ms to 177 ms during compactions before this change.
  - It holds the WAL lock only to clone the file handle (`try_clone`, a `dup`), then fsyncs the clone with no lock held. fsync acts on the file, not the descriptor, so writers keep appending during the fsync.
  - `Drop` stops the thread and does one final fsync, so a clean close loses nothing in either mode.
- **Measured** (`cargo run --release --example durability`; a laptop NVMe SSD under btrfs; 100-byte values; 3 s per run; ops/sec ranges over 2–3 runs):

  | mode | threads | ops/sec | p50 | p99 | max | writes per fsync |
  |---|---:|---:|---:|---:|---:|---:|
  | Always | 1 | 1,330–1,350 | 0.68 ms | 1.4 ms | 28 ms | 1.0 |
  | Always | 4 | ~3,300 | 1.3 ms | 2.4 ms | 30–49 ms | 2.5 |
  | Always | 16 | 7,300–12,800 | 1.4 ms | 2.5–3.0 ms | 51 ms | 10.4 |
  | Periodic(100ms) | 1 | ~335,000 | 1.5 µs | 3.5 µs | 140 ms | – |
  | Periodic(100ms) | 4 | ~230,000 | 8 µs | 17 µs | 450 ms | – |
  | Periodic(100ms) | 16 | ~220,000 | 24 µs | 64 µs | 570 ms | – |

  - Group commit gives 16 writers 5–9.5x the single-writer throughput at the same p50: a group of about 10 shares each fsync. (The first 16-thread run measured 7.3k; later runs 12.2–12.8k on unchanged code: disk variance.)
  - `Periodic` is about 250x faster for one writer: the fsync, not the engine, is the cost of durability.
  - **The max column is M8's job:** flush and compaction still run inline under the state lock, so every writer stalls behind them (140–570 ms).
  - **`Periodic` gets slower with more threads:** with no fsync to hide behind, coordination cost dominates (two lock round-trips per write and `notify_all` waking every waiter). LevelDB uses one condition variable per writer to wake only the next leader. That's an M8 item.
  - Benchmarks must run on a real disk: on tmpfs (this machine's `/tmp`) fsync is free and `Always` would look like `Periodic`.
- **Verified by:**
  - Fsyncs are counted by the WAL itself (`Wal::sync_count`), so the stats can't claim fsyncs that didn't happen.
  - `kill_9_loses_no_acknowledged_write` (`tests/kill9.rs`): a child process writes from 4 threads with small memtables (flushes and compactions mid-flight) and prints each acknowledged key. The parent `kill -9`s it after 2,000, 3,500 or 5,000 acks, in both modes, then reopens and finds every acknowledged key.
  - Staged deterministic tests (a test-only slow-WAL delay holds a leader for 150 ms): flush waits for the group in flight; groups apply in queue order; a failed group fails every member; writers behind a failed group are refused; the periodic thread keeps syncing while the state lock is held.
  - Mutation checks, each caught:
    - `Always` never fsyncing
    - followers acknowledged when their group's fsync fails
    - flush not waiting for the group in flight
    - the group never applied to the memtable
    - the memtable applied in reverse of WAL order
    - no grouping
    - the leader ignoring a poison set while its group waited
    - the leader logging only its own record
    - `Periodic` acknowledging before the bytes reach the OS (`kill -9` lost 25 of 2,000 acknowledged writes)
    - the lost wakeup
    - the periodic thread taking the state lock
  - Two mutations survived the first round of tests (the uncounted fsync, and three races the randomized test couldn't hit reliably). That's why the staged tests exist.
- **Not covered:** power-loss durability. It needs a VM or a fault-injecting filesystem (LazyFS, dm-log-writes); see M10.

### M8: Concurrency, approved 2026-10-04 (D12–D18)
The owner approved the whole proposal ("go"). Each entry is filled in as its section lands.
- **D12 SuperVersion:** reads grab an `Arc` snapshot of (memtable, immutable memtable, levels) under a brief lock, then read with no lock held.
- **D13 Memtable:** `crossbeam-skiplist` (lock-free reads while it's being written).
- **D14 Background flush:** a full memtable becomes immutable (one at most, like LevelDB), a fresh one and a new WAL take over, and a background thread flushes it. Writers stall only if a second memtable fills first.
- **D15 Background compaction:** the same single background thread, flush first. Compaction picks its inputs under the lock, merges without it, and commits under it. Background errors poison the database.
- **D16 L0 backpressure:** LevelDB's triggers: compact at 4 L0 files, slow each write by 1 ms at 8, stop writes at 12.
- **D17 Writer wakeups:** one condition variable per writer instead of `notify_all`.
- **D18 Sequence numbers and snapshots:** done first, in M8 rather than Tier 3, because M9's range scans need a consistent view too.

### D12: Reads without the state lock: SuperVersion (approved 2026-10-04)
- **Before:** `get` held the state lock for its whole lookup, disk reads included, so every read waited behind any write group's bookkeeping, and behind every inline flush and compaction (hundreds of ms).
- **What:** a `SuperVersion` is an immutable bundle of everything a read needs: the memtable (`Arc<MemTable>`) and the table levels (`Vec<Vec<Arc<Table>>>`). The current one sits in a small `ReadView` mutex together with `last_seq`. `get` locks it only to clone one `Arc` and copy one number (tens of ns), releases it, and then searches with no lock held.
- **Changes never mutate a SuperVersion:** flush and compaction build the next one (cloning the level vectors is just `Arc` clones) and `State::install` it, which also publishes it to the read view. `State` holds the current one as an `Arc<SuperVersion>`, which Rust won't let you mutate, so a change that forgets to publish doesn't compile. The compiler caught exactly that while this was being built: compaction's in-place `levels.retain(...)` stopped compiling.
- **A reader with an old SuperVersion stays correct:** the `Arc`s keep its memtable and tables alive. Compaction deletes its input files right after installing, while a reader may still be searching them. That's safe on POSIX: an unlinked file stays readable through a descriptor that's already open, and `SstReader` holds its descriptor until the last `Arc<Table>` drops (`an_old_super_version_stays_readable_after_compaction`). On Windows this would need deferred deletion (RocksDB keeps a list of obsolete files and deletes them when the last reference goes).
- **One lock, not two atomics:** `last_seq` and the SuperVersion are read under the same lock, so a reader can never pair a new snapshot number with an old SuperVersion that's missing the memtable those writes went into. With separate atomics, the load order would have to be argued very carefully. RocksDB goes further (thread-local cached SuperVersions, no shared lock at all), which only matters at millions of reads per second.
- **Lock order:** state, then the WAL or the read view. Readers take only the read view.
- **Verified by:**
  - `reads_never_wait_for_the_state_lock`: a test holds the state lock, and reads of a table key, a memtable key and a missing key all finish.
  - `readers_see_every_acknowledged_write_across_flushes_and_compactions`: one writer counts a key up through hundreds of flushes and compactions while 3 readers check that the value never goes backwards and is never below the last acknowledged value.
  - Mutation checks, each caught: `get` taking the state lock; flush publishing the emptied memtable before its table (caught 5 of 5 runs); a group never published to readers; `install` not publishing.

### D13: Memtable: a lock-free skiplist (approved 2026-10-04)
- **What:** `crossbeam_skiplist::SkipMap<InternalKey, Entry>` instead of a `BTreeMap`. Inserts take `&self`, so the group commit leader inserts while readers search the same map, and neither blocks the other.
- **Alternatives:**
  - `RwLock<BTreeMap>`: readers would block while a group is applied, and the writer would wait for the readers to drain.
  - A hand-written skiplist: LevelDB's own uses one writer plus atomic pointers. In Rust it needs `unsafe` code and manual memory reclamation, which is a project of its own.
  - crossbeam's version handles memory reclamation with epochs. It's the one dependency besides `crc32fast` and `thiserror` that sits on the core path.
- **Why a skiplist at all:** it's the standard structure for this (LevelDB, RocksDB). A sorted linked list with express lanes allows lock-free insertion, since a new node is linked in with one compare-and-swap per level. Balanced trees rebalance, which touches many nodes at once.
- **Half-applied groups are invisible:** the leader inserts a group's versions one by one, but they carry numbers above the published `last_seq` until the whole group is in. Readers read at the published number, so they skip all of them (`readers_see_exactly_their_snapshot_while_a_writer_inserts`: one writer and 3 readers checking exact versions at 20,000 snapshots).
- **Costs:**
  - A `get` now clones the value out of the map (it returned a reference before); `Db::get` copied it anyway.
  - Nothing is ever removed from a memtable, so epoch reclamation has nothing to do until the whole memtable drops.

### D14: Immutable memtable and background flush (approved 2026-10-04)
- **Before:** the leader whose group filled the memtable flushed it inline, holding the state lock: every writer (and, before D12, every reader) waited for a whole table write plus a manifest fsync.
- **The switch** (`State::switch_memtable`, LevelDB's `MakeRoomForWrite`): before taking its group, a leader checks for room. If the memtable is full, it creates a new WAL, fsyncs the directory, fsyncs the old WAL, makes the full memtable *immutable* and installs a fresh one. Then it wakes the background thread and goes on writing. All of this happens under the state lock, but it's only file creation and two fsyncs, no table write.
  - **It needs no group in flight,** like M7's flush: the leader is the only possible group, and `Db::flush` waits for `writing == false`.
  - **Why sync the old WAL:** it holds the immutable memtable's writes until their table commits. In `Periodic` mode its tail may not be on disk yet, and once it's swapped out, the periodic thread syncs only the new WAL. Without this, a power cut during a slow flush could lose more than one interval of writes (`switch_syncs_the_old_log_even_in_periodic_mode`, which counts fsyncs with the WAL's own counter).
- **One immutable memtable at most** (LevelDB). If the new memtable fills before the old one is flushed, the leader waits: a *stall*, counted in `write_stalls` and `stall_micros`. RocksDB allows several (`max_write_buffer_number`), which absorbs bursts at the cost of memory and longer recovery.
- **Reads** check the memtable, then the immutable memtable, then the tables. Every version in the immutable memtable is older than every version in the active one, so D18's "first visible version wins" still holds.
- **The flush job** (`background.rs`):
  1. Write the table with no lock held.
  2. **Commit:** one manifest write adds the table, sets the log number to the active WAL (retiring the old one) and records the immutable memtable's last sequence number.
  3. Install the table in place of the immutable memtable in one step, so a reader finds each version in exactly one of them. Then delete the old WAL.
- **Recovery is unchanged:** a crash between the switch and the commit leaves two live WALs, and M4's recovery already replays both, oldest first.
- **File numbers:** the switch takes the log number first, then the flush takes the table number. A flush used to take table N and log N+1; now it's log N, table N+1 (the leftovers test changed accordingly).

### D15: Background compaction, and errors (approved 2026-10-04)
- **One background thread** (`background::run`), one job at a time, flush first (LevelDB). It waits on a condition variable that a switch, a `compact_all` request or `Drop` signals.
- **Compaction in three phases:**
  1. `start_compaction`, lock held: pick the inputs, clone their `Arc<Table>`s and the current SuperVersion, and copy out the settings. A trivial move is only a manifest edit, so it's done right here.
  2. `CompactionJob::run`, no lock: merge and write the outputs. File numbers come from a closure that takes the lock briefly.
  3. `finish_compaction`, lock held: commit, install, and delete the inputs.
- **The levels can't change during phase 2,** because only the background thread changes them, and it runs one job at a time. Writers only touch the memtables, so `finish_compaction` keeps whatever memtables are current.
- **The oldest snapshot is taken at phase 1.** A snapshot created during the merge has a newer number than that, so it can't need a version the merge drops.
- **Still under the lock:** manifest fsyncs at commit time, about 1–5 ms. LevelDB releases its mutex for this too, relying on a single manifest writer. Possible here for the same reason, but not done yet.
- **`flush()` and `compact_all()`:**
  - `flush()` switches the memtable, then waits until the background thread is idle with nothing to do (no immutable memtable, no level over its limit).
  - `compact_all()` sets a request that the background thread works through level by level.
  - The request only moves *down* the levels (`manual_compaction` remembers how far it got), so tables flushed meanwhile can't keep it going forever (mutation: restart from level 0 each step, caught).
- **Errors poison** (LevelDB's sticky `bg_error_`). A failed switch, flush or compaction poisons the database: writes are refused, reads work, and a reopen recovers from the disk.
  - **This replaces M4's "a failure before the commit is retryable".** A background job has no caller to hand a retryable error to. Retrying automatically would mean telling transient failures from persistent ones, and fsyncgate (D7) says not to trust a retry after a failed fsync anyway.
  - **A refused write is never logged:** the leader checks for poison after making room, so a write that gets an error was never acknowledged *and* is not in the WAL. The crash tests used to expect "the failing write's record is durable" (inline flush failed after logging it); now they expect exactly the acknowledged writes.
- **`Drop`** stops the thread after its current job. An unflushed immutable memtable is still in its WAL, so nothing is lost.

### D16: Level-0 backpressure (approved 2026-10-04)
- **Why:** with compaction in the background, writers can outrun it. Level 0 would grow without bound, and every read checks every level-0 table.
- **What** (LevelDB's triggers, now `Options`):
  - **Compaction trigger (4):** L0 compaction starts.
  - **Slowdown (8):** each write group's leader first sleeps 1 ms, once. Spreading many small delays avoids one long stall later.
  - **Stop (12):** a leader that needs a memtable switch waits until compaction brings L0 back under the trigger. Writes into a memtable that still has room aren't blocked: the stop applies at the switch.
- **Options must satisfy compaction <= slowdown <= stop** (`open_with` refuses otherwise). With stop below the compaction trigger, writes would stop at a level-0 size that never triggers the compaction that would let them continue.
- **Counted** in `Stats`: `write_slowdowns`, `write_stalls`, `stall_micros` (also shown by the REPL's `stats`).

### Verification for D14–D16
- **Staged tests**, each with a test-only `slow_background` delay holding the job in place for 400 ms:
  - `writes_and_reads_continue_while_a_flush_runs`: 50 puts and every get finish in under 100 ms while the flush runs. The test watches the read view, not `stats` (which takes the state lock). A second fill stalls, and its wait is counted.
  - `writes_and_reads_continue_while_a_compaction_runs`: the same during a 4-table L0 compaction.
  - `deep_level_0_slows_then_stops_writes_until_compaction_catches_up`: with compactions paused (test hook), L0 reaches the stop trigger and the writer stops, with slowdowns counted. Unpausing lets it finish.
- **Crash tests:** every switch, flush and compaction failpoint poisons; reopening recovers exactly the acknowledged writes, with no orphans.
- **Mutation checks, each caught:**
  - the flush retiring the active WAL
  - the switch not syncing the old WAL (it survived until the WAL counted its own fsyncs, as M7 already did for groups)
  - reads skipping the immutable memtable
  - the flush leaving the immutable memtable installed
  - switching while a flush is pending
  - no stop trigger
  - no slowdown
  - the flush writing its table while holding the lock (first caught only indirectly, until the test stopped probing through the state lock)
  - the compaction merging while holding the lock
  - background errors not poisoning
  - the manual compaction restarting from level 0
  - `flush()` returning mid-job
  - the switch forgetting the immutable memtable's last sequence number
- **Early numbers** (D11's benchmark, same machine): max write latency fell from 140/450/570 ms to 22/140/199 ms for `Periodic` with 1/4/16 threads, and from 28–51 ms to 27–30 ms for `Always`. `Periodic` throughput rose to 422k/329k/246k ops/s (from 335k/230k/220k). The proper before/after measurement is section 5.

### D17: One condition variable per writer (approved 2026-10-04)
- **What:** each queued write carries its own `Arc<Condvar>` (LevelDB gives each `Writer` its own `port::CondVar`). A leader wakes exactly the writers whose state it changed: its followers (their result is in), and the new front of the queue (the next leader). Before, it called `notify_all` on one shared condition variable, which woke every queued writer, including ones that only went back to sleep.
- **Why not a thread-local condition variable** (to save the allocation): std documents that a `Condvar` may panic if used with more than one mutex over time, and one thread can write to several `Db`s.
- **The shared `turn` condition variable stays** for `flush` / `compact_all` callers and stalled leaders, which wait on conditions no single writer owns.
- **No lost wakeups:** a writer checks "am I done / am I the leader" under the state lock before sleeping, and a leader changes that state under the same lock before it notifies. So a notify can't fall between a writer's check and its wait.
- **Measured** (`durability -- target/bench Periodic 16`, 3 alternating runs of each build, context switches from `getrusage(RUSAGE_CHILDREN)`):

  | 16 `Periodic` writers | `notify_all` | per-writer |
  |---|---:|---:|
  | voluntary context switches per write | 1.33–1.49 | 1.09–1.10 |
  | involuntary context switches (3 s run) | 38–40k | 11–15k |
  | CPU per write | 9.5–11.6 µs | 8.5–11.0 µs |
  | writes/sec | 267–308k | 251–309k |

- **A negative result:** wasted wakeups dropped 20–30% and CPU per write a little, but throughput didn't change. D11 guessed that `notify_all` was why `Periodic` slows from 1 to 16 threads, and that was wrong.
  - **What it actually is:** the serial leader path. One thread at a time takes the lock, writes the group with one syscall, applies it, and then wakes each follower with its own futex syscall. Every writer still sleeps about once per write.
  - **How RocksDB attacks it:** adaptive spinning before sleeping (`enable_write_thread_adaptive_yield`), and followers inserting into the memtable themselves, in parallel (`allow_concurrent_memtable_write`). Not done here.
- **Kept anyway:** it's cheaper and not slower, and it's the design LevelDB uses.
- **Verified by:** the whole suite, including the M7 staged group tests. Mutations: the leader not waking the next leader (everything behind it hangs), or not waking its followers. Both caught.

### D18: Sequence numbers and the internal key order (approved 2026-10-04)
- **What:** every write gets the next sequence number (`SeqNo`, a `u64`; 0 means "before any write"). Every stored version carries it: WAL records, memtable keys and table entries. A read picks a snapshot number and sees, per key, the newest version at or below it.
- **Order:** versions sort by user key ascending, then by seq **descending**, so a key's newest version comes first and a lookup stops at the first version it may see (LevelDB's internal key order).
  - The pair is compared field by field (`key::compare`), never as one byte string. With the seq bytes appended to the key, bytewise order would put `"a"`+seq after `"ab"`+seq whenever the seq's first byte is above `b'b'`, though `"a"` is a prefix of `"ab"` and must come first. LevelDB gets away with appending because it plugs in a comparator that splits them again.
  - Tables store the seq as its own 8-byte field in every entry and index entry, rather than as a key suffix, for the same reason.
- **Lookup** (memtable, block and index alike): find the first entry at or after (key, snapshot) in this order. If it belongs to `key`, it's the newest version the snapshot may see; if it belongs to a later key, there is none. In the index, the target block is the first one whose last entry is at or after (key, snapshot), so a key whose versions span two blocks still costs one block read.
- **Read path across sources:** memtable, then L0 newest first, then one table per deeper level. Every version in a newer source is newer than every version in an older one (flush and compaction preserve that), so the first visible version found is the answer.
- **Who assigns numbers:** the group commit leader, under the state lock, right after the last applied write (only one group is ever in flight). The group's records go to the WAL with their numbers, are applied in the same order, and only then does `last_seq` move. Reads use `last_seq` as their snapshot, so a group becomes visible all at once, and never before it's durable.
- **Recovery:** WAL records carry their numbers, so a replay rebuilds the same versions. A flush deletes the WAL that held its numbers, so the flush's manifest edit also records `SetLastSequence`. On open, `last_seq = max(manifest's last sequence, highest seq in the live WALs)`. Without that edit, numbering would restart at 1 after a reopen: new writes would be invisible next to tables holding higher numbers, and the reads would miss data (`sequence_numbers_survive_replay_and_flush`).
- **Garbage (`key::Shadowed`):** a version can be dropped once a newer version of the same key is visible to every reader (that newer version's seq is at or below the oldest snapshot). This is LevelDB's rule. It tracks one number, and keeps a few versions a per-snapshot rule would drop.
  - Flush applies it too, so a hot key overwritten 1,000 times becomes one table entry, not 1,000. The memtable itself keeps every version until the flush (`approx_size` now counts each one, which also answers M1's "never shrinks on overwrite" question: there's nothing to shrink).
  - A tombstone is dropped only if every reader already sees it, and no deeper level can hold the key (D8's rule, plus the snapshot condition).
  - Compaction outputs end only between user keys, so one key's versions never span two tables, and tables in a level still never overlap.
- **Format version:** this changes the WAL, table and manifest formats. The manifest's first record is now `Format(2)`, and the table magic is `LSMKVSS2`. Directories from before M8 have no format record and are refused with a clear error. Opening them anyway would be worse than failing: an M7 WAL read as format 2 parses as garbage, which the torn-tail rule (D2) would truncate, silently losing data. No migration tool exists; the project has no users with old data.
- **Cost:** 8 bytes more per WAL record, per table entry and per index entry; the filter still hashes each distinct key once.
- **Verified by:** unit tests for the order and the shadowing rule; snapshot lookups in the memtable and a block; a randomized table test with several versions per key, checked at random snapshots against a `BTreeMap` model; the reopen/flush numbering test; the flush-garbage test; pre-M8 manifests refused.
  - Mutation checks, each caught: no `SetLastSequence` at flush; replay ignoring WAL numbers; nothing ever shadowed; a block or memtable lookup returning the next key's version; the index searched by user key only; a group reusing the last number; compaction keeping shadowed versions; the filter skipping each key's first version; the format record not required.
  - A compaction splitting one key's versions across two tables survived at first: with no snapshots, a key has only one live version at compaction time. Section 4's `one_keys_versions_never_span_two_tables` kills it (20 snapshots pin 20 big versions of one key, more than one output table holds).
- **Snapshots (`Db::snapshot`, section 4):**
  - **The handle:** `Snapshot<'db>` is only a sequence number, the read view's `last_seq` when it was taken. It sees exactly the writes acknowledged before `snapshot()` returned (plus possibly some applied but not yet acknowledged), and nothing later. `snapshot.get(key)` reads the *current* SuperVersion at that number: compaction keeps every version the snapshot can see, so the current tables still answer correctly.
  - **Registration:** a multiset (`BTreeMap<SeqNo, count>`) in the read-view mutex, so taking and dropping a snapshot never touches the state lock. `Drop` unregisters it, so it's RAII: a snapshot can't be leaked by forgetting a release call (it can only be held too long).
  - **`oldest_snapshot()`** is now the smallest registered number (or `last_seq` if none). Flush and compaction read it when a job starts. A snapshot taken during a job has a newer number than that, so the job can't drop anything it needs.
  - **Bottom level:** automatic compaction never compacts the bottom level, so versions a snapshot pinned while they were pushed down would stay there after it's released. `compact_all` ends by rewriting the bottom level in place (RocksDB's `bottommost_level_compaction = kForce`). Automatic cleanup of such leftovers (RocksDB marks bottom files for compaction once the oldest snapshot moves past them) is not done.
  - **Not provided:** snapshots don't survive a reopen (they're in-memory reader state, as in LevelDB and RocksDB), and there's no snapshot iterator yet (M9's range scans).
  - **Verified by:**
    - a point-in-time test through overwrites, deletes and two full compactions, including garbage collection after the snapshot drops;
    - `reads_through_one_snapshot_agree_under_concurrent_writes`: a writer sets `a = i` then `b = i`; readers must never see `b > a` within one snapshot;
    - a randomized test (10 seeds × 3,000 steps) checking random live snapshots against copies of a model taken with them, through flushes and compactions.
    - Mutation checks, each caught: `oldest_snapshot` ignoring snapshots or using the newest one; a snapshot reading the latest data; a dropped snapshot staying registered; a tombstone dropped though a snapshot sees past it; a key split across tables; `compact_all` skipping the bottom level.
  - **A bug found on the way:** `State::stats` called `lock(&self.view)` twice inside one struct literal. A temporary's guard lives until the end of the whole statement, so the second call deadlocked on the first (std's `Mutex` isn't reentrant). Three tests hung, and the fix was to take the lock once beforehand.

### M8 results: before and after
`cargo run --release --example concurrency`: 200,000 preloaded keys (100-byte values), reader threads doing random gets while one writer writes new keys as fast as it can (`Periodic(100ms)`, so a memtable fills about every 0.1 s and flushes and compactions run throughout). The same benchmark file was run against M7's code (commit `6f566a3`) on the same machine and disk. M8 ranges are over 2 runs (the second was slower across the board).

| readers + writer | reads/sec M7 → M8 | read max M7 → M8 | writes/sec M7 → M8 | write p99.9 M7 → M8 | write max M7 → M8 |
|---|---:|---:|---:|---:|---:|
| 4, no writer | 439k → 0.93–1.11M | 3.3 ms → 0.5–0.6 ms | – | – | – |
| 1 + writer | 401k → 344–413k | 120.6 ms → 0.15 ms | 171k → 317–390k | 26 µs → 11 µs | 120.6 ms → 20–38 ms |
| 4 + writer | 368k → 685–885k | 15.2 ms → 1.8–2.4 ms | 42k → 217–266k | 149 µs → 13–17 µs | 15.0 ms → 36–41 ms |
| 8 + writer | 366k → 620–803k | 130.4 ms → 4.9–6.2 ms | 21k → 153–196k | 262 µs → 34–46 µs | 130.3 ms → 27–33 ms |

- **Reads scale with threads now.** In M7, every `get` took the state lock, so 4 readers maxed out at one reader's worth; now they run in parallel (2–2.5x).
- **Reads no longer stall** behind inline flushes and compactions: read max went from 120–130 ms to under 7 ms.
- **The writer is 2–9x faster with readers running.** In M7 it competed with every reader for the state lock, and 8 readers cut it to 21k writes/s.
- **The remaining write max (20–41 ms)** is the memtable switch (a new WAL, a directory fsync and the old WAL's fsync, under the state lock) plus level-0 stop stalls when the writer outruns compaction. In M7 it was a full inline flush, up to 130 ms.


### M9: Range scans, approved 2026-10-04 (D19–D21)
The owner approved the combined M9–M12 proposal in one "go".

### D19: The scan API (approved 2026-10-04)
- **What:** `db.scan(range)` and `snapshot.scan(range)` return a `DbIter`, an `Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>` of live keys in key order. `db.iter()` / `snapshot.iter()` scan everything. The range is any `RangeBounds<K>` with `K: AsRef<[u8]>`, so `db.scan("user:".."user;")`, byte-slice ranges and `Vec<u8>` ranges all work.
  - A first attempt took `impl RangeBounds<[u8]>`. It compiled, but `&b"a"[..]..&b"m"[..]` didn't: std implements `RangeBounds<T> for Range<&T>` only for sized `T`. The generic `K` fixes that. A bare `..` can't infer `K`, hence `iter()`.
- **Forward only.** Reverse iteration needs `prev` on every source (blocks would need restart points to walk backwards cheaply, the merge a max-heap mode). It's on the Tier 3 list.
- **Owned items:** each pair is copied out. A borrowing iterator (`&[u8]` valid until the next call) saves a copy per key, but it's a lending iterator, which Rust's `Iterator` can't express.
- **One point in time:** a scan reads at `last_seq` as of the call, so it sees every write acknowledged before it and none after, however long it runs. Snapshot isolation comes for free, because the scan holds an immutable SuperVersion (D21).
- **Errors:** an I/O error or a bad checksum comes out as one `Err` item, and then the scan ends.

### D20: Merging iterator and streaming sources (approved 2026-10-04)
- **The merge:** a binary min-heap holding each source's next entry, in internal key order (key ascending, seq descending). `next` pops the smallest and refills from the same source: O(log k) per entry, with one entry per source in memory.
- **Sources** (`SuperVersion::scan_sources`):
  - the memtable and the immutable memtable;
  - each level-0 table on its own (they overlap);
  - one `LevelIter` per deeper level. A level's tables don't overlap and are sorted, so it reads them one after another and opens each table's iterator only when it gets there.
  - Tables whose key range misses the scan are left out, so a narrow scan reads only the tables it needs.
- **On top, `DbIter`** turns versions into answers: it skips versions above the snapshot, takes the first visible version of each key (the newest one, by the merge order), skips the key's older versions, and hides the key if that version is a tombstone. That's `get`'s rule, applied to a stream.
- **Table iterator:** one block in memory at a time. A seek is the same index binary search `get` uses, then a walk within the block. Scan blocks come from the cache if they're there, but a scan never *adds* blocks to the cache: a full scan would flush out every hot block point reads use (the same reason compaction skipped the cache in D10). RocksDB makes this a per-read option (`fill_cache`).
- **Memtable iterator:** a crossbeam `SkipMap` iterator borrows the map, so an iterator that owns its `Arc<MemTable>` can't also hold one without `unsafe` (a self-referential struct). Instead, it searches again from just past the last version it copied and copies the next 64: one O(log n) search per 64 entries. Versions written after the scan started may or may not show up in the copy, but they're above the snapshot, so `DbIter` skips them either way.
- **Compaction uses the same merge.** It used to collect every input into a `BTreeMap`, so `compact_all` held the whole bottom level in memory. Now it streams with one block per input in memory.
  - Measured (`examples/scan.rs`, 1M keys, `compact_all` rewriting 129 MiB of tables): **peak RSS 24 MiB, down from 265 MiB**, and 1.52 s instead of 2.00 s. The old number was measured by running the same example against the section 2 commit.

### D21: What an open scan holds (approved 2026-10-04)
- **No snapshot registration.** The original M9 plan said an open iterator must pin its sequence number like a `Snapshot`, or compaction could drop versions it's about to read. It doesn't need to:
  - the scan holds `Arc`s of the memtables and of every table it might read (the SuperVersion as of its start);
  - none of those ever change (D12: flush and compaction build new ones);
  - a table file that compaction deletes stays readable through the descriptor the scan's reader holds open (POSIX).
  - So everything the scan can see stays where it was. RocksDB iterators work the same way: they pin a SuperVersion (files), not a sequence number.
- **`Snapshot::scan`** reads the *current* SuperVersion at the snapshot's number, like `Snapshot::get`. That works because the registered snapshot makes compaction keep its versions. Once the scan starts, it holds its own SuperVersion, so dropping the snapshot mid-scan is fine too.
- **The cost:** an open scan holds deleted files' disk space and old memtables' memory until it's dropped, so don't keep scans open for a long time. RocksDB's docs give the same warning.
- **`DbIter` borrows nothing**, so it's `'static + Send` and can outlive the `Db` handle.
- **Verified by:**
  - `iter_from_every_start_matches_entries` (every start key, between keys, before and after all, with a key's versions split across two blocks) and `iter_from_matches_the_map_across_batches` (refill edges inside one key's versions);
  - `iter_from_sees_every_older_version_while_a_writer_inserts` (the memtable iterator under a concurrent writer);
  - `scan_merges_every_source_and_honors_bounds`: every bound type, a deleted start key, empty ranges and start > end, with data in L1+, L0 and the memtable;
  - `scan_is_one_point_in_time` and `scan_keeps_reading_tables_compaction_deleted`: a scan started before overwrites, deletes and `compact_all` (which deletes the files it's reading) still returns the old data;
  - `scan_reads_the_immutable_memtable_during_a_flush`: staged, with the flush held in place by the test-only slow-disk hook;
  - `concurrent_scans_see_a_prefix_of_ordered_writes`: 3 scanners against a writer adding `k0000`, `k0001`, ... through flushes and compactions; a scan must never see a key without every earlier one;
  - `narrow_scan_skips_tables_outside_its_range`: every other table's block 0 damaged on disk, and a scan of one table's range still succeeds;
  - `randomized_scans_match_a_model`: 10 seeds × 3,000 steps, scans with random bounds, now or at a random live snapshot.
  - **Mutation checks:** 20 planted bugs. 19 caught, 1 equivalent:
    - The equivalent one: removing the table iterator's "stop after an error". `load` clears the block before reading the next one, so a failed read already ends the iteration; the explicit stop only matters for a malformed entry inside a block whose checksum passed (a writer bug).
    - "Ordering only by user key, then source" was caught only by the `MergeIter` unit test. In the database, sources are listed newest first, so source order happens to match seq order. The seq comparison doesn't depend on that, and the unit test pins it down.
    - "Open every table regardless of range" survived at first. The test damaged the middle of each table, but a scan that steps into the next table only reads its block 0 before seeing it's past the end. It now damages block 0.

### M9 results
`cargo run --release --example scan` (1M keys of 16 B with 100 B values, after `compact_all`; one run):

| operation | rate |
|---|---:|
| full scan | 5.8M keys/s |
| short scans (seek + 100 keys) | 76k seeks/s (7.6M keys/s) |
| point gets (for comparison) | 400k/s |
| `compact_all` over 129 MiB | 1.52 s, peak RSS 24 MiB (was 2.00 s and 265 MiB with the `BTreeMap` merge) |

### M10: Crash harness and fuzzing, approved 2026-10-04 (D22–D23)

### D22: Crash harness: many `kill -9`s, one directory, an exact checker (approved 2026-10-04)
- **Before (M7):** one kill per fresh directory, 6 in all, and the only check was "every acknowledged key exists". That misses an older value coming back, a lost delete, and anything that goes wrong only when recovery runs on top of an earlier recovery.
- **What (`tests/kill9.rs`):** each round, a child process (the test binary re-run with an environment variable) opens the *same* directory and runs 4 writer threads. Each thread owns 2,000 keys and does random puts (75%) and deletes (25%), printing `S` before each operation and `A` once it's acknowledged. The parent kills it with SIGKILL at a random moment (20–300 ms in), reopens the database itself, and checks every key.
- **The checker is exact.** A thread starts its next operation only after the last one is acknowledged, so at most one per thread is in flight at the kill. Each key must hold the result of its last acknowledged operation, or, if the in-flight operation was on that key, that operation's result. Values name the operation that wrote them (`r3.t1.o57.xxx`), so an old value coming back can't pass as a new one. Then a full scan must match the point reads, so there are no ghost keys and recovered scans agree with recovered gets.
- **The model carries over:** after the checks, the model takes the database's actual state, and the next round's child continues from it. So each round also tests recovering a directory that was itself recovered, often with an immutable memtable's WAL and a half-finished compaction left behind.
- **The child checks itself too:** every 50 operations, each thread scans its own keys, now or through a snapshot, and compares them with what it wrote. A mismatch exits with an error, which the parent reports.
- **Pressure:** a 16 KiB memtable, level-0 compaction at 2 tables, a 64 KiB level 1 growing 4x per level. Flushes and compactions are always in flight, stalls happen, and the data reaches level 3. Rounds alternate `Always` and a `Periodic` mode that never fsyncs within the test: a process crash keeps the kernel's page cache, so neither mode may lose anything.
- **Run time:** 6 rounds in `cargo test` (about 1.3 s, ~21k acknowledged operations checked); `kill_9_soak` (`#[ignore]`) runs 300, or `LSMKV_CRASH_ROUNDS`.
- **Out of scope: power loss.** A process crash keeps everything the kernel already has. A power cut drops the page cache, including writes the code wrote but didn't fsync, and testing that needs a fault-injecting filesystem (LazyFS) or a block-device recorder (dm-log-writes) to replay every crash point. Neither is cheap to set up here. The argument for power-loss safety is the fsync ordering instead: WAL fsync before acknowledging in `Always` (D11), table fsync before rename before manifest (D5, D6), the directory fsync after creating files (D6), the old WAL's fsync at a memtable switch (D14), and poisoning on a failed fsync (D7).
- **A harness bug found on the way:** libtest in the child prints `test <name> ... ` with no newline before the test body runs, so the first `S` line arrived glued to it and was dropped, and its `A` then referred to an operation the parent never saw. The child now prints a `ready` line after opening the database, which ends libtest's line and starts the kill clock after recovery.
- **Verified by planting crash-only bugs**, judged by `tests/kill9.rs` alone:
  - caught: recovery replaying only the newest WAL (loses the immutable memtable's writes), recovery ignoring the sequence numbers in the WALs (new writes get numbers below old versions, so old values win), and open deleting the active WAL;
  - not caught by this harness, as expected: not cutting off a torn WAL tail (a `kill -9` can't tear a write; a write to a regular file completes before the signal lands, so only a power cut tears one), and a flush not recording the last sequence number (that only shows when the active WAL is empty at reopen, which a crash mid-write almost never leaves). The unit tests catch both (`writes_after_torn_tail_are_not_lost`, `crash_at_every_flush_step_loses_nothing`). The layers cover each other.

### D23: Model-based fuzzing with proptest (approved 2026-10-04)
- **What (`tests/model.rs`):** proptest generates random options and a sequence of up to 300 operations: put, delete, get, scan (random bounds, now or through a live snapshot), take a snapshot, drop one, flush, `compact_all`, and close + reopen. The test runs each sequence against the database and against a `BTreeMap`, checking every read. Snapshots are checked against a copy of the model taken with them. After each reopen, a full scan must equal the model.
- **Random options too:** memtables of 128 B–4 KiB and tables of 256 B–4 KiB (so a hundred operations run flushes and multi-level compactions), either sync mode, bloom filters on or off, the block cache on or off.
- **Keys from a tiny space** (0–3 letters of "abc", 40 keys): operations keep hitting the same keys, the empty key and prefixes. Random bytes would almost never collide.
- **Only the public API**, as an integration test, so it checks what a user can see, not internals.
- **Why proptest over our own seeded loops** (which the unit tests already use): shrinking. On a failure, proptest cuts the sequence and the options down to a minimal failing case (often 3–5 operations) and saves it in `tests/model.proptest-regressions`, which is committed, so every later run replays it first. A seeded loop reports "seed 7, step 2,113" and leaves the minimizing to you. proptest is a dev-dependency only.
- **Not cargo-fuzz:** it needs nightly Rust, and coverage-guided fuzzing pays off most on parsers. Our parsers (WAL records, blocks, the index, the footer, the manifest) are covered by the every-byte-flip and truncation tests.
- **Run time:** 128 cases by default; `PROPTEST_CASES=5000 cargo test --release --test model` for a longer search.
- **Verified by planting earlier milestones' bugs**, judged by `tests/model.rs` alone. Caught: compaction dropping a tombstone that still shadows deeper data, compaction ignoring live snapshots, a block cache key without the table id, bloom filters leaving tombstones out, a scan's tombstone not hiding older versions, and an excluded scan start being included.
  - **One survived at first:** a memtable `get` ignoring the snapshot. The model test read through snapshots only with scans, which use a different memtable path. Snapshot `get`s are now in the generator, and it's caught.
  - **Shrinking in practice:** the planted tombstone bug came down to 7 operations: `Put a; CompactAll; Delete a; Flush; Put ""; Flush; Snapshot`. The second flush triggers a level-0 compaction that drops the tombstone while the old `a` still sits in a deeper level, and the final check through the snapshot finds `a` back. The shared-cache-key bug came down to 8.

### M10 results
- **Crash soak** (`cargo test --release --test kill9 -- --ignored`): 300 `kill -9` rounds against one directory, alternating `Always` and `Periodic`; **1,022,073 acknowledged operations checked, 573 in flight at a kill, none lost or wrong.** The data reached level 4 (108 tables). 85 s.
- **Long fuzz** (`PROPTEST_CASES=5000 cargo test --release --test model`): 5,000 random option sets and operation sequences (up to 300 operations each), no failure. 41 s.

### M11: Benchmarks vs RocksDB, approved 2026-10-04 (D24)

### D24: Benchmark methodology (approved 2026-10-04)
- **Harness:** a separate crate, `bench/` (its own `[workspace]`), depending on `lsmkv` and the `rocksdb` crate 0.25 (RocksDB 11.8.1) with default features off (no compression libraries) plus `bindgen-runtime`. `cargo test` on the main crate never builds librocksdb. The first build compiles RocksDB's C++: about 7 minutes on this laptop.
- **Workloads,** named after `db_bench`: `fillseq`, `fillrandom`, `overwrite`, `readrandom`, `readmissing` (16-byte keys that sort among the real ones but were never written), `seekrandom` (seek + 100 `next`), `readwhilewriting` (4 readers + 1 writer for 10 s), and `fillrandom` with fsync per write (20k keys). 1M keys of 16 bytes, 100-byte values.
- **Matched settings:** 4 MiB memtable with one immutable memtable behind it, 10-bit bloom filters, 8 MiB block cache, 4 KiB blocks, no compression, level-0 triggers 4 / 8 / 12, 10 MiB level 1 growing 10x, 2 MiB tables, one background thread, WAL written but not fsynced per write (`Periodic(1s)` vs `sync = false`).
- **Metrics:** ops/s; p50/p99/p99.9 from every operation's latency (`Instant` around each call, sorted at the end; no histogram dependency). Write amplification is SSTable bytes written by flushes and compactions over user bytes: ours from `Stats`, RocksDB's from its `rocksdb.flush.write.bytes` and `rocksdb.compact.write.bytes` tickers. WAL bytes are left out on both sides.
- **Fairness fixes, made after the first run showed lsmkv ahead almost everywhere:**
  - RocksDB statistics at the default level time every operation. Set to counters only (`ExceptHistogramOrTimers`).
  - RocksDB sizes levels dynamically by default since 8.x. Set to static (`level_compaction_dynamic_level_bytes = false`), like lsmkv.
  - Checked that librocksdb is built with `NDEBUG` (no assertions).
  - Rebuilt both with `-C target-cpu=native`, in case RocksDB's CRC32C was falling back to software while `crc32fast` picks SSE4.2/PCLMUL at run time. The results didn't change, so that wasn't it.
- **Results** (native builds, ranges over 2 runs; the same table is in the README):
| workload | lsmkv ops/s | RocksDB ops/s | p99 µs, lsmkv vs RocksDB | write amp, lsmkv vs RocksDB |
|---|---:|---:|---:|---:|
| fillseq | 390k–414k | 406k–421k | 4.9 vs 4.6–4.7 | 2.28 vs 1.00 |
| fillrandom | 293k–314k | 339k–348k | 6.7–6.9 vs 6.0–6.3 | 5.81 vs 4.26–4.66 |
| overwrite | 256k–276k | 179k–318k | 6.9–7.2 vs 7.0–9.5 | 6.86–6.89 vs 5.21–6.51 |
| readrandom | 459k–472k | 236k–249k | 4.7–4.9 vs 7.7–9.4 |  |
| readmissing | 1.75M–1.87M | 1.14M–1.16M | 1.8–1.9 vs 2.9 |  |
| seekrandom (+100 next) | 43k–45k | 29k–31k | 39.4–39.7 vs 56.7–58.7 |  |
| readwhilewriting: reads (4 threads) | 406k–410k | 398k–406k | 24.2–25.2 vs 19.4–20.4 |  |
| readwhilewriting: writes (1 thread) | 138k–144k | 82k–171k | 10.6–11.0 vs 11.3–13.9 |  |
| fillrandom (fsync each) | 586–840 | 493–722 | 9168–9774 vs 9227–16263 |  |
- **Reading it honestly:**
  - **Writes are roughly even, and RocksDB's compaction is better.** It fills randomly 8–19% faster with lower write amplification, and on sequential keys it moves level-0 tables down without rewriting them (write amp 1.00 vs our 2.28: our level-0 compaction always merges into level 1, D8).
  - **Single-threaded point reads and seeks are 1.4–2.0x faster in lsmkv. Not profiled** (`perf` isn't available here). Ruled out: value copying (the crate's `get` is `get_pinned` + one `to_vec`, like ours) and CPU-specific CRC code (the native build). Likely: RocksDB's more general read path (merge operators, range tombstones, per-read options, a thread-local SuperVersion) and the C API boundary. That's a statement about this setup, not about read speed in general.
  - **Under concurrency it evens out:** with 4 readers and a writer, read throughput is equal, and RocksDB has the better read p99 and steadier writes.
  - **The run-to-run variance is large** (RocksDB `overwrite` 179k vs 318k; one run had a ~1 ms p99.9 from a stall). Two runs aren't enough for fine distinctions; differences under ~20% shouldn't be read as real.
  - **Not compared:** compression, prefix-compressed blocks, multiple background threads, column families, and RocksDB's tuning for large datasets that don't fit in memory. Here 129 MiB of tables sit mostly in the OS page cache.
- **A benchmark bug on the way:** `shuffled(i, n)` cycle-walks a permutation of `0..2^k` until it lands below `n`. That only terminates when the input is already below `n`. Called with `i * 7 + 3`, it looped forever (a 44-minute hung run). `examples/scan.rs` had the same latent bug. Both now reduce the input mod `n` first.

### M13: Atomic batches and transactions, approved 2026-10-05 (D25–D26)
Tier 3, approved together with M14 and M15 in one "go".

### D25: Atomic write batches (approved 2026-10-05)
- **API:** `let mut b = WriteBatch::new(); b.put(k, v).delete(k2); db.write(b)?`. All the operations land or none do, and readers see all or none. Later operations on the same key win.
- **One WAL record per batch** (LevelDB's approach): kind 3, the usual 21-byte header with its two length slots reused for the operation count and the payload length, then the operations back to back. One CRC covers the whole batch, so a torn tail drops all of it, never part of it (`a_batch_torn_anywhere_disappears_whole` cuts it at every byte offset).
  - **Rejected alternative:** begin/end marker records around ordinary records. Recovery would need to buffer records until the end marker and handle a missing one, which is more states to get right for the same guarantee.
  - **A single put stays a plain record,** so the common case pays nothing and old logs still read.
- **Numbering and visibility:** the operations take consecutive sequence numbers. The group commit leader already publishes `last_seq` once per group, after applying all of it, so a reader's snapshot either includes the whole batch or none of it (`readers_never_see_half_a_batch`: 3 readers, snapshots and scans, 2,000 ten-key batches through flushes).
- **Every write is a batch inside the engine:** the writer queue holds batches, and `put`/`delete` queue a batch of one. Group sizing counts a batch's bytes.
- **Format 3.** An older build would read a batch record as a bad (torn) tail and truncate it: silent data loss. So the manifest format went to 3:
  - New databases start at 3.
  - Opening a format-2 database appends `Format(3)` to its manifest before anything new is written. A format-2 build accepts only one format record, first, so it now refuses the directory instead of misreading it (`format_2_is_upgraded_and_newer_formats_are_refused`).
- **Untrusted counts:** the decoder first allocated `Vec::with_capacity(count)` from the on-disk count. A test with `count = u32::MAX` aborted the process (allocation failure), the same class of bug as D4's lengths. Capacity is now capped by what the payload can hold (each operation takes at least 9 bytes).

### D26: Optimistic transactions (approved 2026-10-05)
- **API:** `let mut tx = db.transaction(); tx.get(k)?; tx.put(k, v); tx.delete(k); tx.commit()?`. Reads see the snapshot taken at `transaction()`, plus the transaction's own buffered writes. Dropping it discards the writes.
- **Commit, first committer wins:** the transaction's writes go into the writer queue as one batch carrying its snapshot number. The group leader checks each key it writes: if the key has any version (a value or a tombstone) newer than the snapshot, another writer committed to it first, and the commit fails with `Error::Conflict`, applying nothing. A conflict is safe to retry.
- **Where the check runs:** in the leader, with the state lock released, against the SuperVersion as of the group's start. That's safe because only one write group is ever in flight (D11): no write can be applied between the check and the commit, and a flush that installs a new SuperVersion meanwhile only moves data. Entries earlier in the *same* group count as newer too (`conflicting_transactions_in_one_group_one_wins`, staged with the slow-WAL hook). A refused entry is never logged and takes no sequence numbers.
- **Finding the newest version:** `SuperVersion::newest_seq` checks the memtables, then level 0 newest first, then one table per deeper level, like `get` but returning the version's number (`SstReader::get_versioned`). Bloom filters make the common no-such-key case cheap.
- **Isolation level: snapshot isolation.** It prevents lost updates (`concurrent_transfers_conserve_money`: 4 threads moving money between 10 accounts with retries; an auditor checks every snapshot's total, and it survives a reopen). It allows **write skew**: two transactions that each read both keys and write *different* ones both commit. `write_skew_is_allowed_under_snapshot_isolation` pins this down with the on-call doctors example. Preventing it (serializable isolation) needs read-set tracking: checking that nothing a transaction *read* changed either, like PostgreSQL's SSI. Full serializability isn't built, but there's the targeted fix databases offer: **`get_for_update(key)`** (RocksDB's `GetForUpdate`, SQL's `SELECT ... FOR UPDATE`) reads a key *and* adds it to the commit's conflict check, so a transaction can protect the keys its decision rests on (`get_for_update_prevents_write_skew`). A transaction that only locked keys still runs the check at commit; that's what Redis's `WATCH` needs (M14).
- **Rejected alternative: pessimistic locking** (lock each key on first write). That needs a lock table, deadlock detection or timeouts, and holds locks across user code. Optimistic is right when conflicts are rare, and it reuses the sequence numbers the engine already has. RocksDB offers both (`OptimisticTransactionDB` and `TransactionDB`).
- **Verified by:** the tests named above, plus the crash harness (1 in 5 operations is a 2–4 key batch, and after every kill an in-flight batch must be entirely present or entirely absent) and the proptest model (batches, plus transactions with direct writes racing them that must conflict exactly when they share a key). **Mutation checks: 15 planted bugs, all caught:** same-group writes not counted, the conflict check skipping tables, refused entries logged or applied anyway, half a batch published to readers, a transaction ignoring its own writes, commit skipping the check, `>=` instead of `>`, batch operations replayed at one sequence number, a batch logged as separate records, no format upgrade, any format accepted, a malformed payload accepted, and two for `get_for_update`.
- **Limits:** a transaction's `get` reads its snapshot plus its own writes, but there's no transactional `scan` yet (it would merge the write buffer into a `DbIter`). Long transactions hold a snapshot, so compaction keeps old versions for them (D18).

### D27: Redis-protocol server (approved 2026-10-05)
- **What:** `lsmkv-server` (`src/bin/lsmkv-server.rs`) speaks RESP2, Redis's wire protocol, so `redis-cli`, `valkey-cli` and `redis-benchmark` work unchanged. The protocol is in `src/resp.rs` and the commands in `src/server.rs`; both are in the library, so tests run them in-process.
- **Commands,** each mapped onto an engine feature:
  - `GET SET DEL EXISTS PING ECHO`: plain reads and writes.
  - `MSET`: one atomic batch (D25). `MGET`: all keys read at **one snapshot**, which is stronger than Redis promises.
  - `INCR INCRBY DECR`: a transaction with retry on conflict (D26). 8 clients × 200 concurrent `INCR`s always total exactly 1,600.
  - `MULTI`/`EXEC`/`DISCARD`: the queued commands run in one transaction. Reads inside see earlier queued writes. A malformed queued command makes `EXEC` fail with `EXECABORT`, as in Redis.
  - `WATCH`: starts the transaction at `WATCH` time and `get_for_update`s the watched keys. If another client writes one before `EXEC`, the commit conflicts and `EXEC` returns a null array, nothing applied. That's Redis's optimistic locking on top of lsmkv's.
  - `SCAN`/`KEYS`/`DBSIZE` (key iteration), `RANGE start end [LIMIT n]` (lsmkv's own: a range scan returning pairs), `INFO` (engine stats).
- **One thread per connection,** with std's `TcpListener` only. The engine is already thread-safe (group commit for writes, lock-free reads), so connections share an `Arc<Db>`. Connections are capped at 1,024 so a flood can't create unbounded threads.
  - **Rejected alternative: async (tokio).** It's a large dependency, and it wins with tens of thousands of mostly idle connections. Here the bottleneck is the engine, not thread count.
- **Pipelining:** a connection reads whatever has arrived, answers every complete command in it, then writes all the replies with one `write`. A command cut across two reads is kept for the next one (the test sends 400 KB in one burst, and a mutant that dropped the tail was caught once the burst was big enough to span reads).
- **Untrusted input:** argument counts, bulk lengths and line lengths have limits (1M arguments, 64 MiB values, 64 KiB lines), and nothing is allocated from a count before the bytes arrive (D4 again). A malformed request gets an error and the connection is closed, since there's no reliable way to find the next command. The parser is fuzzed: random bytes never panic, and pipelined commands cut at random points parse back exactly.
- **Glob patterns** (`KEYS`, `SCAN MATCH`): the first version was the obvious recursion, which is exponential on `*a*a*a*…b`. One command could pin a thread; Redis itself had this bug. It's now an iterative match with one backtrack point, O(pattern × key) (`glob_has_no_exponential_case`).
- **`SCAN` cursors** are the number of keys already returned, because `redis-cli --scan` parses cursors as integers. Each call rescans from the start: O(cursor). Encoding the last key in the cursor would fix that, but it isn't done.
- **Security:** no AUTH and no TLS, so it binds to 127.0.0.1 by default.
- **Durability:** the server opens the database in `Periodic(100ms)` by default (`--sync always` for fsync per write). A SIGKILL of the server loses nothing acknowledged, which was checked by hand: write, `kill -9`, restart, read.
- **Verified by:** 7 socket-level integration tests (`tests/server.rs`), 4 protocol unit tests + 2 proptest fuzz properties, 2 glob tests. **Mutation checks: 11 planted bugs, all caught** (one only after the pipelining test was enlarged, see above).

### D28: Deterministic simulation and power-loss testing (approved 2026-10-05)
- **Why:** the `kill -9` harness (D22) can't test power loss. A killed process keeps everything it handed the kernel, so a missing fsync, a missing directory fsync or a torn write never shows. D22 argued power-loss safety from fsync ordering instead. This milestone tests it.
- **All file I/O goes through an `Fs` trait** (`src/vfs.rs`), like RocksDB's `FileSystem` and LevelDB's `Env`: `RealFs` in production, and `SimFs`, an in-memory disk, in tests. `Options::fs` picks one. The WAL, manifest, table writer and reader, recovery and file cleanup all take it; `fsutil.rs` is gone.
- **`SimFs`'s rules** (POSIX at its weakest):
  - A file's contents are durable only up to its last successful `sync`.
  - A create, rename or remove is durable only after a `sync_dir` of its directory.
  - **A power cut** keeps each file's synced bytes plus a random prefix (half the time, none) of its unsynced tail, so writes tear anywhere, and different files independently, so writeback order varies. Names roll back to the last directory sync.
  - **A failed `sync`** returns EIO and drops the unsynced data (Linux's behavior, "fsyncgate", D7).
  - Faults are armed by count: "the machine dies at the Nth disk operation from now" (that and every later operation fails, reads included), or "the Nth sync fails".
- **Determinism:** `Options::inline_background` starts no threads, and the WAL is synced by `Db::sync_wal` (new, public; RocksDB's `SyncWAL`). Flushes and compactions run on the writing thread, but **lazily**: only where a writer would otherwise wait for the background thread (the memtable is full and the previous one isn't flushed yet, or level 0 has reached the slowdown trigger), and in `flush`/`compact_all`. That models a slow background thread: acknowledged writes pile up in a new WAL while the old one still waits to be flushed.
  - **The first version flushed right after every memtable switch, and that hid two planted bugs.** A missing directory fsync for a new WAL was masked because the immediate flush's own directory fsync always came first. A missing fsync of the old WAL at a switch (D14) was masked because its data went straight into a synced table. With the real thread, both are real windows. Making inline mode lazy exposed both. With one caller thread and a seeded disk, a seed replays exactly. `a_seed_replays_exactly` runs seeds twice and compares a hash of every disk operation and its arguments. One fix was needed first: compaction built its manifest edits from a `HashSet`, whose order changes per process, so it's a `BTreeSet` now.
  - **Not deterministic:** the real threaded mode. The simulation tests the durability logic, not thread interleavings (those have the staged tests, D17–D21, and the concurrent tests).
- **The test** (`tests/sim.rs`): per seed, 6 epochs on one disk. Each epoch opens the database, arms a fault (70% power cut at a random operation, 20% failed fsync, 10% none), runs random puts, deletes, batches, transactions, flushes, compactions, scans and WAL syncs until one fails, then cuts the power or exits. Each epoch is randomly `Always` or `Periodic`.
- **The check, after every reopen:** the database holds the last state known to be durable **plus some prefix** of the operations after it, applied in order.
  - In `Always` mode every acknowledged write is durable, so only the in-flight operation is uncertain.
  - In `Periodic` mode, writes since the last `sync_wal` may be lost, **but only from the end**: never a hole in the middle, never half a batch.
- **It found a real bug, which nothing else had: manifest commits weren't atomic.**
  - A compaction commits by appending several manifest records (remove the inputs, add the outputs) in one write plus one fsync. Each record has its own CRC.
  - Seed 44: the power died during that fsync, and the disk kept a torn prefix of the write. Replay saw valid-looking "remove input" records without their "add output" records, dropped the inputs, never added the outputs, deleted the outputs as orphans, and lost 37 keys (older values came back).
  - A flush commit (add table + retire WAL + last sequence) had the same flaw.
  - `kill -9` can't tear a write, so the 300-round crash soak, 5,000 fuzz cases and every unit test missed it.
  - **Fix:** a commit of several edits is now a `Group(n)` header record followed by its n edits, in one write. Replay applies a group whole or not at all, and an incomplete group at the tail is a torn tail, cut off (`a_commit_torn_anywhere_applies_whole_or_not_at_all` cuts a commit at every byte; it failed before the fix). That's the same idea as WAL batches (D25).
  - **Format 4.** A format-3 build refuses the directory (it accepts no later format record above 3) instead of misreading a group header as a torn tail.
- **It found a second bug, in a 20,000-seed run (seed 14676): recovery didn't make what it replayed durable.** A process exits with an acknowledged `Periodic` write only in the page cache. The next open replays it and serves it. Then an fsync of that WAL fails (or the power dies), the kernel drops the dirty pages, and a write the database had already served is gone. LevelDB and RocksDB persist recovered data during open. Fix: recovery fsyncs every WAL it replays (`recovered_writes_survive_a_later_power_cut`, which uses the new `SimFs::process_restart`, a `kill -9` that keeps the page cache). The simulation now also kills the process in 10% of epochs.
- **A third bug, the same mistake in the manifest (seed 2793, 20,000-seed run): recovery acted on a manifest commit that wasn't durable.** A flush writes its commit (add the table, retire the old WAL) and the process dies during the manifest's fsync, so the commit is only in the page cache. The next open replays it, deletes the retired WAL as obsolete, and serves the data from the table. A power cut then drops the manifest's unsynced tail: the next recovery sees the old log number, but that WAL is gone, and it deletes the new table as an orphan. Writes the database had served came back older or missing. Fix: `Manifest::open` fsyncs the manifest it replayed before anything acts on it (`a_replayed_commit_survives_a_later_power_cut`). General rule: **recovery must make everything it replays durable before acting on it**, the WALs and the manifest alike. (The same 20,000-seed run also exposed a harness bug, seed 2186: a fault armed for one epoch outlived a clean close and fired in the next open. `SimFs::disarm` now ends it with the epoch.)
- **Verified by planting 10 durability bugs, each judged by the simulation and by the kill -9 harness:**

  | planted bug | simulation | kill -9 harness |
  |---|---|---|
  | `Always` mode acks before the WAL fsync | caught | missed |
  | a new WAL's name not made durable (no directory fsync) | caught | missed |
  | memtable switch skips the old WAL's fsync (D14) | caught | missed |
  | table not fsynced before its rename | caught | missed |
  | table rename not made durable | caught | missed |
  | manifest commit not fsynced | caught | missed |
  | manifest commit without its group header | caught | missed |
  | `sync_wal` doesn't sync | caught | missed |
  | a fresh database's first WAL name not made durable | caught | missed |
  | a torn WAL tail cut off without an fsync | equivalent | missed |

  The equivalent one: the next WAL fsync makes the shorter length durable anyway (fdatasync covers the file size), and a power cut before then leaves the same torn tail, which recovery cuts again. **9 of 10 caught by the simulation, 0 of 10 by `kill -9`:** the two harnesses test different failures, and both are needed. The kill -9 harness covers the real binary, real threads and the real kernel.
- **Rejected alternatives:** LazyFS (a FUSE filesystem that drops unsynced data) or dm-log-writes (records block writes to replay every crash point). Both test the real binary on a real kernel, which is more faithful, but they need root and setup, aren't deterministic or seedable, and can't run in `cargo test`. FoundationDB's full simulation also virtualizes the network, time and threads. This one virtualizes only the disk, since that's where this engine's correctness lives.
- **Limits of the model:** a torn write keeps a prefix, never scattered sectors. Directory operations are durable only after a directory sync (real filesystems sometimes persist them earlier, which is the safer direction). No bit rot (the checksum tests cover that). One thread.
### D29: Trivial moves out of level 0 (approved 2026-10-05)
- **Problem:** a level-0 compaction always rewrote every level-0 table into level 1, even when the tables were disjoint and level 1 held nothing in their range. With sequential keys (a bulk load, time-ordered ids) that's every compaction: write amplification 2.28 on `fillseq`, against RocksDB's 1.00 (D24).
- **Rule:** a compaction's tables move down by a manifest edit alone (no data read or written) when (1) it moves down a level, (2) nothing in the next level overlaps its range, and (3) no two of its tables share a user key, **not even at their edges** (`largest < next smallest`, strictly). An edge can be shared in level 0: a key overwritten in a later memtable has versions in two tables, and moving both would put that key in two tables of one level, which levels 1+ forbid (reads binary-search for the one table that can hold a key). Levels 1+ already moved a lone table this way (M5); this generalizes it to all of level 0 at once.
- **Empty tables never move:** open refuses an empty table in levels 1+ (it has no key range to place). A flush never writes one, but a moved empty table would make the database unopenable, so the check costs nothing to keep.
- **One commit:** the removals and additions go in one `Group(n)` manifest commit (D28), so a crash leaves the tables in level 0 or in level 1, never in both or neither. The compaction failpoints cover it like any compaction.
- **Why all-or-nothing:** RocksDB can move a subset of level 0 and merge the rest. All-or-nothing is simpler and covers the case that matters (sequential keys, where every table is disjoint); a mixed workload just merges, as before.
- **Tests:** sequential keys rewrite nothing (`compaction_bytes == 0`); edge-sharing tables, tables overlapping level 1 (including a level-1 table in the gap between two level-0 tables, which overlaps the compaction's range but neither table), and an empty table all merge; every case reopens, which re-validates levels 1+. Mutation-checked: 5 planted bugs (edges allowed to touch, no disjointness check, next-level overlap ignored, empty tables moved, moved tables left unsorted), all caught. The first version of the tests ran `compact_all`, whose bottom-level rewrite repaired a bad move before anything looked: every planted bug survived it.

### D30: Restart points in data blocks (approved 2026-10-05)
- **Problem:** a point read binary-searches the index for its block, then scanned the block entry by entry from the start: ~30 entries for 4 KiB blocks of 100-byte values, hundreds for small values or big blocks. LevelDB and RocksDB binary-search inside the block too.
- **Layout (format 5):** after a block's entries come the byte offsets of entries 0, 16, 32, ... (u32 each), then their count, then the CRC, which now covers the trailer too. Keys stay whole (no prefix compression), so every entry can still be read where it starts, and iterators keep borrowing keys straight out of the block. `Block::seek(key, seq)` binary-searches the restart points for the last one before the target, then scans at most 16 entries; `get` and the table iterator's seek both use it.
- **Restart interval 16:** LevelDB's default. Each restart point costs 4 bytes (about 3% of a block of 130-byte entries); a smaller interval means more bytes and a shorter scan. With whole keys, an interval of 1 would be possible, but 16 keeps the trailer small for small entries.
- **Trailer checking:** the CRC proves the bytes are what the writer wrote, not that the writer was right. When a block is read from disk (not on cache hits), it walks the entries once and requires restart k to be exactly the offset of entry 16k, and the count to match. A restart pointing into the middle of an entry would otherwise be parsed as an entry: garbage served as data. The walk costs about what the CRC does.
- **Compatibility:** a table says which layout its blocks use with its footer's magic: "LSMKVSS3" (restart points) or "LSMKVSS2" (formats 2 to 4, no trailer). Old tables stay readable and get rewritten by compaction. The database format goes to 5 (an old directory is upgraded on open by appending `Format(5)`), so a format-4 build refuses the directory instead of failing on the first new table. `tests/compat.rs` opens a database written by the format-4 build (checked in under `tests/fixtures/`), reads every key, mixes old and new tables, and rewrites it.
- **Found while building it:** "LSMKVSS3" and "LSMKVSS2" differ in one bit, and the footer CRC covered only the five u64s, not the magic. One flipped bit would have made a new table read as an old one, its restart trailer parsed as entries. The existing every-byte-flip test caught it. Format-5 footers now include the magic in their CRC.
- **Rejected for now:** prefix compression (LevelDB stores each key as shared-prefix length + suffix, with full keys only at restart points). It saves space on keys with long common prefixes, but entries could no longer be read in place: iterators would have to rebuild each key into a buffer, and `RawEntry` couldn't borrow from the block. It's the natural next step on top of this layout.
- **Tests:** `seek` matches a linear scan for every version of 100 keys plus keys between them; old-layout blocks read the same; wrong trailers behind a good CRC (a restart off by a byte, a first restart not at 0, a count one short, a count past the block) are corruption; the fuzzers (`sstable_roundtrip`, `sstable_read`) ran against the new layout. Planted bugs: see `scripts/mutants.toml`.

### D31: Testing infrastructure (approved 2026-10-05)
- **CI** (`.github/workflows/ci.yml`): every push runs `cargo fmt --check`, `clippy -D warnings`, `cargo test` (with the real `redis-cli` installed), and a release soak: 2,000 simulation seeds, 30 `kill -9` rounds, 1,000 model-test cases, and 30 s per fuzz target. Nightly, the same at full length: 20,000 seeds, 300 rounds, 5,000 cases, 10 min per fuzz target. Short on push so a push gets an answer in minutes; long nightly because rare seeds (2793 was 1 in 20,000) only show up in volume. **Its second run caught a real flake** in `crash_at_every_compaction_step_loses_nothing`: the orphan check listed files, then counted live tables, and a background compaction could finish in between. It now waits for the background thread to go idle first (fixed in M17).
- **Fuzzing** (`fuzz/`, libFuzzer via `cargo fuzz`, nightly Rust): one target per parser of untrusted bytes. `wal_replay` (and re-logging what replay accepts must reproduce it), `manifest_open` (and opening is idempotent), `sstable_read` (raw bytes), `sstable_roundtrip` (structured: arbitrary entries, block sizes and filter sizes, written and read back through every point read and seek), `resp_parse` (and feeding a command one byte at a time gives the same answer as feeding it whole, which is what pipelining across reads relies on). Random bytes rarely get past a CRC, so `sstable_read` mostly tests the footer and index checks; `sstable_roundtrip` is the one that exercises block decoding and restart points. About 10M executions per target-minute for the raw parsers on this machine; no crashes.
- **Mutation runner** (`scripts/mutate.py`, `scripts/mutants.toml`): the catalog of planted bugs is now in the repo, so the "N of M caught" claims can be rerun. Each mutant replaces one exact snippet (which must appear exactly once, or the run stops: a stale catalog fails loudly instead of "surviving"), builds before starting the clock (a slow build or a held cargo lock is never scored as "caught"), runs the named tests, and restores the file. Earlier milestones' planted bugs were run from a scratch script that wasn't kept; the catalog re-creates the durability ones from D28 plus the recovery fixes, the main correctness ones, and all of D29 and D30. **27 of 27 caught**, each by the tests meant for it (the per-mutant logs in `target/mutants/` name the failing tests; one mutant, a conflict check comparing the wrong way, makes a transaction retry loop spin, and counts as caught by timeout). `--control` runs every mutant's tests on the unmodified source, since a "caught" only means something if those tests pass without the bug: it found `writes_and_reads_continue_while_a_flush_runs` failing under the load of a full run (a 400 ms held flush raced the writer filling the next memtable; now 2 s).
- **LazyFS** (`scripts/lazyfs.sh`, `lazyfs_power_cuts` in `tests/kill9.rs`): a FUSE filesystem that keeps written-but-unsynced data in its own cache. The `kill -9` harness runs on the mount, and after each kill tells LazyFS to drop that cache (`lazyfs::clear-cache`): a real power cut, on the real kernel, against the real multi-threaded binary, which is what D28's `SimFs` can't cover. Only `Always` rounds run (a power cut may take back unsynced `Periodic` writes by design). Planting "ack before the fsync" fails in the first round; plain `kill -9` can never see that bug. The harness opens LazyFS's completion FIFO once and reads a line per command: LazyFS holds the FIFO's write end for its whole life, so reading to end-of-file hung, and closing it between rounds would leave LazyFS writing into a pipe with no reader. Building LazyFS needed one workaround here: its build downloads `spdlog` as a GitHub archive, which this sandbox's network blocks, so the script fetches it with git instead.
- **The real `redis-cli`** (`the_real_redis_cli_works` in `tests/server.rs`, skipped if it isn't installed): `SET`/`GET`/`MGET`/`INCRBY`, a `WATCH`/`MULTI`/`EXEC` that commits, and one that aborts with a null reply after another client writes the watched key. `redis-benchmark` with 16-deep pipelining ran 94k `SET`/s and 524k `GET`/s on this machine. Commands outside lsmkv's subset (`APPEND`, `TYPE`, `HELLO`, ...) get `ERR unknown command`, as Redis does.

## Not done

These were scoped as Tier 3 (stretch) and not built:
- **Reverse iteration:** every source would need `prev`; blocks now have restart points (D30), which is what walking backwards inside a block needs.
- **Serializable transactions:** M13's transactions are snapshot isolation (D26); preventing write skew needs read-set validation.
- **Compression and prefix-compressed blocks:** blocks store full keys (binary-searchable since D30, but not compressed).
- **More than one background thread** for flushes and compactions.
- **Simulating threads and time,** not just the disk (D28 covers the disk only; LazyFS, D31, covers real threads on a real kernel, but not deterministically).
- **Replication (Raft).**
