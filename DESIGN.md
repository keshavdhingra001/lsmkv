# lsmkv design

Living document. Every non-obvious decision gets a short entry: **what**, **alternatives**, **why**.

## Architecture

```
put/delete ──> WAL (append + fsync) ──> MemTable (BTreeMap of (key, seq)) ──flush──> SSTable L0 ──compact──> L1..Ln
get ─────────> MemTable ──miss──> SSTables newest-first (bloom filter -> index -> block)
```

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

### D4: Replay reads the whole log into memory
- **What:** `Wal::replay` does one `fs::read` and decodes from the buffer.
- **Why:** The log is bounded by the memtable size (it gets rotated once flushes exist in M4), so it's small. A single read is simpler than a streaming reader.
- **Guard:** Lengths read from disk are untrusted, so `checked_add` and a bounds check run before slicing, so a garbage `u32::MAX` length can't overflow or panic.

### D5: SSTable format (approved 2026-10-04)
- **Layout:** `[data blocks][filter block][index block][footer]`. Full byte layout is in `src/sstable/mod.rs` and `block.rs`.
- **Data blocks:** about 4 KiB target (one OS page, one disk read), entries `[kind][key_len][val_len][key][value]`, then a CRC32 trailer. Tombstones are stored, because they must shadow older tables.
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
  - **Survived (for now):** a compaction splitting one key's versions across two tables. With no snapshots yet, a key has only one live version at compaction time, so it can't happen. Section 4 (snapshots) must add a test that kills it.

