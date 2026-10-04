# lsmkv design

Living document. Every non-obvious decision gets a short entry: **what**, **alternatives**, **why**.

## Architecture

```
put/delete ──> WAL (append + fsync) ──> MemTable (BTreeMap) ──flush──> SSTable L0 ──compact──> L1..Ln
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
- **Revisit at M7:** with group commit, several unsynced records can be torn at once. The tail rule still holds, because they're all at the end.

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
- **Filter block:** reserved and empty, so M6 bloom filters drop in without a format change.
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

<!-- Add D9+ as milestones land: SSTable layout, flush threshold, compaction strategy, fsync policy... -->
