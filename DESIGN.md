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

<!-- Add D8+ as milestones land: SSTable layout, flush threshold, compaction strategy, fsync policy... -->
