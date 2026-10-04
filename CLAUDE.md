# lsmkv: working agreement

LSM-tree KV store in Rust. Portfolio project aimed at SWE / backend interviews, so the owner
must be able to defend every design decision and line of core logic.

## Pair mode (agreed 2026-10-04)
- **The owner implements the core logic:** memtable, WAL encode/decode, SSTable format,
  flush, compaction, bloom filter, iterators. These are marked `TODO(you, Mx)` / `todo!()`.
- **Claude** scaffolds, writes tests and harnesses, reviews, explains, and writes glue,
  CLI and benchmarks. Don't fill in a `todo!()` in core logic unless the owner explicitly
  asks for that specific piece. Give hints, failing tests, or review comments instead.
- Stop and consult the owner at each design decision (format choices, compaction strategy,
  durability defaults). Record the decision in DESIGN.md.

## Process
- Read CHECKPOINT.md first. Update it at the end of every session (status, next step, blockers).
- One milestone = one or more small commits. `cargo fmt && cargo clippy && cargo test` before committing.
- Commit messages: `M<n>: <what>`.
- GitHub repo `lsmkv` stays private until Tier 1 (M1–M4) is done.
