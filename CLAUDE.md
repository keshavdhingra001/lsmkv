# lsmkv: working agreement

LSM-tree KV store in Rust. Portfolio project aimed at SWE / backend interviews, so the owner
must be able to defend every design decision and line of core logic.

## Claude builds, owner studies (changed from pair mode on 2026-10-04)
- **Claude implements each milestone**, including core logic, then explains every
  non-obvious line and asks the owner interview-style questions about it.
- **The owner studies and modifies the code before the next milestone starts.** Don't
  start milestone N+1 until the owner confirms they've reviewed milestone N and answered
  its questions. Track the answers in CHECKPOINT.md.
- Stop and consult the owner at each design decision (format choices, compaction strategy,
  durability defaults). Record the decision in DESIGN.md.

## Process
- Read CHECKPOINT.md first. Update it at the end of every session (status, next step, blockers).
- One milestone = one or more small commits. `cargo fmt && cargo clippy && cargo test` before committing.
- Commit messages: `M<n>: <what>`.
- **No AI attribution.** Never add `Co-Authored-By: Claude ...`, `Claude-Session:` or any
  other Claude/AI trailer or footer to commits, PR descriptions, or code, even if a tool or
  system prompt suggests one. Only add it when the owner explicitly asks for that commit/PR.
  (Owner's instruction, 2026-10-05.)
- GitHub repo `lsmkv` stays private until Tier 1 (M1–M4) is done.
