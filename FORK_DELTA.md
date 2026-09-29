# Fork Delta Manifest

This file is the maintained boundary between upstream Codex and the RAM-authority fork.

## Baseline

Repository: `txw7/codex`

Upstream parent: `openai/codex`

The implementation starts from real upstream ancestry. This sounds obvious because it is. We retain the sentence because we already explored the alternative timeline.

## Stable invariants

- loaded thread => complete logical session resident in RAM;
- loaded thread => no journal reads during ordinary execution;
- open turn => no persistent session-journal data writes;
- terminal committed turn => exactly one positive application data append;
- decoded history => bounded by hot-cache policy;
- compressed history => complete for loaded thread;
- catalog/index => derived, never canonical;
- thread ID != endpoint identity;
- Factory route => explicit authority generation;
- compatibility resume => explicit compatible authority, never inferred fallback;
- path != identity;
- session-scoped SQLite projection != authority.

## Permanent upstream seams

Every permanent fork touchpoint gets an `UPSTREAM-SEAM` or `REBASE-NOTE` comment and an entry here.

### RAM-TS-001

Upstream symbol: `codex_config::config_toml::ThreadStoreToml`

Fork behavior: adds `ram_journal` backend selection.

Reason: select RAM-authoritative production storage through the same config seam upstream already owns.

Fork implementation: `codex-rs/ram-authority`

Merge rule: if upstream changes thread-store selection, move this adapter; do not spread backend selection through callers.

### RAM-TS-002

Upstream symbol: `codex_core::config::ThreadStoreConfig`

Fork behavior: carries resolved RamJournal configuration through core.

Reason: preserve upstream config layering while selecting fork-owned runtime behavior.

### RAM-TS-003

Upstream symbol: `codex_core::thread_manager::thread_store_from_config`

Fork behavior: constructs the fork-owned RamJournal backend.

Reason: keep backend composition at upstream's existing narrow boundary.

Test expectation: Local and InMemory remain unchanged.


## Phase 01 bootstrap status

The first executable RamJournal slice is intentionally conservative:

- config selects a distinct `RamJournal` backend;
- core constructs it only at `thread_store_from_config()`;
- the fork-owned crate currently delegates canonical thread storage to upstream `InMemoryThreadStore`;
- RamJournal explicitly does **not** attach `StateDbHandle` to that thread store;
- complete history is still fully expanded in RAM;
- no journal, compression, memfd arena, residency eviction, or Factory authority protocol is claimed yet.

This is a proof scaffold, not the final representation.

The next phase replaces expanded resident history with `PendingTurnV1` plus terminal CJR commits. Until that work lands, any comment claiming one-turn-one-append would be marketing, and this fork has enough maintenance obligations without maintaining fictional accomplishments.


## Phase 02 terminal journal status

RamJournal now owns an actual terminal-turn durability edge.

Implemented:

- ordinary upstream persistence checkpoints remain RAM-only fences;
- canonical persisted RolloutItems accumulate in `PendingTurn`;
- `TurnComplete` / `TurnAborted` seal one logical turn;
- CJR V1 frames preserve upstream RolloutItem semantics;
- each turn payload is independently zstd-compressed;
- BLAKE3 protects each compressed payload;
- frames carry a previous-frame digest for chain continuity;
- sequence 1 carries `CreateThreadParams` bootstrap metadata;
- the complete frame is assembled in RAM before I/O;
- successful commit performs one application data `write()`, then `sync_data()`;
- a short positive write faults instead of being completed by a write loop;
- sealed resident state is released only after write + sync acknowledgement;
- identical terminal retries are idempotent by turn id + terminal-event digest;
- conflicting retries fail as `ThreadStoreError::Conflict`;
- cold recovery validates frame format, thread id, sequence, digest chain, and terminal semantics;
- only an incomplete final frame is treated as a recoverable tail;
- a partial first-ever frame truncates back to zero durable bytes;
- cold hydration rebuilds the upstream in-memory authority once, after which ordinary reads remain RAM-only;
- Phase 02 `thread/list` can rediscover durable journals after resident RAM is gone.

Explicitly **not** claimed yet:

- cold historical frames are not yet kept compressed in resident RAM; recovery expands them;
- decoded-history memory is not yet bounded;
- `thread/list` currently gets correctness by eagerly hydrating cold journals;
- memfd / strict swap containment is not implemented;
- multi-process writer ownership/authority fencing is not implemented;
- standalone archive/rename/revert/metadata administration is not yet CJR-durable;
- Factory/Bifrost authority routing is not implemented;
- local Cargo checks and focused Rust tests still need to be executed outside this GitHub-only editing surface.

The final bullet is deliberately boring and therefore important. A test existing in source is not the same thing as a test having run. This fork is already opinionated enough without becoming metaphysical about CI receipts.
