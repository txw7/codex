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


### RAM-TS-004

Upstream symbol: `codex_thread_store::ThreadStore`

Fork behavior: wraps the complete current trait surface in `RamJournalThreadStore` while Phase 01 delegates semantics to upstream `InMemoryThreadStore`.

Reason: core must see a fork-owned production backend identity before journal/residency behavior begins to diverge.

Fork implementation: `codex-rs/ram-authority/src/thread_store.rs`

Merge rule: when upstream adds or changes ThreadStore methods, update the wrapper deliberately. Do not allow a newly added upstream capability to vanish merely because the fork wrapper forgot it exists.

Test expectation: RamJournal resolves to `RamJournalThreadStore`; Phase 01 delegate remains in-memory and carries no StateDbHandle.

Snark note: owning the wrapper means future storage physics can change behind one seam instead of making `thread_manager.rs` participate in every new architectural hobby.


### RAM-TS-005

Upstream symbol: `ThreadStore::resume_thread` and snapshot revision fields on `ResumeThreadParams`, `StoredThreadHistory`, and `StoredModelContext`

Fork behavior: preserve upstream's authoritative-resume contract, but validate RamJournal snapshots with `ResidentRevisionV1 { durable_sequence, durable_head_digest }`.

Reason: cold-loaded state may become stale before live ownership is established. The live authority must publish a replay view proven current against the journal head.

Fork implementation: `codex-rs/ram-authority/src/revision.rs`

Merge rule: if upstream changes snapshot-validation semantics again, preserve the semantic requirement first. Do not inherit LocalThreadStore's filesystem-derived revision encoding unless the journal somehow develops an inode-based personality.

Test expectation: RamJournal revisions round-trip through the opaque upstream slot; foreign revision namespaces are rejected and force canonical reload.
