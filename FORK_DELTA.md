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


### RAM-TS-005

Upstream symbols:
- `codex_thread_store::ThreadStore::requires_terminal_durability_before_delivery`
- `codex_thread_store::LiveThread::requires_terminal_durability_before_delivery`
- `Session::send_event_raw_with_persistence`

Fork behavior: RamJournal opts into terminal-delivery gating. If terminal persistence fails, Session retries the sealed durability fence and requires an idempotent acknowledgement before delivering `TurnComplete` or `TurnAborted`.

Reason: in RamJournal, a terminal event is also the durability receipt. Announcing completion after the journal rejected the turn violates the storage contract even if upstream Local storage prefers a more permissive failure policy.

Merge rule: if upstream changes event persistence/delivery ordering, re-audit this gate before resolving the merge. Do not replace it with a RamJournal concrete-type check; capability belongs to the storage boundary.

Tests/receipts:
- faulted sealed commit remains retryable in RAM;
- truncated tail is repaired before retry append;
- complete unacknowledged frame is synced rather than duplicated;
- Local/InMemory retain the default non-strict policy.

Snark note: "completed, except the part where persistence failed" is not a useful terminal state.


## Phase 03 compressed residency status

RamJournal now separates canonical committed history from decoded working state.

Implemented:

- committed turns remain as independently compressed CJR frames in `ResidentHistories`;
- successful live commits promote the exact Arc-backed frame handed to the writer into resident canonical history;
- cold hydration installs verified compressed frames without expanding the committed transcript into `InMemoryThreadStore`;
- open-turn persisted items remain in `PendingTurn`;
- complete-history APIs explicitly decode response values without changing canonical residency;
- latest-model-context reads use a byte-bounded LRU decoded cache;
- paginated model-context reconstruction scans compressed frames newest-to-oldest and stops when upstream `ModelContextScan` is complete;
- checkpoint baselines ride inside ordinary terminal frames instead of creating extra writes;
- checkpoint cadence is currently 32 turns or 16 MiB of uncheckpointed logical payload, whichever arrives first;
- idle shutdown refuses open turns, unresolved journal faults, and resident/durable head mismatch;
- durable idle threads release compressed frames + decoded hot cache and become explicitly unloaded;
- a later history/context read performs one cold hydration and returns to disk-independent loaded behavior;
- `thread/list` performs cold journal discovery once per process and then reuses a rebuildable resident catalog;
- catalog state remains a projection, never canonical history.

### RAM-RES-001

Fork implementation:
- `codex-rs/ram-authority/src/resident_history.rs`
- `codex-rs/ram-authority/src/decoded_context_cache.rs`
- `codex-rs/ram-authority/src/catalog.rs`

Invariant:

`LOADED => complete committed logical history available from compressed RAM frames`

Decoded context may be evicted independently. Compressed committed history may not disappear while the thread remains loaded.

### RAM-RES-002

Upstream seam: `ThreadStore::shutdown_thread`

Fork behavior: RamJournal treats shutdown of a durable idle thread as the whole-thread unload boundary.

Eviction preconditions include:

- no open pending turn;
- no unresolved terminal commit fault;
- resident committed head digest equals acknowledged durable head digest.

Merge rule: if upstream changes the semantics of thread unload/shutdown, review this boundary before merging. Do not quietly turn `shutdown_thread` into "close some handles, probably" while the fork relies on it as the T3 -> T4 transition.

### RAM-RES-003

Upstream seam: `ThreadStore::list_threads`

Fork behavior: the first cold listing may discover CJR journal placement; subsequent ordinary listing uses the process-resident catalog and retained lightweight metadata.

Reason: sidebar refresh is not a storage recovery protocol.

Receipt: after first discovery, the test replaces the journal `v1` directory with a regular file. A second list must still succeed from RAM; old per-call discovery would fail with `NotADirectory`.

Explicitly not claimed yet:

- process-wide compressed-resident memory budget / LRU admission across many threads;
- memfd arena backing;
- strict swap policy;
- RAM replacements for queue / graph / goal / session-log SQLite services;
- multi-process authority fencing and Factory/Bifrost routing.

Those are later phases. Phase 03 makes one loaded thread's history representation honest before we start governing the rest of the process.


## Phase 04 runtime-store status

RamJournal now removes three session-scoped SQLite side authorities through existing upstream abstractions.

Implemented:

- `RamAgentGraphStore` implements the complete current `AgentGraphStore` contract in RAM;
- graph traversal preserves upstream stable ordering and status-filter subtree semantics;
- `RamQueueStore` implements the complete current `QueueStore` contract in RAM;
- queue mutations preserve monotonic global change-version and per-thread revision semantics used by app-server watchers;
- queue capacity, paging, update/delete, and full-permutation reorder semantics remain storage-neutral;
- app-server queue and graph construction now goes through core-owned backend composition helpers;
- Local keeps SQLite-backed queue/graph stores;
- upstream InMemory keeps its existing no-store behavior;
- RamJournal selects the fork-owned RAM queue/graph stores;
- RamJournal message boards reuse upstream `InMemoryMessageBoards` unless an explicit remote board is configured.

### RAM-RUNTIME-001

Upstream seams:
- `QueueStore`
- `AgentGraphStore`
- `thread_manager` backend composition
- app-server `MessageProcessor::new`

Fork behavior: session-scoped queue and agent graph storage follows the same backend selection that owns thread persistence.

Merge rule: new session-scoped stores belong at the central composition boundary. Do not add another backend `match` in app-server because the nearest file had a convenient blank line.

### RAM-RUNTIME-002

Upstream seam: `install_agent_message_board`

Fork behavior: RamJournal selects upstream's existing in-memory message-board implementation unless the user explicitly configured a remote board.

Reason: a RAM-authoritative session does not need a SQLite-authoritative side conversation.

Explicitly still in progress:

- goal state remains coupled to `StateRuntime::thread_goals()` inside both request handling and turn-lifecycle accounting;
- session runtime logs still have SQLite-backed paths;
- those require explicit storage-neutral abstractions rather than another superficial config branch.

The goal coupling is intentionally called out instead of being hidden behind "Phase 04 mostly done". A side authority does not stop being authoritative because the roadmap is impatient.
