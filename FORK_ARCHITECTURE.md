# Codex RAM Authority Fork

> Yes, this is a real maintained fork. No, "put ~/.codex on tmpfs" is not the architecture.

## Purpose

This fork follows `openai/codex` continuously while deliberately replacing local session authority and persistence semantics.

**Loaded session state is RAM-authoritative. Persistent storage is a recovery journal, not part of the hot execution loop.**

A loaded thread contains the complete logical session in RAM. Cold history remains independently compressed in RAM; only the bounded hot working set is decoded. An open turn performs no session-journal writes. A terminal turn is sealed, serialized, compressed, hashed, and committed by one application data append before terminal completion is exposed to clients.

## Authority model

A thread ID identifies a logical thread. It does not identify a process, socket, store, compatibility service, or resume endpoint.

Every live thread has explicit authority identity and generation. Factory/router delivery must preserve that authority. Native-core authority may become compatibility authority only through an explicit authority transition/projection.

UUID-shaped strings are identifiers. They are not routing tables. We have receipts proving why this sentence exists.

## Storage tiers

- T0: current pending turn and active tool state, decoded and pinned.
- T1: current model-visible context, decoded and pinned.
- T2: recent UI/history material, decoded and byte-bounded.
- T3: complete logical loaded history, independently compressed and RAM-resident.
- T4: unloaded history, journal only.

A thread may move from T3 to T4 only as an explicit whole-thread unload. Quietly dropping cold frames and rereading them from disk while keeping the thread marked loaded would save RAM by deleting the architecture.

## Compatibility

Upstream protocol semantics, tools, model behavior, app-server APIs, TUI behavior, compaction semantics, sandboxing, and `RolloutItem` meaning remain upstream-owned wherever possible.

Upstream `LocalThreadStore` remains available permanently as a compatibility and differential-test oracle.

SQLite remains useful. SQLite is simply no longer appointed governor of active agent memory.

## Source-layout rule

Fork-specific behavior belongs in fork-owned modules/crates. Upstream files receive narrow adapter seams only.

Every permanent divergence is tagged using the comment dialect in `docs/fork/COMMENT_DIALECT.md`.

If a future upstream merge makes it tempting to solve a conflict by scattering another twenty backend branches through core, do not. That is merge-conflict Stockholm syndrome.
