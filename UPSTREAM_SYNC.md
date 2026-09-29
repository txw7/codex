# Upstream Sync Procedure

This is a maintained fork of `openai/codex`.

## Rule zero

Preserve upstream commits.

Do not vendor a new source snapshot over this repository. Do not squash imported upstream history into a ceremonial "sync" commit.

Git already solved related-history merging. We should take advantage of this breakthrough.

## Sync branch

Use:

`sync/upstream-YYYY-MM-DD`

A sync PR contains upstream integration plus only the adapter repairs required to restore fork invariants.

## Required review watchlist

Always inspect upstream changes touching:

- `codex-rs/thread-store/src/store.rs`
- `codex-rs/thread-store/src/live_thread.rs`
- `codex-rs/thread-store/src/in_memory.rs`
- `codex-rs/core/src/thread_manager.rs`
- `codex-rs/core/src/session/mod.rs`
- `codex-rs/core/src/session/turn.rs`
- `codex-rs/core/src/tasks/mod.rs`
- `codex-rs/config/src/config_toml.rs`
- `codex-rs/core/src/config/mod.rs`
- app-server runtime-store composition
- agent residency / queue / graph / goal-store changes

A conflict in these files is a semantic review point, not a contest to see which side has fewer red lines.

## Sync PR opening

> Upstream has once again exercised its legal right to change the code we deliberately forked. This PR imports those changes while preserving the RAM-authority invariants below. Textual conflicts are resolved mechanically where possible; persistence, terminal-event ordering, residency, routing, and ThreadStore changes receive actual human attention because Git remains unable to reason about ontology.
