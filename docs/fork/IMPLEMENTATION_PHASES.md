# RAM Authority Implementation Phases

## 00 — Contract

Architecture, delta manifest, sync law, comment dialect, branch train. No runtime behavior.

## 01 — RAM store scaffold

Add RamJournal config and fork-owned crate. Complete logical thread remains expanded in RAM. Ordinary appends are RAM mutations. No journal yet.

Exit: RamJournal runs without LocalThreadStore rollout persistence.

## 02 — Terminal journal

PendingTurnV1, terminal recognition, CJR framing, one append per terminal turn, idempotency, prefix recovery.

If the one-turn-one-append proof fails, stop. Compression will not make a confused durability boundary smarter.

## 03 — Compressed residency

Independent zstd turn frames, resident arena, bounded decoded hot cache, checkpoints, whole-thread LRU unload.

## 04 — Runtime stores

RAM graph, queue, message board, goals, runtime log, one RuntimeStores composition point.

## 05 — Authority

ThreadAuthorityRefV1, generations, endpoint/store fingerprints, Factory receipts, explicit compat projections.

## 06 — Strict Linux

memfd, swap verifier, RAM temp roots, FD audit, syscall receipts.

## 07 — Lineage

Structural forks, logical revert heads, administrative commits, lineage-aware deletion.

## 08 — Migration

Legacy JSONL import, verification, CJR migration, compatibility export.

## 09 — Default

Fork default becomes RamJournal. Local remains the upstream oracle.

## 10 — Cleanup

Remove temporary fork scaffolding, not the oracle.
