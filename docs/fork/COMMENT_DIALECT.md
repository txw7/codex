# Fork Maintainer Comment Dialect

The fork voice is permanently irritated but technically useful.

Badmouth architecture, accidental complexity, duplicate authority, unnecessary persistence, and path-based ontology. Do not badmouth individual upstream contributors.

Every joke must also tell a future maintainer something useful.

## Stable tags

- `FORK-RAM` — RamJournal intentionally differs from upstream.
- `FORK-INVARIANT` — invariant that must survive upstream merges.
- `UPSTREAM-SEAM` — narrow adapter around upstream-owned API.
- `REBASE-NOTE` — semantic watchpoint for future upstream merges.
- `AUTHORITY-NOTE` — ownership, generation, placement, Factory/router.
- `JOURNAL-NOTE` — framing, append, recovery, durability.
- `RESIDENCY-NOTE` — RAM tiers, budget, unload/eviction.
- `COMPAT-NOTE` — legacy/upstream compatibility only.
- `SQLITE-NOTE` — SQLite is a replaceable projection here.
- `IO-NOTE` — approved persistent I/O edge.
- `LEGACY-NOTE` — old representation retained for migration only.

These are grep targets and therefore part of the maintenance network.

## Example

```rust
// FORK-INVARIANT: LOADED means the complete logical thread is available from
// RAM. Ordinary reads may decompress resident frames; they may not reopen the
// journal.
//
// "Mostly loaded except for the bits we fetch from disk whenever convenient"
// is called a cache. We are building an authority.
```

## Density

Not every function gets a joke.

Every fork divergence, persistent-I/O site, authority boundary, residency transition, compatibility adapter, and upstream merge seam gets one useful tagged comment.

Constant voice, finite noise. We are trying to maintain a fork, not write a novelty cereal box.
