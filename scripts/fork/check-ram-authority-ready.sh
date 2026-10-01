#!/usr/bin/env bash
set -euo pipefail

# FORK-INVARIANT: Phase 01 begins only when the current upstream storage-neutral
# seams exist in this ancestry-preserving fork.
#
# We already visited the alternate timeline where the repository was a snapshot.
# It was educational. We do not need a sequel.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

required_paths=(
  "codex-rs/thread-store/src/store.rs"
  "codex-rs/thread-store/src/live_thread.rs"
  "codex-rs/thread-store/src/in_memory.rs"
  "codex-rs/config/src/config_toml.rs"
  "codex-rs/core/src/config/mod.rs"
  "codex-rs/core/src/thread_manager.rs"
)

for path in "${required_paths[@]}"; do
  if [[ ! -f "$path" ]]; then
    printf 'RAM-FORK-NOT-READY: missing upstream seam: %s\n' "$path" >&2
    exit 1
  fi
done

# UPSTREAM-SEAM: These symbols are the current maintained composition boundary.
# If upstream replaces them, review the new abstraction. Do not teach this guard
# increasingly creative grep because a merge conflict looked inconvenient.
grep -q 'enum ThreadStoreConfig' codex-rs/core/src/config/mod.rs
grep -q 'ThreadStoreConfig::Local' codex-rs/core/src/thread_manager.rs
grep -q 'ThreadStoreConfig::InMemory' codex-rs/core/src/thread_manager.rs

printf 'RAM-FORK-READY: current ThreadStore seams are present.\n'
