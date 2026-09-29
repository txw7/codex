//! Fork-owned RAM-authoritative session runtime.
//!
//! Phase 01 intentionally starts with the simplest representation that proves
//! the authority boundary: the complete logical thread remains expanded in RAM.
//! Later phases replace that representation behind this crate boundary with
//! compressed resident frames and terminal-turn journal commits.

use std::sync::Arc;

use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::ThreadStore;

/// Build the Phase 01 RamJournal backend.
///
/// FORK-RAM: This is deliberately backed by upstream's in-memory store without
/// attaching a StateDbHandle. In RamJournal mode the selected thread-store
/// authority therefore lives in RAM from the first implementation slice.
///
/// This is *not* the final RamJournal representation. Keeping complete history
/// expanded is temporary scaffolding used to prove backend selection and hot
/// authority before compression, memfd arenas, and journal semantics arrive.
///
/// Replacing the boring proof with the clever implementation before the proof
/// works would be an excellent way to debug four architectures simultaneously.
pub fn build_bootstrap_thread_store(id: &str) -> Arc<dyn ThreadStore> {
    let shared = InMemoryThreadStore::for_id(id);

    // SQLITE-NOTE: Do not attach upstream StateDbHandle here.
    //
    // The Local backend remains available as the compatibility oracle. The RAM
    // backend should not become "in memory, except for the database we quietly
    // kept authoritative because it was nearby."
    Arc::new(shared.with_state_db(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_backend_is_the_explicit_in_memory_authority() {
        let store = build_bootstrap_thread_store("ram-authority-bootstrap-test");

        // FORK-INVARIANT: Phase 01 must resolve to a RAM-owned implementation,
        // not LocalThreadStore with a more aspirational config name.
        assert!(store.as_any().is::<InMemoryThreadStore>());
    }
}
