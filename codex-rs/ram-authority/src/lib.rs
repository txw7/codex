//! Fork-owned RAM-authoritative session runtime.
//!
//! Phase 01 intentionally starts with the simplest representation that proves
//! the authority boundary: the complete logical thread remains expanded in RAM.
//! Later phases replace that representation behind this crate boundary with
//! compressed resident frames and terminal-turn journal commits.

use std::path::PathBuf;
use std::sync::Arc;

mod agent_graph_store;
mod catalog;
mod decoded_context_cache;
mod journal;
mod pending_turn;
mod queue_store;
mod resident_history;
mod thread_store;

use codex_thread_store::ThreadStore;

pub use agent_graph_store::RamAgentGraphStore;
pub use queue_store::RamQueueStore;
pub use thread_store::RamJournalThreadStore;

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
pub fn build_bootstrap_thread_store(
    id: &str,
    codex_home: PathBuf,
) -> Arc<dyn ThreadStore> {
    // AUTHORITY-NOTE: CODEX_HOME determines journal placement, not thread
    // identity. Thread identity remains ThreadId; placement can move later
    // without asking pathnames to become ontology.
    let journal_root = codex_home.join("ram-journal");
    Arc::new(RamJournalThreadStore::new(id, journal_root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_backend_is_the_explicit_in_memory_authority() {
        let store = build_bootstrap_thread_store(
            "ram-authority-bootstrap-test",
            std::env::temp_dir(),
        );

        // FORK-INVARIANT: the production selection now resolves to the
        // fork-owned wrapper, not directly to an upstream backend.
        assert!(store.as_any().is::<RamJournalThreadStore>());
    }
}
