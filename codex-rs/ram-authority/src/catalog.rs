use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::ThreadId;

#[derive(Debug, Default)]
struct CatalogState {
    discovered: bool,
    thread_ids: HashSet<ThreadId>,
}

/// Process-resident discovery catalog for RamJournal threads.
///
/// RESIDENCY-NOTE: This catalog is a projection, not canonical history. Its job
/// is to stop ordinary thread/list calls from repeatedly asking the filesystem
/// which conversations exist after we already paid for discovery once.
///
/// Lose the catalog and we can rebuild it from journals. Lose the journals and
/// the catalog has approximately the archival value of a sticky note.
#[derive(Debug, Default)]
pub struct ResidentCatalog {
    state: Mutex<CatalogState>,
}

impl ResidentCatalog {
    pub fn needs_discovery(&self) -> bool {
        !self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .discovered
    }

    pub fn install_discovery(&self, discovered_ids: impl IntoIterator<Item = ThreadId>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.thread_ids.extend(discovered_ids);
        state.discovered = true;
    }

    pub fn note_thread(&self, thread_id: ThreadId) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .thread_ids
            .insert(thread_id);
    }

    pub fn remove_thread(&self, thread_id: ThreadId) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .thread_ids
            .remove(&thread_id);
    }

    pub fn thread_ids(&self) -> Vec<ThreadId> {
        let mut ids = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .thread_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        ids.sort_by_key(ToString::to_string);
        ids
    }

    #[cfg(test)]
    pub fn is_discovered(&self) -> bool {
        !self.needs_discovery()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_is_one_way_until_process_restart() {
        let catalog = ResidentCatalog::default();
        let first = ThreadId::new();
        let live = ThreadId::new();

        assert!(catalog.needs_discovery());

        catalog.note_thread(live);
        catalog.install_discovery([first]);

        assert!(catalog.is_discovered());
        assert!(!catalog.needs_discovery());

        let ids = catalog.thread_ids();
        assert!(ids.contains(&first));
        assert!(ids.contains(&live));
    }

    #[test]
    fn removing_runtime_metadata_does_not_reopen_discovery() {
        let catalog = ResidentCatalog::default();
        let id = ThreadId::new();

        catalog.install_discovery([id]);
        catalog.remove_thread(id);

        assert!(catalog.is_discovered());
        assert!(catalog.thread_ids().is_empty());
    }
}
