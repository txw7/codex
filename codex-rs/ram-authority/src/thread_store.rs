use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::ThreadId;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::DeleteThreadParams;
use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::MoveThreadToSectionParams;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadByRolloutPathParams;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::StoredModelContext;
use codex_thread_store::StoredThread;
use codex_thread_store::StoredThreadHistory;
use codex_thread_store::ThreadPage;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::UpdateThreadMetadataParams;
use tokio::sync::Mutex as AsyncMutex;

use crate::journal::JournalWriter;
use crate::journal::encode_turn_frame;
use crate::pending_turn::PendingTurn;

#[derive(Debug)]
struct ThreadJournalState {
    pending: PendingTurn,
    next_sequence: u64,
}

impl Default for ThreadJournalState {
    fn default() -> Self {
        Self {
            pending: PendingTurn::default(),
            next_sequence: 1,
        }
    }
}

/// Fork-owned interception point for RAM-authoritative thread storage.
///
/// FORK-RAM: Upstream's InMemoryThreadStore remains the Phase 02 semantic
/// resident store. RamJournalThreadStore now owns the durability edge around it:
/// ordinary appends mutate RAM only, while a terminal turn is encoded and
/// journaled before append_items() returns.
///
/// The complete history is still expanded in memory. Compression residency is
/// Phase 03. We are changing one axis at a time because storage bugs become
/// remarkably philosophical when representation and durability change together.
pub struct RamJournalThreadStore {
    resident: Arc<InMemoryThreadStore>,
    journal: JournalWriter,
    journal_states: Mutex<HashMap<ThreadId, Arc<AsyncMutex<ThreadJournalState>>>>,
}

impl RamJournalThreadStore {
    pub fn new(id: &str, journal_root: PathBuf) -> Self {
        Self {
            resident: InMemoryThreadStore::for_id(id),
            journal: JournalWriter::new(journal_root),
            journal_states: Mutex::new(HashMap::new()),
        }
    }

    fn journal_state(&self, thread_id: ThreadId) -> Arc<AsyncMutex<ThreadJournalState>> {
        let mut states = self
            .journal_states
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        states
            .entry(thread_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(ThreadJournalState::default())))
            .clone()
    }
}

fn internal_error(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: error.to_string(),
    }
}

impl ThreadStore for RamJournalThreadStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::create_thread(self.resident.as_ref(), params)
    }

    fn resume_thread(&self, params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::resume_thread(self.resident.as_ref(), params)
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        let resident = Arc::clone(&self.resident);
        let journal = self.journal.clone();
        let thread_id = params.thread_id.clone();
        let journal_state = self.journal_state(thread_id.clone());

        Box::pin(async move {
            // FORK-INVARIANT: serialize append/commit activity per thread.
            // Distinct threads remain independent, while one thread cannot race
            // its own turn sequence into two different versions of reality.
            let mut state = journal_state.lock().await;

            // RAM remains primary. The resident store observes the canonical
            // rollout items before the durability edge is considered.
            ThreadStore::append_items(resident.as_ref(), params.clone()).await?;

            state
                .pending
                .push(&params.items)
                .map_err(internal_error)?;

            let Some(sealed) = state.pending.sealed() else {
                // FORK-RAM: Nonterminal append. Deliberately no persistent I/O.
                return Ok(());
            };

            let sequence = state.next_sequence;
            let frame = encode_turn_frame(
                thread_id.clone(),
                &sealed.turn_id,
                sequence,
                &sealed.items,
            )
            .map_err(internal_error)?;

            let writer = journal.clone();
            let write_thread_id = thread_id.clone();
            tokio::task::spawn_blocking(move || {
                writer.append_turn_frame(write_thread_id, &frame)
            })
            .await
            .map_err(|error| internal_error(format!("journal writer task failed: {error}")))?
            .map_err(internal_error)?;

            // JOURNAL-NOTE: Resident pending state advances only after the
            // complete frame has been appended and sync_data() has succeeded.
            state
                .pending
                .mark_committed(&sealed.turn_id)
                .map_err(internal_error)?;
            state.next_sequence = state
                .next_sequence
                .checked_add(1)
                .ok_or_else(|| internal_error("journal sequence overflow"))?;

            Ok(())
        })
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        // FORK-RAM: Upstream persistence checkpoints remain RAM fences for this
        // backend. The terminal RolloutItem, not PersistContext, owns the
        // Phase 02 journal commit.
        ThreadStore::persist_thread(self.resident.as_ref(), thread_id, context)
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        // FORK-RAM: A pre-terminal flush does not manufacture disk traffic.
        ThreadStore::flush_thread(self.resident.as_ref(), thread_id)
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::shutdown_thread(self.resident.as_ref(), thread_id)
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::discard_thread(self.resident.as_ref(), thread_id)
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        ThreadStore::load_history(self.resident.as_ref(), params)
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        ThreadStore::load_latest_model_context(self.resident.as_ref(), params)
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        ThreadStore::read_thread(self.resident.as_ref(), params)
    }

    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        ThreadStore::read_thread_by_rollout_path(self.resident.as_ref(), params)
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        ThreadStore::list_threads(self.resident.as_ref(), params)
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        ThreadStore::update_thread_metadata(self.resident.as_ref(), params)
    }

    fn move_thread_to_section(
        &self,
        params: MoveThreadToSectionParams,
    ) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::move_thread_to_section(self.resident.as_ref(), params)
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::archive_thread(self.resident.as_ref(), params)
    }

    fn unarchive_thread(
        &self,
        params: ArchiveThreadParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        ThreadStore::unarchive_thread(self.resident.as_ref(), params)
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::delete_thread(self.resident.as_ref(), params)
    }
}
