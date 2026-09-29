use std::any::Any;
use std::sync::Arc;

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
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::UpdateThreadMetadataParams;

/// Fork-owned interception point for RAM-authoritative thread storage.
///
/// FORK-RAM: Phase 02 starts as a transparent wrapper around upstream's
/// InMemoryThreadStore. The resident store still owns all thread semantics;
/// later commits intercept append/persist/flush here to add terminal journal
/// behavior without teaching upstream callers about the fork.
///
/// The wrapper exists before the journal on purpose. Mixing structural
/// ownership and durability semantics in one commit would save a SHA and spend
/// considerably more maintainer attention later.
pub struct RamJournalThreadStore {
    resident: Arc<InMemoryThreadStore>,
}

impl RamJournalThreadStore {
    pub fn new(id: &str) -> Self {
        Self {
            resident: InMemoryThreadStore::for_id(id),
        }
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
        ThreadStore::append_items(self.resident.as_ref(), params)
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::persist_thread(self.resident.as_ref(), thread_id, context)
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
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
