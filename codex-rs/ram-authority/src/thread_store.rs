use std::any::Any;
use std::sync::Arc;

use codex_thread_store::*;

/// Phase-01 fork-owned thread-store identity.
///
/// FORK-RAM: This wrapper deliberately delegates the complete current
/// `ThreadStore` surface to upstream's `InMemoryThreadStore`.
///
/// The point of Phase 01 is to establish *authority ownership* without also
/// rewriting every storage behavior at once. Later phases replace individual
/// operations behind this type with resident compression and the terminal-turn
/// journal.
///
/// In other words: first own the type, then change the physics. Debugging both
/// simultaneously is how perfectly innocent storage refactors become folklore.
pub struct RamJournalThreadStore {
    inner: Arc<InMemoryThreadStore>,
}

impl RamJournalThreadStore {
    pub fn for_id(id: impl Into<String>) -> Self {
        let inner = InMemoryThreadStore::for_id(id);

        // SQLITE-NOTE: RamJournal intentionally does not attach StateDbHandle.
        //
        // The Local backend remains the compatibility oracle. This backend is
        // not "RAM-authoritative except for the SQLite authority we forgot to
        // stop carrying around."
        Self {
            inner: Arc::new(inner.with_state_db(None)),
        }
    }

    pub fn bootstrap_inner(&self) -> &InMemoryThreadStore {
        self.inner.as_ref()
    }
}

impl ThreadStore for RamJournalThreadStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn default_history_mode(&self) -> codex_protocol::protocol::ThreadHistoryMode {
        self.inner.default_history_mode()
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.create_thread(params)
    }

    fn stage_pending_thread_metadata(
        &self,
        thread_id: codex_protocol::ThreadId,
        patch: ThreadMetadataPatch,
    ) -> ThreadStoreFuture<'_, ()> {
        self.inner.stage_pending_thread_metadata(thread_id, patch)
    }

    fn read_pending_thread_metadata(
        &self,
        thread_id: codex_protocol::ThreadId,
    ) -> ThreadStoreFuture<'_, Option<ThreadMetadataPatch>> {
        self.inner.read_pending_thread_metadata(thread_id)
    }

    fn remove_pending_thread_metadata(
        &self,
        thread_id: codex_protocol::ThreadId,
    ) -> ThreadStoreFuture<'_, ()> {
        self.inner.remove_pending_thread_metadata(thread_id)
    }

    fn resume_thread(&self, params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.resume_thread(params)
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        // FORK-RAM: Phase 01 append is a resident mutation only.
        //
        // There is deliberately no durable side effect here. Terminal-turn
        // framing arrives in Phase 02; until then disk does not get a courtesy
        // notification every time the model has a thought.
        self.inner.append_items(params)
    }

    fn record_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, ()> {
        self.inner.record_thread_metadata(params)
    }

    fn persist_thread(
        &self,
        thread_id: codex_protocol::ThreadId,
        context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        // FORK-RAM: In the bootstrap backend, upstream persistence checkpoints
        // are RAM fences because the delegate has no durable store attached.
        //
        // REBASE-NOTE: inspect new PersistContext variants before forwarding
        // them here. A new upstream checkpoint does not automatically deserve a
        // filesystem ceremony.
        self.inner.persist_thread(thread_id, context)
    }

    fn flush_thread(&self, thread_id: codex_protocol::ThreadId) -> ThreadStoreFuture<'_, ()> {
        self.inner.flush_thread(thread_id)
    }

    fn shutdown_thread(&self, thread_id: codex_protocol::ThreadId) -> ThreadStoreFuture<'_, ()> {
        self.inner.shutdown_thread(thread_id)
    }

    fn discard_thread(&self, thread_id: codex_protocol::ThreadId) -> ThreadStoreFuture<'_, ()> {
        self.inner.discard_thread(thread_id)
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        self.inner.load_history(params)
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        self.inner.load_latest_model_context(params)
    }

    fn prepare_fork(&self, params: PrepareForkParams) -> ThreadStoreFuture<'_, PreparedFork> {
        self.inner.prepare_fork(params)
    }

    fn revert_thread(&self, params: RevertThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.revert_thread(params)
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.read_thread(params)
    }

    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.read_thread_by_rollout_path(params)
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        self.inner.list_threads(params)
    }

    fn supports_thread_sections(&self) -> bool {
        self.inner.supports_thread_sections()
    }

    fn list_thread_sections(
        &self,
        params: ListThreadSectionsParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSectionsPage> {
        self.inner.list_thread_sections(params)
    }

    fn create_thread_section(
        &self,
        params: CreateThreadSectionParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSection> {
        self.inner.create_thread_section(params)
    }

    fn rename_thread_section(
        &self,
        params: RenameThreadSectionParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThreadSection>> {
        self.inner.rename_thread_section(params)
    }

    fn delete_thread_section(
        &self,
        params: DeleteThreadSectionParams,
    ) -> ThreadStoreFuture<'_, bool> {
        self.inner.delete_thread_section(params)
    }

    fn supports_thread_attachments(&self) -> bool {
        self.inner.supports_thread_attachments()
    }

    fn copy_thread_attachments(
        &self,
        source_thread_id: codex_protocol::ThreadId,
        destination_thread_id: codex_protocol::ThreadId,
    ) -> ThreadStoreFuture<'_, ()> {
        self.inner
            .copy_thread_attachments(source_thread_id, destination_thread_id)
    }

    fn add_thread_attachment(
        &self,
        params: AddThreadAttachmentParams,
    ) -> ThreadStoreFuture<'_, AddThreadAttachmentOutcome> {
        self.inner.add_thread_attachment(params)
    }

    fn list_thread_attachments(
        &self,
        params: ListThreadAttachmentsParams,
    ) -> ThreadStoreFuture<'_, ThreadAttachmentPage> {
        self.inner.list_thread_attachments(params)
    }

    fn remove_thread_attachment(
        &self,
        params: RemoveThreadAttachmentParams,
    ) -> ThreadStoreFuture<'_, RemoveThreadAttachmentOutcome> {
        self.inner.remove_thread_attachment(params)
    }

    fn supports_projects(&self) -> bool {
        self.inner.supports_projects()
    }

    fn list_projects(
        &self,
        params: ListProjectsParams,
    ) -> ThreadStoreFuture<'_, StoredProjectsPage> {
        self.inner.list_projects(params)
    }

    fn read_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<StoredProject>> {
        self.inner.read_project(project_id)
    }

    fn create_project(
        &self,
        params: CreateProjectParams,
    ) -> ThreadStoreFuture<'_, CreatedProject> {
        self.inner.create_project(params)
    }

    fn update_project(
        &self,
        params: UpdateProjectParams,
    ) -> ThreadStoreFuture<'_, Option<UpdatedProject>> {
        self.inner.update_project(params)
    }

    fn move_project(
        &self,
        params: MoveProjectParams,
    ) -> ThreadStoreFuture<'_, Option<ProjectMoveOutcome>> {
        self.inner.move_project(params)
    }

    fn delete_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<DeletedProject>> {
        self.inner.delete_project(project_id)
    }

    fn supports_paginated_history_lists(&self) -> bool {
        self.inner.supports_paginated_history_lists()
    }

    fn search_threads(
        &self,
        params: SearchThreadsParams,
    ) -> ThreadStoreFuture<'_, ThreadSearchPage> {
        self.inner.search_threads(params)
    }

    fn search_thread_occurrences(
        &self,
        params: SearchThreadOccurrencesParams,
    ) -> ThreadStoreFuture<'_, ThreadOccurrenceSearchPage> {
        self.inner.search_thread_occurrences(params)
    }

    fn list_turns(&self, params: ListTurnsParams) -> ThreadStoreFuture<'_, TurnPage> {
        self.inner.list_turns(params)
    }

    fn list_items(&self, params: ListItemsParams) -> ThreadStoreFuture<'_, ItemPage> {
        self.inner.list_items(params)
    }

    fn list_timeline(
        &self,
        params: ListTimelineParams,
    ) -> ThreadStoreFuture<'_, TimelinePage> {
        self.inner.list_timeline(params)
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        self.inner.update_thread_metadata(params)
    }

    fn move_thread_to_section(
        &self,
        params: MoveThreadToSectionParams,
    ) -> ThreadStoreFuture<'_, ()> {
        self.inner.move_thread_to_section(params)
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.archive_thread(params)
    }

    fn archive_threads(
        &self,
        params: ArchiveThreadsParams,
    ) -> ThreadStoreFuture<'_, Vec<codex_protocol::ThreadId>> {
        self.inner.archive_threads(params)
    }

    fn unarchive_thread(
        &self,
        params: ArchiveThreadParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.unarchive_thread(params)
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.delete_thread(params)
    }

    fn delete_threads(&self, params: DeleteThreadsParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.delete_threads(params)
    }
}
