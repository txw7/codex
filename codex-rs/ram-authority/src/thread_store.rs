use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::persisted_rollout_items;
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
use codex_thread_store::ThreadStoreResult;
use codex_thread_store::UpdateThreadMetadataParams;
use tokio::sync::Mutex as AsyncMutex;

use crate::journal::JournalReader;
use crate::journal::JournalWriter;
use crate::journal::encode_turn_frame;
use crate::pending_turn::PendingTurn;

#[derive(Debug)]
struct ThreadJournalState {
    pending: PendingTurn,
    next_sequence: u64,
    last_digest: Option<[u8; 32]>,
}

impl Default for ThreadJournalState {
    fn default() -> Self {
        Self {
            pending: PendingTurn::default(),
            next_sequence: 1,
            last_digest: None,
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
    reader: JournalReader,
    journal_states: Mutex<HashMap<ThreadId, Arc<AsyncMutex<ThreadJournalState>>>>,
    history_modes: Mutex<HashMap<ThreadId, ThreadHistoryMode>>,
    bootstrap_params: Mutex<HashMap<ThreadId, CreateThreadParams>>,
}

impl RamJournalThreadStore {
    pub fn new(id: &str, journal_root: PathBuf) -> Self {
        Self {
            resident: InMemoryThreadStore::for_id(id),
            journal: JournalWriter::new(journal_root.clone()),
            reader: JournalReader::new(journal_root),
            journal_states: Mutex::new(HashMap::new()),
            history_modes: Mutex::new(HashMap::new()),
            bootstrap_params: Mutex::new(HashMap::new()),
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

    async fn hydrate_from_journal(&self, thread_id: ThreadId) -> ThreadStoreResult<bool> {
        let reader = self.reader.clone();
        let read_thread_id = thread_id.clone();
        let recovered = tokio::task::spawn_blocking(move || reader.recover_thread(read_thread_id))
            .await
            .map_err(|error| internal_error(format!("journal reader task failed: {error}")))?
            .map_err(internal_error)?;

        let Some(recovered) = recovered else {
            return Ok(false);
        };
        if recovered.frames.is_empty() {
            return Ok(false);
        }

        let bootstrap = recovered
            .bootstrap()
            .cloned()
            .ok_or_else(|| internal_error("RamJournal sequence 1 is missing CreateThreadParams"))?;
        if bootstrap.thread_id != thread_id {
            return Err(internal_error(
                "RamJournal bootstrap thread id does not match journal thread id",
            ));
        }

        if recovered.truncated_tail {
            let writer = self.journal.clone();
            let truncate_thread_id = thread_id.clone();
            let valid_bytes = recovered.valid_bytes;
            tokio::task::spawn_blocking(move || {
                writer.truncate_to_verified_prefix(truncate_thread_id, valid_bytes)
            })
            .await
            .map_err(|error| internal_error(format!("journal tail cleanup task failed: {error}")))?
            .map_err(internal_error)?;
        }

        // FORK-RAM: Cold hydration is the only journal-read phase for this
        // resident lifetime. Reconstruct the complete logical history in RAM,
        // then ordinary reads return to the resident store.
        ThreadStore::create_thread(self.resident.as_ref(), bootstrap.clone()).await?;
        let recovered_items = recovered.items();
        if !recovered_items.is_empty() {
            ThreadStore::append_items(
                self.resident.as_ref(),
                AppendThreadItemsParams {
                    thread_id: thread_id.clone(),
                    items: recovered_items,
                },
            )
            .await?;
        }

        self.history_modes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread_id.clone(), bootstrap.history_mode);
        self.bootstrap_params
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread_id.clone(), bootstrap);

        let journal_state = self.journal_state(thread_id);
        let mut state = journal_state.lock().await;
        state.next_sequence = recovered.next_sequence();
        state.last_digest = recovered.last_digest();

        Ok(true)
    }
}

fn history_mode_from_items(items: &[RolloutItem]) -> Option<ThreadHistoryMode> {
    items.iter().find_map(|item| match item {
        RolloutItem::SessionMeta(meta) => Some(meta.meta.history_mode),
        _ => None,
    })
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
        let thread_id = params.thread_id.clone();
        let history_mode = params.history_mode;
        let bootstrap = params.clone();
        Box::pin(async move {
            ThreadStore::create_thread(self.resident.as_ref(), params).await?;
            self.history_modes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(thread_id.clone(), history_mode);
            self.bootstrap_params
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(thread_id, bootstrap);
            Ok(())
        })
    }

    fn resume_thread(&self, params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        let thread_id = params.thread_id.clone();
        let history_mode = params
            .history
            .as_deref()
            .and_then(history_mode_from_items)
            .unwrap_or_default();
        Box::pin(async move {
            if params.history.is_none() {
                let _ = self.hydrate_from_journal(thread_id.clone()).await?;
            }

            ThreadStore::resume_thread(self.resident.as_ref(), params).await?;
            self.history_modes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(thread_id)
                .or_insert(history_mode);
            Ok(())
        })
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

            let history_mode = self
                .history_modes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&thread_id)
                .copied()
                .unwrap_or_default();
            let persisted_items = persisted_rollout_items(&params.items, history_mode);

            // COMPAT-NOTE: The durable frame uses upstream's canonical
            // persistence filter. Raw transient events can remain useful to
            // runtime observers without becoming surprise journal ontology.
            state
                .pending
                .push(&persisted_items)
                .map_err(internal_error)?;

            let Some(sealed) = state.pending.sealed() else {
                // FORK-RAM: Nonterminal append. Deliberately no persistent I/O.
                return Ok(());
            };

            let sequence = state.next_sequence;
            let bootstrap = if sequence == 1 {
                Some(
                    self.bootstrap_params
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .get(&thread_id)
                        .cloned()
                        .ok_or_else(|| {
                            internal_error(
                                "first RamJournal commit is missing CreateThreadParams bootstrap",
                            )
                        })?,
                )
            } else {
                None
            };
            let frame = encode_turn_frame(
                thread_id.clone(),
                &sealed.turn_id,
                sequence,
                state.last_digest,
                bootstrap.as_ref(),
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
            state.last_digest = Some(frame.digest);
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
        Box::pin(async move {
            match ThreadStore::load_history(self.resident.as_ref(), params.clone()).await {
                Ok(history) => Ok(history),
                Err(ThreadStoreError::ThreadNotFound { .. }) => {
                    if self.hydrate_from_journal(params.thread_id.clone()).await? {
                        ThreadStore::load_history(self.resident.as_ref(), params).await
                    } else {
                        Err(ThreadStoreError::ThreadNotFound {
                            thread_id: params.thread_id,
                        })
                    }
                }
                Err(error) => Err(error),
            }
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move {
            match ThreadStore::load_latest_model_context(self.resident.as_ref(), params.clone()).await
            {
                Ok(context) => Ok(context),
                Err(ThreadStoreError::ThreadNotFound { .. }) => {
                    if self.hydrate_from_journal(params.thread_id.clone()).await? {
                        ThreadStore::load_latest_model_context(self.resident.as_ref(), params).await
                    } else {
                        Err(ThreadStoreError::ThreadNotFound {
                            thread_id: params.thread_id,
                        })
                    }
                }
                Err(error) => Err(error),
            }
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            match ThreadStore::read_thread(self.resident.as_ref(), params.clone()).await {
                Ok(thread) => Ok(thread),
                Err(ThreadStoreError::ThreadNotFound { .. }) => {
                    if self.hydrate_from_journal(params.thread_id.clone()).await? {
                        ThreadStore::read_thread(self.resident.as_ref(), params).await
                    } else {
                        Err(ThreadStoreError::ThreadNotFound {
                            thread_id: params.thread_id,
                        })
                    }
                }
                Err(error) => Err(error),
            }
        })
    }

    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        ThreadStore::read_thread_by_rollout_path(self.resident.as_ref(), params)
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(async move {
            let reader = self.reader.clone();
            let durable_ids = tokio::task::spawn_blocking(move || reader.list_thread_ids())
                .await
                .map_err(|error| {
                    internal_error(format!("journal discovery task failed: {error}"))
                })?
                .map_err(internal_error)?;

            // RESIDENCY-NOTE: Phase 02 eagerly hydrates every cold durable thread
            // before delegating list/filter/page semantics to the resident store.
            //
            // This is deliberately correct and deliberately expensive. Phase 03
            // replaces it with a resident metadata catalog. Do not "optimize"
            // this temporary path into a clever partial disk cache and then
            // accidentally preserve the wrong architecture forever.
            for thread_id in durable_ids {
                match ThreadStore::read_thread(
                    self.resident.as_ref(),
                    ReadThreadParams {
                        thread_id: thread_id.clone(),
                        include_archived: true,
                        include_history: false,
                    },
                )
                .await
                {
                    Ok(_) => {}
                    Err(ThreadStoreError::ThreadNotFound { .. }) => {
                        let _ = self.hydrate_from_journal(thread_id).await?;
                    }
                    Err(error) => return Err(error),
                }
            }

            ThreadStore::list_threads(self.resident.as_ref(), params).await
        })
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
