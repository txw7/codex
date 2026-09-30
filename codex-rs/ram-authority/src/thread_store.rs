use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
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
use crate::journal::decode_turn_frame;
use crate::journal::encode_turn_frame;
use crate::pending_turn::PendingTurn;
use crate::pending_turn::terminal_event_identity;
use crate::resident_history::ResidentHistories;

#[derive(Debug)]
struct ThreadJournalState {
    pending: PendingTurn,
    next_sequence: u64,
    last_digest: Option<[u8; 32]>,
    committed_terminals: HashMap<String, [u8; 32]>,
    commit_faulted: bool,
}

impl Default for ThreadJournalState {
    fn default() -> Self {
        Self {
            pending: PendingTurn::default(),
            next_sequence: 1,
            last_digest: None,
            committed_terminals: HashMap::new(),
            commit_faulted: false,
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
/// Phase 03 introduces ResidentHistories beside the existing expanded delegate
/// first. The following commits move hydration, writes, and reads across that
/// boundary separately so each authority change has a receipt instead of one
/// impressive 500-line shrug.
pub struct RamJournalThreadStore {
    resident: Arc<InMemoryThreadStore>,
    journal: JournalWriter,
    reader: JournalReader,
    journal_states: Mutex<HashMap<ThreadId, Arc<AsyncMutex<ThreadJournalState>>>>,
    history_modes: Mutex<HashMap<ThreadId, ThreadHistoryMode>>,
    bootstrap_params: Mutex<HashMap<ThreadId, CreateThreadParams>>,
    resident_histories: ResidentHistories,
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
            resident_histories: ResidentHistories::default(),
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

    async fn session_meta_line(&self, thread_id: ThreadId) -> ThreadStoreResult<SessionMetaLine> {
        let context = ThreadStore::load_latest_model_context(
            self.resident.as_ref(),
            LoadThreadHistoryParams {
                thread_id,
                include_archived: true,
            },
        )
        .await?;

        context
            .items
            .into_iter()
            .find_map(|item| match item {
                RolloutItem::SessionMeta(meta) => Some(meta),
                _ => None,
            })
            .ok_or_else(|| internal_error("resident metadata store is missing SessionMeta"))
    }

    fn decode_resident_frames(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Vec<RolloutItem>> {
        let mut items = Vec::new();
        for frame in self.resident_histories.frames(thread_id) {
            let (decoded, consumed) =
                decode_turn_frame(frame.bytes.as_ref()).map_err(internal_error)?;
            if consumed != frame.bytes.len() {
                return Err(internal_error(
                    "resident CJR frame decoder did not consume the complete frame",
                ));
            }
            items.extend(decoded.items);
        }
        Ok(items)
    }

    async fn pending_items(&self, thread_id: ThreadId) -> Vec<RolloutItem> {
        let state = self.journal_state(thread_id);
        state.lock().await.pending.items()
    }

    async fn materialize_complete_history(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        let session_meta = self.session_meta_line(thread_id).await?;
        let mut items = vec![RolloutItem::SessionMeta(session_meta)];
        items.extend(self.decode_resident_frames(thread_id)?);
        items.extend(self.pending_items(thread_id).await);

        Ok(StoredThreadHistory { thread_id, items })
    }

    async fn materialize_latest_model_context(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<StoredModelContext> {
        let history_mode = self
            .history_modes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&thread_id)
            .copied()
            .unwrap_or_default();

        if history_mode == ThreadHistoryMode::Legacy {
            let history = self.materialize_complete_history(thread_id).await?;
            return Ok(StoredModelContext {
                thread_id,
                items: history.items,
            });
        }

        let session_meta = self.session_meta_line(thread_id).await?;
        let mut scan = ModelContextScan::default();

        for item in self.pending_items(thread_id).await.into_iter().rev() {
            if matches!(scan.push(item), ModelContextScanProgress::Complete) {
                return Ok(StoredModelContext {
                    thread_id,
                    items: scan.finish(session_meta),
                });
            }
        }

        'frames: for frame in self.resident_histories.frames(thread_id).into_iter().rev() {
            let (decoded, consumed) =
                decode_turn_frame(frame.bytes.as_ref()).map_err(internal_error)?;
            if consumed != frame.bytes.len() {
                return Err(internal_error(
                    "resident CJR frame decoder did not consume the complete frame",
                ));
            }
            for item in decoded.items.into_iter().rev() {
                if matches!(scan.push(item), ModelContextScanProgress::Complete) {
                    break 'frames;
                }
            }
        }

        Ok(StoredModelContext {
            thread_id,
            items: scan.finish(session_meta),
        })
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

        // A failed first-ever append has a verified prefix of zero bytes. Clean
        // the partial tail above, then correctly report that no durable thread
        // exists yet.
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

        // FORK-RAM: Cold hydration is the only journal-read phase for this
        // resident lifetime. Reconstruct the complete logical history in RAM,
        // then ordinary reads return to the resident store.
        ThreadStore::create_thread(self.resident.as_ref(), bootstrap.clone()).await?;
        let mut committed_terminals = HashMap::new();
        for frame in &recovered.frames {
            if let Some(previous) = committed_terminals.insert(
                frame.encoded.turn_id.clone(),
                frame.terminal_digest,
            ) {
                return Err(internal_error(format!(
                    "journal contains duplicate durable turn id {} with digests {:x?} and {:x?}",
                    frame.encoded.turn_id, previous, frame.terminal_digest
                )));
            }
        }

        // RESIDENCY-NOTE: Install the verified compressed frame set as one
        // resident metadata operation. This commit intentionally keeps the old
        // decoded delegate copy too; the next authority cuts remove that
        // redundancy after read/write behavior has moved over.
        self.resident_histories.replace(
            thread_id,
            recovered
                .frames
                .iter()
                .map(|frame| frame.encoded.clone()),
        );

        let recovered_items = recovered.items().map_err(internal_error)?;
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
        state.committed_terminals = committed_terminals;

        Ok(true)
    }

    async fn commit_sealed_turn(
        &self,
        thread_id: ThreadId,
        state: &mut ThreadJournalState,
    ) -> ThreadStoreResult<()> {
        let Some(sealed) = state.pending.sealed() else {
            return Ok(());
        };

        let terminal = terminal_event_identity(&sealed.items)
            .map_err(internal_error)?
            .ok_or_else(|| internal_error("sealed RamJournal turn is missing a terminal event"))?;
        if terminal.turn_id != sealed.turn_id {
            return Err(internal_error(format!(
                "sealed RamJournal turn id {} does not match terminal event {}",
                sealed.turn_id, terminal.turn_id
            )));
        }

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
            thread_id,
            &sealed.turn_id,
            sequence,
            state.last_digest,
            bootstrap.as_ref(),
            &sealed.items,
        )
        .map_err(internal_error)?;

        if state.commit_faulted {
            // JOURNAL-NOTE: A failed attempt can leave one of three things:
            // no new bytes, a truncated tail, or the complete frame followed by
            // a failed durability fence. Inspect exactly once on the retry path;
            // ordinary loaded commits still perform zero journal reads.
            let reader = self.reader.clone();
            let recovered = tokio::task::spawn_blocking(move || reader.recover_thread(thread_id))
                .await
                .map_err(|error| {
                    internal_error(format!("journal retry recovery task failed: {error}"))
                })?
                .map_err(internal_error)?;

            match recovered {
                None => {
                    if state.last_digest.is_some() || sequence != 1 {
                        return Err(internal_error(
                            "faulted RamJournal retry lost an existing durable prefix",
                        ));
                    }
                }
                Some(recovered) => {
                    let prefix_matches = recovered.last_digest() == state.last_digest
                        && recovered.next_sequence() == sequence;

                    if recovered.truncated_tail {
                        if !prefix_matches {
                            return Err(internal_error(
                                "faulted RamJournal tail does not follow the resident durable head",
                            ));
                        }

                        let writer = self.journal.clone();
                        let valid_bytes = recovered.valid_bytes;
                        tokio::task::spawn_blocking(move || {
                            writer.truncate_to_verified_prefix(thread_id, valid_bytes)
                        })
                        .await
                        .map_err(|error| {
                            internal_error(format!(
                                "journal retry tail cleanup task failed: {error}"
                            ))
                        })?
                        .map_err(internal_error)?;
                    } else if recovered.last_digest() == Some(frame.digest)
                        && recovered
                            .frames
                            .last()
                            .is_some_and(|recovered_frame| {
                                recovered_frame.encoded.sequence == sequence
                                    && recovered_frame.encoded.turn_id == sealed.turn_id
                            })
                    {
                        // The data write completed and only the durability fence
                        // failed. Do not append the same turn twice; sync the
                        // already verified bytes and acknowledge that frame.
                        let recovered_frame = recovered
                            .frames
                            .last()
                            .expect("matching recovered tail was checked above")
                            .encoded
                            .clone();
                        let writer = self.journal.clone();
                        tokio::task::spawn_blocking(move || writer.sync_thread(thread_id))
                            .await
                            .map_err(|error| {
                                internal_error(format!(
                                    "journal retry sync task failed: {error}"
                                ))
                            })?
                            .map_err(internal_error)?;

                        // RESIDENCY-NOTE: Promote the exact verified frame bytes
                        // recovery read from disk. A sync retry does not get to
                        // manufacture a second canonical compressed allocation
                        // merely because serialization is deterministic.
                        self.resident_histories.push(thread_id, recovered_frame);

                        state
                            .pending
                            .mark_committed(&sealed.turn_id)
                            .map_err(internal_error)?;
                        state
                            .committed_terminals
                            .insert(terminal.turn_id, terminal.digest);
                        state.last_digest = Some(frame.digest);
                        state.next_sequence = state
                            .next_sequence
                            .checked_add(1)
                            .ok_or_else(|| internal_error("journal sequence overflow"))?;
                        state.commit_faulted = false;
                        return Ok(());
                    } else if !prefix_matches {
                        return Err(internal_error(
                            "faulted RamJournal retry found an unexpected durable tail",
                        ));
                    }
                }
            }

            state.commit_faulted = false;
        }

        let frame_digest = frame.digest;
        let resident_frame = frame.clone();
        let writer = self.journal.clone();
        let append_result =
            tokio::task::spawn_blocking(move || writer.append_turn_frame(thread_id, &frame))
                .await
                .map_err(|error| {
                    internal_error(format!("journal writer task failed: {error}"))
                });

        let append_result = match append_result {
            Ok(result) => result.map_err(internal_error),
            Err(error) => Err(error),
        };

        if let Err(error) = append_result {
            // JOURNAL-NOTE: Keep the exact sealed transaction resident and mark
            // the disk tail suspect. The next retry reconciles the journal
            // before it considers another data write.
            state.commit_faulted = true;
            return Err(error);
        }

        // RESIDENCY-NOTE: The exact Arc-backed frame the writer just
        // acknowledged becomes canonical committed RAM history. There is no
        // reserialization step and no second compressed payload allocation.
        self.resident_histories.push(thread_id, resident_frame);

        state
            .pending
            .mark_committed(&sealed.turn_id)
            .map_err(internal_error)?;
        state
            .committed_terminals
            .insert(terminal.turn_id, terminal.digest);
        state.last_digest = Some(frame_digest);
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| internal_error("journal sequence overflow"))?;
        state.commit_faulted = false;

        Ok(())
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

    fn requires_terminal_durability_before_delivery(&self) -> bool {
        // FORK-INVARIANT: RamJournal terminal events are durability receipts.
        //
        // If the frame did not commit, the client does not get to hear
        // "completed" merely because upstream's generic failure policy is more
        // optimistic than this backend contract.
        true
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
        let thread_id = params.thread_id;
        let journal_state = self.journal_state(thread_id);

        Box::pin(async move {
            // FORK-INVARIANT: serialize append/commit activity per thread.
            // Distinct threads remain independent, while one thread cannot race
            // its own turn sequence into two different versions of reality.
            let mut state = journal_state.lock().await;

            let history_mode = self
                .history_modes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&thread_id)
                .copied()
                .unwrap_or_default();
            let persisted_items = persisted_rollout_items(&params.items, history_mode);
            let terminal = terminal_event_identity(&persisted_items).map_err(internal_error)?;

            if let Some(terminal) = terminal.as_ref()
                && let Some(committed_digest) =
                    state.committed_terminals.get(&terminal.turn_id)
            {
                if committed_digest == &terminal.digest && persisted_items.len() == 1 {
                    // JOURNAL-NOTE: Lost acknowledgement retry. The same
                    // terminal event is already durable, so do not mutate RAM
                    // again and definitely do not append another frame.
                    return Ok(());
                }
                return Err(ThreadStoreError::Conflict {
                    message: format!(
                        "turn {} is already durable; retry carries different or additional persisted content",
                        terminal.turn_id
                    ),
                });
            }

            if let Some(sealed) = state.pending.sealed() {
                let sealed_terminal = terminal_event_identity(&sealed.items)
                    .map_err(internal_error)?
                    .ok_or_else(|| {
                        internal_error("sealed RamJournal turn is missing a terminal event")
                    })?;

                if let Some(incoming) = terminal.as_ref()
                    && incoming.turn_id == sealed.turn_id
                    && incoming.digest == sealed_terminal.digest
                    && persisted_items.len() == 1
                {
                    // JOURNAL-NOTE: The previous durability attempt failed
                    // after resident state already observed the terminal item.
                    // Retry the sealed transaction directly; re-appending the
                    // event would duplicate RAM history before disk even gets
                    // another chance to behave.
                    return self.commit_sealed_turn(thread_id, &mut state).await;
                }

                return Err(ThreadStoreError::Conflict {
                    message: format!(
                        "turn {} is sealed but not durable; settle that commit before appending new persisted items",
                        sealed.turn_id
                    ),
                });
            }

            // RAM remains primary. Only after retry/conflict checks does the
            // resident store observe this append.
            ThreadStore::append_items(resident.as_ref(), params).await?;

            // COMPAT-NOTE: The durable frame uses upstream's canonical
            // persistence filter. Raw transient events can remain useful to
            // runtime observers without becoming surprise journal ontology.
            state
                .pending
                .push(&persisted_items)
                .map_err(internal_error)?;

            if state.pending.sealed().is_none() {
                // FORK-RAM: Nonterminal append. Deliberately no persistent I/O.
                return Ok(());
            }

            self.commit_sealed_turn(thread_id, &mut state).await
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
        Box::pin(async move {
            let journal_state = self.journal_state(thread_id);
            let mut state = journal_state.lock().await;

            // FORK-RAM: A pre-terminal flush still creates no disk traffic.
            // A sealed turn means a prior terminal commit attempt failed, so
            // flush is the durability fence that retries that exact resident
            // transaction without re-appending its terminal event.
            self.commit_sealed_turn(thread_id, &mut state).await?;
            drop(state);

            ThreadStore::flush_thread(self.resident.as_ref(), thread_id).await
        })
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


#[cfg(test)]
#[path = "thread_store_tests.rs"]
mod tests;
