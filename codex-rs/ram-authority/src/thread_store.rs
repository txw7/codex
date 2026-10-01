use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
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

use crate::catalog::ResidentCatalog;
use crate::decoded_context_cache::DecodedContextCache;
use crate::journal::JournalReader;
use crate::journal::JournalWriter;
use crate::journal::decode_turn_frame;
use crate::journal::encode_turn_frame;
use crate::journal::encode_turn_frame_with_checkpoint;
use crate::pending_turn::PendingTurn;
use crate::pending_turn::terminal_event_identity;
use crate::resident_history::ResidentHistories;
use crate::revision::ResidentRevisionV1;

// RESIDENCY-NOTE: This is a provisional fork default, not sacred geometry.
// The cache is byte-bounded today; Phase 03/06 telemetry and deployment config
// can tune the number without changing the authority model.
const DEFAULT_DECODED_CONTEXT_CACHE_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_CHECKPOINT_TURN_INTERVAL: u64 = 32;
const DEFAULT_CHECKPOINT_DELTA_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct CheckpointPolicy {
    turn_interval: u64,
    delta_bytes: usize,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            turn_interval: DEFAULT_CHECKPOINT_TURN_INTERVAL,
            delta_bytes: DEFAULT_CHECKPOINT_DELTA_BYTES,
        }
    }
}

impl CheckpointPolicy {
    fn is_due(
        self,
        turns_since_checkpoint: u64,
        bytes_since_checkpoint: usize,
        next_turn_uncompressed_bytes: u64,
    ) -> bool {
        let turn_due = self.turn_interval > 0
            && turns_since_checkpoint.saturating_add(1) >= self.turn_interval;
        let next_bytes =
            usize::try_from(next_turn_uncompressed_bytes).unwrap_or(usize::MAX);
        let byte_due = self.delta_bytes > 0
            && bytes_since_checkpoint.saturating_add(next_bytes) >= self.delta_bytes;
        turn_due || byte_due
    }
}

#[derive(Debug)]
struct ThreadJournalState {
    pending: PendingTurn,
    next_sequence: u64,
    last_digest: Option<[u8; 32]>,
    committed_terminals: HashMap<String, [u8; 32]>,
    commit_faulted: bool,
    turns_since_checkpoint: u64,
    bytes_since_checkpoint: usize,
}

impl Default for ThreadJournalState {
    fn default() -> Self {
        Self {
            pending: PendingTurn::default(),
            next_sequence: 1,
            last_digest: None,
            committed_terminals: HashMap::new(),
            commit_faulted: false,
            turns_since_checkpoint: 0,
            bytes_since_checkpoint: 0,
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
    catalog: ResidentCatalog,
    decoded_contexts: DecodedContextCache,
    unloaded_threads: Mutex<HashSet<ThreadId>>,
    checkpoint_policy: CheckpointPolicy,
}

impl RamJournalThreadStore {
    pub fn new(id: &str, journal_root: PathBuf) -> Self {
        Self::new_with_policies(
            id,
            journal_root,
            DEFAULT_DECODED_CONTEXT_CACHE_BYTES,
            CheckpointPolicy::default(),
        )
    }

    pub fn new_with_decoded_context_budget(
        id: &str,
        journal_root: PathBuf,
        decoded_context_budget_bytes: usize,
    ) -> Self {
        Self::new_with_policies(
            id,
            journal_root,
            decoded_context_budget_bytes,
            CheckpointPolicy::default(),
        )
    }

    fn new_with_policies(
        id: &str,
        journal_root: PathBuf,
        decoded_context_budget_bytes: usize,
        checkpoint_policy: CheckpointPolicy,
    ) -> Self {
        Self {
            resident: InMemoryThreadStore::for_id(id),
            journal: JournalWriter::new(journal_root.clone()),
            reader: JournalReader::new(journal_root),
            journal_states: Mutex::new(HashMap::new()),
            history_modes: Mutex::new(HashMap::new()),
            bootstrap_params: Mutex::new(HashMap::new()),
            resident_histories: ResidentHistories::default(),
            catalog: ResidentCatalog::default(),
            decoded_contexts: DecodedContextCache::new(decoded_context_budget_bytes),
            unloaded_threads: Mutex::new(HashSet::new()),
            checkpoint_policy,
        }
    }

    fn note_committed_frame(
        state: &mut ThreadJournalState,
        frame: &crate::journal::EncodedTurnFrame,
    ) {
        if frame.has_checkpoint {
            state.turns_since_checkpoint = 0;
            state.bytes_since_checkpoint = 0;
            return;
        }

        state.turns_since_checkpoint = state.turns_since_checkpoint.saturating_add(1);
        let frame_bytes = usize::try_from(frame.uncompressed_len).unwrap_or(usize::MAX);
        state.bytes_since_checkpoint =
            state.bytes_since_checkpoint.saturating_add(frame_bytes);
    }

    fn checkpoint_distance(
        frames: &[crate::journal::RecoveredFrame],
    ) -> (u64, usize) {
        let suffix = frames
            .iter()
            .rposition(|frame| frame.encoded.has_checkpoint)
            .map_or(frames, |index| &frames[index + 1..]);

        let turns = u64::try_from(suffix.len()).unwrap_or(u64::MAX);
        let bytes = suffix.iter().fold(0_usize, |total, frame| {
            total.saturating_add(
                usize::try_from(frame.encoded.uncompressed_len).unwrap_or(usize::MAX),
            )
        });
        (turns, bytes)
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

    fn existing_journal_state(
        &self,
        thread_id: ThreadId,
    ) -> Option<Arc<AsyncMutex<ThreadJournalState>>> {
        self.journal_states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&thread_id)
            .cloned()
    }

    async fn resident_revision(&self, thread_id: ThreadId) -> Option<String> {
        let state = self.existing_journal_state(thread_id)?;
        let state = state.lock().await;
        let durable_head_digest = state.last_digest?;
        let durable_sequence = state.next_sequence.checked_sub(1)?;

        Some(
            ResidentRevisionV1::new(durable_sequence, durable_head_digest)
                .to_opaque_string(),
        )
    }

    fn release_heavy_residency(&self, thread_id: ThreadId) {
        self.resident_histories.remove(thread_id);
        self.decoded_contexts.invalidate(thread_id);
        self.journal_states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&thread_id);
        self.mark_unloaded(thread_id);
    }

    fn has_ram_journal_authority(&self, thread_id: ThreadId) -> bool {
        self.bootstrap_params
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&thread_id)
    }

    fn is_unloaded(&self, thread_id: ThreadId) -> bool {
        self.unloaded_threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&thread_id)
    }

    fn mark_loaded(&self, thread_id: ThreadId) {
        self.unloaded_threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&thread_id);
    }

    fn mark_unloaded(&self, thread_id: ThreadId) {
        self.unloaded_threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread_id);
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

        Ok(StoredThreadHistory {
            thread_id,
            items,
            revision: self.resident_revision(thread_id).await,
        })
    }

    async fn build_latest_model_context(
        &self,
        thread_id: ThreadId,
        pending_items: Vec<RolloutItem>,
    ) -> ThreadStoreResult<StoredModelContext> {
        let frames = self.resident_histories.frames(thread_id);

        if let Some(checkpoint_index) = frames.iter().rposition(|frame| frame.has_checkpoint) {
            let checkpoint_frame = &frames[checkpoint_index];
            let (decoded, consumed) =
                decode_turn_frame(checkpoint_frame.bytes.as_ref()).map_err(internal_error)?;
            if consumed != checkpoint_frame.bytes.len() {
                return Err(internal_error(
                    "checkpoint CJR frame decoder did not consume the complete frame",
                ));
            }
            let mut context = decoded.checkpoint.ok_or_else(|| {
                internal_error("resident frame advertises checkpoint but payload has none")
            })?;
            if context.thread_id != thread_id {
                return Err(internal_error(
                    "resident checkpoint belongs to the wrong thread",
                ));
            }

            // RESIDENCY-NOTE: The checkpoint is a replay-safe model-context
            // baseline. Decode only its newer suffix, not every frame that
            // existed before the checkpoint was born.
            for frame in frames.iter().skip(checkpoint_index + 1) {
                let (decoded, consumed) =
                    decode_turn_frame(frame.bytes.as_ref()).map_err(internal_error)?;
                if consumed != frame.bytes.len() {
                    return Err(internal_error(
                        "resident CJR frame decoder did not consume the complete frame",
                    ));
                }
                context.items.extend(decoded.items);
            }
            context.items.extend(pending_items);
            return Ok(context);
        }

        let history_mode = self
            .history_modes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&thread_id)
            .copied()
            .unwrap_or_default();

        if history_mode == ThreadHistoryMode::Legacy {
            let session_meta = self.session_meta_line(thread_id).await?;
            let mut items = vec![RolloutItem::SessionMeta(session_meta)];
            for frame in frames {
                let (decoded, consumed) =
                    decode_turn_frame(frame.bytes.as_ref()).map_err(internal_error)?;
                if consumed != frame.bytes.len() {
                    return Err(internal_error(
                        "resident CJR frame decoder did not consume the complete frame",
                    ));
                }
                items.extend(decoded.items);
            }
            items.extend(pending_items);
            return Ok(StoredModelContext {
                thread_id,
                items,
                revision: self.resident_revision(thread_id).await,
            });
        }

        let session_meta = self.session_meta_line(thread_id).await?;
        let mut scan = ModelContextScan::default();
        let mut complete = false;

        for item in pending_items.into_iter().rev() {
            if matches!(scan.push(item), ModelContextScanProgress::Complete) {
                complete = true;
                break;
            }
        }

        if !complete {
            'frames: for frame in frames.into_iter().rev() {
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
        }

        let mut items = scan.finish();
        items.insert(0, RolloutItem::SessionMeta(session_meta));
        Ok(StoredModelContext {
            thread_id,
            items,
            revision: self.resident_revision(thread_id).await,
        })
    }

    async fn materialize_latest_model_context(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<StoredModelContext> {
        if let Some(context) = self.decoded_contexts.get(thread_id) {
            // RESIDENCY-NOTE: This is the one retained decoded hot projection.
            // Canonical history remains the compressed frame set underneath it.
            return Ok(context);
        }

        let pending_items = self.pending_items(thread_id).await;
        let context = self
            .build_latest_model_context(thread_id, pending_items)
            .await?;

        self.decoded_contexts.insert(context.clone());
        Ok(context)
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
        // resident lifetime. Lightweight delegate metadata may survive an
        // in-process unload; do not recreate it and append another SessionMeta
        // merely because the compressed frames were evicted.
        let metadata_exists = ThreadStore::read_thread(
            self.resident.as_ref(),
            ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            },
        )
        .await
        .is_ok();
        if !metadata_exists {
            ThreadStore::create_thread(self.resident.as_ref(), bootstrap.clone()).await?;
        }

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

        self.decoded_contexts.invalidate(thread_id);

        // RESIDENCY-NOTE: Do not expand recovered committed history into the
        // upstream delegate. create_thread() already installs SessionMeta there;
        // the verified CJR frames above are the canonical committed transcript.
        //
        // Cold load is allowed to validate history. It does not need to unpack
        // the entire moving truck into a second resident object graph.
        self.catalog.note_thread(thread_id);
        self.history_modes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread_id, bootstrap.history_mode);
        self.bootstrap_params
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread_id.clone(), bootstrap);

        let journal_state = self.journal_state(thread_id);
        let mut state = journal_state.lock().await;
        let (turns_since_checkpoint, bytes_since_checkpoint) =
            Self::checkpoint_distance(&recovered.frames);
        state.next_sequence = recovered.next_sequence();
        state.last_digest = recovered.last_digest();
        state.committed_terminals = committed_terminals;
        state.commit_faulted = false;
        state.turns_since_checkpoint = turns_since_checkpoint;
        state.bytes_since_checkpoint = bytes_since_checkpoint;
        drop(state);

        self.mark_loaded(thread_id);
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

        let ordinary_frame = encode_turn_frame(
            thread_id,
            &sealed.turn_id,
            sequence,
            state.last_digest,
            bootstrap.as_ref(),
            &sealed.items,
        )
        .map_err(internal_error)?;

        let checkpoint_due = sequence > 1
            && self.checkpoint_policy.is_due(
                state.turns_since_checkpoint,
                state.bytes_since_checkpoint,
                ordinary_frame.uncompressed_len,
            );

        let frame = if checkpoint_due {
            // JOURNAL-NOTE: Build the replay baseline from already-resident
            // committed frames plus this sealed turn. It rides inside this
            // terminal frame, so checkpointing does not create a second write
            // or a second durable object.
            let checkpoint = self
                .build_latest_model_context(thread_id, sealed.items.clone())
                .await?;
            encode_turn_frame_with_checkpoint(
                thread_id,
                &sealed.turn_id,
                sequence,
                state.last_digest,
                bootstrap.as_ref(),
                Some(&checkpoint),
                &sealed.items,
            )
            .map_err(internal_error)?
        } else {
            ordinary_frame
        };

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
                        Self::note_committed_frame(state, &recovered_frame);
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
        Self::note_committed_frame(state, &resident_frame);
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
            self.decoded_contexts.invalidate(thread_id);
            self.catalog.note_thread(thread_id);
            self.mark_loaded(thread_id);
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

    fn resume_thread(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreFuture<'_, Arc<Vec<RolloutItem>>> {
        let thread_id = params.thread_id;
        let history_mode = params
            .history
            .as_deref()
            .and_then(history_mode_from_items)
            .unwrap_or_default();

        Box::pin(async move {
            self.decoded_contexts.invalidate(thread_id);
            let already_compressed = self.resident_histories.with(thread_id, |history| {
                history.is_some_and(|history| !history.frames().is_empty())
            });
            let hydrated = if already_compressed {
                true
            } else {
                self.hydrate_from_journal(thread_id).await?
            };

            let mut resident_params = params;
            if already_compressed || hydrated {
                // RESIDENCY-NOTE: Core may hand decoded ResumedHistory back to
                // ThreadStore after it loaded that history from us. When a CJR
                // authority exists, do not store that object graph again.
                //
                // COMPAT-NOTE: If no RamJournal history exists, preserve the
                // caller-supplied history. Legacy/external resume paths remain
                // functional until migration gets its own explicit phase.
                resident_params.history = None;
            }

            let _delegate_history =
                ThreadStore::resume_thread(self.resident.as_ref(), resident_params).await?;
            self.mark_loaded(thread_id);
            self.history_modes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(thread_id)
                .or_insert(history_mode);

            // AUTHORITY-NOTE: The replay Arc published to core is reconstructed
            // from the canonical resident authority after ownership is acquired.
            // The delegate exists for upstream metadata semantics; compressed
            // CJR frames remain the transcript authority when present.
            if self.has_ram_journal_authority(thread_id) {
                let context = self.materialize_latest_model_context(thread_id).await?;
                return Ok(Arc::new(context.items));
            }

            ThreadStore::resume_thread(
                self.resident.as_ref(),
                ResumeThreadParams {
                    thread_id,
                    rollout_path: None,
                    history: None,
                    history_revision: None,
                    include_archived: true,
                    metadata: self
                        .bootstrap_params
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .get(&thread_id)
                        .map(|params| params.metadata.clone())
                        .unwrap_or_else(|| resident_params.metadata.clone()),
                },
            )
            .await
        })
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
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

            if !persisted_items.is_empty() {
                // RESIDENCY-NOTE: Invalidate only when canonical model-visible
                // state actually changes. Duplicate terminal acknowledgement
                // paths returned above and do not churn the hot projection.
                self.decoded_contexts.invalidate(thread_id);
            }

            // FORK-INVARIANT: Canonical session transcript no longer enters
            // the upstream in-memory history Vec. Open-turn items live in
            // PendingTurn; committed turns live as compressed CJR frames.
            //
            // Metadata projection remains upstream-owned through
            // LiveThread::record_thread_metadata. We are removing duplicate
            // transcript storage, not staging a coup against every useful trait.
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
        Box::pin(async move {
            if self.has_ram_journal_authority(thread_id)
                && let Some(journal_state) = self.existing_journal_state(thread_id)
            {
                let mut state = journal_state.lock().await;

                if state.pending.sealed().is_some() {
                    // RESIDENCY-NOTE: shutdown may be the final durability fence
                    // after an earlier terminal append/sync fault. Settle that
                    // exact sealed transaction before releasing its RAM state.
                    self.commit_sealed_turn(thread_id, &mut state).await?;
                }

                if !state.pending.is_empty() {
                    return Err(internal_error(format!(
                        "refusing to unload thread {thread_id} with an open RamJournal transaction"
                    )));
                }
                if state.commit_faulted {
                    return Err(internal_error(format!(
                        "refusing to unload thread {thread_id} with an unresolved journal fault"
                    )));
                }

                let resident_head = self.resident_histories.last_digest(thread_id);
                if resident_head.is_some() && resident_head != state.last_digest {
                    // FORK-INVARIANT: eviction is legal only when the complete
                    // resident committed head is exactly the durable journal
                    // head we last acknowledged.
                    //
                    // "Same number of turns, probably" is not an equivalence
                    // witness. Digests are cheaper than future archaeology.
                    return Err(internal_error(format!(
                        "refusing to unload thread {thread_id}: resident head does not match durable head"
                    )));
                }
            }

            ThreadStore::shutdown_thread(self.resident.as_ref(), thread_id).await?;

            if self.has_ram_journal_authority(thread_id)
                && self.resident_histories.frame_count(thread_id) > 0
            {
                // RESIDENCY-NOTE: Upstream has already established that this
                // runtime is idle/unsubscribed before app-server unload reaches
                // us. Release the heavy canonical session bytes here instead of
                // inventing a competing residency scheduler inside storage.
                //
                // Bootstrap/history metadata stays small and resident so list
                // views remain cheap. A subsequent history/model-context read
                // will perform one explicit cold CJR hydration.
                self.release_heavy_residency(thread_id);
            } else {
                self.decoded_contexts.invalidate(thread_id);
            }

            Ok(())
        })
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        ThreadStore::discard_thread(self.resident.as_ref(), thread_id)
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(async move {
            if self.is_unloaded(params.thread_id)
                && !self.hydrate_from_journal(params.thread_id).await?
            {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }

            if self.resident_histories.frames(params.thread_id).is_empty() {
                let resident_exists = ThreadStore::read_thread(
                    self.resident.as_ref(),
                    ReadThreadParams {
                        thread_id: params.thread_id,
                        include_archived: params.include_archived,
                        include_history: false,
                    },
                )
                .await
                .is_ok();

                if !resident_exists && !self.hydrate_from_journal(params.thread_id).await? {
                    return Err(ThreadStoreError::ThreadNotFound {
                        thread_id: params.thread_id,
                    });
                }
            }

            if !self.has_ram_journal_authority(params.thread_id) {
                // COMPAT-NOTE: A caller-supplied legacy/external resume can
                // temporarily live only in the delegate until migration owns
                // that history. Do not call an empty compressed frame set
                // "authority" merely because we would prefer it aesthetically.
                return ThreadStore::load_history(self.resident.as_ref(), params).await;
            }

            // RESIDENCY-NOTE: Complete-history APIs may explicitly materialize
            // decoded RolloutItems, but the resulting Vec is a response value,
            // not canonical resident state. Compressed frames remain authority.
            self.materialize_complete_history(params.thread_id).await
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move {
            if self.is_unloaded(params.thread_id)
                && !self.hydrate_from_journal(params.thread_id).await?
            {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }

            let resident_exists = ThreadStore::read_thread(
                self.resident.as_ref(),
                ReadThreadParams {
                    thread_id: params.thread_id,
                    include_archived: params.include_archived,
                    include_history: false,
                },
            )
            .await
            .is_ok();

            if !resident_exists && !self.hydrate_from_journal(params.thread_id).await? {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }

            if !self.has_ram_journal_authority(params.thread_id) {
                return ThreadStore::load_latest_model_context(self.resident.as_ref(), params).await;
            }

            // RESIDENCY-NOTE: Paginated context scans walk pending hot items and
            // compressed frames from newest to oldest. They stop when upstream's
            // ModelContextScan says enough context exists instead of decoding
            // the historical museum because somebody asked for the current room.
            self.materialize_latest_model_context(params.thread_id).await
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            if self.is_unloaded(params.thread_id)
                && !self.hydrate_from_journal(params.thread_id).await?
            {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }

            let metadata_params = ReadThreadParams {
                thread_id: params.thread_id,
                include_archived: params.include_archived,
                include_history: false,
            };

            let mut thread = match ThreadStore::read_thread(
                self.resident.as_ref(),
                metadata_params.clone(),
            )
            .await
            {
                Ok(thread) => thread,
                Err(ThreadStoreError::ThreadNotFound { .. }) => {
                    if !self.hydrate_from_journal(params.thread_id).await? {
                        return Err(ThreadStoreError::ThreadNotFound {
                            thread_id: params.thread_id,
                        });
                    }
                    ThreadStore::read_thread(self.resident.as_ref(), metadata_params).await?
                }
                Err(error) => return Err(error),
            };

            if params.include_history {
                if self.has_ram_journal_authority(params.thread_id) {
                    thread.history =
                        Some(self.materialize_complete_history(params.thread_id).await?);
                } else {
                    // COMPAT-NOTE: no CJR bootstrap means the delegate is still
                    // the only canonical source for this compatibility thread.
                    return ThreadStore::read_thread(self.resident.as_ref(), params).await;
                }
            }

            Ok(thread)
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
            if self.catalog.needs_discovery() {
                let reader = self.reader.clone();
                let durable_ids = tokio::task::spawn_blocking(move || reader.list_thread_ids())
                    .await
                    .map_err(|error| {
                        internal_error(format!("journal discovery task failed: {error}"))
                    })?
                    .map_err(internal_error)?;

                // RESIDENCY-NOTE: Cold discovery is paid once per process.
                //
                // The catalog is a rebuildable projection, so a restart may
                // scan journal placement again. Ordinary thread/list calls in
                // the same process do not get to repeatedly interrogate disk
                // about facts we already retained in RAM.
                self.catalog.install_discovery(durable_ids);
            }

            for thread_id in self.catalog.thread_ids() {
                match ThreadStore::read_thread(
                    self.resident.as_ref(),
                    ReadThreadParams {
                        thread_id,
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
