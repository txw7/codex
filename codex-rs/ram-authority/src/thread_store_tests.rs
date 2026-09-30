use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_rollout::RolloutItem;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::SortDirection;
use codex_thread_store::ThreadSortKey;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreError;

use super::CheckpointPolicy;
use super::RamJournalThreadStore;

fn create_thread_params(thread_id: ThreadId) -> CreateThreadParams {
    CreateThreadParams {
        creator_user_id: None,
        creator_account_id: None,
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Cli,
        thread_source: None,
        originator: "ram-authority-test".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: ThreadHistoryMode::Legacy,
        history_base: None,
        subagent_history_start_ordinal: None,
        initial_window_id: "ram-authority-test-window".to_string(),
        runtime_workspace_roots: None,
        metadata: ThreadPersistenceMetadata {
            cwd: None,
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}

fn terminal_turn(turn_id: &str) -> RolloutItem {
    terminal_turn_with_message(turn_id, "done")
}

fn terminal_turn_with_message(turn_id: &str, message: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: Some(message.to_string()),
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
    }))
}

fn contains_terminal_turn(items: &[RolloutItem], turn_id: &str) -> bool {
    items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TurnComplete(event))
                if event.turn_id == turn_id
        )
    })
}

#[tokio::test]
async fn checkpoint_rides_inside_the_due_terminal_frame_and_survives_cold_recovery() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let thread_id = ThreadId::new();
    let policy = CheckpointPolicy {
        turn_interval: 2,
        delta_bytes: 0,
    };

    let first = RamJournalThreadStore::new_with_policies(
        "checkpoint-writer",
        root.clone(),
        1024 * 1024,
        policy,
    );
    ThreadStore::create_thread(&first, create_thread_params(thread_id))
        .await
        .expect("create resident thread");

    for turn_id in ["turn-cp-1", "turn-cp-2", "turn-cp-3"] {
        ThreadStore::append_items(
            &first,
            AppendThreadItemsParams {
                thread_id,
                items: vec![terminal_turn(turn_id)],
            },
        )
        .await
        .expect("terminal turn should commit");
    }

    let frames = first.resident_histories.frames(thread_id);
    assert_eq!(frames.len(), 3);
    assert!(!frames[0].has_checkpoint);
    assert!(frames[1].has_checkpoint);
    assert!(!frames[2].has_checkpoint);

    let (checkpointed, consumed) =
        crate::journal::decode_turn_frame(frames[1].bytes.as_ref())
            .expect("decode checkpointed frame");
    assert_eq!(consumed, frames[1].bytes.len());
    let checkpoint = checkpointed.checkpoint.expect("turn 2 checkpoint");
    assert!(contains_terminal_turn(&checkpoint.items, "turn-cp-1"));
    assert!(contains_terminal_turn(&checkpoint.items, "turn-cp-2"));

    first.decoded_contexts.invalidate(thread_id);
    let hot = ThreadStore::load_latest_model_context(
        &first,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("checkpoint should reconstruct latest context");
    assert!(contains_terminal_turn(&hot.items, "turn-cp-1"));
    assert!(contains_terminal_turn(&hot.items, "turn-cp-2"));
    assert!(contains_terminal_turn(&hot.items, "turn-cp-3"));

    // JOURNAL-NOTE: three terminal turns produced three frames. The checkpoint
    // lives inside turn 2; it did not negotiate a fourth append for itself.
    let recovered = crate::journal::JournalReader::new(root.clone())
        .recover_thread(thread_id)
        .expect("recover journal")
        .expect("journal exists");
    assert_eq!(recovered.frames.len(), 3);
    assert!(recovered.frames[1].encoded.has_checkpoint);

    let second = RamJournalThreadStore::new_with_policies(
        "checkpoint-reader",
        root,
        1024 * 1024,
        policy,
    );
    let cold = ThreadStore::load_latest_model_context(
        &second,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("cold checkpoint recovery");
    assert!(contains_terminal_turn(&cold.items, "turn-cp-3"));

    let journal_state = second.journal_state(thread_id);
    let state = journal_state.lock().await;
    // turn 2 reset the schedule; turn 3 is one durable turn beyond it.
    assert_eq!(state.turns_since_checkpoint, 1);
    assert!(state.bytes_since_checkpoint > 0);
}

#[tokio::test]
async fn checkpoint_can_be_triggered_by_durable_byte_distance() {
    let temp = tempfile::tempdir().expect("tempdir");
    let thread_id = ThreadId::new();
    let store = RamJournalThreadStore::new_with_policies(
        "checkpoint-bytes",
        temp.path().to_path_buf(),
        1024 * 1024,
        CheckpointPolicy {
            turn_interval: 0,
            delta_bytes: 1,
        },
    );

    ThreadStore::create_thread(&store, create_thread_params(thread_id))
        .await
        .expect("create resident thread");

    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-bytes-1")],
        },
    )
    .await
    .expect("first turn should commit");
    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-bytes-2")],
        },
    )
    .await
    .expect("second turn should checkpoint");

    let frames = store.resident_histories.frames(thread_id);
    assert_eq!(frames.len(), 2);
    // Sequence 1 deliberately stays the bootstrap-only turn even under an
    // absurdly tiny test threshold. The next durable turn carries the baseline.
    assert!(!frames[0].has_checkpoint);
    assert!(frames[1].has_checkpoint);

    let journal_state = store.journal_state(thread_id);
    let state = journal_state.lock().await;
    assert_eq!(state.turns_since_checkpoint, 0);
    assert_eq!(state.bytes_since_checkpoint, 0);
}

#[tokio::test]
async fn shutdown_evicts_heavy_residency_and_history_read_cold_loads_once() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let thread_id = ThreadId::new();
    let journal_path = crate::journal::journal_thread_path(&root, thread_id);
    let store = RamJournalThreadStore::new_with_decoded_context_budget(
        "whole-thread-unload",
        root,
        1024 * 1024,
    );

    ThreadStore::create_thread(&store, create_thread_params(thread_id))
        .await
        .expect("create resident thread");
    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-unload")],
        },
    )
    .await
    .expect("terminal turn should commit");
    ThreadStore::load_latest_model_context(
        &store,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("populate decoded hot cache");

    assert_eq!(store.resident_histories.frame_count(thread_id), 1);
    assert_eq!(store.decoded_contexts.entry_count(), 1);
    assert!(!store.is_unloaded(thread_id));

    ThreadStore::shutdown_thread(&store, thread_id)
        .await
        .expect("idle durable thread should unload");

    assert!(store.is_unloaded(thread_id));
    assert_eq!(store.resident_histories.frame_count(thread_id), 0);
    assert_eq!(store.decoded_contexts.entry_count(), 0);

    let page = ThreadStore::list_threads(
        &store,
        ListThreadsParams {
            page_size: 100,
            cursor: None,
            sort_key: ThreadSortKey::CreatedAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: Vec::new(),
            model_providers: None,
            cwd_filters: None,
            section: None,
            project_id: None,
            archived: false,
            search_term: None,
            relation_filter: None,
            use_state_db_only: false,
        },
    )
    .await
    .expect("metadata-only list after unload");
    assert!(page.items.iter().any(|thread| thread.thread_id == thread_id));

    // RESIDENCY-NOTE: listing uses retained lightweight metadata. It does not
    // cold-load the transcript just to draw a row.
    assert!(store.is_unloaded(thread_id));
    assert_eq!(store.resident_histories.frame_count(thread_id), 0);

    let loaded = ThreadStore::load_history(
        &store,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("first history read should cold hydrate");
    assert!(contains_terminal_turn(&loaded.items, "turn-unload"));
    assert!(!store.is_unloaded(thread_id));
    assert_eq!(store.resident_histories.frame_count(thread_id), 1);

    std::fs::remove_file(&journal_path).expect("remove journal after explicit cold load");

    let resident_again = ThreadStore::load_history(
        &store,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("loaded history should remain disk-independent");
    assert!(contains_terminal_turn(&resident_again.items, "turn-unload"));

    // FORK-INVARIANT: once reloaded, complete logical history is RAM-resident
    // again. Disk does not receive an encore because the caller asked twice.
    assert_eq!(store.resident_histories.frame_count(thread_id), 1);
}

#[tokio::test]
async fn latest_model_context_cache_is_bounded_and_invalidated_by_new_turns() {
    let temp = tempfile::tempdir().expect("tempdir");
    let thread_id = ThreadId::new();
    let store = RamJournalThreadStore::new_with_decoded_context_budget(
        "decoded-cache",
        temp.path().to_path_buf(),
        1024 * 1024,
    );

    ThreadStore::create_thread(&store, create_thread_params(thread_id))
        .await
        .expect("create resident thread");
    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-cache-1")],
        },
    )
    .await
    .expect("first turn should commit");

    let first = ThreadStore::load_latest_model_context(
        &store,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("first model context materialization");
    assert!(contains_terminal_turn(&first.items, "turn-cache-1"));
    assert_eq!(store.decoded_contexts.entry_count(), 1);
    assert!(store.decoded_contexts.retained_bytes() > 0);
    assert!(store.decoded_contexts.retained_bytes() <= 1024 * 1024);

    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-cache-2")],
        },
    )
    .await
    .expect("second turn should commit");

    // RESIDENCY-NOTE: canonical history changed, so the decoded projection is
    // gone immediately. It is not allowed to become a stale shadow authority.
    assert_eq!(store.decoded_contexts.entry_count(), 0);
    assert_eq!(store.decoded_contexts.retained_bytes(), 0);

    let second = ThreadStore::load_latest_model_context(
        &store,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("second model context materialization");
    assert!(contains_terminal_turn(&second.items, "turn-cache-2"));
    assert_eq!(store.decoded_contexts.entry_count(), 1);
    assert!(store.decoded_contexts.retained_bytes() <= 1024 * 1024);
}

#[tokio::test]
async fn caller_supplied_legacy_history_remains_delegate_authority_without_cjr() {
    let temp = tempfile::tempdir().expect("tempdir");
    let thread_id = ThreadId::new();
    let params = create_thread_params(thread_id);
    let store = RamJournalThreadStore::new("legacy-compat", temp.path().to_path_buf());

    // Model an already-existing compatibility thread without teaching
    // RamJournal that it owns a CJR bootstrap for this identity.
    ThreadStore::create_thread(store.resident.as_ref(), params.clone())
        .await
        .expect("create compatibility delegate thread");
    let mut history = ThreadStore::load_history(
        store.resident.as_ref(),
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("read delegate bootstrap history")
    .items;
    history.push(terminal_turn("legacy-turn"));

    ThreadStore::resume_thread(
        &store,
        ResumeThreadParams {
            thread_id,
            rollout_path: None,
            history: Some(Arc::new(history)),
            include_archived: true,
            metadata: params.metadata.clone(),
        },
    )
    .await
    .expect("compatibility resume");

    assert_eq!(store.resident_histories.frame_count(thread_id), 0);
    assert!(!store.has_ram_journal_authority(thread_id));

    let loaded = ThreadStore::load_history(
        &store,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("legacy delegate history must remain readable");
    assert!(contains_terminal_turn(&loaded.items, "legacy-turn"));

    // COMPAT-NOTE: no journal + no CJR bootstrap means the supplied history is
    // still authority. Calling it RamJournal-owned because the config selected
    // this store would be ontology by branding.
    let thread = ThreadStore::read_thread(
        &store,
        ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: true,
        },
    )
    .await
    .expect("legacy thread read");
    assert!(contains_terminal_turn(
        &thread.history.expect("legacy history").items,
        "legacy-turn"
    ));
}

#[tokio::test]
async fn committed_turn_lives_compressed_not_in_the_delegate_transcript() {
    let temp = tempfile::tempdir().expect("tempdir");
    let thread_id = ThreadId::new();
    let store = RamJournalThreadStore::new("compressed-authority", temp.path().to_path_buf());

    ThreadStore::create_thread(&store, create_thread_params(thread_id))
        .await
        .expect("create resident thread");
    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-compressed")],
        },
    )
    .await
    .expect("terminal turn should commit");

    assert_eq!(store.resident_histories.frame_count(thread_id), 1);
    assert!(store.resident_histories.compressed_bytes(thread_id) > 0);

    let delegate_history = ThreadStore::load_history(
        store.resident.as_ref(),
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("delegate bootstrap history");

    // FORK-INVARIANT: the delegate keeps SessionMeta for metadata/bootstrap
    // semantics, not a second decoded copy of the committed transcript.
    assert_eq!(delegate_history.items.len(), 1);
    assert!(matches!(
        delegate_history.items.first(),
        Some(RolloutItem::SessionMeta(_))
    ));

    let canonical = ThreadStore::load_history(
        &store,
        LoadThreadHistoryParams {
            thread_id,
            include_archived: true,
        },
    )
    .await
    .expect("compressed canonical history should materialize");
    assert!(contains_terminal_turn(&canonical.items, "turn-compressed"));
}

#[tokio::test]
async fn terminal_turn_survives_resident_authority_replacement() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let thread_id = ThreadId::new();
    let journal_path = crate::journal::journal_thread_path(&root, thread_id);

    let first = RamJournalThreadStore::new("writer-resident", root.clone());
    ThreadStore::create_thread(&first, create_thread_params(thread_id))
        .await
        .expect("create resident thread");

    ThreadStore::persist_thread(&first, thread_id, PersistContext::TurnStart)
        .await
        .expect("turn-start RAM fence");
    ThreadStore::flush_thread(&first, thread_id)
        .await
        .expect("pre-terminal RAM flush");

    // FORK-INVARIANT: upstream checkpoints and flushes do not manufacture a
    // session journal before the turn reaches its terminal boundary.
    assert!(!journal_path.exists());

    ThreadStore::append_items(
        &first,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-1")],
        },
    )
    .await
    .expect("terminal turn should commit");

    assert!(journal_path.exists());

    // A different store id gives us a fresh upstream InMemoryThreadStore,
    // approximating process loss: the new authority has no resident history and
    // must reconstruct it from CJR.
    let second = RamJournalThreadStore::new("reader-resident", root);
    let recovered = ThreadStore::read_thread(
        &second,
        ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: true,
        },
    )
    .await
    .expect("cold journal should hydrate");

    let history = recovered.history.expect("hydrated history");
    assert!(contains_terminal_turn(&history.items, "turn-1"));

    std::fs::remove_file(&journal_path).expect("remove journal after cold load");

    let resident = ThreadStore::read_thread(
        &second,
        ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: true,
        },
    )
    .await
    .expect("loaded thread should not need the journal again");

    // FORK-INVARIANT: if this succeeds after the backing journal was removed,
    // the loaded read came from RAM. We already paid for the cold load; disk
    // does not get an encore because somebody called read_thread twice.
    let history = resident.history.expect("resident history");
    assert!(contains_terminal_turn(&history.items, "turn-1"));
}


#[tokio::test]
async fn duplicate_terminal_retry_is_idempotent_after_cold_recovery() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let thread_id = ThreadId::new();
    let journal_path = crate::journal::journal_thread_path(&root, thread_id);

    let first = RamJournalThreadStore::new("idempotency-writer", root.clone());
    ThreadStore::create_thread(&first, create_thread_params(thread_id))
        .await
        .expect("create resident thread");
    ThreadStore::append_items(
        &first,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-idempotent")],
        },
    )
    .await
    .expect("first terminal commit");

    let committed_len = std::fs::metadata(&journal_path)
        .expect("journal metadata")
        .len();

    let second = RamJournalThreadStore::new("idempotency-reader", root);
    ThreadStore::read_thread(
        &second,
        ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: true,
        },
    )
    .await
    .expect("cold recovery should rebuild committed terminal receipts");

    let resident_frames_before_retry = second.resident_histories.frame_count(thread_id);
    assert_eq!(resident_frames_before_retry, 1);

    ThreadStore::append_items(
        &second,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-idempotent")],
        },
    )
    .await
    .expect("identical terminal retry should be a no-op");

    // RESIDENCY-NOTE: lost acknowledgement replay must not grow canonical RAM
    // history any more than it grows the journal.
    assert_eq!(
        second.resident_histories.frame_count(thread_id),
        resident_frames_before_retry
    );

    assert_eq!(
        std::fs::metadata(&journal_path)
            .expect("journal metadata after retry")
            .len(),
        committed_len
    );

    let conflict = ThreadStore::append_items(
        &second,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn_with_message(
                "turn-idempotent",
                "different terminal content",
            )],
        },
    )
    .await
    .expect_err("different terminal content for a durable turn id must conflict");

    // JOURNAL-NOTE: turn identity is stable across acknowledgement loss.
    // Same receipt => no-op. Same id, different receipt => conflict. We do not
    // append both and leave the historian to choose a favorite.
    assert!(matches!(conflict, ThreadStoreError::Conflict { .. }));
    assert_eq!(
        std::fs::metadata(&journal_path)
            .expect("journal metadata after conflict")
            .len(),
        committed_len
    );
}


#[tokio::test]
async fn thread_list_discovers_a_cold_durable_journal_without_knowing_its_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let thread_id = ThreadId::new();

    let first = RamJournalThreadStore::new("list-writer", root.clone());
    ThreadStore::create_thread(&first, create_thread_params(thread_id))
        .await
        .expect("create resident thread");
    ThreadStore::append_items(
        &first,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal_turn("turn-list")],
        },
    )
    .await
    .expect("terminal turn should commit");

    // A different resident-store id has no in-memory knowledge of the thread.
    // list_threads must discover the CJR namespace, verify/hydrate the thread,
    // then delegate listing semantics to RAM.
    let second = RamJournalThreadStore::new("list-reader", root.clone());
    let page = ThreadStore::list_threads(
        &second,
        ListThreadsParams {
            page_size: 100,
            cursor: None,
            sort_key: ThreadSortKey::CreatedAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: Vec::new(),
            model_providers: None,
            cwd_filters: None,
            section: None,
            project_id: None,
            archived: false,
            search_term: None,
            relation_filter: None,
            use_state_db_only: false,
        },
    )
    .await
    .expect("cold durable thread should be listable");

    assert!(page.items.iter().any(|thread| thread.thread_id == thread_id));

    let discovery_root = root.join("v1");
    std::fs::remove_dir_all(&discovery_root).expect("remove journal placement tree");
    std::fs::write(&discovery_root, b"not a directory")
        .expect("poison future journal discovery");

    let resident_page = ThreadStore::list_threads(
        &second,
        ListThreadsParams {
            page_size: 100,
            cursor: None,
            sort_key: ThreadSortKey::CreatedAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: Vec::new(),
            model_providers: None,
            cwd_filters: None,
            section: None,
            project_id: None,
            archived: false,
            search_term: None,
            relation_filter: None,
            use_state_db_only: false,
        },
    )
    .await
    .expect("resident catalog should make repeated listing disk-independent");

    // FORK-INVARIANT: after cold discovery, ordinary thread/list must not touch
    // journal placement again. The path is deliberately poisoned above; old
    // per-call discovery would fail with NotADirectory instead of reaching this
    // assertion.
    assert!(
        resident_page
            .items
            .iter()
            .any(|thread| thread.thread_id == thread_id)
    );
}


#[tokio::test]
async fn flush_repairs_a_truncated_faulted_terminal_tail_before_retrying() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let thread_id = ThreadId::new();
    let params = create_thread_params(thread_id);
    let terminal = terminal_turn("turn-truncated-retry");
    let journal_path = crate::journal::journal_thread_path(&root, thread_id);

    let store = RamJournalThreadStore::new("truncated-retry", root.clone());
    ThreadStore::create_thread(&store, params.clone())
        .await
        .expect("create resident thread");

    std::fs::create_dir_all(journal_path.parent().expect("journal parent"))
        .expect("create journal parent");
    std::fs::create_dir(&journal_path).expect("block journal file with directory");

    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal.clone()],
        },
    )
    .await
    .expect_err("blocked journal path should fault the terminal commit");

    std::fs::remove_dir(&journal_path).expect("remove journal blocker");

    let expected = crate::journal::encode_turn_frame(
        thread_id,
        "turn-truncated-retry",
        1,
        None,
        Some(&params),
        &[terminal],
    )
    .expect("encode expected frame");
    let split = expected.bytes.len() / 2;
    std::fs::write(&journal_path, &expected.bytes[..split]).expect("write truncated tail");

    ThreadStore::flush_thread(&store, thread_id)
        .await
        .expect("flush should repair and retry the sealed terminal commit");

    // JOURNAL-NOTE: The retry path must first truncate the corpse fragment,
    // then append the one canonical frame. Appending after the fragment would
    // technically be more bytes, which is not the same thing as recovery.
    let recovered = std::fs::read(&journal_path).expect("read repaired journal");
    assert_eq!(recovered.as_slice(), expected.bytes.as_ref());
}

#[tokio::test]
async fn flush_syncs_a_complete_unacknowledged_frame_without_appending_it_twice() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let thread_id = ThreadId::new();
    let params = create_thread_params(thread_id);
    let terminal = terminal_turn("turn-sync-retry");
    let journal_path = crate::journal::journal_thread_path(&root, thread_id);

    let store = RamJournalThreadStore::new("sync-retry", root.clone());
    ThreadStore::create_thread(&store, params.clone())
        .await
        .expect("create resident thread");

    std::fs::create_dir_all(journal_path.parent().expect("journal parent"))
        .expect("create journal parent");
    std::fs::create_dir(&journal_path).expect("block journal file with directory");

    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal.clone()],
        },
    )
    .await
    .expect_err("blocked journal path should fault the terminal commit");

    std::fs::remove_dir(&journal_path).expect("remove journal blocker");

    let expected = crate::journal::encode_turn_frame(
        thread_id,
        "turn-sync-retry",
        1,
        None,
        Some(&params),
        &[terminal.clone()],
    )
    .expect("encode expected frame");
    std::fs::write(&journal_path, expected.bytes.as_ref()).expect("materialize complete unacknowledged frame");
    let before = std::fs::metadata(&journal_path)
        .expect("journal metadata before sync retry")
        .len();

    ThreadStore::flush_thread(&store, thread_id)
        .await
        .expect("flush should acknowledge the existing complete frame");

    assert_eq!(
        std::fs::metadata(&journal_path)
            .expect("journal metadata after sync retry")
            .len(),
        before
    );
    assert_eq!(
        std::fs::read(&journal_path).expect("read synced journal"),
        expected.bytes.as_ref()
    );

    ThreadStore::append_items(
        &store,
        AppendThreadItemsParams {
            thread_id,
            items: vec![terminal],
        },
    )
    .await
    .expect("lost acknowledgement replay should remain idempotent");

    // JOURNAL-NOTE: Full frame already present + retry means sync/acknowledge,
    // not "append it again and let future archaeology sort out the twins."
    assert_eq!(
        std::fs::metadata(&journal_path)
            .expect("journal metadata after acknowledgement replay")
            .len(),
        before
    );
}
