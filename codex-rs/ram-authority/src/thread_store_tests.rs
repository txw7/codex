use std::path::PathBuf;

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
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadStore;

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
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: Some("done".to_string()),
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

#[allow(dead_code)]
fn _pathbuf_type_receipt(_: PathBuf) {}
