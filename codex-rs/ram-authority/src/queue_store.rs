use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::ThreadId;
use codex_state::QueuedUserSubmissionRecord;
use codex_thread_store::MAX_QUEUE_ITEMS;
use codex_thread_store::QueueStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use uuid::Uuid;

#[derive(Debug, Default)]
struct ThreadQueue {
    items: Vec<QueuedUserSubmissionRecord>,
    revision: i64,
}

#[derive(Debug, Default)]
struct QueueState {
    change_version: i64,
    threads: HashMap<ThreadId, ThreadQueue>,
}

/// Process-resident implementation of upstream's queue contract.
///
/// FORK-RAM: queued input is live scheduling state. In RamJournal mode it does
/// not earn a SQLite database merely by existing between two turns.
///
/// The monotonic global version and per-thread revisions mirror the information
/// upstream's SQLite watcher exposes, so app-server notification logic can keep
/// using the same abstraction instead of learning our storage religion.
#[derive(Debug, Default)]
pub struct RamQueueStore {
    state: Mutex<QueueState>,
}

impl RamQueueStore {
    fn mark_changed(state: &mut QueueState, thread_id: ThreadId) {
        state.change_version = state.change_version.saturating_add(1);
        let revision = state.change_version;
        state.threads.entry(thread_id).or_default().revision = revision;
    }
}

impl QueueStore for RamQueueStore {
    fn change_version(&self) -> ThreadStoreFuture<'_, i64> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .change_version)
        })
    }

    fn changes_since<'a>(
        &'a self,
        revision: i64,
        thread_ids: &'a [ThreadId],
    ) -> ThreadStoreFuture<'a, Vec<(ThreadId, i64)>> {
        Box::pin(async move {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let mut changed = thread_ids
                .iter()
                .filter_map(|thread_id| {
                    state
                        .threads
                        .get(thread_id)
                        .filter(|queue| queue.revision > revision)
                        .map(|queue| (*thread_id, queue.revision))
                })
                .collect::<Vec<_>>();
            changed.sort_by_key(|(_, revision)| *revision);
            Ok(changed)
        })
    }

    fn enqueue(
        &self,
        thread_id: ThreadId,
        payload: String,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state
                .threads
                .get(&thread_id)
                .is_some_and(|queue| queue.items.len() >= MAX_QUEUE_ITEMS)
            {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!(
                        "queue cannot contain more than {MAX_QUEUE_ITEMS} submissions"
                    ),
                });
            }

            let record = QueuedUserSubmissionRecord {
                id: Uuid::now_v7().to_string(),
                thread_id,
                payload,
            };
            state
                .threads
                .entry(thread_id)
                .or_default()
                .items
                .push(record.clone());
            Self::mark_changed(&mut state, thread_id);
            Ok(record)
        })
    }

    fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> ThreadStoreFuture<'_, Vec<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(queue) = state.threads.get(&thread_id) else {
                return Ok(Vec::new());
            };
            Ok(queue
                .items
                .iter()
                .skip(offset)
                .take(limit)
                .cloned()
                .collect())
        })
    }

    fn update(
        &self,
        thread_id: ThreadId,
        item_id: String,
        payload: String,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(queue) = state.threads.get_mut(&thread_id) else {
                return Ok(None);
            };
            let Some(item) = queue.items.iter_mut().find(|item| item.id == item_id) else {
                return Ok(None);
            };
            item.payload = payload;
            let updated = item.clone();
            Self::mark_changed(&mut state, thread_id);
            Ok(Some(updated))
        })
    }

    fn delete(&self, thread_id: ThreadId, item_id: String) -> ThreadStoreFuture<'_, bool> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(queue) = state.threads.get_mut(&thread_id) else {
                return Ok(false);
            };
            let Some(index) = queue.items.iter().position(|item| item.id == item_id) else {
                return Ok(false);
            };
            queue.items.remove(index);
            Self::mark_changed(&mut state, thread_id);
            Ok(true)
        })
    }

    fn reorder(&self, thread_id: ThreadId, item_ids: Vec<String>) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let queue = state.threads.entry(thread_id).or_default();

            let mut expected = queue
                .items
                .iter()
                .map(|item| item.id.clone())
                .collect::<Vec<_>>();
            let mut requested = item_ids.clone();
            expected.sort();
            requested.sort();
            if expected != requested {
                return Err(ThreadStoreError::InvalidRequest {
                    message:
                        "queue reorder must include every queued submission exactly once".to_string(),
                });
            }

            let mut by_id = queue
                .items
                .drain(..)
                .map(|item| (item.id.clone(), item))
                .collect::<HashMap<_, _>>();
            queue.items = item_ids
                .into_iter()
                .map(|id| {
                    by_id
                        .remove(&id)
                        .expect("validated queue permutation must contain every id")
                })
                .collect();

            Self::mark_changed(&mut state, thread_id);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queue_mutations_advance_thread_revisions() {
        let store = RamQueueStore::default();
        let thread_id = ThreadId::new();
        let initial = store.change_version().await.expect("initial version");

        let item = store
            .enqueue(thread_id, "one".to_string())
            .await
            .expect("enqueue");
        let after_enqueue = store.change_version().await.expect("enqueue version");
        assert!(after_enqueue > initial);
        assert_eq!(
            store
                .changes_since(initial, &[thread_id])
                .await
                .expect("changes"),
            vec![(thread_id, after_enqueue)]
        );

        store
            .update(thread_id, item.id.clone(), "two".to_string())
            .await
            .expect("update")
            .expect("existing item");
        let after_update = store.change_version().await.expect("update version");
        assert!(after_update > after_enqueue);
    }

    #[tokio::test]
    async fn reorder_requires_the_complete_queue_permutation() {
        let store = RamQueueStore::default();
        let thread_id = ThreadId::new();
        let first = store
            .enqueue(thread_id, "first".to_string())
            .await
            .expect("first enqueue");
        let second = store
            .enqueue(thread_id, "second".to_string())
            .await
            .expect("second enqueue");

        let error = store
            .reorder(thread_id, vec![first.id.clone()])
            .await
            .expect_err("partial reorder must fail");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));

        store
            .reorder(thread_id, vec![second.id.clone(), first.id.clone()])
            .await
            .expect("full reorder");

        let page = store
            .list_page(thread_id, 0, 10)
            .await
            .expect("list queue");
        assert_eq!(
            page.iter().map(|item| item.id.as_str()).collect::<Vec<_>>(),
            vec![second.id.as_str(), first.id.as_str()]
        );
    }

    #[tokio::test]
    async fn missing_update_and_delete_do_not_advance_change_version() {
        let store = RamQueueStore::default();
        let thread_id = ThreadId::new();
        let before = store.change_version().await.expect("before");

        assert!(
            store
                .update(thread_id, "missing".to_string(), "payload".to_string())
                .await
                .expect("missing update")
                .is_none()
        );
        assert!(
            !store
                .delete(thread_id, "missing".to_string())
                .await
                .expect("missing delete")
        );
        assert_eq!(store.change_version().await.expect("after"), before);
    }
}
