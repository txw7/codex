use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::ThreadId;
use codex_thread_store::StoredModelContext;

#[derive(Clone, Debug)]
struct CachedContext {
    context: StoredModelContext,
    estimated_bytes: usize,
    last_used: u64,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<ThreadId, CachedContext>,
    retained_bytes: usize,
    clock: u64,
}

/// Byte-bounded cache for the latest decoded model context of loaded threads.
///
/// RESIDENCY-NOTE: This cache is explicitly *not* canonical history. Complete
/// committed history remains in compressed CJR frames. We retain only a bounded
/// decoded working set because making zstd decode the same current context for
/// every prompt would be an impressively literal interpretation of "cold."
#[derive(Debug)]
pub struct DecodedContextCache {
    max_bytes: usize,
    state: Mutex<CacheState>,
}

impl DecodedContextCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(CacheState::default()),
        }
    }

    pub fn get(&self, thread_id: ThreadId) -> Option<StoredModelContext> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.clock = state.clock.wrapping_add(1).max(1);
        let tick = state.clock;
        let entry = state.entries.get_mut(&thread_id)?;
        entry.last_used = tick;
        Some(entry.context.clone())
    }

    pub fn insert(&self, context: StoredModelContext) {
        let Some(estimated_bytes) = estimated_context_bytes(&context) else {
            // RESIDENCY-NOTE: Cache accounting failure is not allowed to make
            // model context unavailable. Skip retention and return the freshly
            // materialized value to the caller instead.
            return;
        };

        let thread_id = context.thread_id;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

        if let Some(previous) = state.entries.remove(&thread_id) {
            state.retained_bytes = state
                .retained_bytes
                .saturating_sub(previous.estimated_bytes);
        }

        if estimated_bytes > self.max_bytes {
            return;
        }

        state.clock = state.clock.wrapping_add(1).max(1);
        let tick = state.clock;
        state.retained_bytes = state.retained_bytes.saturating_add(estimated_bytes);
        state.entries.insert(
            thread_id,
            CachedContext {
                context,
                estimated_bytes,
                last_used: tick,
            },
        );

        while state.retained_bytes > self.max_bytes {
            let Some(victim) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(thread_id, _)| *thread_id)
            else {
                break;
            };

            if let Some(removed) = state.entries.remove(&victim) {
                state.retained_bytes = state
                    .retained_bytes
                    .saturating_sub(removed.estimated_bytes);
            }
        }
    }

    pub fn invalidate(&self, thread_id: ThreadId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(previous) = state.entries.remove(&thread_id) {
            state.retained_bytes = state
                .retained_bytes
                .saturating_sub(previous.estimated_bytes);
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retained_bytes
    }

    pub fn entry_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .len()
    }
}

fn estimated_context_bytes(context: &StoredModelContext) -> Option<usize> {
    serde_json::to_vec(context).ok().map(|encoded| encoded.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_context() -> StoredModelContext {
        StoredModelContext {
            thread_id: ThreadId::new(),
            items: Vec::new(),
        }
    }

    #[test]
    fn zero_budget_retains_nothing() {
        let cache = DecodedContextCache::new(0);
        let context = empty_context();
        let thread_id = context.thread_id;

        cache.insert(context);

        assert!(cache.get(thread_id).is_none());
        assert_eq!(cache.retained_bytes(), 0);
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn insert_get_and_invalidate_are_accounted() {
        let cache = DecodedContextCache::new(1024 * 1024);
        let context = empty_context();
        let thread_id = context.thread_id;

        cache.insert(context);
        assert!(cache.get(thread_id).is_some());
        assert!(cache.retained_bytes() > 0);
        assert_eq!(cache.entry_count(), 1);

        cache.invalidate(thread_id);
        assert!(cache.get(thread_id).is_none());
        assert_eq!(cache.retained_bytes(), 0);
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn least_recent_entry_is_evicted_under_byte_pressure() {
        let first = empty_context();
        let second = empty_context();
        let third = empty_context();
        let first_id = first.thread_id;
        let second_id = second.thread_id;
        let third_id = third.thread_id;

        let one = estimated_context_bytes(&first).expect("context should serialize");
        let two = estimated_context_bytes(&second).expect("context should serialize");
        let budget = one.saturating_add(two);

        let cache = DecodedContextCache::new(budget);
        cache.insert(first);
        cache.insert(second);
        assert!(cache.get(first_id).is_some());

        cache.insert(third);

        // first was touched after second, so second is the LRU victim.
        assert!(cache.get(first_id).is_some());
        assert!(cache.get(second_id).is_none());
        assert!(cache.get(third_id).is_some());
        assert!(cache.retained_bytes() <= budget);
    }
}
