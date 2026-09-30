use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_agent_graph_store::AgentGraphStore;
use codex_agent_graph_store::AgentGraphStoreFuture;
use codex_agent_graph_store::ThreadSpawnEdgeStatus;
use codex_protocol::ThreadId;

#[derive(Clone, Copy, Debug)]
struct Edge {
    parent: ThreadId,
    status: ThreadSpawnEdgeStatus,
}

/// RAM-authoritative parent/child topology for spawned agents.
///
/// FORK-RAM: The live graph is runtime state. Durable ancestry is reconstructed
/// from committed thread lineage, not maintained in a second SQLite authority
/// just so two stores can eventually disagree with more confidence.
///
/// The map is keyed by child because upstream's contract permits at most one
/// persisted parent per child.
#[derive(Debug, Default)]
pub struct RamAgentGraphStore {
    edges_by_child: Mutex<HashMap<ThreadId, Edge>>,
}

impl RamAgentGraphStore {
    fn matching_children(
        edges: &HashMap<ThreadId, Edge>,
        parent: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> Vec<ThreadId> {
        let mut children = edges
            .iter()
            .filter_map(|(child, edge)| {
                (edge.parent == parent
                    && status_filter.is_none_or(|status| edge.status == status))
                .then_some(*child)
            })
            .collect::<Vec<_>>();
        children.sort_by_key(ToString::to_string);
        children
    }
}

impl AgentGraphStore for RamAgentGraphStore {
    fn upsert_thread_spawn_edge(
        &self,
        parent_thread_id: ThreadId,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()> {
        Box::pin(async move {
            self.edges_by_child
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(
                    child_thread_id,
                    Edge {
                        parent: parent_thread_id,
                        status,
                    },
                );
            Ok(())
        })
    }

    fn set_thread_spawn_edge_status(
        &self,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()> {
        Box::pin(async move {
            if let Some(edge) = self
                .edges_by_child
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_mut(&child_thread_id)
            {
                edge.status = status;
            }
            Ok(())
        })
    }

    fn list_thread_spawn_children(
        &self,
        parent_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move {
            let edges = self
                .edges_by_child
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            Ok(Self::matching_children(
                &edges,
                parent_thread_id,
                status_filter,
            ))
        })
    }

    fn list_thread_spawn_descendants(
        &self,
        root_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move {
            let edges = self
                .edges_by_child
                .lock()
                .unwrap_or_else(PoisonError::into_inner);

            let mut descendants = Vec::new();
            let mut queue = VecDeque::from([root_thread_id]);

            while let Some(parent) = queue.pop_front() {
                let children = Self::matching_children(&edges, parent, status_filter);
                queue.extend(children.iter().copied());
                descendants.extend(children);
            }

            Ok(descendants)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thread_id(suffix: u128) -> ThreadId {
        ThreadId::from_string(&format!("00000000-0000-0000-0000-{suffix:012}"))
            .expect("valid thread id")
    }

    #[tokio::test]
    async fn descendants_are_breadth_first_then_stable_by_thread_id() {
        let store = RamAgentGraphStore::default();
        let root = thread_id(1);
        let child_b = thread_id(3);
        let child_a = thread_id(2);
        let grandchild = thread_id(4);

        store
            .upsert_thread_spawn_edge(root, child_b, ThreadSpawnEdgeStatus::Open)
            .await
            .expect("insert child b");
        store
            .upsert_thread_spawn_edge(root, child_a, ThreadSpawnEdgeStatus::Open)
            .await
            .expect("insert child a");
        store
            .upsert_thread_spawn_edge(child_a, grandchild, ThreadSpawnEdgeStatus::Open)
            .await
            .expect("insert grandchild");

        assert_eq!(
            store
                .list_thread_spawn_descendants(root, None)
                .await
                .expect("list descendants"),
            vec![child_a, child_b, grandchild]
        );
    }

    #[tokio::test]
    async fn status_filter_prunes_closed_subtrees() {
        let store = RamAgentGraphStore::default();
        let root = thread_id(10);
        let closed_child = thread_id(11);
        let open_grandchild = thread_id(12);

        store
            .upsert_thread_spawn_edge(root, closed_child, ThreadSpawnEdgeStatus::Closed)
            .await
            .expect("insert closed child");
        store
            .upsert_thread_spawn_edge(
                closed_child,
                open_grandchild,
                ThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("insert open grandchild");

        assert!(
            store
                .list_thread_spawn_descendants(root, Some(ThreadSpawnEdgeStatus::Open))
                .await
                .expect("list open descendants")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn upserting_a_child_replaces_its_parent_and_status() {
        let store = RamAgentGraphStore::default();
        let first_parent = thread_id(20);
        let second_parent = thread_id(21);
        let child = thread_id(22);

        store
            .upsert_thread_spawn_edge(first_parent, child, ThreadSpawnEdgeStatus::Open)
            .await
            .expect("first edge");
        store
            .upsert_thread_spawn_edge(second_parent, child, ThreadSpawnEdgeStatus::Closed)
            .await
            .expect("replacement edge");

        assert!(
            store
                .list_thread_spawn_children(first_parent, None)
                .await
                .expect("old parent children")
                .is_empty()
        );
        assert_eq!(
            store
                .list_thread_spawn_children(second_parent, Some(ThreadSpawnEdgeStatus::Closed))
                .await
                .expect("new parent children"),
            vec![child]
        );
    }
}
