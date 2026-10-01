use std::future::Future;
use std::pin::Pin;

use codex_protocol::ThreadId;

use crate::GoalAccountingMode;
use crate::GoalAccountingOutcome;
use crate::GoalUpdate;
use crate::ThreadGoal;
use crate::ThreadGoalStatus;

/// Future returned by storage-neutral thread-goal operations.
pub type ThreadGoalStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// Storage-neutral authority for thread-goal state.
///
/// UPSTREAM-SEAM: goal execution/accounting needs goal state, not a SQLite
/// runtime. Keeping this trait in codex-state lets Local continue using the
/// existing SQL implementation while RamJournal supplies a RAM authority
/// without making the goal extension learn backend topology.
///
/// A feature asking for nine goal operations is a reasonable interface.
/// A feature asking for "the entire database, please" is a dependency leak
/// with excellent branding.
pub trait ThreadGoalStore: Send + Sync {
    fn get_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>>;

    fn replace_thread_goal_snapshot<'a>(
        &'a self,
        goal: &'a ThreadGoal,
    ) -> ThreadGoalStoreFuture<'a, ()>;

    fn has_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, bool>;

    fn clear_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, ()>;

    fn replace_thread_goal<'a>(
        &'a self,
        thread_id: ThreadId,
        objective: &'a str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> ThreadGoalStoreFuture<'a, ThreadGoal>;

    fn insert_thread_goal<'a>(
        &'a self,
        thread_id: ThreadId,
        objective: &'a str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> ThreadGoalStoreFuture<'a, Option<ThreadGoal>>;

    fn update_thread_goal(
        &self,
        thread_id: ThreadId,
        update: GoalUpdate,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>>;

    fn delete_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>>;

    fn account_thread_goal_usage<'a>(
        &'a self,
        thread_id: ThreadId,
        time_delta_seconds: i64,
        token_delta: i64,
        mode: GoalAccountingMode,
        expected_goal_id: Option<&'a str>,
    ) -> ThreadGoalStoreFuture<'a, GoalAccountingOutcome>;
}


/// Compatibility adapter for upstream callers that still hold the process
/// StateRuntime directly.
///
/// UPSTREAM-SEAM: The semantic interface is ThreadGoalStore. Delegating an
/// existing StateRuntime through its GoalStore keeps upstream tests/callers
/// source-compatible without making new code depend on the database runtime.
impl ThreadGoalStore for crate::StateRuntime {
    fn get_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move { self.thread_goals().get_thread_goal(thread_id).await })
    }

    fn replace_thread_goal_snapshot<'a>(
        &'a self,
        goal: &'a ThreadGoal,
    ) -> ThreadGoalStoreFuture<'a, ()> {
        Box::pin(async move { self.thread_goals().replace_thread_goal_snapshot(goal).await })
    }

    fn has_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, bool> {
        Box::pin(async move {
            self.thread_goals()
                .has_thread_goal_continuation_deferral(thread_id)
                .await
        })
    }

    fn clear_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, ()> {
        Box::pin(async move {
            self.thread_goals()
                .clear_thread_goal_continuation_deferral(thread_id)
                .await
        })
    }

    fn replace_thread_goal<'a>(
        &'a self,
        thread_id: ThreadId,
        objective: &'a str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> ThreadGoalStoreFuture<'a, ThreadGoal> {
        Box::pin(async move {
            self.thread_goals()
                .replace_thread_goal(thread_id, objective, status, token_budget)
                .await
        })
    }

    fn insert_thread_goal<'a>(
        &'a self,
        thread_id: ThreadId,
        objective: &'a str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> ThreadGoalStoreFuture<'a, Option<ThreadGoal>> {
        Box::pin(async move {
            self.thread_goals()
                .insert_thread_goal(thread_id, objective, status, token_budget)
                .await
        })
    }

    fn update_thread_goal(
        &self,
        thread_id: ThreadId,
        update: GoalUpdate,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move {
            self.thread_goals()
                .update_thread_goal(thread_id, update)
                .await
        })
    }

    fn delete_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move { self.thread_goals().delete_thread_goal(thread_id).await })
    }

    fn account_thread_goal_usage<'a>(
        &'a self,
        thread_id: ThreadId,
        time_delta_seconds: i64,
        token_delta: i64,
        mode: GoalAccountingMode,
        expected_goal_id: Option<&'a str>,
    ) -> ThreadGoalStoreFuture<'a, GoalAccountingOutcome> {
        Box::pin(async move {
            self.thread_goals()
                .account_thread_goal_usage(
                    thread_id,
                    time_delta_seconds,
                    token_delta,
                    mode,
                    expected_goal_id,
                )
                .await
        })
    }
}
