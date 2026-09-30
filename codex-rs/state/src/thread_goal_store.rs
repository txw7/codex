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
