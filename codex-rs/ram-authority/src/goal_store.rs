use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::PoisonError;

use chrono::Utc;
use codex_protocol::ThreadId;
use codex_state::GoalAccountingMode;
use codex_state::GoalAccountingOutcome;
use codex_state::GoalUpdate;
use codex_state::ThreadGoal;
use codex_state::ThreadGoalStatus;
use codex_state::ThreadGoalStore;
use codex_state::ThreadGoalStoreFuture;
use uuid::Uuid;

#[derive(Debug, Default)]
struct GoalState {
    goals: HashMap<ThreadId, ThreadGoal>,
    continuation_deferrals: HashSet<ThreadId>,
}

/// RAM-authoritative thread-goal state.
///
/// FORK-RAM: Goal execution, accounting, and continuation policy are live
/// session state. RamJournal does not require a SQLite side authority merely so
/// a feature can remember one objective and a few counters.
///
/// Durable goal changes still enter the thread journal through the existing
/// ThreadGoalUpdated rollout items. This store owns runtime state, not a second
/// historical narrative.
#[derive(Debug, Default)]
pub struct RamGoalStore {
    state: Mutex<GoalState>,
}

impl RamGoalStore {
    fn new_goal(
        thread_id: ThreadId,
        objective: &str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> ThreadGoal {
        let now = Utc::now();
        ThreadGoal {
            thread_id,
            goal_id: Uuid::new_v4().to_string(),
            objective: objective.to_string(),
            status: status_after_budget_limit(status, 0, token_budget),
            token_budget,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: now,
            updated_at: now,
        }
    }
}

impl ThreadGoalStore for RamGoalStore {
    fn get_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .goals
                .get(&thread_id)
                .cloned())
        })
    }

    fn replace_thread_goal_snapshot<'a>(
        &'a self,
        goal: &'a ThreadGoal,
    ) -> ThreadGoalStoreFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.goals.insert(goal.thread_id, goal.clone());
            state.continuation_deferrals.insert(goal.thread_id);
            Ok(())
        })
    }

    fn has_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, bool> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .continuation_deferrals
                .contains(&thread_id))
        })
    }

    fn clear_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, ()> {
        Box::pin(async move {
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .continuation_deferrals
                .remove(&thread_id);
            Ok(())
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
            let goal = Self::new_goal(thread_id, objective, status, token_budget);
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .goals
                .insert(thread_id, goal.clone());
            Ok(goal)
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
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state
                .goals
                .get(&thread_id)
                .is_some_and(|goal| goal.status != ThreadGoalStatus::Complete)
            {
                return Ok(None);
            }

            let goal = Self::new_goal(thread_id, objective, status, token_budget);
            state.goals.insert(thread_id, goal.clone());
            Ok(Some(goal))
        })
    }

    fn update_thread_goal(
        &self,
        thread_id: ThreadId,
        update: GoalUpdate,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(goal) = state.goals.get_mut(&thread_id) else {
                return Ok(None);
            };

            if update
                .expected_goal_id
                .as_ref()
                .is_some_and(|expected| expected != &goal.goal_id)
            {
                return Ok(None);
            }

            let mut changed = false;
            if let Some(objective) = update.objective {
                goal.objective = objective;
                changed = true;
            }

            if let Some(token_budget) = update.token_budget {
                goal.token_budget = token_budget;
                changed = true;
            }

            if let Some(requested_status) = update.status {
                goal.status = next_status(
                    goal.status,
                    requested_status,
                    goal.tokens_used,
                    goal.token_budget,
                );
                changed = true;
            } else if update.token_budget.is_some()
                && goal.status == ThreadGoalStatus::Active
                && goal
                    .token_budget
                    .is_some_and(|budget| goal.tokens_used >= budget)
            {
                goal.status = ThreadGoalStatus::BudgetLimited;
            }

            if changed {
                goal.updated_at = Utc::now();
            }
            Ok(Some(goal.clone()))
        })
    }

    fn delete_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> ThreadGoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .goals
                .remove(&thread_id))
        })
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
            let time_delta_seconds = time_delta_seconds.max(0);
            let token_delta = token_delta.max(0);
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(goal) = state.goals.get_mut(&thread_id) else {
                return Ok(GoalAccountingOutcome::Unchanged(None));
            };

            if expected_goal_id.is_some_and(|expected| expected != goal.goal_id)
                || !status_matches_mode(goal.status, mode)
                || (time_delta_seconds == 0 && token_delta == 0)
            {
                return Ok(GoalAccountingOutcome::Unchanged(Some(goal.clone())));
            }

            let previous_status = goal.status;
            goal.time_used_seconds = goal
                .time_used_seconds
                .saturating_add(time_delta_seconds);
            goal.tokens_used = goal.tokens_used.saturating_add(token_delta);

            if budget_limit_applies(previous_status, mode)
                && goal
                    .token_budget
                    .is_some_and(|budget| goal.tokens_used >= budget)
            {
                goal.status = ThreadGoalStatus::BudgetLimited;
            }
            goal.updated_at = Utc::now();

            Ok(GoalAccountingOutcome::Updated(goal.clone()))
        })
    }
}

fn status_after_budget_limit(
    status: ThreadGoalStatus,
    tokens_used: i64,
    token_budget: Option<i64>,
) -> ThreadGoalStatus {
    if status == ThreadGoalStatus::Active
        && token_budget.is_some_and(|budget| tokens_used >= budget)
    {
        ThreadGoalStatus::BudgetLimited
    } else {
        status
    }
}

fn next_status(
    current: ThreadGoalStatus,
    requested: ThreadGoalStatus,
    tokens_used: i64,
    token_budget: Option<i64>,
) -> ThreadGoalStatus {
    if current == ThreadGoalStatus::BudgetLimited
        && matches!(
            requested,
            ThreadGoalStatus::Paused | ThreadGoalStatus::Blocked
        )
    {
        return current;
    }
    status_after_budget_limit(requested, tokens_used, token_budget)
}

fn status_matches_mode(status: ThreadGoalStatus, mode: GoalAccountingMode) -> bool {
    match mode {
        GoalAccountingMode::ActiveStatusOnly => status == ThreadGoalStatus::Active,
        GoalAccountingMode::ActiveOnly => {
            matches!(
                status,
                ThreadGoalStatus::Active | ThreadGoalStatus::BudgetLimited
            )
        }
        GoalAccountingMode::ActiveOrComplete => {
            matches!(
                status,
                ThreadGoalStatus::Active
                    | ThreadGoalStatus::BudgetLimited
                    | ThreadGoalStatus::Complete
            )
        }
        GoalAccountingMode::ActiveOrStopped => {
            matches!(
                status,
                ThreadGoalStatus::Active
                    | ThreadGoalStatus::Paused
                    | ThreadGoalStatus::Blocked
                    | ThreadGoalStatus::UsageLimited
                    | ThreadGoalStatus::BudgetLimited
            )
        }
    }
}

fn budget_limit_applies(status: ThreadGoalStatus, mode: GoalAccountingMode) -> bool {
    match mode {
        GoalAccountingMode::ActiveStatusOnly
        | GoalAccountingMode::ActiveOnly
        | GoalAccountingMode::ActiveOrComplete => status == ThreadGoalStatus::Active,
        GoalAccountingMode::ActiveOrStopped => status_matches_mode(status, mode),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn zero_budget_limits_a_new_active_goal_immediately() {
        let store = RamGoalStore::default();
        let thread_id = ThreadId::new();
        let goal = store
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                ThreadGoalStatus::Active,
                Some(0),
            )
            .await
            .expect("replace goal");

        assert_eq!(goal.status, ThreadGoalStatus::BudgetLimited);
    }

    #[tokio::test]
    async fn stale_goal_id_cannot_mutate_a_replacement() {
        let store = RamGoalStore::default();
        let thread_id = ThreadId::new();
        let original = store
            .replace_thread_goal(
                thread_id,
                "old",
                ThreadGoalStatus::Active,
                Some(100),
            )
            .await
            .expect("original goal");
        let replacement = store
            .replace_thread_goal(
                thread_id,
                "new",
                ThreadGoalStatus::Active,
                Some(100),
            )
            .await
            .expect("replacement goal");

        let stale = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: Some(original.goal_id),
                },
            )
            .await
            .expect("stale update");

        assert!(stale.is_none());
        assert_eq!(
            store
                .get_thread_goal(thread_id)
                .await
                .expect("read replacement"),
            Some(replacement)
        );
    }

    #[tokio::test]
    async fn active_only_accounting_applies_budget_limit() {
        let store = RamGoalStore::default();
        let thread_id = ThreadId::new();
        let goal = store
            .replace_thread_goal(
                thread_id,
                "count",
                ThreadGoalStatus::Active,
                Some(10),
            )
            .await
            .expect("goal");

        let outcome = store
            .account_thread_goal_usage(
                thread_id,
                3,
                10,
                GoalAccountingMode::ActiveOnly,
                Some(&goal.goal_id),
            )
            .await
            .expect("account usage");

        let GoalAccountingOutcome::Updated(updated) = outcome else {
            panic!("goal should update");
        };
        assert_eq!(updated.tokens_used, 10);
        assert_eq!(updated.time_used_seconds, 3);
        assert_eq!(updated.status, ThreadGoalStatus::BudgetLimited);
    }

    #[tokio::test]
    async fn snapshot_replacement_sets_and_clears_continuation_deferral() {
        let store = RamGoalStore::default();
        let thread_id = ThreadId::new();
        let goal = RamGoalStore::new_goal(
            thread_id,
            "resume later",
            ThreadGoalStatus::Paused,
            None,
        );

        store
            .replace_thread_goal_snapshot(&goal)
            .await
            .expect("replace snapshot");
        assert!(
            store
                .has_thread_goal_continuation_deferral(thread_id)
                .await
                .expect("read deferral")
        );

        store
            .clear_thread_goal_continuation_deferral(thread_id)
            .await
            .expect("clear deferral");
        assert!(
            !store
                .has_thread_goal_continuation_deferral(thread_id)
                .await
                .expect("read cleared deferral")
        );
    }
}
