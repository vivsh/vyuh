//! Operation-local expanded flush budgeting; pending outcomes keep normal lease renewal.

use super::*;

/// Backpressures only a lane with deferred fan-out, borrowing existing uncommitted accounting.
pub(super) fn claim_capacity(
    lane: &LaneQueue,
    pending: &VecDeque<TaskCommit>,
    available: usize,
) -> usize {
    if !pending.iter().any(|commit| {
        commit.lane == lane.conf.lane()
            && matches!(commit.outcome, crate::tasks::TaskOutcome::All { .. })
    }) {
        return available;
    }
    available.min(
        lane.conf
            .concurrency()
            .saturating_sub(lane.running + lane.tasks.len() + lane.uncommitted),
    )
}

impl<S: AbstractTaskStore + Send + Sync + 'static> AbstractTaskRunner<S> {
    /// Trims only a join-containing turn, preserving the entire deferred suffix in order.
    pub(super) fn limit_commits(&mut self, commits: &mut Vec<TaskCommit>) {
        if !commits
            .iter()
            .any(|commit| matches!(commit.outcome, crate::tasks::TaskOutcome::All { .. }))
        {
            return;
        }
        let budget = self
            .batch_size
            .max(self.registry.config.all_limit().saturating_add(1));
        let mut weight = 0usize;
        let accepted = commits
            .iter()
            .take_while(|commit| {
                weight = weight.saturating_add(crate::tasks::store::all::weight(&commit.outcome));
                weight <= budget
            })
            .count();
        for commit in commits.drain(accepted..).rev() {
            self.pending_commits.push_front(commit);
        }
    }
}
