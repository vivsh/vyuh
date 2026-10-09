//! Atomic reference-store reconciliation using the existing state mutex.
use crate::tasks::{
    LaneClaim, LaneHookAction, LaneOwnerPhase, LaneOwnerPoll, LanePoll, TaskCommit, TaskLane,
    TaskPoll, TaskRecord, TaskRuntimeError, TaskStatus,
};
use std::time::Duration;

use super::lane_owner::memory_owner_live;
use super::queries::checked_deadline;
use super::store::{MemoryState, MemoryTaskStore};
use super::workflow;

impl MemoryTaskStore {
    /// Stages recovery alongside cancellation delivery; errors restore the complete state.
    pub(super) async fn recover_poll(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
        prepared: &TaskPoll,
    ) -> Result<TaskPoll, TaskRuntimeError> {
        let mut state = self.state.lock().await;
        let mut staged = state.clone();
        let now = chrono::Utc::now();
        let mut cancelled = Vec::new();
        let mut lanes = Vec::new();
        for lane in &prepared.lanes {
            let claim = claims.iter().find(|claim| claim.lane == lane.lane);
            let owner = recover_owner(
                &mut staged,
                node,
                lane.lane,
                claim,
                prepared,
                now,
                self.lease_duration,
            )?;
            if owner.as_ref().is_some_and(|owner| owner.token.is_none()) {
                release_lane(&mut staged, node, lane.lane, now);
            }
            let tasks = recover_tasks(&mut staged, node, lane, &owner, now, &mut cancelled);
            lanes.push(LanePoll {
                lane: lane.lane,
                tasks,
                owner,
                reclaimed: 0,
                saturated: false,
                next_wake_in: Some(Duration::ZERO),
            });
        }
        let (_, result) = workflow::atomic_operation(
            &mut staged,
            None,
            &[],
            &cancelled,
            self.commit_batch_size,
            self.lease_duration,
            now,
        )?;
        let mut wake_lanes = prepared.wake_lanes.clone();
        crate::tasks::store::merge_wakes(&mut wake_lanes, result.wake_lanes);
        *state = staged;
        Ok(TaskPoll { lanes, wake_lanes })
    }
}

/// Releases abandoned ownership only; counters, continuation and scheduling remain unchanged.
fn release_lane(
    state: &mut MemoryState,
    node: &uuid::Uuid,
    lane: TaskLane,
    now: chrono::DateTime<chrono::Utc>,
) {
    for task in &mut state.tasks {
        if task.node_id == Some(*node)
            && task.lane == lane.as_str()
            && task.status == TaskStatus::Running
        {
            task.node_id = None;
            task.updated_at = now;
        }
    }
}

/// Restores live durable ownership and renews its lease before publishing recovered snapshots.
fn recover_owner(
    state: &mut MemoryState,
    node: &uuid::Uuid,
    lane: TaskLane,
    claim: Option<&LaneClaim>,
    prepared: &TaskPoll,
    now: chrono::DateTime<chrono::Utc>,
    lease: Duration,
) -> Result<Option<LaneOwnerPoll>, TaskRuntimeError> {
    let Some(claim) = claim.filter(|claim| claim.owner.is_some()) else {
        return Ok(None);
    };
    let row = state
        .lane_locks
        .get_mut(lane.as_str())
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))?;
    let live = crate::tasks::store::recovery_token(claim, prepared, row.owner_token.as_deref())
        && row.owner_id.as_deref() == Some(node.to_string().as_str())
        && memory_owner_live(row, now);
    if live {
        row.leased_until = checked_deadline(now, lease)?;
    }
    let action = match row.phase {
        LaneOwnerPhase::Idling => Some(LaneHookAction::Idle),
        LaneOwnerPhase::Busying => Some(LaneHookAction::Busy),
        _ => None,
    };
    Ok(Some(LaneOwnerPoll {
        token: live.then(|| row.owner_token.clone()).flatten(),
        generation: row.generation,
        phase: row.phase,
        action: live.then_some(action).flatten(),
        takeover: false,
    }))
}

/// Copies only still-current snapshots; authoritative mutations stay inside the store.
fn recover_tasks(
    state: &mut MemoryState,
    node: &uuid::Uuid,
    lane: &LanePoll,
    owner: &Option<LaneOwnerPoll>,
    now: chrono::DateTime<chrono::Utc>,
    cancelled: &mut Vec<TaskCommit>,
) -> Vec<TaskRecord> {
    let mut tasks = Vec::new();
    for expected in &lane.tasks {
        let Some(task) = state.tasks.iter_mut().find(|task| {
            task.id == expected.id
                && task.attempts == expected.attempts
                && task.node_id == Some(*node)
                && task.status == TaskStatus::Running
        }) else {
            continue;
        };
        if owner.as_ref().is_some_and(|owner| owner.token.is_none()) {
            task.node_id = None;
            task.updated_at = now;
        } else if task.cancelled {
            cancelled.push(TaskCommit::Cancel { task_id: task.id });
        } else {
            tasks.push(task.clone());
        }
    }
    tasks
}
