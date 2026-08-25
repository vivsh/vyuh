//! Durable single-owner coordination for opt-in task lanes.

use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::{
    db,
    tasks::{
        LaneClaim, LaneHookAction, LaneHookResult, LaneOwnerPhase, LaneOwnerPoll, LanePoll,
        TaskError, TaskLaneConf,
    },
};

use super::{
    claim::{next_task_deadline, probe_candidates},
    common::{DbTaskStore, add_time},
    model::{TaskLaneLockPatch, TaskLaneLockRow, TaskRow},
};

struct OwnerTurn<'a> {
    claim: &'a LaneClaim,
    lane: &'a TaskLaneConf,
    conf: &'a crate::tasks::TaskStoreConf,
    runner_id: &'a str,
    now: DateTime<Utc>,
}

impl DbTaskStore {
    /// Coordinates one locked lane and claims work only after its flush condition fires.
    pub(super) async fn claim_owned_lane(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        claim: &LaneClaim,
        lane: &TaskLaneConf,
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
    ) -> Result<LanePoll, TaskError> {
        let mut row = load_for_update(transaction, claim.lane.as_str())
            .await?
            .ok_or_else(|| {
                TaskError::InvalidConfig(format!(
                    "task lane lock '{}' is not initialized",
                    claim.lane
                ))
            })?;
        let had_owner = row.owner_token.is_some();
        let acquiring = claim
            .owner
            .as_ref()
            .is_some_and(|owner| owner.token.is_none());
        let turn = OwnerTurn {
            claim,
            lane,
            conf,
            runner_id,
            now,
        };
        let result = if !self.resolve_owner(&mut row, &turn)? {
            wait_poll(transaction, row, &turn).await
        } else {
            if let Some(result) = claim.owner.as_ref().and_then(|owner| owner.hook.as_ref()) {
                self.apply_hook(transaction, &mut row, result, &turn)
                    .await?;
            }
            if row.owner_token.is_none() {
                persist(transaction, &row).await?;
                wait_poll(transaction, row, &turn).await
            } else {
                self.poll_owner(transaction, row, &turn).await
            }
        };
        let mut poll = result?;
        mark_takeover(&mut poll, acquiring && had_owner);
        Ok(poll)
    }

    fn resolve_owner(
        &self,
        row: &mut TaskLaneLockRow,
        turn: &OwnerTurn<'_>,
    ) -> Result<bool, TaskError> {
        let requested = turn
            .claim
            .owner
            .as_ref()
            .and_then(|owner| owner.token.as_deref());
        if requested.is_some_and(|token| current_owner(row, turn, token)) {
            row.leased_until = Some(owner_deadline(turn.now, self.lease_duration)?);
            row.updated_at = turn.now;
            return Ok(true);
        }
        if requested.is_some() || owner_live(row, turn.now) {
            return Ok(false);
        }
        row.owner_id = Some(turn.runner_id.into());
        row.owner_token = Some(uuid::Uuid::now_v7().to_string());
        row.leased_until = Some(owner_deadline(turn.now, self.lease_duration)?);
        row.updated_at = turn.now;
        Ok(true)
    }

    async fn apply_hook(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        row: &mut TaskLaneLockRow,
        result: &LaneHookResult,
        turn: &OwnerTurn<'_>,
    ) -> Result<(), TaskError> {
        let phase = LaneOwnerPhase::from_i16(row.phase)?;
        if row.generation != result.generation || !hook_matches(phase, result.action) {
            return Ok(());
        }
        match (&result.action, &result.result) {
            (LaneHookAction::Idle, Ok(())) => self.finish_idle(transaction, row, turn).await?,
            (LaneHookAction::Idle, Err(error)) => fail_idle(row, error, turn.now),
            (LaneHookAction::Busy, Ok(())) => activate(row, turn.now),
            (LaneHookAction::Busy, Err(error)) => fail_busy(row, error, turn)?,
        }
        Ok(())
    }

    async fn finish_idle(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        row: &mut TaskLaneLockRow,
        turn: &OwnerTurn<'_>,
    ) -> Result<(), TaskError> {
        let candidates = self.candidates(transaction, turn).await?;
        if candidates.is_empty() {
            release(row, LaneOwnerPhase::Idle, turn.now);
        } else if turn
            .lane
            .lane_lock()
            .and_then(|lock| lock.busy_hook())
            .is_some()
        {
            transition(row, LaneOwnerPhase::Busying, turn.now)?;
        } else {
            activate(row, turn.now);
        }
        Ok(())
    }

    async fn poll_owner(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        mut row: TaskLaneLockRow,
        turn: &OwnerTurn<'_>,
    ) -> Result<LanePoll, TaskError> {
        let phase = LaneOwnerPhase::from_i16(row.phase)?;
        if let Some(action) = phase_action(phase) {
            persist(transaction, &row).await?;
            return owned_poll(turn.claim.lane, row, action, None);
        }
        let quiescent = turn
            .claim
            .owner
            .as_ref()
            .is_some_and(|owner| owner.quiescent);
        if matches!(phase, LaneOwnerPhase::Active | LaneOwnerPhase::IdleFailed) && !quiescent {
            persist(transaction, &row).await?;
            return owned_poll(turn.claim.lane, row, None, None);
        }
        let candidates = self.candidates(transaction, turn).await?;
        let poll = self
            .phase_poll(transaction, &mut row, candidates, turn)
            .await?;
        persist(transaction, &row).await?;
        Ok(poll)
    }

    async fn phase_poll(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        row: &mut TaskLaneLockRow,
        candidates: Vec<TaskRow>,
        turn: &OwnerTurn<'_>,
    ) -> Result<LanePoll, TaskError> {
        let mut phase = LaneOwnerPhase::from_i16(row.phase)?;
        let completed_work = turn
            .claim
            .owner
            .as_ref()
            .is_some_and(|owner| owner.completed_work);
        if phase == LaneOwnerPhase::IdleFailed && completed_work {
            activate(row, turn.now);
            phase = LaneOwnerPhase::Active;
        }
        if candidates.is_empty() {
            row.flushing = false;
            return self.empty_poll(transaction, row, phase, turn).await;
        }
        row.empty_since = None;
        if matches!(phase, LaneOwnerPhase::Idle | LaneOwnerPhase::BusyFailed) {
            return self.start_busy(transaction, row, turn).await;
        }
        if row.flushing || should_flush(&candidates, turn.lane, turn.now)? {
            row.flushing = true;
            let allow_claim = turn
                .claim
                .owner
                .as_ref()
                .is_some_and(|owner| owner.allow_claim);
            if allow_claim {
                return self.claim_flush(transaction, row, turn).await;
            }
            return owned_poll(turn.claim.lane, row.clone(), None, Some(Duration::ZERO));
        }
        let wake = flush_wake(&candidates, turn.lane, turn.now)?;
        owned_poll(turn.claim.lane, row.clone(), None, wake)
    }

    async fn empty_poll(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        row: &mut TaskLaneLockRow,
        phase: LaneOwnerPhase,
        turn: &OwnerTurn<'_>,
    ) -> Result<LanePoll, TaskError> {
        if matches!(
            phase,
            LaneOwnerPhase::Idle | LaneOwnerPhase::IdleFailed | LaneOwnerPhase::BusyFailed
        ) {
            release(row, phase, turn.now);
            return owned_poll(
                turn.claim.lane,
                row.clone(),
                None,
                next_task_deadline(transaction, turn.claim.lane.as_str(), turn.now).await?,
            );
        }
        let quiescent = turn
            .claim
            .owner
            .as_ref()
            .is_some_and(|owner| owner.quiescent);
        if !quiescent {
            return owned_poll(turn.claim.lane, row.clone(), None, None);
        }
        let idle_after = turn
            .lane
            .lane_lock()
            .map_or(Duration::ZERO, |lock| lock.idle_duration());
        let started = *row.empty_since.get_or_insert(turn.now);
        if elapsed(started, turn.now) < idle_after {
            return owned_poll(
                turn.claim.lane,
                row.clone(),
                None,
                Some(idle_after.saturating_sub(elapsed(started, turn.now))),
            );
        }
        self.start_idle(row, turn)
    }

    fn start_idle(
        &self,
        row: &mut TaskLaneLockRow,
        turn: &OwnerTurn<'_>,
    ) -> Result<LanePoll, TaskError> {
        if turn
            .lane
            .lane_lock()
            .and_then(|lock| lock.idle_hook())
            .is_some()
        {
            transition(row, LaneOwnerPhase::Idling, turn.now)?;
            return owned_poll(
                turn.claim.lane,
                row.clone(),
                Some(LaneHookAction::Idle),
                None,
            );
        }
        release(row, LaneOwnerPhase::Idle, turn.now);
        owned_poll(turn.claim.lane, row.clone(), None, None)
    }

    async fn start_busy(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        row: &mut TaskLaneLockRow,
        turn: &OwnerTurn<'_>,
    ) -> Result<LanePoll, TaskError> {
        if row.hook_retry_at.is_some_and(|retry| retry > turn.now) {
            release(row, LaneOwnerPhase::BusyFailed, turn.now);
            persist(transaction, row).await?;
            return owned_poll(
                turn.claim.lane,
                row.clone(),
                None,
                row.hook_retry_at
                    .and_then(|at| (at - turn.now).to_std().ok()),
            );
        }
        if turn
            .lane
            .lane_lock()
            .and_then(|lock| lock.busy_hook())
            .is_some()
        {
            transition(row, LaneOwnerPhase::Busying, turn.now)?;
            return owned_poll(
                turn.claim.lane,
                row.clone(),
                Some(LaneHookAction::Busy),
                None,
            );
        }
        activate(row, turn.now);
        self.claim_flush(transaction, row, turn).await
    }

    async fn claim_flush(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        row: &TaskLaneLockRow,
        turn: &OwnerTurn<'_>,
    ) -> Result<LanePoll, TaskError> {
        let lock = turn
            .lane
            .lane_lock()
            .ok_or_else(|| TaskError::InvalidConfig("locked lane lost its policy".into()))?;
        let claim = LaneClaim {
            lane: turn.claim.lane,
            limit: lock.batch_size(),
            owner: None,
        };
        let claim_turn = super::claim::ClaimTurn {
            runner_id: turn.runner_id,
            claim: &claim,
            rate: turn.lane.global_rate(),
            retry: turn.lane.retry_policy(),
            conf: turn.conf,
            now: turn.now,
        };
        let mut poll = self.claim_lane(transaction, claim_turn).await?;
        poll.owner = Some(owner_poll(row, None)?);
        Ok(poll)
    }

    async fn candidates(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        turn: &OwnerTurn<'_>,
    ) -> Result<Vec<TaskRow>, TaskError> {
        let size = turn.lane.lane_lock().map_or(1, |lock| lock.batch_size());
        probe_candidates(transaction, turn.now, turn.claim.lane.as_str(), size).await
    }
}

fn mark_takeover(poll: &mut LanePoll, candidate: bool) {
    if candidate
        && let Some(owner) = &mut poll.owner
        && owner.token.is_some()
    {
        owner.takeover = true;
    }
}

fn should_flush(
    rows: &[TaskRow],
    lane: &TaskLaneConf,
    now: DateTime<Utc>,
) -> Result<bool, TaskError> {
    let lane_lock = lane
        .lane_lock()
        .ok_or_else(|| TaskError::InvalidConfig("locked lane lost its policy".into()))?;
    Ok(
        rows.len() >= lane_lock.batch_size()
            || flush_wake(rows, lane, now)? == Some(Duration::ZERO),
    )
}

fn flush_wake(
    rows: &[TaskRow],
    lane: &TaskLaneConf,
    now: DateTime<Utc>,
) -> Result<Option<Duration>, TaskError> {
    let Some(deadline) = lane.lane_lock().and_then(|lock| lock.batch_deadline()) else {
        return Ok(None);
    };
    let Some(oldest) = rows
        .first()
        .map(|row| row.ready_at.unwrap_or(row.created_at))
    else {
        return Ok(None);
    };
    let due = add_time(
        oldest,
        ChronoDuration::from_std(deadline).map_err(|_| {
            TaskError::InvalidConfig("lane lock deadline is outside chrono bounds".into())
        })?,
        "lane lock deadline",
    )?;
    Ok(Some((due - now).to_std().unwrap_or(Duration::ZERO)))
}

fn owner_poll(
    row: &TaskLaneLockRow,
    action: Option<LaneHookAction>,
) -> Result<LaneOwnerPoll, TaskError> {
    Ok(LaneOwnerPoll {
        token: row.owner_token.clone(),
        generation: row.generation,
        phase: LaneOwnerPhase::from_i16(row.phase)?,
        action,
        takeover: false,
    })
}

fn owned_poll(
    lane: crate::tasks::TaskLane,
    row: TaskLaneLockRow,
    action: impl Into<Option<LaneHookAction>>,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskError> {
    Ok(LanePoll {
        lane,
        tasks: Vec::new(),
        reclaimed: 0,
        saturated: false,
        next_wake_in: wake,
        owner: Some(owner_poll(&row, action.into())?),
    })
}

async fn wait_poll(
    transaction: &mut db::DbTransaction<'_>,
    row: TaskLaneLockRow,
    turn: &OwnerTurn<'_>,
) -> Result<LanePoll, TaskError> {
    let wake = row
        .leased_until
        .and_then(|at| (at - turn.now).to_std().ok())
        .or(next_task_deadline(transaction, turn.claim.lane.as_str(), turn.now).await?);
    Ok(LanePoll {
        lane: turn.claim.lane,
        tasks: Vec::new(),
        reclaimed: 0,
        saturated: false,
        next_wake_in: wake,
        owner: Some(LaneOwnerPoll {
            token: None,
            generation: row.generation,
            phase: LaneOwnerPhase::from_i16(row.phase)?,
            action: None,
            takeover: false,
        }),
    })
}

fn current_owner(row: &TaskLaneLockRow, turn: &OwnerTurn<'_>, token: &str) -> bool {
    row.owner_id.as_deref() == Some(turn.runner_id)
        && row.owner_token.as_deref() == Some(token)
        && owner_live(row, turn.now)
}

fn owner_live(row: &TaskLaneLockRow, now: DateTime<Utc>) -> bool {
    row.owner_token.is_some() && row.leased_until.is_some_and(|deadline| deadline > now)
}

fn hook_matches(phase: LaneOwnerPhase, action: LaneHookAction) -> bool {
    matches!(
        (phase, action),
        (LaneOwnerPhase::Idling, LaneHookAction::Idle)
            | (LaneOwnerPhase::Busying, LaneHookAction::Busy)
    )
}

fn phase_action(phase: LaneOwnerPhase) -> Option<LaneHookAction> {
    match phase {
        LaneOwnerPhase::Idling => Some(LaneHookAction::Idle),
        LaneOwnerPhase::Busying => Some(LaneHookAction::Busy),
        _ => None,
    }
}

fn transition(
    row: &mut TaskLaneLockRow,
    phase: LaneOwnerPhase,
    now: DateTime<Utc>,
) -> Result<(), TaskError> {
    row.generation = row.generation.checked_add(1).ok_or_else(|| {
        TaskError::TaskExecutionError("lane lifecycle generation overflowed".into())
    })?;
    row.phase = phase as i16;
    row.last_hook_error = None;
    row.hook_retry_at = None;
    row.updated_at = now;
    Ok(())
}

fn activate(row: &mut TaskLaneLockRow, now: DateTime<Utc>) {
    row.phase = LaneOwnerPhase::Active as i16;
    row.empty_since = None;
    row.hook_retry_at = None;
    row.last_hook_error = None;
    row.updated_at = now;
}

fn release(row: &mut TaskLaneLockRow, phase: LaneOwnerPhase, now: DateTime<Utc>) {
    row.owner_id = None;
    row.owner_token = None;
    row.leased_until = None;
    row.phase = phase as i16;
    row.flushing = false;
    row.updated_at = now;
}

fn fail_idle(row: &mut TaskLaneLockRow, error: &str, now: DateTime<Utc>) {
    row.last_hook_error = Some(error.into());
    release(row, LaneOwnerPhase::IdleFailed, now);
}

fn fail_busy(
    row: &mut TaskLaneLockRow,
    error: &str,
    turn: &OwnerTurn<'_>,
) -> Result<(), TaskError> {
    row.last_hook_error = Some(error.into());
    row.hook_retry_at = Some(owner_deadline(turn.now, turn.conf.poll_interval)?);
    release(row, LaneOwnerPhase::BusyFailed, turn.now);
    Ok(())
}

fn elapsed(started: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    (now - started).to_std().unwrap_or(Duration::ZERO)
}

fn owner_deadline(now: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>, TaskError> {
    let duration = ChronoDuration::from_std(duration).map_err(|_| {
        TaskError::InvalidConfig("lane owner duration is outside chrono bounds".into())
    })?;
    add_time(now, duration, "lane owner duration")
}

async fn persist(
    transaction: &mut db::DbTransaction<'_>,
    row: &TaskLaneLockRow,
) -> Result<(), TaskError> {
    let table = DbTaskStore::lane_lock_table();
    let patch = TaskLaneLockPatch {
        owner_id: row.owner_id.clone(),
        owner_token: row.owner_token.clone(),
        leased_until: row.leased_until,
        phase: row.phase,
        flushing: row.flushing,
        empty_since: row.empty_since,
        generation: row.generation,
        hook_retry_at: row.hook_retry_at,
        last_hook_error: row.last_hook_error.clone(),
        updated_at: row.updated_at,
    };
    db::from(&table)
        .filter(table.lane_name.eq(db::val(row.lane_name.clone())))
        .update(&patch)
        .exec(transaction)
        .await?;
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
async fn load_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<TaskLaneLockRow>, TaskError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::lane_lock_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .for_update()
        .first::<TaskLaneLockRow>()
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
async fn load_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<TaskLaneLockRow>, TaskError> {
    let table = DbTaskStore::lane_lock_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .first::<TaskLaneLockRow>()
        .exec(transaction)
        .await?)
}
