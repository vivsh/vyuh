//! Error-path reconciliation. Normal operations never call these locking reads.
use std::collections::HashMap;

use super::{common::DbTaskStore, flush, model::TaskRow, runtime};
use crate::tasks::store::StoreFailure;
use crate::{
    db,
    tasks::{
        CommitResult, LaneClaim, LaneHookAction, LaneOwnerPhase, LaneOwnerPoll, LanePoll,
        TaskCommit, TaskId, TaskPoll, TaskRecord, TaskRuntimeError,
    },
};

impl DbTaskStore {
    /// Returns only this incarnation's abandoned lane claims under the existing lane lock.
    pub(super) async fn release_lane_claims(
        &self,
        tx: &mut db::DbTransaction<'_>,
        node: &uuid::Uuid,
        lane: crate::tasks::TaskLane,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), TaskRuntimeError> {
        let table = Self::table();
        loop {
            let query = db::from(&table)
                .filter(table.node_id.eq(db::val(Some(*node))))
                .filter(table.lane_name.eq(db::val(lane.as_str().to_string())))
                .filter(table.status.eq(db::val(1_i16)))
                .sort(table.id.asc());
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            let query = {
                use crate::db::backend::RowLockExt as _;
                query.for_update()
            };
            let mut rows = query
                .slice::<TaskRow>(0, self.batch_size)
                .exec(&mut *tx)
                .await?;
            if rows.is_empty() {
                return Ok(());
            }
            for row in &mut rows {
                row.node_id = None;
                row.updated_at = now;
            }
            flush::batch_update_rows(tx, &rows, self.batch_size).await?;
        }
    }

    /// Reads exact prepared identities after waiting for their uncertain transaction.
    pub(super) async fn recover_claims_impl(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
        prepared: &TaskPoll,
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        let mut tx = self.pool.begin().await?;
        serialize_sqlite(&mut tx).await?;
        let owners = recover_owners(&mut tx, node, claims, prepared, self.lease_duration).await?;
        let now = runtime::statement_now(&mut tx).await?;
        let mut lost: Vec<_> = owners
            .iter()
            .filter(|(_, owner)| owner.token.is_none())
            .map(|(lane, _)| *lane)
            .collect();
        lost.sort_unstable_by_key(|lane| lane.as_str());
        for lane in lost {
            self.release_lane_claims(&mut tx, node, lane, now).await?;
        }
        let ids = prepared
            .lanes
            .iter()
            .flat_map(|lane| &lane.tasks)
            .map(|task| task.id.into_uuid())
            .collect();
        let mut rows = load_rows(&mut tx, ids, self.batch_size).await?;
        let (poll, cancelled, released) = recovered_poll(node, prepared, owners, &mut rows, now)?;
        flush::batch_update_rows(&mut tx, &released, self.batch_size).await?;
        let mut poll = self
            .finish_recovered(&mut tx, node, poll, &cancelled, now)
            .await?;
        crate::tasks::store::merge_wakes(&mut poll.wake_lanes, prepared.wake_lanes.iter().copied());
        match tx.commit().await {
            Ok(()) => Ok(poll),
            Err(error) => Err(StoreFailure::CommitUnknown {
                error: error.into(),
                prepared: poll,
            }),
        }
    }

    /// Retries only current executions, using the existing workflow transition engine.
    pub(super) async fn recover_outputs_impl(
        &self,
        _node: &uuid::Uuid,
        commits: &[TaskCommit],
        prepared: &CommitResult,
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        let conf = &self.runtime_conf;
        let commands = flush::prepare_commits(commits, self.batch_size, conf.max_all_children)?;
        let mut tx = self.pool.begin().await?;
        serialize_sqlite(&mut tx).await?;
        lock_output_lanes(&mut tx, commits, conf).await?;
        let ids = commits
            .iter()
            .map(|commit| commit.task_id().into_uuid())
            .collect();
        let rows = load_rows(&mut tx, ids, self.batch_size).await?;
        let now = runtime::statement_now(&mut tx).await?;
        let owned = rows.into_values().collect();
        let (children, deliveries, waits, rejected) = self
            .commit_outcomes_tx(&mut tx, commits, commands, conf, now, Some(owned))
            .await?;
        let mut wake_lanes = flush::finalize_workflow(
            &mut tx,
            &children,
            deliveries,
            &waits,
            conf,
            now,
            self.batch_size,
        )
        .await?;
        crate::tasks::store::merge_wakes(&mut wake_lanes, prepared.wake_lanes.iter().copied());
        let result = CommitResult {
            wake_lanes,
            rejected,
        };
        match tx.commit().await {
            Ok(()) => Ok(result),
            Err(error) => Err(StoreFailure::CommitUnknown {
                error: error.into(),
                prepared: result,
            }),
        }
    }

    /// Finalizes cancellations found during claim reconciliation atomically with delivery.
    async fn finish_recovered(
        &self,
        tx: &mut db::DbTransaction<'_>,
        _node: &uuid::Uuid,
        mut poll: TaskPoll,
        cancelled: &[TaskCommit],
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<TaskPoll, TaskRuntimeError> {
        if cancelled.is_empty() {
            return Ok(poll);
        }
        let conf = &self.runtime_conf;
        let commands = flush::prepare_commits(cancelled, self.batch_size, conf.max_all_children)?;
        let (children, deliveries, waits, _) = self
            .commit_outcomes_tx(tx, cancelled, commands, conf, now, None)
            .await?;
        poll.wake_lanes.extend(
            flush::finalize_workflow(
                tx,
                &children,
                deliveries,
                &waits,
                conf,
                now,
                self.batch_size,
            )
            .await?,
        );
        Ok(poll)
    }
}

/// Reserves SQLite's writer before the first read, so an old snapshot cannot settle a commit.
/// The zero-row write changes no data and is restricted to error-path recovery.
async fn serialize_sqlite(tx: &mut db::DbTransaction<'_>) -> Result<(), TaskRuntimeError> {
    #[cfg(feature = "sqlite")]
    {
        use crate::db::DbSession as _;
        tx.execute(db::Statement::raw(
            "UPDATE vyuh_tasks SET node_id = node_id WHERE 0",
        ))
        .await?;
    }
    #[cfg(not(feature = "sqlite"))]
    let _ = tx;
    Ok(())
}

/// Locks lanes in the same order as outcome acceptance, before reading any tasks.
async fn lock_output_lanes(
    tx: &mut db::DbTransaction<'_>,
    commits: &[TaskCommit],
    conf: &crate::tasks::TaskStoreConf,
) -> Result<(), TaskRuntimeError> {
    let names: std::collections::BTreeSet<_> = commits
        .iter()
        .filter(|commit| {
            conf.lanes
                .iter()
                .any(|lane| Some(lane.lane()) == commit.lane() && lane.lane_lock().is_some())
        })
        .filter_map(|commit| commit.lane().map(|lane| lane.as_str()))
        .collect();
    for name in names {
        flush::load_lane_lock(tx, name).await?;
    }
    Ok(())
}

/// Rebuilds lifecycle evidence from durable rows, never from an old response's phase.
async fn recover_owners(
    tx: &mut db::DbTransaction<'_>,
    node: &uuid::Uuid,
    claims: &[LaneClaim],
    prepared: &TaskPoll,
    lease: std::time::Duration,
) -> Result<HashMap<crate::tasks::TaskLane, LaneOwnerPoll>, TaskRuntimeError> {
    let mut ordered: Vec<_> = claims
        .iter()
        .filter(|claim| claim.owner.is_some())
        .collect();
    ordered.sort_unstable_by_key(|claim| claim.lane.as_str());
    let mut rows = Vec::new();
    for claim in ordered {
        let row = flush::load_lane_lock(tx, claim.lane.as_str())
            .await?
            .ok_or_else(|| TaskRuntimeError::UnknownLane(claim.lane.to_string()))?;
        let known =
            crate::tasks::store::recovery_token(claim, prepared, row.owner_token.as_deref());
        rows.push((claim.lane, row, known));
    }
    let now = runtime::statement_now(tx).await?;
    let node = node.to_string();
    for (_, row, known) in &mut rows {
        if *known
            && row.owner_id.as_deref() == Some(&node)
            && row.leased_until.is_some_and(|until| until > now)
        {
            let duration = chrono::Duration::from_std(lease).map_err(|_| {
                TaskRuntimeError::InvalidConfig("invalid lane lease duration".into())
            })?;
            row.leased_until = Some(super::common::add_time(
                now,
                duration,
                "lane recovery lease",
            )?);
            row.updated_at = now;
            super::lane_owner::persist(tx, row).await?;
        }
    }
    rows.into_iter()
        .map(|(lane, row, known)| Ok((lane, owner_snapshot(row, &node, now, known)?)))
        .collect()
}

/// Converts the current durable lane row into invocation-free lifecycle evidence.
fn owner_snapshot(
    row: super::model::TaskLaneLockRow,
    node: &str,
    now: chrono::DateTime<chrono::Utc>,
    known: bool,
) -> Result<LaneOwnerPoll, TaskRuntimeError> {
    let phase = LaneOwnerPhase::from_i16(row.phase)?;
    let live = known
        && row.owner_id.as_deref() == Some(node)
        && row.leased_until.is_some_and(|until| until > now);
    let action = match phase {
        LaneOwnerPhase::Idling => Some(LaneHookAction::Idle),
        LaneOwnerPhase::Busying => Some(LaneHookAction::Busy),
        _ => None,
    };
    Ok(LaneOwnerPoll {
        token: live.then_some(row.owner_token).flatten(),
        generation: row.generation,
        phase,
        action: live.then_some(action).flatten(),
        takeover: false,
    })
}

/// Current reads cannot skip locks: absence must not mean an unfinished commit.
async fn load_rows(
    tx: &mut db::DbTransaction<'_>,
    mut ids: Vec<uuid::Uuid>,
    batch_size: usize,
) -> Result<HashMap<uuid::Uuid, TaskRow>, TaskRuntimeError> {
    ids.sort_unstable();
    ids.dedup();
    let table = DbTaskStore::table();
    let mut rows = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(batch_size) {
        let query = db::from(&table)
            .filter(table.id.in_values(chunk.to_vec()))
            .sort(table.id.asc());
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        let query = {
            use crate::db::backend::RowLockExt as _;
            query.for_update()
        };
        for row in query.all::<TaskRow>().exec(&mut *tx).await? {
            rows.insert(row.id, row);
        }
    }
    Ok(rows)
}

/// Keeps the original order and only returns the same still-owned execution.
fn recovered_poll(
    node: &uuid::Uuid,
    prepared: &TaskPoll,
    mut owners: HashMap<crate::tasks::TaskLane, LaneOwnerPoll>,
    rows: &mut HashMap<uuid::Uuid, TaskRow>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(TaskPoll, Vec<TaskCommit>, Vec<TaskRow>), TaskRuntimeError> {
    let mut lanes = Vec::new();
    let mut cancelled = Vec::new();
    let mut released = Vec::new();
    for lane in &prepared.lanes {
        let owner = owners.remove(&lane.lane);
        let mut tasks = Vec::new();
        for expected in &lane.tasks {
            let Some(row) = rows.remove(&expected.id.into_uuid()) else {
                continue;
            };
            if row.node_id != Some(*node) || row.attempts != expected.attempts || row.status != 1 {
                continue;
            }
            recover_record(
                row,
                lane.lane,
                owner.as_ref(),
                now,
                &mut tasks,
                &mut cancelled,
                &mut released,
            )?;
        }
        lanes.push(LanePoll {
            lane: lane.lane,
            tasks,
            owner,
            reclaimed: 0,
            saturated: false,
            next_wake_in: Some(std::time::Duration::ZERO),
        });
    }
    Ok((
        TaskPoll {
            lanes,
            wake_lanes: Vec::new(),
        },
        cancelled,
        released,
    ))
}

/// Separates runnable, cancelled and relinquished rows without changing execution counters.
fn recover_record(
    mut row: TaskRow,
    _lane: crate::tasks::TaskLane,
    owner: Option<&LaneOwnerPoll>,
    now: chrono::DateTime<chrono::Utc>,
    tasks: &mut Vec<TaskRecord>,
    cancelled: &mut Vec<TaskCommit>,
    released: &mut Vec<TaskRow>,
) -> Result<(), TaskRuntimeError> {
    if owner.is_some_and(|owner| owner.token.is_none()) {
        row.node_id = None;
        row.updated_at = now;
        released.push(row);
    } else if row.cancelled {
        cancelled.push(TaskCommit::Cancel {
            task_id: TaskId::new(row.id),
        });
    } else {
        tasks.push(TaskRecord::try_from(row)?);
    }
    Ok(())
}
