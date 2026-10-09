//! Fenced outcome acceptance, cancellation finalization, and atomic lifecycle commits.

use super::model::ResumePatch;
use super::queries::{StoreRow, statement_now};
use super::store::DbTaskStore;
use super::workflow::{self, queue_delivery};
use super::{
    model::{IdempotencyExpiryPatch, TaskRow},
    queries::add_time,
};
use crate::db::{DbSession as _, Record as _};
use crate::tasks::store::StoreFailure;
use crate::{
    db,
    tasks::{
        IdempotencyRetention, TaskCommit, TaskId, TaskOutcome, TaskRecord, TaskRetry,
        TaskRuntimeError, TaskStatus,
    },
};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

impl DbTaskStore {
    /// Executes an independent submission or control transaction.
    pub(super) async fn resume_impl(
        &self,
        id: TaskId,
        input: String,
    ) -> Result<bool, TaskRuntimeError> {
        crate::tasks::result::validate_resume(&input)?;
        let mut transaction = self.pool.begin().await?;
        let now = statement_now(&mut transaction).await?;
        #[cfg(not(feature = "postgres"))]
        let table = Self::table();
        let patch = ResumePatch {
            status: TaskStatus::Pending.as_i16(),
            resume_input: Some(input),
            ready_at: Some(now),
            updated_at: now,
        };
        #[cfg(feature = "postgres")]
        let changed = {
            use db::DbSession as _;
            let sql = super::notify::notifying(
                "UPDATE vyuh_tasks SET status = 0, resume_input = $1, ready_at = $2, updated_at = $2 WHERE id = $3 AND status = 2 AND NOT cancelled",
            );
            let (changed, _): (i64, i64) = transaction
                .fetch_one(
                    db::Statement::raw(&sql)
                        .bind(patch.resume_input)
                        .bind(now)
                        .bind(id.into_uuid()),
                )
                .await?;
            changed as u64
        };
        #[cfg(not(feature = "postgres"))]
        let changed = db::from(&table)
            .filter(table.id.eq(db::val(id.into_uuid())))
            .filter(table.status.eq(db::val(TaskStatus::Suspended.as_i16())))
            .filter(table.cancelled.eq(db::val(false)))
            .update(&patch)
            .exec(&mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(changed > 0)
    }

    /// Commits one bounded outcome batch in a shared transaction.
    #[allow(dead_code)]
    pub(super) async fn commit_tasks_impl(
        &self,
        commits: &[TaskCommit],
    ) -> Result<crate::tasks::CommitResult, StoreFailure<crate::tasks::CommitResult>> {
        self.commit_admitted_impl(commits, None, &[]).await
    }

    /// Uses reserved local capacity within the existing atomic outcome transaction.
    pub(super) async fn commit_admitted_impl(
        &self,
        commits: &[TaskCommit],
        delivery: Option<(uuid::Uuid, usize)>,
        cached: &[std::sync::Arc<TaskRecord>],
    ) -> Result<crate::tasks::CommitResult, StoreFailure<crate::tasks::CommitResult>> {
        if commits.is_empty() {
            return Ok(crate::tasks::CommitResult {
                wake_lanes: Vec::new(),
                rejected: Vec::new(),
                tasks: Vec::new(),
            });
        }
        let conf = &self.runtime_conf;
        let commands = prepare_commits(commits, self.commit_batch_size, conf.max_all_children)?;
        let mut transaction = self.pool.begin().await?;
        let (now, loaded) = self
            .load_outcomes(&mut transaction, commits, &commands)
            .await?;
        let (mut children, deliveries, waits, rejected, mut tasks) = self
            .commit_outcomes_tx(&mut transaction, commits, commands, conf, now, loaded)
            .await?;
        let admitted = self.admit_rows(&mut children, delivery, now)?;
        let delivery = delivery.map(|(node, limit)| (node, limit.saturating_sub(admitted.len())));
        tasks.extend(admitted.into_iter().map(std::sync::Arc::new));
        let wake_lanes = workflow::finalize_admitted(
            &mut transaction,
            &children,
            deliveries,
            &waits,
            conf,
            now,
            super::queries::WRITE_CHUNK_SIZE,
            delivery,
            cached,
            &mut tasks,
        )
        .await?;
        let prepared = crate::tasks::CommitResult {
            wake_lanes,
            rejected,
            tasks,
        };
        match transaction.commit().await {
            Ok(()) => Ok(prepared),
            Err(error) => Err(StoreFailure::CommitUnknown {
                error: error.into(),
                prepared,
            }),
        }
    }

    /// Preserves existing lane-before-task lock ordering and ordinary clock/read batching.
    async fn load_outcomes(
        &self,
        tx: &mut db::DbTransaction<'_>,
        commits: &[TaskCommit],
        commands: &HashMap<TaskId, &TaskCommit>,
    ) -> Result<(DateTime<Utc>, Option<Vec<TaskRow>>), TaskRuntimeError> {
        if commits.iter().all(|commit| {
            commit
                .lane()
                .is_none_or(|lane| !locked_lane(&self.runtime_conf, lane.as_str()))
        }) {
            let (now, rows) =
                outcomes(tx, commits, commands, super::queries::WRITE_CHUNK_SIZE).await?;
            Ok((now, Some(rows)))
        } else {
            Ok((super::queries::statement_now(tx).await?, None))
        }
    }

    /// Commits owned outcomes inside an already-authorized scheduler transaction.
    pub(super) async fn commit_outcomes_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        commits: &[TaskCommit],
        mut commands: HashMap<TaskId, &TaskCommit>,
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
        loaded: Option<Vec<TaskRow>>,
    ) -> Result<
        (
            Vec<TaskRow>,
            Vec<(TaskId, String)>,
            Vec<super::workflow::WaitWrite>,
            Vec<TaskId>,
            Vec<std::sync::Arc<TaskRecord>>,
        ),
        TaskRuntimeError,
    > {
        if commits.is_empty() {
            return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()));
        }
        let allowed = fenced_commits(transaction, commits, conf, now).await?;
        let mut rows = match loaded {
            Some(rows) => rows,
            None => {
                self::outcomes(
                    transaction,
                    commits,
                    &commands,
                    super::queries::WRITE_CHUNK_SIZE,
                )
                .await?
                .1
            }
        };
        accept_rows(&mut rows, &commands, &allowed)?;
        let (children, conflicts, waits) = workflow::prepare_children(
            transaction,
            &rows,
            &commands,
            conf,
            now,
            super::queries::WRITE_CHUNK_SIZE,
        )
        .await?;
        let deliveries = apply_owned_outcomes(&mut rows, &mut commands, &conflicts, conf, now)?;
        update_idempotency_batch(transaction, &mut rows, conf, now).await?;
        batch_update_rows(transaction, &rows, super::queries::WRITE_CHUNK_SIZE).await?;
        let rejected = commands.keys().copied().collect();
        let tasks = rows
            .into_iter()
            .filter(|row| row.kind == 1)
            .map(TaskRecord::try_from)
            .map(|record| record.map(std::sync::Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((children, deliveries, waits, rejected, tasks))
    }
}

fn locked_lane(conf: &crate::tasks::TaskStoreConf, lane_name: &str) -> bool {
    conf.lanes
        .iter()
        .find(|lane| lane.lane().as_str() == lane_name)
        .is_some_and(|lane| lane.lane_lock().is_some())
}

/// Verifies lane-owner tokens only for commits belonging to opt-in lanes.
async fn fenced_commits(
    transaction: &mut db::DbTransaction<'_>,
    commits: &[TaskCommit],
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<std::collections::HashSet<TaskId>, TaskRuntimeError> {
    let mut allowed = std::collections::HashSet::with_capacity(commits.len());
    let mut lanes = std::collections::BTreeMap::new();
    for commit in commits {
        let TaskCommit::Outcome {
            lane,
            node_id,
            owner_token,
            ..
        } = commit
        else {
            allowed.insert(commit.task_id());
            continue;
        };
        if !locked_lane(conf, lane.as_str()) {
            allowed.insert(commit.task_id());
            continue;
        }
        let evidence = (*node_id, owner_token.as_deref());
        if let Some(previous) = lanes.insert(lane.as_str(), evidence)
            && previous != evidence
        {
            return Err(TaskRuntimeError::InvalidOptions(
                "conflicting lane ownership in commit batch".into(),
            ));
        }
    }
    let mut valid_lanes = HashSet::new();
    for (lane, (node, token)) in lanes {
        if locked_commit(transaction, &node, lane, token, now).await? {
            valid_lanes.insert(lane);
        }
    }
    for commit in commits {
        if commit
            .lane()
            .is_some_and(|lane| valid_lanes.contains(lane.as_str()))
        {
            allowed.insert(commit.task_id());
        }
    }
    Ok(allowed)
}

async fn locked_commit(
    transaction: &mut db::DbTransaction<'_>,
    runner_id: &uuid::Uuid,
    lane: &str,
    token: Option<&str>,
    now: DateTime<Utc>,
) -> Result<bool, TaskRuntimeError> {
    let row = load_lane_lock(transaction, lane).await?;
    Ok(row.is_some_and(|row| {
        row.owner_id.as_deref() == Some(runner_id.to_string().as_str())
            && row.owner_token.as_deref() == token
            && row.leased_until.is_some_and(|deadline| deadline > now)
    }))
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(super) async fn load_lane_lock(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<super::model::TaskLaneLockRow>, TaskRuntimeError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::lane_lock_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .for_update()
        .first::<super::model::TaskLaneLockRow>()
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
pub(super) async fn load_lane_lock(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<super::model::TaskLaneLockRow>, TaskRuntimeError> {
    let table = DbTaskStore::lane_lock_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .first::<super::model::TaskLaneLockRow>()
        .exec(transaction)
        .await?)
}

/// Validates each independent commit entrypoint and borrows one command index for acceptance.
pub(super) fn prepare_commits(
    commits: &[TaskCommit],
    batch_size: usize,
    max_all_children: usize,
) -> Result<HashMap<TaskId, &TaskCommit>, TaskRuntimeError> {
    crate::tasks::store::transitions::validate_turn(commits, batch_size, max_all_children)?;
    let mut commands = HashMap::with_capacity(commits.len());
    for commit in commits {
        if commands.insert(commit.task_id(), commit).is_some() {
            return Err(TaskRuntimeError::TaskExecutionError(format!(
                "task outcome batch contains duplicate task {}",
                commit.task_id()
            )));
        }
    }
    Ok(commands)
}

/// Keeps the original owner-filtered read for ordinary single-node output batches.
fn commit_node(commits: &[TaskCommit]) -> Option<uuid::Uuid> {
    let node = commits.first()?.node_id()?;
    commits
        .iter()
        .all(|commit| commit.node_id() == Some(node))
        .then_some(node)
}

/// Applies each command's distinct authority to already-locked rows before child preparation.
fn accept_rows(
    rows: &mut Vec<TaskRow>,
    commands: &HashMap<TaskId, &TaskCommit>,
    allowed: &HashSet<TaskId>,
) -> Result<(), TaskRuntimeError> {
    let mut error = None;
    rows.retain_mut(|row| {
        let id = TaskId::new(row.id);
        let Some(commit) = commands.get(&id) else {
            return false;
        };
        let status = match TaskStatus::from_i16(row.status) {
            Ok(status) => status,
            Err(cause) => {
                error = Some(cause);
                return false;
            }
        };
        let accepted = allowed.contains(&id)
            && commit
                .lane()
                .is_none_or(|lane| lane.as_str() == row.lane_name)
            && commit.accepts(status, row.node_id, row.attempts);
        if accepted {
            row.cancelled |= commit.is_cancel();
        }
        accepted
    });
    error.map_or(Ok(()), Err)
}

/// Applies one lifecycle transition using statement time.
pub(super) fn apply_outcome(
    row: &mut TaskRow,
    outcome: &TaskOutcome,
    retry: TaskRetry,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let cancellation = row
        .cancelled
        .then(crate::tasks::store::transitions::cancellation);
    let outcome = cancellation.as_ref().unwrap_or(outcome);
    let preserve_resume = matches!(outcome, TaskOutcome::Retry { .. });
    match outcome {
        TaskOutcome::Complete => finish(row, TaskStatus::Succeeded, None, now),
        TaskOutcome::CompleteWith { output } => complete_output(row, output, now),
        TaskOutcome::Suspend { state }
        | TaskOutcome::Spawn { state, .. }
        | TaskOutcome::All { state, .. } => {
            row.last_result = None;
            row.step_attempts = 0;
            row.status = TaskStatus::Suspended.as_i16();
            row.state = Some(state.clone());
            row.ready_at = None;
        }
        TaskOutcome::Sleep { state, delay } => {
            row.last_result = None;
            row.step_attempts = 0;
            row.status = TaskStatus::Pending.as_i16();
            row.state = Some(state.clone());
            row.ready_at = Some(add_time(
                now,
                chrono_duration(*delay)?,
                "task sleep duration",
            )?);
        }
        TaskOutcome::Retry { error } => apply_retry(row, retry, error.clone(), now)?,
        TaskOutcome::Fail { error } => finish(row, TaskStatus::Failed, Some(error.clone()), now),
    }
    if !preserve_resume {
        row.resume_input = None;
    }
    row.node_id = None;
    row.updated_at = now;
    Ok(())
}

/// Persists validated output or contains invalid low-level output as a terminal failure.
fn complete_output(row: &mut TaskRow, output: &str, now: DateTime<Utc>) {
    match crate::tasks::result::validate_output(output) {
        Ok(()) => {
            row.status = TaskStatus::Succeeded.as_i16();
            row.ready_at = None;
            row.completed_at = Some(now);
            row.last_result = Some(crate::tasks::result::success(output));
        }
        Err(error) => finish(row, TaskStatus::Failed, Some(error.to_string()), now),
    }
}

/// Schedules another attempt or terminates a task at its retry bound.
fn apply_retry(
    row: &mut TaskRow,
    retry: TaskRetry,
    error: String,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    if retry.exhausted(row.step_attempts)? {
        finish(row, TaskStatus::Failed, Some(error), now);
    } else {
        row.status = TaskStatus::Pending.as_i16();
        row.last_result = Some(crate::tasks::result::failure(TaskId::new(row.id), error));
        row.ready_at = Some(add_time(
            now,
            chrono_duration(retry.delay(row.step_attempts)?)?,
            "task retry duration",
        )?);
    }
    Ok(())
}

pub(super) fn lane_retry(
    lanes: &[crate::tasks::TaskLaneConf],
    name: &str,
) -> Result<TaskRetry, TaskRuntimeError> {
    lanes
        .iter()
        .find(|lane| lane.lane().as_str() == name)
        .map(crate::tasks::TaskLaneConf::retry_policy)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(name.into()))
}

pub(super) fn finish(
    row: &mut TaskRow,
    status: TaskStatus,
    error: Option<String>,
    now: DateTime<Utc>,
) {
    row.status = status.as_i16();
    row.last_result = Some(match error {
        Some(error) => crate::tasks::result::failure(TaskId::new(row.id), error),
        None => crate::tasks::result::UNIT_RESULT.into(),
    });
    row.ready_at = None;
    row.completed_at = Some(now);
}

/// Releases or archives every terminal key through one set-based mutation.
pub(super) async fn update_idempotency_batch(
    transaction: &mut db::DbTransaction<'_>,
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::idempotency_table();
    let (active, retained) = retention_groups(rows, conf)?;
    if !active.is_empty() {
        db::from(&table)
            .filter(table.task_id.in_values(active))
            .delete()
            .exec(transaction)
            .await?;
    }
    for (duration, ids) in retained {
        let expires = add_time(now, chrono_duration(duration)?, "idempotency retention")?;
        let patch = IdempotencyExpiryPatch {
            expires_at: Some(expires),
            updated_at: now,
        };
        db::from(&table)
            .filter(table.task_id.in_values(ids))
            .update(&patch)
            .exec(transaction)
            .await?;
        apply_expiry(rows, conf, duration, expires)?;
    }
    Ok(())
}

type RetentionGroup = (Duration, Vec<uuid::Uuid>);
type RetentionGroups = (Vec<uuid::Uuid>, Vec<RetentionGroup>);

/// Groups terminal idempotent rows by their finalized per-handler lane policy.
fn retention_groups(
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
) -> Result<RetentionGroups, TaskRuntimeError> {
    let mut active = Vec::new();
    let mut retained: Vec<RetentionGroup> = Vec::new();
    for row in rows.iter_mut() {
        if !is_terminal_idempotent(row)? {
            continue;
        }
        match retention_for(row, conf)? {
            IdempotencyRetention::ActiveOnly => {
                row.idempotency_expires_at = None;
                active.push(row.id);
            }
            IdempotencyRetention::RetainFor(duration) => {
                if let Some((_, ids)) = retained.iter_mut().find(|(value, _)| *value == duration) {
                    ids.push(row.id);
                } else {
                    retained.push((duration, vec![row.id]));
                }
            }
        }
    }
    Ok((active, retained))
}

/// Reflects one set-based retained-key write in the loaded task rows.
fn apply_expiry(
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
    duration: Duration,
    expires: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    for row in rows {
        if is_terminal_idempotent(row)?
            && matches!(retention_for(row, conf)?, IdempotencyRetention::RetainFor(value) if value == duration)
        {
            row.idempotency_expires_at = Some(expires);
        }
    }
    Ok(())
}

/// Returns the retention inherited by one idempotent task row.
fn retention_for(
    row: &TaskRow,
    conf: &crate::tasks::TaskStoreConf,
) -> Result<IdempotencyRetention, TaskRuntimeError> {
    conf.idempotency_for(&row.name).ok_or_else(|| {
        TaskRuntimeError::InvalidConfig(format!("task '{}' has no idempotency policy", row.name))
    })
}

fn is_terminal_idempotent(row: &TaskRow) -> Result<bool, TaskRuntimeError> {
    Ok(row.idempotency_key.is_some()
        && matches!(
            TaskStatus::from_i16(row.status)?,
            TaskStatus::Succeeded | TaskStatus::Failed
        ))
}

/// Persists lifecycle fields; lifetime attempts are written exclusively when claiming.
pub(super) async fn batch_update_rows(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    batch_size: usize,
) -> Result<(), TaskRuntimeError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::table();
    use db::queries::ColumnSet as _;
    let mut columns = (
        &table.status,
        &table.state,
        &table.resume_input,
        &table.step_attempts,
        &table.last_result,
        &table.ready_at,
        &table.completed_at,
        &table.node_id,
        &table.updated_at,
        &table.idempotency_expires_at,
    )
        .into_column_refs();
    if rows.iter().any(|row| row.cancelled) {
        columns.extend((&table.cancelled).into_column_refs());
    }
    #[cfg(feature = "postgres")]
    let changed = if rows.iter().any(|row| row.status == 0) {
        let mut names = vec![
            "status",
            "state",
            "resume_input",
            "step_attempts",
            "last_result",
            "ready_at",
            "completed_at",
            "node_id",
            "updated_at",
            "idempotency_expires_at",
        ];
        if rows.iter().any(|row| row.cancelled) {
            names.push("cancelled");
        }
        super::notify::update(transaction, rows, &names, batch_size).await?
    } else {
        db::from(&table)
            .update_many(rows, columns)
            .batch_size(batch_size)
            .exec(&mut *transaction)
            .await?
    };
    #[cfg(not(feature = "postgres"))]
    let changed = db::from(&table)
        .update_many(rows, columns)
        .batch_size(batch_size)
        .exec(&mut *transaction)
        .await?;
    if changed != rows.len() as u64 {
        return Err(TaskRuntimeError::TaskExecutionError(
            "task outcome batch changed an unexpected number of rows".into(),
        ));
    }
    let terminal_waits = rows
        .iter()
        .filter(|row| {
            matches!(row.status, 3 | 4)
                && (row.waiting_children.is_some() || row.remaining_completions > 0)
        })
        .map(|row| row.id)
        .collect::<Vec<_>>();
    super::workflow::clear(transaction, &terminal_waits, batch_size).await?;
    Ok(())
}

pub(super) fn chrono_duration(duration: Duration) -> Result<chrono::Duration, TaskRuntimeError> {
    chrono::Duration::from_std(duration).map_err(|_| {
        TaskRuntimeError::TaskExecutionError("task duration is outside the supported range".into())
    })
}

/// Applies only outcomes whose rows remain owned by the committing runner.
pub(super) fn apply_owned_outcomes(
    rows: &mut [TaskRow],
    commands: &mut HashMap<TaskId, &TaskCommit>,
    conflicts: &HashSet<TaskId>,
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<Vec<(TaskId, String)>, TaskRuntimeError> {
    let mut deliveries = Vec::new();
    let cancellation = commands
        .values()
        .any(|commit| commit.is_cancel())
        .then(crate::tasks::store::transitions::cancellation);
    for row in rows {
        let commit = commands.remove(&TaskId::new(row.id)).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError(format!("task {} has no pending outcome", row.id))
        })?;
        let outcome = commit.outcome().or(cancellation.as_ref()).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("cancellation outcome missing".into())
        })?;
        let failure = conflicts
            .contains(&TaskId::new(row.id))
            .then(|| TaskOutcome::fail("Child task identity conflicts with an existing task"));
        let invalid = if row.cancelled {
            None
        } else {
            crate::tasks::store::transitions::capability_error(
                crate::tasks::TaskKind::from_i16(row.kind)?,
                outcome,
                &conf.handlers,
            )
            .map(TaskOutcome::fail)
        };
        let outcome = invalid.as_ref().or(failure.as_ref()).unwrap_or(outcome);
        let retry = lane_retry(&conf.lanes, &row.lane_name)?;
        apply_outcome(row, outcome, retry, now)?;
        queue_delivery(row, &mut deliveries)?;
    }
    Ok(deliveries)
}

/// Locks owned tasks in ID order and returns database time, including all-stale batches.
pub(super) async fn outcomes(
    tx: &mut db::DbTransaction<'_>,
    commits: &[TaskCommit],
    commands: &HashMap<TaskId, &TaskCommit>,
    batch_size: usize,
) -> Result<(DateTime<Utc>, Vec<TaskRow>), TaskRuntimeError> {
    let mut now = None;
    let mut tasks = Vec::with_capacity(commits.len());
    let mut ids = commands.keys().map(|id| id.into_uuid()).collect::<Vec<_>>();
    ids.sort_unstable();
    let node = commit_node(commits);
    for chunk in ids.chunks(batch_size.max(1).min(db::backend::PARAMETER_LIMIT - 1)) {
        let rows = tx.fetch_all::<StoreRow>(row_statement(node, chunk)).await?;
        let mut clock_seen = false;
        for row in rows {
            match row {
                StoreRow::Clock(runtime) => {
                    now.get_or_insert(runtime.now);
                    clock_seen = true;
                }
                StoreRow::Snapshot(task) => tasks.push(task),
                _ => return Err(unexpected_projection()),
            }
        }
        if !clock_seen {
            return Err(super::queries::missing_clock());
        }
    }
    let now = match now {
        Some(now) => now,
        None => super::queries::statement_now(tx).await?,
    };
    Ok((now, tasks))
}

/// Reports an internal read-shape mismatch rather than silently dropping a task.
fn unexpected_projection() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("unexpected task turn projection".into())
}

/// Combines database time and deterministic task locks without selecting join data.
fn row_statement(runner: Option<uuid::Uuid>, ids: &[uuid::Uuid]) -> db::Statement {
    let sql = row_sql(ids.len(), runner.is_some());
    let mut statement = db::Statement::raw(&sql);
    if let Some(runner) = runner {
        statement = statement.bind(runner);
    }
    for id in ids {
        statement = statement.bind(*id);
    }
    statement
}

/// Renders only schema-owned column names; task inputs never enter SQL text.
fn row_projection() -> (String, String, String) {
    let columns = TaskRow::record_column_names();
    let task_columns = columns
        .iter()
        .map(|name| format!("t.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let null_columns = columns
        .iter()
        .map(|name| format!("NULL AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    (columns.join(", "), task_columns, null_columns)
}

/// Renders one locked snapshot statement for bounded, still-owned task identities.
fn row_sql(count: usize, owned: bool) -> String {
    let (columns, task_columns, null_columns) = row_projection();
    let parameters = (0..count)
        .map(|offset| parameter(offset + if owned { 2 } else { 1 }))
        .collect::<Vec<_>>()
        .join(", ");
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    let exclusive = " FOR UPDATE";
    #[cfg(feature = "sqlite")]
    let exclusive = "";
    let predicate = if owned {
        format!("t.node_id = {} AND t.status = 1", parameter(1))
    } else {
        "t.status IN (0, 1, 2)".to_owned()
    };
    format!(
        "WITH clock AS (SELECT CURRENT_TIMESTAMP AS now),
         owned AS (SELECT {task_columns} FROM vyuh_tasks t
         WHERE {predicate} AND t.id IN ({parameters})
         ORDER BY t.id{exclusive})
         SELECT 0 AS turn_kind, now, {null_columns} FROM clock
         UNION ALL SELECT 1 AS turn_kind, NULL, {columns} FROM owned ORDER BY turn_kind",
    )
}

/// Generates backend placeholders only; caller values are always bound parameters.
fn parameter(index: usize) -> String {
    if cfg!(feature = "postgres") {
        format!("${index}")
    } else {
        "?".into()
    }
}

#[cfg(all(
    test,
    feature = "migrations",
    any(feature = "postgres", feature = "sqlite")
))]
#[path = "tests/commit.rs"]
mod tests;
