//! Fenced outcome acceptance, cancellation finalization, and atomic lifecycle commits.

#[cfg(feature = "mysql")]
use super::model::IdempotencyExpiryPatch;
use super::model::TaskRow;
use super::queries::add_time;
use super::queries::{StoreRow, statement_now};
use super::store::DbTaskStore;
use super::workflow::{self, queue_delivery};
use crate::db::{DbSession as _, Record as _};
#[cfg(feature = "mysql")]
use crate::tasks::IdempotencyRetention;
use crate::tasks::store::StoreFailure;
use crate::{
    db,
    tasks::{TaskCommit, TaskId, TaskOutcome, TaskRecord, TaskRetry, TaskRuntimeError, TaskStatus},
};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

impl DbTaskStore {
    /// Changes only readiness in one statement, using the claim clock format on SQLite.
    pub(super) async fn wake_impl(&self, id: TaskId) -> Result<bool, TaskRuntimeError> {
        #[cfg(feature = "postgres")]
        let (clock, parameter) = ("statement_timestamp()", "$1");
        #[cfg(feature = "mysql")]
        let (clock, parameter) = ("CURRENT_TIMESTAMP(6)", "?");
        #[cfg(feature = "sqlite")]
        let (clock, parameter) = (
            "strftime('%Y-%m-%dT%H:%M:%S+00:00', CURRENT_TIMESTAMP)",
            "?",
        );
        let pending = TaskStatus::Pending.as_i16();
        let sql = format!(
            "UPDATE vyuh_tasks SET ready_at = {clock} WHERE id = {parameter} AND status = {pending} \
             AND node_id IS NULL AND NOT cancelled AND ready_at > {clock}"
        );
        #[cfg(feature = "postgres")]
        let sql = super::notify::notifying(&sql);
        let statement = db::Statement::raw(&sql).bind(id.into_uuid());
        #[cfg(feature = "postgres")]
        let changed = {
            let (count, _): (i64, i64) = self.pool.clone().fetch_one(statement).await?;
            count > 0
        };
        #[cfg(not(feature = "postgres"))]
        let changed = self.pool.clone().execute(statement).await? > 0;
        Ok(changed)
    }

    /// Executes an independent direct resume, excluding topic waits.
    pub(super) async fn resume_impl(
        &self,
        id: TaskId,
        input: String,
    ) -> Result<bool, TaskRuntimeError> {
        crate::tasks::result::validate_resume(&input)?;
        let mut transaction = self.pool.begin().await?;
        let now = statement_now(&mut transaction).await?;
        #[cfg(feature = "mysql")]
        {
            let changed = super::publication::resume(&mut transaction, id, input, now).await?;
            transaction.commit().await?;
            return Ok(changed);
        }
        #[cfg(not(feature = "mysql"))]
        {
            #[cfg(feature = "postgres")]
            let changed = {
                use db::DbSession as _;
                let sql = super::notify::notifying(
                    "UPDATE vyuh_tasks SET status = 0, resume_input = $1, ready_at = $2, updated_at = $2 WHERE id = $3 AND status = 2 AND NOT cancelled AND wait_topic IS NULL",
                );
                let (changed, _): (i64, i64) = transaction
                    .fetch_one(
                        db::Statement::raw(&sql)
                            .bind(input)
                            .bind(now)
                            .bind(id.into_uuid()),
                    )
                    .await?;
                changed as u64
            };
            #[cfg(feature = "sqlite")]
        let changed = transaction.execute(db::Statement::raw(
            "UPDATE vyuh_tasks SET status = 0, resume_input = ?1, ready_at = ?2, updated_at = ?2
             WHERE id = ?3 AND status = 2 AND NOT cancelled AND wait_topic IS NULL"
        ).bind(input).bind(now).bind(id.into_uuid())).await?;
            transaction.commit().await?;
            Ok(changed > 0)
        }
    }

    /// Commits one bounded outcome batch in a shared transaction.
    #[allow(dead_code)]
    pub(super) async fn commit_tasks_impl(
        &self,
        commits: &[TaskCommit],
    ) -> Result<crate::tasks::CommitResult, StoreFailure<crate::tasks::CommitResult>> {
        #[cfg(feature = "mysql")]
        if !commits.is_empty() {
            self.detect_mysql_server(None).await?;
        }
        self.commit_admitted_impl(commits, None, &[]).await
    }

    /// Stages the same cancellation writes without committing or admitting local work.
    pub(crate) async fn cancel_tx(
        &self,
        tx: &mut db::DbTransaction<'_>,
        id: TaskId,
    ) -> Result<(), TaskRuntimeError> {
        #[cfg(feature = "mysql")]
        self.detect_mysql_server(Some(tx)).await?;
        let commits = [TaskCommit::Cancel { task_id: id }];
        let commands = prepare_commits(
            &commits,
            self.commit_batch_size,
            self.runtime_conf.max_all_children,
        )?;
        self.stage_commits(tx, &commits, commands, None, &[])
            .await?;
        Ok(())
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
        let commands = prepare_commits(
            commits,
            self.commit_batch_size,
            self.runtime_conf.max_all_children,
        )?;
        let mut transaction = self.pool.begin().await?;
        let prepared = self
            .stage_commits(&mut transaction, commits, commands, delivery, cached)
            .await?;
        match transaction.commit().await {
            Ok(()) => Ok(prepared),
            Err(error) => Err(StoreFailure::CommitUnknown {
                error: error.into(),
                prepared,
            }),
        }
    }

    /// Stages the complete outcome and workflow mutation without owning the commit boundary.
    async fn stage_commits(
        &self,
        tx: &mut db::DbTransaction<'_>,
        commits: &[TaskCommit],
        commands: HashMap<TaskId, &TaskCommit>,
        delivery: Option<(uuid::Uuid, usize)>,
        cached: &[std::sync::Arc<TaskRecord>],
    ) -> Result<crate::tasks::CommitResult, TaskRuntimeError> {
        let conf = &self.runtime_conf;
        let (now, loaded) = self.load_outcomes(tx, commits, &commands).await?;
        let (mut children, deliveries, waits, rejected, mut tasks) = self
            .commit_outcomes_tx(tx, commits, commands, conf, now, loaded)
            .await?;
        let admitted = self.admit_rows(&mut children, delivery, now)?;
        let delivery = delivery.map(|(node, limit)| (node, limit.saturating_sub(admitted.len())));
        tasks.extend(admitted.into_iter().map(std::sync::Arc::new));
        let wake_lanes = workflow::finalize_admitted(
            tx,
            children,
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
        Ok(crate::tasks::CommitResult {
            wake_lanes,
            rejected,
            tasks,
        })
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
            workflow::Deliveries,
            Vec<super::workflow::WaitWrite>,
            Vec<TaskId>,
            Vec<std::sync::Arc<TaskRecord>>,
        ),
        TaskRuntimeError,
    > {
        if commits.is_empty() {
            return Ok((
                Vec::new(),
                workflow::Deliveries::default(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ));
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
        let publishers = accept_rows(&mut rows, &commands, &allowed)?;
        if publishers > 0 {
            verify_publisher_budget(publishers, self.commit_batch_size, conf.max_all_children)?;
        }
        let mut deliveries = workflow::Deliveries::default();
        let (children, conflicts, waits) = workflow::prepare_children(
            transaction,
            &rows,
            &commands,
            conf,
            now,
            super::queries::WRITE_CHUNK_SIZE,
            &mut deliveries,
        )
        .await?;
        let mut topics = Vec::new();
        apply_owned_outcomes(
            &mut rows,
            &mut commands,
            &conflicts,
            conf,
            now,
            &mut topics,
            &mut deliveries,
        )?;
        self.update_idempotency_batch(transaction, &mut rows, conf, now)
            .await?;
        batch_update_rows(transaction, &rows, super::queries::WRITE_CHUNK_SIZE).await?;
        super::publication::store_changes(transaction, topics).await?;
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

/// Bounds terminal publication on the cancellation-only bulk entrypoint too.
fn verify_publisher_budget(
    publishers: usize,
    batch: usize,
    maximum: usize,
) -> Result<(), TaskRuntimeError> {
    let weight = crate::tasks::store::sharing::PUBLISH_WEIGHT;
    if publishers > batch.max(maximum.saturating_add(1)).max(weight) / weight {
        return Err(TaskRuntimeError::InvalidOptions(
            "Publisher flush exceeds its fan-out budget".into(),
        ));
    }
    Ok(())
}

#[cfg(all(test, feature = "migrations", feature = "test-support"))]
#[path = "tests/wake.rs"]
mod wake_tests;

#[cfg(all(test, feature = "migrations", feature = "test-support"))]
#[path = "tests/cancel_tx.rs"]
mod cancel_tx_tests;

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
) -> Result<usize, TaskRuntimeError> {
    let mut error = None;
    let mut publishers = 0;
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
            if row.share_role == crate::tasks::models::ShareRole::Publisher as i16
                || (row.share_role == crate::tasks::models::ShareRole::Drainer as i16
                    && !commit.is_cancel())
            {
                if let Some(outcome) = commit.outcome()
                    && commit.flush_cost().1
                        < crate::tasks::store::sharing::estimate(
                            if row.share_role == crate::tasks::models::ShareRole::Drainer as i16 {
                                crate::tasks::models::ShareRole::Drainer
                            } else {
                                crate::tasks::models::ShareRole::Publisher
                            },
                            outcome,
                        )
                        .unwrap_or(0)
                {
                    error = Some(TaskRuntimeError::TaskExecutionError(
                        "Missing shared publisher execution hint".into(),
                    ));
                    return false;
                }
                publishers += 1;
            }
            row.cancelled |= commit.is_cancel();
        }
        accepted
    });
    error.map_or(Ok(publishers), Err)
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
        | TaskOutcome::Wait { state, .. }
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

impl DbTaskStore {
    /// Releases or archives terminal keys in the current task mutation transaction.
    pub(super) async fn update_idempotency_batch(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        rows: &mut [TaskRow],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
    ) -> Result<(), TaskRuntimeError> {
        #[cfg(any(feature = "postgres", feature = "sqlite"))]
        {
            let _ = transaction;
            return super::held::finish(rows, conf, now);
        }
        #[cfg(feature = "mysql")]
        {
            if self.uses_mariadb() {
                return retain_mariadb(transaction, rows, conf, now).await;
            }
            let table = DbTaskStore::idempotency_table();
            let (active, retained) = retention_groups(rows, conf, |_, row| row.id)?;
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
    }
}

#[cfg(feature = "mysql")]
type RetentionGroup<K> = (Duration, Vec<K>);
#[cfg(feature = "mysql")]
type RetentionGroups<K> = (Vec<K>, Vec<RetentionGroup<K>>);

/// Groups terminal idempotent rows by the retention policy of their stored lane.
#[cfg(feature = "mysql")]
fn retention_groups<K>(
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
    identify: impl Fn(usize, &TaskRow) -> K,
) -> Result<RetentionGroups<K>, TaskRuntimeError> {
    let mut active = Vec::new();
    let mut retained: Vec<RetentionGroup<K>> = Vec::new();
    for (index, row) in rows.iter_mut().enumerate() {
        if !is_terminal_idempotent(row)? {
            continue;
        }
        match retention_for(row, conf)? {
            IdempotencyRetention::ActiveOnly => {
                row.idempotency_expires_at = None;
                active.push(identify(index, row));
            }
            IdempotencyRetention::RetainFor(duration) => {
                if let Some((_, ids)) = retained.iter_mut().find(|(value, _)| *value == duration) {
                    ids.push(identify(index, row));
                } else {
                    retained.push((duration, vec![identify(index, row)]));
                }
            }
        }
    }
    Ok((active, retained))
}

#[cfg(feature = "mysql")]
const MARIADB_RESERVATION_BATCH: &str =
    "JSON_TABLE(?, '$[*]' COLUMNS (item JSON PATH '$')) AS batch
    STRAIGHT_JOIN vyuh_task_idempotency AS owner FORCE INDEX (idx_vyuh_task_idempotency_owner)
    ON owner.task_name = JSON_VALUE(batch.item, '$[0]')
    AND owner.key_value = JSON_VALUE(batch.item, '$[1]')
    AND owner.task_id = UNHEX(REPLACE(JSON_VALUE(batch.item, '$[2]'), '-', ''))";

/// Keeps the existing mutation groups, but drives MariaDB writes from exact owner keys.
#[cfg(feature = "mysql")]
async fn retain_mariadb(
    tx: &mut db::DbTransaction<'_>,
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let (active, retained) = retention_groups(rows, conf, |index, _| index)?;
    if !active.is_empty() {
        let sql = format!("DELETE owner FROM {MARIADB_RESERVATION_BATCH}");
        tx.execute(db::Statement::raw(&sql).bind(reservation_keys(rows, &active)?))
            .await?;
    }
    for (duration, indices) in retained {
        let expires = add_time(now, chrono_duration(duration)?, "idempotency retention")?;
        let sql = format!(
            "UPDATE {MARIADB_RESERVATION_BATCH} SET owner.expires_at = ?, owner.updated_at = ?"
        );
        let statement = db::Statement::raw(&sql)
            .bind(reservation_keys(rows, &indices)?)
            .bind(expires)
            .bind(now);
        tx.execute(statement).await?;
        apply_expiry(rows, conf, duration, expires)?;
    }
    Ok(())
}

/// Binds only handler, key, and task ID from the already-fenced rows.
#[cfg(feature = "mysql")]
fn reservation_keys(rows: &[TaskRow], indices: &[usize]) -> Result<String, TaskRuntimeError> {
    let keys = indices
        .iter()
        .map(|index| {
            let row = rows.get(*index)?;
            Some((row.name.as_str(), row.idempotency_key.as_deref()?, row.id))
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("missing reservation key".into()))?;
    Ok(serde_json::to_string(&keys)?)
}

/// Reflects one set-based retained-key write in the loaded task rows.
#[cfg(feature = "mysql")]
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
#[cfg(feature = "mysql")]
fn retention_for(
    row: &TaskRow,
    conf: &crate::tasks::TaskStoreConf,
) -> Result<IdempotencyRetention, TaskRuntimeError> {
    conf.idempotency_retention(&row.name, &row.lane_name)
}

#[cfg(feature = "mysql")]
fn is_terminal_idempotent(row: &TaskRow) -> Result<bool, TaskRuntimeError> {
    Ok(
        row.share_role != crate::tasks::models::ShareRole::Subscriber as i16
            && row.idempotency_key.is_some()
            && matches!(
                TaskStatus::from_i16(row.status)?,
                TaskStatus::Succeeded | TaskStatus::Failed
            ),
    )
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
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    let releases_hold = rows.iter().any(|row| !row.idempotency_held);
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    if releases_hold {
        columns.extend((&table.idempotency_held).into_column_refs());
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
        if releases_hold {
            names.push("idempotency_held");
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
    topics: &mut Vec<(uuid::Uuid, Option<Vec<u8>>)>,
    deliveries: &mut workflow::Deliveries,
) -> Result<(), TaskRuntimeError> {
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
        if row.cancelled && row.status == TaskStatus::Suspended.as_i16() {
            topics.push((row.id, None));
        } else if !row.cancelled
            && let TaskOutcome::Wait { topic, .. } = outcome
        {
            topics.push((row.id, Some(topic.as_bytes().to_vec())));
        }
        let retry = lane_retry(&conf.lanes, &row.lane_name)?;
        apply_outcome(row, outcome, retry, now)?;
        queue_delivery(row, deliveries)?;
    }
    Ok(())
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

#[cfg(all(test, feature = "mysql"))]
#[path = "tests/reservation_keys.rs"]
mod reservation_tests;
