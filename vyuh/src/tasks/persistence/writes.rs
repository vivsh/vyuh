//! Mool-backed task writes, reads, and outcome commits.

use chrono::{Duration as ChronoDuration, Utc};

use crate::{
    db,
    tasks::{TaskError, TaskListFilter, TaskListPage, TaskOutcome, TaskRecord, TaskStatus},
};

use super::{
    common::{DbTaskStore, add_time, apply_filter, log_stale_owner, map_store_error, retry_delay},
    model::{CompletePatch, FailPatch, ResumePatch, RetryPatch, SleepPatch, SuspendPatch, TaskRow},
};

impl DbTaskStore {
    /// Persists one task and retains the stable active-identity error behavior.
    pub(super) async fn store_task_impl(&self, record: TaskRecord) -> Result<(), TaskError> {
        let table = Self::table();
        let row = TaskRow::from(record);
        let mut pool = self.pool.clone();
        db::from(&table)
            .insert(&row)
            .exec(&mut pool)
            .await
            .map_err(map_store_error)?;
        Ok(())
    }

    /// Resumes a suspended task only when its state remains suspended.
    pub(super) async fn resume_impl(
        &self,
        id: uuid::Uuid,
        input: String,
    ) -> Result<u64, TaskError> {
        let now = Utc::now();
        let table = Self::table();
        let patch = ResumePatch {
            status: TaskStatus::Pending.as_i16(),
            resume_input: Some(input),
            ready_at: Some(now),
            updated_at: now,
        };
        let mut pool = self.pool.clone();
        Ok(db::from(&table)
            .filter(
                table
                    .id
                    .eq(db::val(id))
                    .and(table.status.eq(db::val(TaskStatus::Suspended.as_i16()))),
            )
            .update(&patch)
            .exec(&mut pool)
            .await?)
    }

    /// Returns one deterministic page of tasks after applying typed filters.
    pub(super) async fn list_tasks_impl(
        &self,
        filter: TaskListFilter,
    ) -> Result<TaskListPage, TaskError> {
        let count = page_count(filter.limit)?;
        let table = Self::table();
        let scope = apply_filter(db::from(&table), &table, &filter);
        let mut pool = self.pool.clone();
        let rows = scope
            .order_by(table.created_at.desc())
            .order_by(table.id.desc())
            .slice::<TaskRow>(filter.offset, count)
            .exec(&mut pool)
            .await?;
        build_page(rows, filter.limit, filter.offset)
    }

    /// Reads one task without exposing the persistence row.
    pub(super) async fn get_task_impl(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<TaskRecord>, TaskError> {
        let table = Self::table();
        let mut pool = self.pool.clone();
        let row = db::from(&table)
            .filter(table.id.eq(db::val(id)))
            .first::<TaskRow>()
            .exec(&mut pool)
            .await?;
        row.map(TaskRecord::try_from).transpose()
    }

    /// Commits an outcome only while the caller still owns the task lease.
    pub(super) async fn commit_outcome_impl(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        outcome: TaskOutcome,
    ) -> Result<(), TaskError> {
        match outcome {
            TaskOutcome::Complete { result } => {
                self.commit_complete(task_id, runner_id, result).await
            }
            TaskOutcome::Suspend { state, output } => {
                self.commit_suspend(task_id, runner_id, state, output).await
            }
            TaskOutcome::Sleep { state, delay } => {
                self.commit_sleep(task_id, runner_id, state, delay).await
            }
            TaskOutcome::Retry { delay, error } => {
                self.commit_retry(task_id, runner_id, delay, error).await
            }
            TaskOutcome::Fail { error } => self.commit_fail(task_id, runner_id, error).await,
        }
    }

    /// Records successful completion and releases the lease.
    async fn commit_complete(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        result: String,
    ) -> Result<(), TaskError> {
        let now = Utc::now();
        self.commit_simple(
            task_id,
            runner_id,
            &CompletePatch {
                status: TaskStatus::Succeeded.as_i16(),
                resume_input: None,
                output: None,
                result: Some(result),
                last_error: None,
                ready_at: None,
                completed_at: Some(now),
                locked_by: None,
                leased_until: None,
                updated_at: now,
            },
        )
        .await
    }

    /// Records suspension state and releases the lease.
    async fn commit_suspend(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        state: String,
        output: Option<String>,
    ) -> Result<(), TaskError> {
        let now = Utc::now();
        self.commit_simple(
            task_id,
            runner_id,
            &SuspendPatch {
                status: TaskStatus::Suspended.as_i16(),
                state,
                resume_input: None,
                output,
                result: None,
                last_error: None,
                ready_at: None,
                completed_at: None,
                locked_by: None,
                leased_until: None,
                updated_at: now,
            },
        )
        .await
    }

    /// Reschedules sleeping work with an absolute Rust-calculated timestamp.
    async fn commit_sleep(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        state: String,
        delay: std::time::Duration,
    ) -> Result<(), TaskError> {
        let now = Utc::now();
        let delay = ChronoDuration::from_std(delay).map_err(|_| {
            TaskError::TaskExecutionError(
                "task sleep duration is outside the supported range".into(),
            )
        })?;
        self.commit_simple(
            task_id,
            runner_id,
            &SleepPatch {
                status: TaskStatus::Pending.as_i16(),
                state,
                resume_input: None,
                output: None,
                result: None,
                last_error: None,
                ready_at: Some(add_time(now, delay, "task sleep duration")?),
                completed_at: None,
                locked_by: None,
                leased_until: None,
                updated_at: now,
            },
        )
        .await
    }

    /// Records a terminal failure and releases the lease.
    async fn commit_fail(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        error: String,
    ) -> Result<(), TaskError> {
        let now = Utc::now();
        self.commit_simple(
            task_id,
            runner_id,
            &FailPatch {
                status: TaskStatus::Failed.as_i16(),
                resume_input: None,
                output: None,
                result: None,
                last_error: Some(error),
                ready_at: None,
                completed_at: Some(now),
                locked_by: None,
                leased_until: None,
                updated_at: now,
            },
        )
        .await
    }

    /// Commits a non-retry outcome only while the runner still owns the lease.
    async fn commit_simple<P>(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        patch: &P,
    ) -> Result<(), TaskError>
    where
        P: db::Record,
    {
        let table = Self::table();
        let mut pool = self.pool.clone();
        let rows = db::from(&table)
            .filter(Self::owned_predicate(&table, task_id, runner_id))
            .update(patch)
            .exec(&mut pool)
            .await?;
        log_stale_owner(rows, task_id, runner_id);
        Ok(())
    }

    /// Commits a retry with ownership and attempt-count compare-and-set checks.
    async fn commit_retry(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        delay: Option<std::time::Duration>,
        error: String,
    ) -> Result<(), TaskError> {
        let mut transaction = self.pool.begin().await?;
        let Some(row) = load_owned_task(&mut transaction, task_id, runner_id).await? else {
            transaction.rollback().await?;
            log_stale_owner(0, task_id, runner_id);
            return Ok(());
        };
        let patch = retry_patch(&row, delay, error)?;
        let table = Self::table();
        let rows = db::from(&table)
            .filter(
                Self::owned_predicate(&table, task_id, runner_id)
                    .and(table.attempts.eq(db::val(row.attempts))),
            )
            .update(&patch)
            .exec(&mut transaction)
            .await?;
        transaction.commit().await?;
        log_stale_owner(rows, task_id, runner_id);
        Ok(())
    }
}

/// Fetches minimal retry state under the same transaction as its compare-and-set update.
async fn load_owned_task(
    transaction: &mut db::DbTransaction<'_>,
    task_id: uuid::Uuid,
    runner_id: &str,
) -> Result<Option<TaskRow>, TaskError> {
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(DbTaskStore::owned_predicate(&table, task_id, runner_id))
        .first::<TaskRow>()
        .exec(transaction)
        .await?)
}

/// Calculates the next retry state outside SQL while preserving overflow checks.
fn retry_patch(
    row: &TaskRow,
    requested_delay: Option<std::time::Duration>,
    error: String,
) -> Result<RetryPatch, TaskError> {
    let now = Utc::now();
    let attempts = row
        .attempts
        .checked_add(1)
        .ok_or_else(|| TaskError::TaskExecutionError("task attempt count overflowed".into()))?;
    let exhausted = row.max_attempts.is_some_and(|max| attempts >= max);
    let delay = retry_delay(requested_delay, row.retry_delay_ms)?;
    Ok(RetryPatch {
        status: if exhausted {
            TaskStatus::Failed
        } else {
            TaskStatus::Pending
        }
        .as_i16(),
        attempts,
        last_error: Some(error),
        ready_at: (!exhausted)
            .then(|| add_time(now, delay, "task retry duration"))
            .transpose()?,
        completed_at: exhausted.then_some(now),
        locked_by: None,
        leased_until: None,
        updated_at: now,
    })
}

/// Calculates the limit-plus-one fetch size without allowing overflow.
fn page_count(limit: usize) -> Result<usize, TaskError> {
    limit
        .checked_add(1)
        .ok_or_else(|| TaskError::TaskExecutionError("task list limit overflowed".into()))
}

/// Converts the fetched look-ahead row into Vyuh's stable cursor format.
fn build_page(
    mut rows: Vec<TaskRow>,
    limit: usize,
    offset: usize,
) -> Result<TaskListPage, TaskError> {
    let next_cursor = if rows.len() > limit {
        rows.truncate(limit);
        Some(
            offset
                .checked_add(limit)
                .ok_or_else(|| TaskError::TaskExecutionError("task cursor overflowed".into()))?
                .to_string(),
        )
    } else {
        None
    };
    Ok(TaskListPage {
        records: DbTaskStore::into_records(rows)?,
        next_cursor,
    })
}
