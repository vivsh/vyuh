//! Shared claim protocol and backend-specific candidate selection.

use chrono::{DateTime, Utc};

use crate::{
    db,
    tasks::{TaskError, TaskRecord, TaskStatus},
};

use super::{
    common::DbTaskStore,
    model::{ClaimPatch, TaskRow},
};

impl DbTaskStore {
    /// Claims due rows through a single transaction-safe compare-and-set protocol.
    pub(super) async fn claim_tasks_impl(
        &self,
        runner_id: &str,
    ) -> Result<Vec<TaskRecord>, TaskError> {
        let now = Utc::now();
        let mut transaction = self.pool.begin().await?;
        let candidates = select_candidates(&mut transaction, now, self.batch_size).await?;
        let ids = self
            .claim_candidates(&mut transaction, &candidates, runner_id, now)
            .await?;
        let claimed = Self::fetch_claimed(&mut transaction, ids).await?;
        transaction.commit().await?;
        Ok(claimed)
    }

    /// Conditionally claims every selected row, retaining only successful owners.
    async fn claim_candidates(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        candidates: &[TaskRow],
        runner_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<uuid::Uuid>, TaskError> {
        let mut ids = Vec::with_capacity(candidates.len());
        for row in candidates {
            if self
                .claim_candidate(transaction, row, runner_id, now)
                .await?
            {
                ids.push(row.id);
            }
        }
        Ok(ids)
    }

    /// Atomically changes one still-due task to this runner's lease ownership.
    async fn claim_candidate(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        row: &TaskRow,
        runner_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, TaskError> {
        let table = Self::table();
        let patch = ClaimPatch {
            status: TaskStatus::Running.as_i16(),
            locked_by: Some(runner_id.to_owned()),
            leased_until: Some(self.lease_until(row, now)?),
            updated_at: now,
        };
        let rows = db::from(&table)
            .filter(
                table
                    .id
                    .eq(db::val(row.id))
                    .and(Self::due_predicate(&table, now)),
            )
            .update(&patch)
            .exec(transaction)
            .await?;
        Ok(rows == 1)
    }

    /// Loads only rows successfully claimed in the current transaction.
    async fn fetch_claimed(
        transaction: &mut db::DbTransaction<'_>,
        ids: Vec<uuid::Uuid>,
    ) -> Result<Vec<TaskRecord>, TaskError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let table = Self::table();
        let rows = db::from(&table)
            .filter(table.id.in_values(ids))
            .order_by(table.priority.desc())
            .order_by(table.created_at.asc())
            .all::<TaskRow>()
            .exec(transaction)
            .await?;
        let mut records = Self::into_records(rows)?;
        crate::tasks::sort_claimed_tasks(&mut records);
        Ok(records)
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Selects locked candidates on backends with skip-locked row locks.
async fn select_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    batch_size: usize,
) -> Result<Vec<TaskRow>, TaskError> {
    use crate::db::backend::{LockWaitExt as _, RowLockExt as _};

    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(DbTaskStore::due_predicate(&table, now))
        .order_by(table.priority.desc())
        .order_by(table.created_at.asc())
        .for_update()
        .skip_locked()
        .slice::<TaskRow>(0, batch_size)
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
/// Selects candidates on SQLite; conditional updates provide ownership safety.
async fn select_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    batch_size: usize,
) -> Result<Vec<TaskRow>, TaskError> {
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(DbTaskStore::due_predicate(&table, now))
        .order_by(table.priority.desc())
        .order_by(table.created_at.asc())
        .slice::<TaskRow>(0, batch_size)
        .exec(transaction)
        .await?)
}
