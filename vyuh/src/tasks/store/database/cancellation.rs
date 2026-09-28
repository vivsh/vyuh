//! Store-owned cancellation intent and renewal-time terminalization.

use super::{
    common::DbTaskStore,
    model::{TaskLeaseRow, TaskRow},
    writes,
};
use crate::{
    db::{self, DbSession as _},
    tasks::{TaskId, TaskRuntimeError, TaskStatus, TaskStoreConf},
};

impl DbTaskStore {
    /// Changes intent and eligibility atomically without completing the task.
    pub(super) async fn cancel_impl(&self, id: TaskId) -> Result<bool, TaskRuntimeError> {
        let (_conf, fingerprint) = self.runtime_conf.read().await.clone().ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task runtime was not initialized".into())
        })?;
        let mut tx = self.pool.begin().await?;
        super::runtime::verify_runtime_policy(&mut tx, &fingerprint).await?;
        // A computed update avoids a read/modify/write race and a new patch record.
        // Assign readiness before status: MySQL evaluates assignments left to right.
        let sql = concat!(
            "UPDATE vyuh_tasks SET ready_at = CASE ",
            "WHEN status = 2 THEN CURRENT_TIMESTAMP ",
            "WHEN status = 0 AND ready_at > CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP ",
            "ELSE ready_at END, status = CASE WHEN status = 2 THEN 0 ELSE status END, ",
            "cancelled = TRUE, updated_at = CURRENT_TIMESTAMP ",
            "WHERE cancelled = FALSE AND status IN (0, 1, 2) AND id = "
        );
        #[cfg(feature = "postgres")]
        let sql = format!("{sql}$1");
        #[cfg(not(feature = "postgres"))]
        let sql = format!("{sql}?");
        let changed = tx
            .execute(db::Statement::raw(&sql).bind(id.into_uuid()))
            .await?;
        tx.commit().await?;
        Ok(changed > 0)
    }

    /// Finalizes cancelled owned rows before renewing the remaining lease batch.
    pub(super) async fn cancel_renewals(
        &self,
        tx: &mut db::DbTransaction<'_>,
        rows: &mut Vec<TaskLeaseRow>,
        conf: &TaskStoreConf,
        now: chrono::DateTime<chrono::Utc>,
        deliveries: &mut Vec<(TaskId, String)>,
    ) -> Result<(), TaskRuntimeError> {
        if !rows.iter().any(|row| row.cancelled) {
            return Ok(());
        }
        let ids = rows
            .extract_if(.., |row| row.cancelled)
            .map(|row| row.id)
            .collect::<Vec<_>>();
        // The narrow read already holds these rows' locks; fetch lifecycle data only
        // for cancellations that actually need the shared terminal transition.
        let table = DbTaskStore::table();
        let mut cancelled = db::from(&table)
            .filter(table.id.in_values(ids))
            .sort(table.id.asc())
            .all::<TaskRow>()
            .exec(&mut *tx)
            .await?;
        let outcome = crate::tasks::store::workflow::cancellation();
        for row in &mut cancelled {
            writes::apply_outcome(row, &outcome, crate::tasks::TaskRetry::default(), now)?;
            writes::queue_delivery(row, deliveries)?;
        }
        writes::update_idempotency_batch(tx, &mut cancelled, conf, now).await?;
        writes::batch_update_rows(tx, &cancelled, self.batch_size).await
    }
}

/// Reads requested identities once, including prior cancellation commits whose ACK was lost.
pub(super) async fn load_renewals(
    tx: &mut db::DbTransaction<'_>,
    ids: Vec<uuid::Uuid>,
) -> Result<Vec<TaskLeaseRow>, TaskRuntimeError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let table = DbTaskStore::table();
    let query = db::from(&table)
        .filter(table.id.in_values(ids))
        .sort(table.id.asc());
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    let query = {
        use crate::db::backend::RowLockExt as _;
        query.for_update()
    };
    Ok(query.all::<TaskLeaseRow>().exec(tx).await?)
}

/// Separates terminal cancellation from genuine ownership loss without mutating unowned rows.
pub(super) fn classify_renewals(rows: &mut Vec<TaskLeaseRow>, runner: &str) -> Vec<TaskId> {
    let mut cancelled = Vec::new();
    rows.retain(|row| {
        let owned =
            row.status == TaskStatus::Running.as_i16() && row.locked_by.as_deref() == Some(runner);
        if row.cancelled && (owned || row.status == TaskStatus::Failed.as_i16()) {
            cancelled.push(TaskId::new(row.id));
        }
        owned
    });
    cancelled
}
