//! Store initialization and database-relative clock reads.

#[cfg(all(test, feature = "migrations"))]
#[path = "tests/nodes.rs"]
mod node_tests;

use super::{
    common::DbTaskStore,
    model::{TaskLaneLockRow, TaskRateRow, TaskRow},
};
use crate::{
    db,
    tasks::{TaskRuntimeError, TaskStatus, TaskStoreConf},
};
use chrono::Utc;

const TOKEN_SCALE: i64 = 1_000_000;

impl DbTaskStore {
    /// Inserts one runner incarnation without resetting an existing registration.
    pub(super) async fn register_node_impl(
        &self,
        node: &uuid::Uuid,
    ) -> Result<(), TaskRuntimeError> {
        use db::DbSession as _;
        #[cfg(feature = "postgres")]
        let sql = "INSERT INTO vyuh_task_nodes (id, started_at, heartbeat_at) VALUES ($1, statement_timestamp(), statement_timestamp()) ON CONFLICT (id) DO NOTHING";
        #[cfg(feature = "sqlite")]
        let sql = "INSERT INTO vyuh_task_nodes (id, started_at, heartbeat_at) VALUES (?, strftime('%Y-%m-%d %H:%M:%f', 'now'), strftime('%Y-%m-%d %H:%M:%f', 'now')) ON CONFLICT (id) DO NOTHING";
        #[cfg(feature = "mysql")]
        let sql = "INSERT INTO vyuh_task_nodes (id, started_at, heartbeat_at) VALUES (?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE id = id";
        self.pool
            .clone()
            .execute(db::Statement::raw(sql).bind(*node))
            .await?;
        Ok(())
    }

    /// Updates only the existing node; SQLx MySQL FOUND_ROWS reports matched rows.
    pub(super) async fn heartbeat_impl(
        &self,
        node: &uuid::Uuid,
        stale_after: std::time::Duration,
    ) -> Result<bool, TaskRuntimeError> {
        use db::DbSession as _;
        #[cfg(feature = "postgres")]
        let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = statement_timestamp() WHERE id = $1";
        #[cfg(feature = "sqlite")]
        let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?";
        #[cfg(feature = "mysql")]
        let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = CURRENT_TIMESTAMP(6) WHERE id = ?";
        let present = self
            .pool
            .clone()
            .execute(db::Statement::raw(sql).bind(*node))
            .await?
            == 1;
        if present {
            self.retire_nodes(node, stale_after).await?;
        }
        Ok(present)
    }

    /// A separate autocommit delete rechecks peer age without retaining our heartbeat lock.
    async fn retire_nodes(
        &self,
        node: &uuid::Uuid,
        stale_after: std::time::Duration,
    ) -> Result<(), TaskRuntimeError> {
        use db::DbSession as _;
        let micros = i64::try_from(stale_after.as_micros()).map_err(|_| {
            TaskRuntimeError::InvalidOptions(
                "node retirement duration exceeds database range".into(),
            )
        })?;
        #[cfg(feature = "postgres")]
        let sql = "DELETE FROM vyuh_task_nodes WHERE id <> $1 AND heartbeat_at < statement_timestamp() - (CAST($2 AS BIGINT) * INTERVAL '1 microsecond')";
        #[cfg(feature = "mysql")]
        let sql = "DELETE FROM vyuh_task_nodes WHERE id <> ? AND heartbeat_at < TIMESTAMPADD(MICROSECOND, -?, CURRENT_TIMESTAMP(6))";
        #[cfg(feature = "sqlite")]
        let sql = "DELETE FROM vyuh_task_nodes WHERE id <> ? AND heartbeat_at < strftime('%Y-%m-%d %H:%M:%f', 'now', printf('-%f seconds', ? / 1000000.0))";
        self.pool
            .clone()
            .execute(db::Statement::raw(sql).bind(*node).bind(micros))
            .await?;
        Ok(())
    }

    /// Uses one conditional update at a quiescent lifecycle boundary, with database time.
    /// This neither discovers abandoned nodes nor changes lane leases or workflow state.
    pub(super) async fn return_claims_impl(
        &self,
        node: Option<uuid::Uuid>,
    ) -> Result<u64, TaskRuntimeError> {
        use db::DbSession as _;
        #[cfg(feature = "postgres")]
        let (clock, parameter) = ("statement_timestamp()", "$1");
        #[cfg(feature = "sqlite")]
        let (clock, parameter) = ("strftime('%Y-%m-%d %H:%M:%f', 'now')", "?");
        #[cfg(feature = "mysql")]
        let (clock, parameter) = ("CURRENT_TIMESTAMP(6)", "?");
        let owner = if node.is_some() {
            format!(" AND node_id = {parameter}")
        } else {
            String::new()
        };
        let sql = format!(
            "UPDATE vyuh_tasks SET status = 0, node_id = NULL, \
             ready_at = {clock}, updated_at = {clock} WHERE status = 1{owner}"
        );
        let statement = db::Statement::raw(&sql);
        let statement = match node {
            Some(node) => statement.bind(node),
            None => statement,
        };
        Ok(self.pool.clone().execute(statement).await?)
    }

    /// Validates registered task targets and creates missing rate and lane-owner rows.
    pub(super) async fn initialize_impl(&self) -> Result<(), TaskRuntimeError> {
        let conf = &self.runtime_conf;
        let mut transaction = self.pool.begin().await?;
        let now = statement_now(&mut transaction).await?;
        reject_orphaned_tasks(&mut transaction, conf).await?;
        initialize_rate_rows(&mut transaction, conf, now).await?;
        initialize_lock_rows(&mut transaction, conf, now).await?;
        transaction.commit().await?;
        Ok(())
    }
}

/// Inserts missing opt-in lane rows without disturbing active ownership.
async fn initialize_lock_rows(
    transaction: &mut db::DbTransaction<'_>,
    conf: &TaskStoreConf,
    now: chrono::DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::lane_lock_table();
    for lane in conf.lanes.iter().filter(|lane| lane.lane_lock().is_some()) {
        let row = TaskLaneLockRow {
            lane_name: lane.lane().to_string(),
            owner_id: None,
            owner_token: None,
            leased_until: None,
            phase: crate::tasks::LaneOwnerPhase::Active as i16,
            flushing: false,
            empty_since: None,
            generation: 0,
            hook_retry_at: None,
            last_hook_error: None,
            created_at: now,
            updated_at: now,
        };
        insert_lock_if_missing(transaction, &table, &row).await?;
    }
    Ok(())
}

/// Rejects active rows whose lane is absent from the configured registry.
async fn reject_orphaned_tasks(
    transaction: &mut db::DbTransaction<'_>,
    conf: &TaskStoreConf,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::table();
    let lanes = conf
        .lanes
        .iter()
        .map(|lane| lane.lane().as_str().to_string())
        .collect::<Vec<_>>();
    let active = [
        TaskStatus::Pending,
        TaskStatus::Running,
        TaskStatus::Suspended,
    ]
    .into_iter()
    .map(TaskStatus::as_i16)
    .collect::<Vec<_>>();
    let orphan = db::from(&table)
        .filter(table.status.in_values(active.clone()))
        .filter(table.lane_name.not_in_values(lanes))
        .first::<TaskRow>()
        .exec(transaction)
        .await?;
    if let Some(task) = orphan {
        return Err(TaskRuntimeError::UnknownLane(task.lane_name));
    }
    let handlers = conf
        .handlers
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let orphan = db::from(&table)
        .filter(table.status.in_values(active))
        .filter(table.name.not_in_values(handlers))
        .first::<TaskRow>()
        .exec(transaction)
        .await?;
    if let Some(task) = orphan {
        return Err(TaskRuntimeError::TaskNotFound(task.name));
    }
    Ok(())
}

/// Creates missing global buckets without resetting existing tokens or refill clocks.
/// Configuration compatibility is not checked against a persisted fingerprint.
async fn initialize_rate_rows(
    transaction: &mut db::DbTransaction<'_>,
    conf: &TaskStoreConf,
    now: chrono::DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::rate_table();
    for lane in &conf.lanes {
        let Some(rate) = lane.global_rate() else {
            continue;
        };
        let row = TaskRateRow {
            id: uuid::Uuid::now_v7(),
            lane_name: lane.lane().to_string(),
            tokens_micros: i64::from(rate.burst_size()).saturating_mul(TOKEN_SCALE),
            updated_at: now,
        };
        insert_rate_if_missing(transaction, &table, &row).await?;
    }
    Ok(())
}

/// Operation-local clock evidence; no persisted runtime-policy row is consulted.
#[derive(db::Record)]
pub(super) struct StoreClockRow {
    pub(super) now: chrono::DateTime<Utc>,
}

/// Missing clock evidence indicates a malformed internal query projection.
pub(super) fn missing_clock() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("task query omitted database clock".into())
}

/// Reads database time without locking or inspecting deployment metadata.
pub(super) async fn statement_now(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<chrono::DateTime<Utc>, TaskRuntimeError> {
    use db::DbSession as _;
    Ok(transaction
        .fetch_scalar(db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}

/// Creates one rate row without resetting an existing bucket's refill clock.
async fn insert_rate_if_missing(
    transaction: &mut db::DbTransaction<'_>,
    table: &db::queries::ModelTable<TaskRateRow>,
    row: &TaskRateRow,
) -> Result<(), TaskRuntimeError> {
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    {
        use crate::db::backend::IgnoreConflictsExt as _;
        db::from(table)
            .insert_many(std::slice::from_ref(row))
            .ignore_conflicts_on(&table.lane_name)
            .exec(transaction)
            .await?;
    }
    #[cfg(all(feature = "mysql", not(any(feature = "postgres", feature = "sqlite"))))]
    {
        use crate::db::backend::IgnoreErrorsExt as _;
        db::from(table)
            .insert_many(std::slice::from_ref(row))
            .ignore_errors()
            .exec(transaction)
            .await?;
    }
    Ok(())
}

/// Creates one lane-lock row while preserving any current owner and phase.
async fn insert_lock_if_missing(
    transaction: &mut db::DbTransaction<'_>,
    table: &db::queries::ModelTable<TaskLaneLockRow>,
    row: &TaskLaneLockRow,
) -> Result<(), TaskRuntimeError> {
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    {
        use crate::db::backend::IgnoreConflictsExt as _;
        db::from(table)
            .insert_many(std::slice::from_ref(row))
            .ignore_conflicts_on(&table.lane_name)
            .exec(transaction)
            .await?;
    }
    #[cfg(all(feature = "mysql", not(any(feature = "postgres", feature = "sqlite"))))]
    {
        use crate::db::backend::IgnoreErrorsExt as _;
        db::from(table)
            .insert_many(std::slice::from_ref(row))
            .ignore_errors()
            .exec(transaction)
            .await?;
    }
    Ok(())
}

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "tests/runtime.rs"]
mod tests;
