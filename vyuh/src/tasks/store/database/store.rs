//! Persistent store construction, startup validation, inspection, and operation entrypoints.
use super::submit::{load_active_lane_for_update, require_runtime_lane};

use super::model::{TaskIdempotencyRow, TaskScheduleRow};
use super::model::{TaskLaneLockRow, TaskRateRow, TaskRow};
use super::queries::statement_now;
use crate::db;
use crate::tasks::store::StoreFailure;
use crate::tasks::{
    CommitResult, LaneClaim, ScheduledTaskWrite, TaskCommit, TaskFilter, TaskId, TaskPoll,
    TaskReceipt, TaskRecord, TaskRuntimeError, TaskStore, TaskWrite,
};
use crate::tasks::{TaskStatus, TaskStoreConf};
use chrono::Utc;

/// Mool-backed persistent task store selected by Vyuh's database feature.
#[derive(Clone)]
pub struct DbTaskStore {
    pub(super) pool: db::DbPool,
    pub(super) commit_batch_size: usize,
    pub(super) lease_duration: std::time::Duration,
    pub(super) runtime_conf: std::sync::Arc<crate::tasks::TaskStoreConf>,
    pub(super) maintenance_turn: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl DbTaskStore {
    /// Executes an independent submission or control transaction.
    pub(super) async fn reassign_lane_impl(
        &self,
        from: &str,
        to: &str,
    ) -> Result<u64, TaskRuntimeError> {
        require_runtime_lane(self, to)?;
        let mut transaction = self.pool.begin().await?;
        let mut rows = load_active_lane_for_update(&mut transaction, from).await?;
        if rows
            .iter()
            .any(|row| row.status == TaskStatus::Running.as_i16())
        {
            transaction.rollback().await?;
            return Err(TaskRuntimeError::LaneBusy(from.into()));
        }
        let now = statement_now(&mut transaction).await?;
        for row in &mut rows {
            row.lane_name = to.into();
            row.node_id = None;
            row.updated_at = now;
        }
        #[cfg(not(feature = "postgres"))]
        let table = Self::table();
        let changed = if rows.is_empty() {
            0
        } else {
            #[cfg(feature = "postgres")]
            {
                super::notify::update(
                    &mut transaction,
                    &rows,
                    &["lane_name", "node_id", "updated_at"],
                    super::queries::WRITE_CHUNK_SIZE,
                )
                .await?
            }
            #[cfg(not(feature = "postgres"))]
            {
                db::from(&table)
                    .update_many(&rows, (&table.lane_name, &table.node_id, &table.updated_at))
                    .batch_size(super::queries::WRITE_CHUNK_SIZE)
                    .exec(&mut transaction)
                    .await?
            }
        };
        transaction.commit().await?;
        Ok(changed)
    }
    /// Configures a task store without querying the database; worker setup happens at startup.
    pub fn new(
        pool: db::Pool,
        batch_size: usize,
        lease_duration: std::time::Duration,
        conf: crate::tasks::TaskStoreConf,
    ) -> Self {
        Self {
            pool: db::DbPool::from_pool(pool),
            commit_batch_size: batch_size.max(1),
            lease_duration,
            runtime_conf: conf.into(),
            maintenance_turn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Reads one task without exposing its persistence row.
    pub(super) async fn get_task_impl(
        &self,
        id: TaskId,
    ) -> Result<Option<TaskRecord>, TaskRuntimeError> {
        let table = Self::table();
        let mut pool = self.pool.clone();
        db::from(&table)
            .filter(table.id.eq(db::val(id.into_uuid())))
            .first::<TaskRow>()
            .exec(&mut pool)
            .await?
            .map(TaskRecord::try_from)
            .transpose()
    }
}

impl TaskStore for DbTaskStore {
    #[cfg(feature = "postgres")]
    async fn start_notifications(
        &self,
        node: uuid::Uuid,
        events: tokio::sync::broadcast::Sender<crate::tasks::client::RunnerEvent>,
        shutdown: crate::notifiers::CancellationNotifier,
    ) -> Result<Option<tokio::task::JoinHandle<()>>, TaskRuntimeError> {
        super::notify::listen(self, node, events, shutdown)
            .await
            .map(Some)
    }
    async fn commit_admitted(
        &self,
        commits: &[TaskCommit],
        node: &uuid::Uuid,
        limit: usize,
        cached: &[std::sync::Arc<TaskRecord>],
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        self.commit_admitted_impl(commits, Some((*node, limit)), cached)
            .await
    }
    async fn register_node(&self, node: &uuid::Uuid) -> Result<(), TaskRuntimeError> {
        self.register_node_impl(node).await
    }

    async fn heartbeat(
        &self,
        node: &uuid::Uuid,
        stale_after: std::time::Duration,
    ) -> Result<bool, TaskRuntimeError> {
        self.heartbeat_impl(node, stale_after).await
    }

    async fn return_claims(&self, node: Option<uuid::Uuid>) -> Result<u64, TaskRuntimeError> {
        self.return_claims_impl(node).await
    }

    async fn initialize(&self) -> Result<(), TaskRuntimeError> {
        self.initialize_impl().await
    }

    async fn claim_tasks(
        &self,
        runner_id: &uuid::Uuid,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        self.claim_tasks_impl(runner_id, claims).await
    }

    async fn commit_tasks(
        &self,
        commits: &[TaskCommit],
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        self.commit_tasks_impl(commits).await
    }

    async fn coordinate_lanes(
        &self,
        node: &uuid::Uuid,
        lanes: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        crate::tasks::store::validate_coordination(lanes)?;
        self.claim_tasks_impl(node, lanes).await
    }

    async fn recover_claims(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
        mut prepared: TaskPoll,
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        match self.recover_claims_impl(node, claims, &prepared).await {
            Ok(poll) => Ok(poll),
            Err(StoreFailure::CommitUnknown {
                error,
                prepared: recovered,
            }) => {
                crate::tasks::store::merge_wakes(&mut prepared.wake_lanes, recovered.wake_lanes);
                Err(StoreFailure::CommitUnknown { error, prepared })
            }
            Err(error) => Err(error.retain(prepared)),
        }
    }

    async fn recover_outputs(
        &self,
        node: &uuid::Uuid,
        commits: &[TaskCommit],
        mut prepared: CommitResult,
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        match self.recover_outputs_impl(node, commits, &prepared).await {
            Ok(result) => Ok(result),
            Err(StoreFailure::CommitUnknown {
                error,
                prepared: recovered,
            }) => {
                crate::tasks::store::merge_wakes(&mut prepared.wake_lanes, recovered.wake_lanes);
                Err(StoreFailure::CommitUnknown { error, prepared })
            }
            Err(error) => Err(error.retain(prepared)),
        }
    }

    async fn store_tasks(
        &self,
        writes: Vec<TaskWrite>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        self.store_tasks_impl(writes).await
    }

    async fn store_admitted(
        &self,
        writes: Vec<TaskWrite>,
        node: &uuid::Uuid,
        limit: usize,
    ) -> Result<(Vec<TaskReceipt>, Vec<TaskRecord>), TaskRuntimeError> {
        self.store_admitted_impl(writes, node, limit).await
    }

    async fn schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<crate::tasks::TaskScheduleSnapshot, TaskRuntimeError> {
        self.schedule_snapshot_impl(names).await
    }

    async fn store_scheduled(
        &self,
        write: ScheduledTaskWrite,
    ) -> Result<Option<TaskReceipt>, TaskRuntimeError> {
        self.store_scheduled_impl(write).await
    }

    async fn reassign_lane(&self, from: &str, to: &str) -> Result<u64, TaskRuntimeError> {
        self.reassign_lane_impl(from, to).await
    }

    async fn resume(&self, id: TaskId, input: String) -> Result<bool, TaskRuntimeError> {
        self.resume_impl(id, input).await
    }

    async fn list_tasks(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskRuntimeError> {
        self.list_tasks_impl(filter).await
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskRuntimeError> {
        self.get_task_impl(id).await
    }
}

const TOKEN_SCALE: i64 = 1_000_000;
impl DbTaskStore {
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

impl DbTaskStore {
    /// Runs bounded archival cleanup on the first and every thirty-second claim turn.
    /// Failed turns may advance this housekeeping cadence; correctness never depends on it.
    pub(super) fn maintenance_due(&self) -> bool {
        self.maintenance_turn
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .is_multiple_of(32)
    }

    /// Returns the selected backend's typed task table handle.
    pub(super) fn table() -> db::queries::ModelTable<TaskRow> {
        <TaskRow as db::Model>::table()
    }

    /// Returns the typed idempotency ownership table.
    pub(super) fn idempotency_table() -> db::queries::ModelTable<TaskIdempotencyRow> {
        <TaskIdempotencyRow as db::Model>::table()
    }

    /// Returns the typed global rate-bucket table.
    pub(super) fn rate_table() -> db::queries::ModelTable<TaskRateRow> {
        <TaskRateRow as db::Model>::table()
    }

    /// Returns the durable lane-owner coordination table.
    pub(super) fn lane_lock_table() -> db::queries::ModelTable<TaskLaneLockRow> {
        <TaskLaneLockRow as db::Model>::table()
    }

    /// Returns the typed durable emitter schedule cursor table.
    pub(super) fn schedule_table() -> db::queries::ModelTable<TaskScheduleRow> {
        <TaskScheduleRow as db::Model>::table()
    }
}

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "tests/store.rs"]
mod tests;
