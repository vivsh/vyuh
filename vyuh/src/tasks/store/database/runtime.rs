//! Persistent task-runtime policy initialization and orphan detection.

use chrono::Utc;

use crate::{
    db,
    tasks::{TaskError, TaskStatus, TaskStoreConf},
};

use super::{
    common::DbTaskStore,
    model::{RuntimePolicyPatch, TaskRateRow, TaskRow, TaskRuntimeRow},
    writes::{batch_update_rows, finish, update_idempotency_batch},
};

const RUNTIME_ID: uuid::Uuid = uuid::Uuid::from_u128(1);
const TOKEN_SCALE: i64 = 1_000_000;

impl DbTaskStore {
    /// Establishes one compatible store-wide policy before any claims are served.
    pub(super) async fn initialize_impl(&self, conf: TaskStoreConf) -> Result<(), TaskError> {
        let fingerprint = crate::tasks::store::policy_fingerprint(&conf);
        let mut transaction = self.pool.begin().await?;
        let now = statement_now(&mut transaction).await?;
        self.fail_unleased_running(&mut transaction, &conf, now)
            .await?;
        reject_orphaned_tasks(&mut transaction, &conf).await?;
        ensure_runtime_policy(&mut transaction, &fingerprint, now).await?;
        initialize_rate_rows(&mut transaction, &conf, &fingerprint, now).await?;
        transaction.commit().await?;
        *self.runtime_conf.write().await = Some(conf);
        Ok(())
    }
}

/// Terminates legacy running rows that have no deadline for safe lease recovery.
impl DbTaskStore {
    /// Terminates legacy running rows that have no deadline for safe lease recovery.
    async fn fail_unleased_running(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        conf: &TaskStoreConf,
        now: chrono::DateTime<Utc>,
    ) -> Result<(), TaskError> {
        loop {
            let table = DbTaskStore::table();
            let mut rows = db::from(&table)
                .filter(table.status.eq(db::val(TaskStatus::Running.as_i16())))
                .filter(table.leased_until.is_null())
                .slice::<TaskRow>(0, self.batch_size)
                .exec(&mut *transaction)
                .await?;
            if rows.is_empty() {
                return Ok(());
            }
            for row in &mut rows {
                finish(
                    row,
                    TaskStatus::Failed,
                    Some("Running task has no lease deadline".into()),
                    now,
                );
                row.locked_by = None;
                row.leased_until = None;
                row.updated_at = now;
            }
            update_idempotency_batch(transaction, &mut rows, conf, now).await?;
            batch_update_rows(transaction, &rows, self.batch_size).await?;
        }
    }
}

/// Creates or safely advances the singleton policy while no work is running.
async fn ensure_runtime_policy(
    transaction: &mut db::DbTransaction<'_>,
    fingerprint: &str,
    now: chrono::DateTime<Utc>,
) -> Result<(), TaskError> {
    let table = DbTaskStore::runtime_table();
    let row = TaskRuntimeRow {
        id: RUNTIME_ID,
        policy_fingerprint: fingerprint.into(),
        updated_at: now,
    };
    insert_runtime_if_missing(transaction, &table, &row).await?;
    let stored = load_runtime_for_update(transaction).await?.ok_or_else(|| {
        TaskError::TaskExecutionError("task runtime policy was not stored".into())
    })?;
    if stored.policy_fingerprint != fingerprint {
        replace_runtime_policy(transaction, fingerprint, now).await?;
    }
    Ok(())
}

/// Rejects active rows whose lane is absent from the candidate policy.
async fn reject_orphaned_tasks(
    transaction: &mut db::DbTransaction<'_>,
    conf: &TaskStoreConf,
) -> Result<(), TaskError> {
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
        return Err(TaskError::UnknownLane(task.lane_name));
    }
    let handlers = conf.handlers.clone();
    let orphan = db::from(&table)
        .filter(table.status.in_values(active))
        .filter(table.name.not_in_values(handlers))
        .first::<TaskRow>()
        .exec(transaction)
        .await?;
    if let Some(task) = orphan {
        return Err(TaskError::TaskNotFound(task.name));
    }
    Ok(())
}

/// Ensures each limited lane has one durable bucket for the current policy.
async fn initialize_rate_rows(
    transaction: &mut db::DbTransaction<'_>,
    conf: &TaskStoreConf,
    fingerprint: &str,
    now: chrono::DateTime<Utc>,
) -> Result<(), TaskError> {
    let table = DbTaskStore::rate_table();
    for lane in &conf.lanes {
        let Some(rate) = lane.global_rate() else {
            continue;
        };
        let row = TaskRateRow {
            id: uuid::Uuid::now_v7(),
            lane_name: lane.lane().to_string(),
            policy_fingerprint: fingerprint.into(),
            tokens_micros: i64::from(rate.burst_size()).saturating_mul(TOKEN_SCALE),
            updated_at: now,
        };
        insert_rate_if_missing(transaction, &table, &row).await?;
        let stored = db::from(&table)
            .filter(table.lane_name.eq(db::val(lane.lane().to_string())))
            .first::<TaskRateRow>()
            .exec(&mut *transaction)
            .await?
            .ok_or_else(|| {
                TaskError::TaskExecutionError("task rate state was not stored".into())
            })?;
        if stored.policy_fingerprint != fingerprint {
            return Err(TaskError::InvalidConfig(format!(
                "task lane '{}' has an incompatible global rate policy",
                lane.lane()
            )));
        }
    }
    Ok(())
}

pub(super) async fn verify_runtime_policy(
    transaction: &mut db::DbTransaction<'_>,
    conf: &TaskStoreConf,
) -> Result<(), TaskError> {
    let expected = crate::tasks::store::policy_fingerprint(conf);
    let stored = load_runtime_for_share(transaction).await?.ok_or_else(|| {
        TaskError::InvalidConfig("task runtime policy has not been initialized".into())
    })?;
    if stored.policy_fingerprint != expected {
        return Err(TaskError::InvalidConfig(
            "task worker policy changed after this worker initialized".into(),
        ));
    }
    Ok(())
}

/// Replaces an idle policy and discards buckets that belong to its old shape.
async fn replace_runtime_policy(
    transaction: &mut db::DbTransaction<'_>,
    fingerprint: &str,
    now: chrono::DateTime<Utc>,
) -> Result<(), TaskError> {
    let tasks = DbTaskStore::table();
    let running = db::from(&tasks)
        .filter(tasks.status.eq(db::val(TaskStatus::Running.as_i16())))
        .exists()
        .exec(&mut *transaction)
        .await?;
    if running {
        return Err(TaskError::InvalidConfig(
            "task lane or global rate policy cannot change while tasks are running".into(),
        ));
    }
    let runtime = DbTaskStore::runtime_table();
    let patch = RuntimePolicyPatch {
        policy_fingerprint: fingerprint.into(),
        updated_at: now,
    };
    db::from(&runtime)
        .filter(runtime.id.eq(db::val(RUNTIME_ID)))
        .update(&patch)
        .exec(&mut *transaction)
        .await?;
    let rates = DbTaskStore::rate_table();
    db::from(&rates)
        .filter(rates.id.is_not_null())
        .delete()
        .exec(transaction)
        .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn load_runtime(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<Option<TaskRuntimeRow>, TaskError> {
    let table = DbTaskStore::runtime_table();
    Ok(db::from(&table)
        .filter(table.id.eq(db::val(RUNTIME_ID)))
        .first::<TaskRuntimeRow>()
        .exec(transaction)
        .await?)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Exclusively locks the singleton while initializing or changing policy.
async fn load_runtime_for_update(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<Option<TaskRuntimeRow>, TaskError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::runtime_table();
    Ok(db::from(&table)
        .filter(table.id.eq(db::val(RUNTIME_ID)))
        .for_update()
        .first::<TaskRuntimeRow>()
        .exec(transaction)
        .await?)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Holds a shared policy lock across each persistent runtime mutation.
async fn load_runtime_for_share(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<Option<TaskRuntimeRow>, TaskError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::runtime_table();
    Ok(db::from(&table)
        .filter(table.id.eq(db::val(RUNTIME_ID)))
        .for_share()
        .first::<TaskRuntimeRow>()
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
async fn load_runtime_for_update(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<Option<TaskRuntimeRow>, TaskError> {
    load_runtime(transaction).await
}

#[cfg(feature = "sqlite")]
async fn load_runtime_for_share(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<Option<TaskRuntimeRow>, TaskError> {
    load_runtime(transaction).await
}

async fn statement_now(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<chrono::DateTime<Utc>, TaskError> {
    use db::DbSession as _;
    Ok(transaction
        .fetch_scalar(db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}

/// Creates the singleton runtime row without mutating an existing policy.
async fn insert_runtime_if_missing(
    transaction: &mut db::DbTransaction<'_>,
    table: &db::queries::ModelTable<TaskRuntimeRow>,
    row: &TaskRuntimeRow,
) -> Result<(), TaskError> {
    db::from(table)
        .batch_upsert(std::slice::from_ref(row), &table.id)
        .update_only(&table.id)
        .exec(transaction)
        .await?;
    Ok(())
}

/// Creates one rate row without resetting an existing bucket's refill clock.
async fn insert_rate_if_missing(
    transaction: &mut db::DbTransaction<'_>,
    table: &db::queries::ModelTable<TaskRateRow>,
    row: &TaskRateRow,
) -> Result<(), TaskError> {
    db::from(table)
        .batch_upsert(std::slice::from_ref(row), &table.lane_name)
        .update_only(&table.lane_name)
        .exec(transaction)
        .await?;
    Ok(())
}
