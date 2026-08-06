//! `AbstractTaskStore` adapter over Mool-native persistence modules.

use crate::tasks::{
    AbstractTaskStore, LaneClaim, ScheduledTaskWrite, TaskCommit, TaskError, TaskFilter, TaskId,
    TaskPoll, TaskReceipt, TaskRecord, TaskStoreConf, TaskTick, TaskWrite,
};

use super::common::DbTaskStore;

impl DbTaskStore {
    /// Applies one paced scheduler turn under one database transaction.
    pub(super) async fn tick_impl(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskId],
    ) -> Result<TaskTick, TaskError> {
        let conf =
            self.runtime_conf.read().await.clone().ok_or_else(|| {
                TaskError::InvalidConfig("task runtime was not initialized".into())
            })?;
        let mut transaction = self.pool.begin().await?;
        super::runtime::verify_runtime_policy(&mut transaction, &conf).await?;
        let now = statement_now(&mut transaction).await?;
        self.commit_outcomes_tx(&mut transaction, runner_id, commits, &conf, now)
            .await?;
        let lost = self
            .renew_leases_tx(&mut transaction, runner_id, renewals, now)
            .await?;
        let poll = self
            .claim_tasks_tx(&mut transaction, runner_id, claims, &conf, now)
            .await?;
        transaction.commit().await?;
        Ok(TaskTick { poll, lost })
    }
}

async fn statement_now(
    transaction: &mut crate::db::DbTransaction<'_>,
) -> Result<chrono::DateTime<chrono::Utc>, TaskError> {
    use crate::db::DbSession as _;
    Ok(transaction
        .fetch_scalar(crate::db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}

impl AbstractTaskStore for DbTaskStore {
    async fn initialize(&self, conf: TaskStoreConf) -> Result<(), TaskError> {
        self.initialize_impl(conf).await
    }

    async fn claim_tasks(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, TaskError> {
        self.claim_tasks_impl(runner_id, claims).await
    }

    async fn commit_outcomes(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskError> {
        self.commit_outcomes_impl(runner_id, commits).await
    }

    async fn renew_leases(
        &self,
        runner_id: &str,
        task_ids: &[TaskId],
    ) -> Result<Vec<TaskId>, TaskError> {
        self.renew_leases_impl(runner_id, task_ids).await
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskId],
    ) -> Result<TaskTick, TaskError> {
        self.tick_impl(runner_id, claims, commits, renewals).await
    }

    async fn store_tasks(&self, writes: Vec<TaskWrite>) -> Result<Vec<TaskReceipt>, TaskError> {
        self.store_tasks_impl(writes).await
    }

    async fn schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<crate::tasks::TaskScheduleSnapshot, TaskError> {
        self.schedule_snapshot_impl(names).await
    }

    async fn store_scheduled(
        &self,
        write: ScheduledTaskWrite,
    ) -> Result<Option<TaskReceipt>, TaskError> {
        self.store_scheduled_impl(write).await
    }

    async fn reassign_lane(&self, from: &str, to: &str) -> Result<u64, TaskError> {
        self.reassign_lane_impl(from, to).await
    }

    async fn resume(&self, id: TaskId, input: String) -> Result<bool, TaskError> {
        self.resume_impl(id, input).await
    }

    async fn list_tasks(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskError> {
        self.list_tasks_impl(filter).await
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskError> {
        self.get_task_impl(id).await
    }
}
