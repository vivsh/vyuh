//! `AbstractTaskStore` adapter over Mool-native persistence modules.

use crate::tasks::{
    AbstractTaskStore, GroupClaim, TaskCommit, TaskError, TaskFilter, TaskId, TaskPoll,
    TaskReceipt, TaskRecord, TaskStoreConf, TaskWrite,
};

use super::common::DbTaskStore;

impl AbstractTaskStore for DbTaskStore {
    async fn initialize(&self, conf: TaskStoreConf) -> Result<(), TaskError> {
        self.initialize_impl(conf).await
    }

    async fn claim_tasks(
        &self,
        runner_id: &str,
        claims: &[GroupClaim],
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

    async fn store_tasks(&self, writes: Vec<TaskWrite>) -> Result<Vec<TaskReceipt>, TaskError> {
        self.store_tasks_impl(writes).await
    }

    async fn reassign_group(&self, from: &str, to: &str) -> Result<u64, TaskError> {
        self.reassign_group_impl(from, to).await
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
