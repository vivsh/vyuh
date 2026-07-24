//! `AbstractTaskStore` adapter over the Mool-native persistence modules.

use crate::tasks::{
    TaskError, TaskListFilter, TaskListPage, TaskOutcome, TaskRecord, store::AbstractTaskStore,
};

use super::common::DbTaskStore;

impl AbstractTaskStore for DbTaskStore {
    async fn claim_tasks(&self, runner_id: &str) -> Result<Vec<TaskRecord>, TaskError> {
        self.claim_tasks_impl(runner_id).await
    }

    async fn commit_outcome(
        &self,
        task_id: uuid::Uuid,
        runner_id: &str,
        outcome: TaskOutcome,
    ) -> Result<(), TaskError> {
        self.commit_outcome_impl(task_id, runner_id, outcome).await
    }

    async fn store_task(&self, record: TaskRecord) -> Result<(), TaskError> {
        self.store_task_impl(record).await
    }

    async fn resume(&self, id: uuid::Uuid, input: String) -> Result<u64, TaskError> {
        self.resume_impl(id, input).await
    }

    async fn list_tasks(&self, filter: TaskListFilter) -> Result<TaskListPage, TaskError> {
        self.list_tasks_impl(filter).await
    }

    async fn get_task(&self, id: uuid::Uuid) -> Result<Option<TaskRecord>, TaskError> {
        self.get_task_impl(id).await
    }

    async fn run_migrations(&self) -> Result<(), TaskError> {
        Err(TaskError::MigrationRequired)
    }
}
