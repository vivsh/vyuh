//! `AbstractTaskStore` adapter over Mool-native persistence modules.

use crate::tasks::store::StoreFailure;
use crate::tasks::{
    AbstractTaskStore, CommitResult, LaneClaim, ScheduledTaskWrite, TaskCommit, TaskFilter, TaskId,
    TaskPoll, TaskReceipt, TaskRecord, TaskRuntimeError, TaskWrite,
};

use super::common::DbTaskStore;

impl AbstractTaskStore for DbTaskStore {
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
