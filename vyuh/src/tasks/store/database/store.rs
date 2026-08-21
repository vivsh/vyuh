//! `AbstractTaskStore` adapter over Mool-native persistence modules.

use std::collections::BTreeSet;

use crate::tasks::{
    AbstractTaskStore, LaneClaim, ScheduledTaskWrite, TaskCommit, TaskError, TaskFilter, TaskId,
    TaskLease, TaskPoll, TaskReceipt, TaskRecord, TaskStoreConf, TaskTick, TaskWrite,
};

use super::common::DbTaskStore;

impl DbTaskStore {
    /// Applies one paced scheduler turn under one database transaction.
    pub(super) async fn tick_impl(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskLease],
    ) -> Result<TaskTick, TaskError> {
        let conf =
            self.runtime_conf.read().await.clone().ok_or_else(|| {
                TaskError::InvalidConfig("task runtime was not initialized".into())
            })?;
        let mut transaction = self.pool.begin().await?;
        super::runtime::verify_runtime_policy(&mut transaction, &conf).await?;
        let now = statement_now(&mut transaction).await?;
        let lanes = locked_turn_lanes(&conf, claims, commits, renewals);
        super::writes::lock_lane_rows(&mut transaction, lanes).await?;
        self.commit_outcomes_tx(&mut transaction, runner_id, commits, &conf, now)
            .await?;
        let lost = self
            .renew_leases_tx(&mut transaction, runner_id, renewals, &conf, now)
            .await?;
        let poll = self
            .claim_tasks_tx(&mut transaction, runner_id, claims, &conf, now)
            .await?;
        transaction.commit().await?;
        Ok(TaskTick { poll, lost })
    }
}

/// Collects opt-in lane rows so one central turn always locks them in name order.
fn locked_turn_lanes(
    conf: &TaskStoreConf,
    claims: &[LaneClaim],
    commits: &[TaskCommit],
    renewals: &[TaskLease],
) -> BTreeSet<crate::tasks::TaskLane> {
    let configured = |lane: crate::tasks::TaskLane| {
        conf.lanes
            .iter()
            .find(|entry| entry.lane() == lane)
            .is_some_and(|entry| entry.lane_lock().is_some())
    };
    claims
        .iter()
        .map(|claim| claim.lane)
        .chain(commits.iter().map(|commit| commit.lane))
        .chain(renewals.iter().map(|lease| lease.lane))
        .filter(|lane| configured(*lane))
        .collect()
}

async fn statement_now(
    transaction: &mut crate::db::DbTransaction<'_>,
) -> Result<chrono::DateTime<chrono::Utc>, TaskError> {
    use crate::db::DbSession as _;
    Ok(transaction
        .fetch_scalar(crate::db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::tasks::{TaskLaneConf, TaskLaneLock};

    const ORDINARY: crate::tasks::TaskLane = crate::tasks::TaskLane::new("ordinary");
    const LOCKED: crate::tasks::TaskLane = crate::tasks::TaskLane::new("locked");

    /// Verifies ordinary task lanes never enter the database lane-row coordination set.
    #[test]
    fn only_opted_in_lanes_lock_durable_owner_rows() {
        let conf = TaskStoreConf {
            handlers: Vec::new(),
            lanes: vec![
                TaskLaneConf::new(ORDINARY, 1),
                TaskLaneConf::new(LOCKED, 1).lock(TaskLaneLock::new(1)),
            ],
            idempotency: Vec::new(),
            schedules: Vec::new(),
            poll_interval: Duration::from_secs(1),
        };
        let claims = [
            LaneClaim {
                lane: ORDINARY,
                limit: 1,
                owner: None,
            },
            LaneClaim {
                lane: LOCKED,
                limit: 1,
                owner: None,
            },
        ];
        let locked = locked_turn_lanes(&conf, &claims, &[], &[]);
        assert_eq!(locked.len(), 1);
        assert!(locked.contains(&LOCKED));
        assert!(!locked.contains(&ORDINARY));
    }
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
        leases: &[TaskLease],
    ) -> Result<Vec<TaskId>, TaskError> {
        self.renew_leases_impl(runner_id, leases).await
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskLease],
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
