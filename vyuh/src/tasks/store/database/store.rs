//! `AbstractTaskStore` adapter over Mool-native persistence modules.

use std::collections::BTreeSet;

use crate::tasks::{
    AbstractTaskStore, LaneClaim, ScheduledTaskWrite, TaskCommit, TaskFilter, TaskId, TaskLease,
    TaskPoll, TaskReceipt, TaskRecord, TaskRuntimeError, TaskStoreConf, TaskTick, TaskWrite,
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
    ) -> Result<TaskTick, TaskRuntimeError> {
        let (conf, fingerprint) = self.runtime_conf.read().await.clone().ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task runtime was not initialized".into())
        })?;
        let lanes = locked_turn_lanes(&conf, claims, commits, renewals);
        crate::tasks::store::all::validate_turn(commits, self.batch_size, conf.max_all_children)?;
        let mut transaction = self.pool.begin().await?;
        let (now, loaded, observations, preloaded) = self
            .read_turn(
                &mut transaction,
                runner_id,
                &conf,
                &fingerprint,
                claims,
                commits,
                renewals,
                lanes.is_empty(),
            )
            .await?;
        super::writes::lock_lane_rows(&mut transaction, lanes).await?;
        let (children, mut deliveries, waits, mut settled) = self
            .commit_outcomes_tx(&mut transaction, runner_id, commits, &conf, now, loaded)
            .await?;
        settled.extend(observations);
        let (lost, cancelled) = self
            .renew_leases_tx(
                &mut transaction,
                runner_id,
                renewals,
                &conf,
                now,
                &mut deliveries,
                settled,
            )
            .await?;
        let (mut poll, new_idle) = self
            .claim_tasks_tx(
                &mut transaction,
                runner_id,
                claims,
                &conf,
                now,
                &mut deliveries,
                preloaded,
            )
            .await?;
        let wake_lanes = self
            .finish_workflow(
                &mut transaction,
                &children,
                deliveries,
                &waits,
                &conf,
                now,
                &mut poll,
                &new_idle,
            )
            .await?;
        transaction.commit().await?;
        Ok(TaskTick {
            poll,
            lost,
            cancelled,
            wake_lanes,
        })
    }

    /// Reads only transaction-local evidence, preserving lane-before-task lock ordering.
    async fn read_turn(
        &self,
        transaction: &mut crate::db::DbTransaction<'_>,
        runner_id: &str,
        conf: &TaskStoreConf,
        fingerprint: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskLease],
        ordinary: bool,
    ) -> Result<
        (
            chrono::DateTime<chrono::Utc>,
            Option<Vec<super::model::TaskRow>>,
            Vec<super::model::TaskLeaseRow>,
            Option<(
                Vec<super::model::TaskRow>,
                Vec<super::model::TaskRow>,
                Option<chrono::DateTime<chrono::Utc>>,
            )>,
        ),
        TaskRuntimeError,
    > {
        let early_claim = if commits.is_empty() && renewals.is_empty() {
            super::claim_read::eligible(conf, claims)
        } else {
            None
        };
        Ok(if let Some(claim) = early_claim {
            let (now, probe, selected, deadline) =
                super::claim_read::read(transaction, claim, fingerprint, self.batch_size).await?;
            (now, None, Vec::new(), Some((probe, selected, deadline)))
        } else if ordinary {
            let (now, rows, leases) = super::turn_read::observations(
                transaction,
                runner_id,
                commits,
                renewals,
                fingerprint,
                self.batch_size,
            )
            .await?;
            (now, rows, leases, None)
        } else {
            (
                super::runtime::verify_runtime_policy(transaction, fingerprint).await?,
                None,
                Vec::new(),
                None,
            )
        })
    }

    /// Finalizes workflow writes and reconciles pre-insert idle evidence before commit.
    async fn finish_workflow(
        &self,
        transaction: &mut crate::db::DbTransaction<'_>,
        children: &[super::model::TaskRow],
        deliveries: Vec<(TaskId, String)>,
        waits: &[super::all::WaitWrite],
        conf: &TaskStoreConf,
        now: chrono::DateTime<chrono::Utc>,
        poll: &mut TaskPoll,
        new_idle: &[crate::tasks::TaskLane],
    ) -> Result<Vec<crate::tasks::TaskLane>, TaskRuntimeError> {
        let wake = super::writes::finalize_workflow(
            transaction,
            children,
            deliveries,
            waits,
            conf,
            now,
            self.batch_size,
        )
        .await?;
        super::lane_owner::reconcile_workflow(transaction, poll, &wake, new_idle, now).await?;
        Ok(wake)
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
            flows: Vec::new(),
            max_all_children: 256,
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
    async fn cancel(&self, id: TaskId) -> Result<bool, TaskRuntimeError> {
        self.cancel_impl(id).await
    }

    async fn initialize(&self, conf: TaskStoreConf) -> Result<(), TaskRuntimeError> {
        self.initialize_impl(conf).await
    }

    async fn claim_tasks(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, TaskRuntimeError> {
        self.claim_tasks_impl(runner_id, claims).await
    }

    async fn commit_outcomes(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskRuntimeError> {
        self.commit_outcomes_impl(runner_id, commits).await
    }

    async fn renew_leases(
        &self,
        runner_id: &str,
        leases: &[TaskLease],
    ) -> Result<Vec<TaskId>, TaskRuntimeError> {
        self.renew_leases_impl(runner_id, leases).await
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskLease],
    ) -> Result<TaskTick, TaskRuntimeError> {
        self.tick_impl(runner_id, claims, commits, renewals).await
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
