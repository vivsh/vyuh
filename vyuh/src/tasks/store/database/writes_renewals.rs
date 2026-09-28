//! Narrow lease observation and same-transaction outcome reuse.

use super::super::{cancellation, model, runtime, turn_read};
use super::*;

impl DbTaskStore {
    /// Extends leases still owned by this runner and reports lost ownership.
    #[allow(dead_code)]
    pub(in super::super) async fn renew_leases_impl(
        &self,
        runner_id: &str,
        leases: &[TaskLease],
    ) -> Result<Vec<TaskId>, TaskRuntimeError> {
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let (conf, fingerprint) = self.runtime_conf.read().await.clone().ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task runtime was not initialized".into())
        })?;
        let mut transaction = self.pool.begin().await?;
        let (now, observations) = if leases
            .iter()
            .all(|lease| !locked_lane(&conf, lease.lane.as_str()))
        {
            turn_read::renewals(&mut transaction, leases, &fingerprint, self.batch_size).await?
        } else {
            (
                runtime::verify_runtime_policy(&mut transaction, &fingerprint).await?,
                Vec::new(),
            )
        };
        let mut deliveries = Vec::new();
        let (lost, _) = self
            .renew_leases_tx(
                &mut transaction,
                runner_id,
                leases,
                &conf,
                now,
                &mut deliveries,
                observations,
            )
            .await?;
        finalize_workflow(
            &mut transaction,
            &[],
            deliveries,
            &[],
            &conf,
            now,
            self.batch_size,
        )
        .await?;
        transaction.commit().await?;
        Ok(lost)
    }

    /// Renews still-owned leases inside an already-authorized scheduler transaction.
    pub(in super::super) async fn renew_leases_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        leases: &[TaskLease],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
        deliveries: &mut Vec<(TaskId, String)>,
        settled: Vec<model::TaskLeaseRow>,
    ) -> Result<(Vec<TaskId>, Vec<TaskId>), TaskRuntimeError> {
        if leases.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let allowed = fenced_leases(transaction, runner_id, leases, conf, now).await?;
        let settled_ids = settled.iter().map(|row| row.id).collect::<HashSet<_>>();
        let ids = leases
            .iter()
            .filter(|lease| allowed.contains(&lease.task_id))
            .map(|lease| lease.task_id.into_uuid())
            .filter(|id| !settled_ids.contains(id))
            .collect::<Vec<_>>();
        let mut rows = cancellation::load_renewals(transaction, ids).await?;
        rows.extend(
            settled
                .into_iter()
                .filter(|row| allowed.contains(&TaskId::new(row.id))),
        );
        rows.sort_unstable_by_key(|row| row.id);
        let lanes = leases
            .iter()
            .map(|lease| (lease.task_id, lease.lane))
            .collect::<std::collections::HashMap<_, _>>();
        rows.retain(|row| {
            lanes
                .get(&TaskId::new(row.id))
                .is_some_and(|lane| lane.as_str() == row.lane_name)
        });
        let cancelled = cancellation::classify_renewals(&mut rows, runner_id);
        self.cancel_renewals(transaction, &mut rows, conf, now, deliveries)
            .await?;
        for row in &mut rows {
            row.leased_until = Some(self.lease_until(row.lease_duration_ms, now)?);
            row.updated_at = now;
        }
        batch_renew(transaction, &rows, self.batch_size).await?;
        let task_ids = leases.iter().map(|lease| lease.task_id).collect::<Vec<_>>();
        Ok((lost_ids(&task_ids, &rows), cancelled))
    }
}

/// Persists renewed ownership deadlines without touching lifecycle fields.
async fn batch_renew(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[model::TaskLeaseRow],
    batch_size: usize,
) -> Result<(), TaskRuntimeError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::table();
    db::from(&table)
        .update_many(rows, (&table.leased_until, &table.updated_at))
        .batch_size(batch_size)
        .exec(transaction)
        .await?;
    Ok(())
}

fn lost_ids(requested: &[TaskId], rows: &[model::TaskLeaseRow]) -> Vec<TaskId> {
    let retained = rows.iter().map(|row| row.id).collect::<HashSet<_>>();
    requested
        .iter()
        .copied()
        .filter(|id| !retained.contains(&id.into_uuid()))
        .collect()
}
