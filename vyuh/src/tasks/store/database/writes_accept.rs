//! Fenced outcome capability validation before shared lifecycle transitions.

use super::*;

/// Applies only outcomes whose rows remain owned by the committing runner.
pub(super) fn apply_owned_outcomes(
    rows: &mut [TaskRow],
    outcomes: &mut HashMap<TaskId, &TaskOutcome>,
    conflicts: &HashSet<TaskId>,
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<Vec<(TaskId, String)>, TaskRuntimeError> {
    let mut deliveries = Vec::new();
    for row in rows {
        let outcome = outcomes.remove(&TaskId::new(row.id)).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError(format!("task {} has no pending outcome", row.id))
        })?;
        let failure = conflicts
            .contains(&TaskId::new(row.id))
            .then(|| TaskOutcome::fail("Child task identity conflicts with an existing task"));
        let invalid = if row.cancelled {
            None
        } else {
            crate::tasks::store::workflow::capability_error(
                crate::tasks::TaskKind::from_i16(row.kind)?,
                outcome,
                &conf.handlers,
            )
            .map(TaskOutcome::fail)
        };
        let outcome = invalid.as_ref().or(failure.as_ref()).unwrap_or(outcome);
        let retry = lane_retry(&conf.lanes, &row.lane_name)?;
        apply_outcome(row, outcome, retry, now)?;
        queue_delivery(row, &mut deliveries)?;
    }
    Ok(deliveries)
}
