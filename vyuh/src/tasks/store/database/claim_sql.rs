//! Ordered claim reads with statement-local recovery evidence, never cached eligibility.

use super::model::{TaskClaimRow, TaskRow};
use crate::db::Record as _;

/// Builds bounded probe/lock branches and wake evidence using an existing runtime CTE.
pub(super) fn query(runtime: &str, lane: &str, limit: &str, guard: &str, policy: bool) -> String {
    let recovery = format!(
        "recovery AS MATERIALIZED (SELECT EXISTS (SELECT 1 FROM vyuh_tasks \
         WHERE lane_name = {lane} AND status = 1 AND leased_until IS NOT NULL \
         AND leased_until <= (SELECT now FROM runtime)) AS needed)"
    );
    let probe = candidates(&TaskRow::record_column_names(), lane, limit, guard, false);
    let selected = candidates(
        &TaskClaimRow::record_column_names(),
        lane,
        limit,
        guard,
        true,
    );
    format!(
        "{runtime}, {recovery}, probes AS MATERIALIZED ({probe}), \
         selected AS MATERIALIZED ({selected}) {} ORDER BY turn_kind, ready_at, created_at, id",
        projection(lane, policy)
    )
}

/// Keeps LIMIT after locking in each mutually exclusive branch, including under contention.
fn candidates(columns: &[String], lane: &str, limit: &str, guard: &str, locked: bool) -> String {
    let columns = columns
        .iter()
        .map(|name| format!("t.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let pending =
        "t.status = 0 AND (t.ready_at IS NULL OR t.ready_at <= (SELECT now FROM runtime))";
    let expired = "t.status = 1 AND t.leased_until IS NOT NULL AND t.leased_until <= (SELECT now FROM runtime)";
    let lock = if locked && cfg!(feature = "postgres") {
        " FOR UPDATE OF t SKIP LOCKED"
    } else {
        ""
    };
    let branch = |predicate: &str, recovery: &str| {
        format!(
            "SELECT {columns} FROM vyuh_tasks t WHERE {guard} AND {recovery} \
         AND t.lane_name = {lane} AND ({predicate}) \
         ORDER BY t.ready_at, t.created_at, t.id LIMIT {limit}{lock}"
        )
    };
    format!(
        "SELECT * FROM ({}) ready UNION ALL SELECT * FROM ({}) recoverable",
        branch(pending, "NOT (SELECT needed FROM recovery)"),
        branch(
            &format!("({pending}) OR ({expired})"),
            "(SELECT needed FROM recovery)"
        )
    )
}

/// Preserves separate row projections without transporting payloads inside JSON envelopes.
fn projection(lane: &str, policy: bool) -> String {
    let columns = TaskClaimRow::record_column_names();
    let nulls = columns
        .iter()
        .map(|name| format!("NULL AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let deadline_columns = columns
        .iter()
        .map(|name| {
            if name == "lane_name" {
                lane.to_owned()
            } else {
                "NULL".into()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let null_deadline = if cfg!(feature = "postgres") {
        "CAST(NULL AS TIMESTAMPTZ)"
    } else {
        "NULL"
    };
    let metadata = if policy {
        format!(
            "SELECT 0 AS turn_kind, policy_fingerprint, now, {nulls}, {null_deadline} AS deadline FROM runtime UNION ALL "
        )
    } else {
        String::new()
    };
    format!(
        "{metadata}SELECT 1 AS turn_kind, NULL AS policy_fingerprint, NULL AS now, \
         probes.*, NULL AS waiting_children, 0 AS remaining_completions, {null_deadline} AS deadline FROM probes \
         UNION ALL SELECT 3, NULL, NULL, selected.*, NULL FROM selected \
         UNION ALL SELECT 4, NULL, NULL, {deadline_columns}, {} FROM runtime",
        deadline(lane)
    )
}

/// Uses two ordered index seeks, never an aggregate over the task backlog.
fn deadline(lane: &str) -> String {
    format!(
        "(SELECT MIN(value) FROM (SELECT (SELECT ready_at FROM vyuh_tasks \
         WHERE lane_name = {lane} AND status = 0 AND ready_at > runtime.now ORDER BY ready_at LIMIT 1) AS value \
         UNION ALL SELECT (SELECT leased_until FROM vyuh_tasks WHERE lane_name = {lane} \
         AND status = 1 AND leased_until > runtime.now ORDER BY leased_until LIMIT 1)) deadlines)"
    )
}

/// Numbers shared parameters on the two backends that support the optimized claim read.
pub(super) fn parameter(index: usize) -> String {
    if cfg!(feature = "postgres") {
        format!("${index}")
    } else {
        format!("?{index}")
    }
}
