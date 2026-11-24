//! Task-row key arbitration and bounded writes on newly inserted tasks.

use super::DbTaskStore;
use super::{commit::chrono_duration, model::TaskRow, queries::add_time, submit::normalize_write};
use crate::{
    db::{self, DbSession as _, Record as _},
    tasks::{
        IdempotencyRetention, TaskId, TaskReceipt, TaskRuntimeError, TaskStoreConf, TaskWrite,
    },
};
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use uuid::Uuid;

#[cfg(all(test, feature = "migrations", feature = "test-support"))]
#[path = "tests/held.rs"]
mod tests;

pub(super) const PREDICATE: &str = "idempotency_held = TRUE AND idempotency_key IS NOT NULL";
pub(super) const EXPIRY_PREDICATE: &str = "idempotency_held = TRUE AND idempotency_key IS NOT NULL AND idempotency_expires_at IS NOT NULL";

/// Tries one task insertion per unique key and uses only returned IDs for receipts.
pub(super) async fn submit(
    store: &DbTaskStore,
    tx: &mut db::DbTransaction<'_>,
    mut writes: Vec<TaskWrite>,
    now: DateTime<Utc>,
    delivery: Option<(Uuid, usize)>,
) -> Result<(Vec<TaskReceipt>, Vec<crate::tasks::TaskRecord>), TaskRuntimeError> {
    let inputs = normalize(&mut writes, now)?;
    let mut rows = distinct_rows(writes);
    let mut admitted = store.admit_rows(&mut rows, delivery, now)?;
    let defer_wake = store.admission_backfill(&rows, &admitted, delivery, now)?;
    let inserted = insert(tx, rows.iter(), !defer_wake).await?;
    let receipts = inputs
        .iter()
        .map(|&(id, keyed)| receipt(id, keyed, &inserted))
        .collect::<Result<Vec<_>, _>>()?;
    admitted.retain(|task| inserted.contains(&task.id.into_uuid()));
    if let Some((node, capacity)) = delivery.filter(|_| defer_wake) {
        rows.retain(|row| inserted.contains(&row.id));
        let backfill = store.admit_rows(
            &mut rows,
            Some((node, capacity.saturating_sub(admitted.len()))),
            now,
        )?;
        promote(
            tx,
            backfill.iter().map(|task| task.id.into_uuid()).collect(),
            node,
            now,
        )
        .await?;
        admitted.extend(backfill);
        #[cfg(feature = "postgres")]
        super::notify::pending(tx, rows.iter()).await?;
    }
    Ok((receipts, admitted))
}

fn normalize(
    writes: &mut [TaskWrite],
    now: DateTime<Utc>,
) -> Result<Vec<(crate::tasks::TaskId, bool)>, TaskRuntimeError> {
    let mut ids = HashSet::with_capacity(writes.len());
    writes
        .iter_mut()
        .map(|write| {
            normalize_write(write, now)?;
            let id = write.record.id;
            if !ids.insert(id) {
                return Err(TaskRuntimeError::AlreadyExists(id.to_string()));
            }
            Ok((id, write.record.idempotency_key.is_some()))
        })
        .collect()
}

fn distinct_rows(writes: Vec<TaskWrite>) -> Vec<TaskRow> {
    let mut seen = HashSet::new();
    writes
        .into_iter()
        .filter_map(|write| {
            let record = write.record;
            if record
                .idempotency_key
                .as_ref()
                .is_some_and(|key| !seen.insert((record.name.clone(), key.clone())))
            {
                return None;
            }
            Some(TaskRow::from(record))
        })
        .collect()
}

/// A skipped keyed input is final even if its hold is subsequently released.
fn receipt(
    id: crate::tasks::TaskId,
    keyed: bool,
    inserted: &HashSet<Uuid>,
) -> Result<TaskReceipt, TaskRuntimeError> {
    if inserted.contains(&id.into_uuid()) {
        return Ok(TaskReceipt::Queued(id));
    }
    if keyed {
        return Ok(TaskReceipt::KeyHeld);
    }
    Err(TaskRuntimeError::TaskExecutionError(
        "unkeyed insert returned no task ID".into(),
    ))
}

/// Inserts bounded chunks and returns only IDs for which this transaction owns the task.
pub(super) async fn insert<'a>(
    tx: &mut db::DbTransaction<'_>,
    rows: impl IntoIterator<Item = &'a TaskRow>,
    notify: bool,
) -> Result<HashSet<Uuid>, TaskRuntimeError> {
    let mut ordered = rows.into_iter().collect::<Vec<_>>();
    if ordered.is_empty() {
        return Ok(HashSet::new());
    }
    // Stable ordering keeps the first candidate for a key and deterministic index arbitration.
    ordered.sort_by_key(|row| {
        row.idempotency_key
            .as_deref()
            .map(|key| (row.name.as_str(), key))
    });
    let columns = TaskRow::record_insert_column_names();
    let names = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let capacity =
        (db::backend::PARAMETER_LIMIT / names.len()).min(super::queries::WRITE_CHUNK_SIZE);
    let mut inserted = HashSet::with_capacity(ordered.len());
    for chunk in ordered.chunks(capacity) {
        let values = bind_slots(&names, chunk.len());
        let base = format!(
            "INSERT INTO vyuh_tasks ({}) VALUES {values} ON CONFLICT (name, idempotency_key) WHERE {PREDICATE} DO NOTHING",
            names.join(",")
        );
        let mut args = db::Arguments::default();
        for row in chunk {
            row.record_bind_insert_values(&mut args)
                .map_err(db::DbError::from)?;
        }
        #[cfg(feature = "postgres")]
        if notify && chunk.iter().any(|row| row.status == 0) {
            let sql = super::notify::notifying_ids(&base);
            let ids: Vec<(Uuid, i64)> = tx.fetch_all(db::Statement::new(&sql, args)).await?;
            inserted.extend(ids.into_iter().map(|(id, _)| id));
            continue;
        }
        #[cfg(feature = "sqlite")]
        let _ = notify;
        let sql = format!("{base} RETURNING id");
        let ids: Vec<(Uuid,)> = tx.fetch_all(db::Statement::new(&sql, args)).await?;
        inserted.extend(ids.into_iter().map(|(id,)| id));
    }
    Ok(inserted)
}

/// Inserts provisional keyed children and removes exactly the inserts of rejected groups.
pub(super) async fn children(
    tx: &mut db::DbTransaction<'_>,
    mut writes: Vec<TaskWrite>,
    now: DateTime<Utc>,
    batch: usize,
) -> Result<(Vec<TaskRow>, HashSet<TaskId>), TaskRuntimeError> {
    let mut rows = Vec::with_capacity(writes.len());
    let mut keyed = false;
    for mut write in writes.drain(..) {
        normalize_write(&mut write, now)?;
        keyed |= write.record.idempotency_key.is_some();
        rows.push(TaskRow::from(write.record));
    }
    if !keyed {
        return Ok((rows, HashSet::new()));
    }
    let mut ids = HashSet::with_capacity(rows.len());
    for row in &rows {
        if !ids.insert(row.id) {
            return Err(TaskRuntimeError::AlreadyExists(row.id.to_string()));
        }
    }
    let keyed = rows.iter().filter(|row| row.idempotency_key.is_some());
    let inserted = insert(tx, keyed.clone(), false).await?;
    let rejected: HashSet<TaskId> = keyed
        .clone()
        .filter(|row| !inserted.contains(&row.id))
        .filter_map(|row| row.parent_id.map(TaskId::new))
        .collect();
    let removed = keyed
        .filter(|row| {
            inserted.contains(&row.id)
                && row
                    .parent_id
                    .is_some_and(|id| rejected.contains(&TaskId::new(id)))
        })
        .map(|row| row.id)
        .collect();
    delete_provisional(tx, removed, batch).await?;
    Ok((rows, rejected))
}

/// Restores atomic group rejection without touching prior key owners or emitting wakes.
pub(super) async fn delete_provisional(
    tx: &mut db::DbTransaction<'_>,
    mut ids: Vec<Uuid>,
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    ids.sort_unstable();
    let table = DbTaskStore::table();
    for chunk in ids.chunks(batch.clamp(1, db::backend::PARAMETER_LIMIT - 1)) {
        let changed = db::from(&table)
            .filter(table.status.eq(db::val(0i16)))
            .filter(table.id.in_values(chunk.to_vec()))
            .delete()
            .exec(&mut *tx)
            .await?;
        if changed != chunk.len() as u64 {
            return Err(TaskRuntimeError::TaskExecutionError(
                "provisional child cleanup did not match".into(),
            ));
        }
    }
    Ok(())
}

/// Publishes final placement of surviving keyed children; only unkeyed rows need inserting now.
pub(super) async fn finish_children(
    tx: &mut db::DbTransaction<'_>,
    children: Vec<TaskRow>,
    delivery: Option<(Uuid, usize)>,
    now: DateTime<Utc>,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    if !children.iter().any(|row| row.idempotency_key.is_some()) {
        return Ok(children);
    }
    let (keyed, unkeyed): (Vec<_>, Vec<_>) = children
        .into_iter()
        .partition(|row| row.idempotency_key.is_some());
    if let Some((node, _)) = delivery {
        let ids = keyed
            .iter()
            .filter(|row| row.node_id == Some(node))
            .map(|row| row.id)
            .collect();
        promote(tx, ids, node, now).await?;
    }
    #[cfg(feature = "postgres")]
    super::notify::pending(tx, &keyed).await?;
    Ok(unkeyed)
}

/// Promotes only acknowledged Pending inserts; local snapshots already checked both increments.
pub(super) async fn promote(
    tx: &mut db::DbTransaction<'_>,
    mut ids: Vec<Uuid>,
    node: Uuid,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    ids.sort_unstable();
    let limit = super::queries::WRITE_CHUNK_SIZE.min(db::backend::PARAMETER_LIMIT - 2);
    for chunk in ids.chunks(limit) {
        let sql = promotion_sql(chunk.len());
        let mut statement = db::Statement::raw(&sql).bind(node).bind(now);
        for id in chunk {
            statement = statement.bind(*id);
        }
        if tx.execute(statement).await? != chunk.len() as u64 {
            return Err(TaskRuntimeError::TaskExecutionError(
                "new task admission did not match".into(),
            ));
        }
    }
    Ok(())
}

/// Looks up newly inserted rows by primary key, then checks admission eligibility.
pub(super) fn promotion_sql(len: usize) -> String {
    #[cfg(feature = "postgres")]
    let ownerless = "node_id IS NULL";
    // Unary + keeps SQLite from driving this UPDATE through the NULL owner bucket.
    // The IS NULL test itself is unchanged and does not depend on column affinity.
    #[cfg(feature = "sqlite")]
    let ownerless = "+node_id IS NULL";
    let slots = (3..3 + len).map(param).collect::<Vec<_>>().join(",");
    format!(
        "UPDATE vyuh_tasks SET status=1, node_id={}, updated_at={},
         attempts=attempts+1, step_attempts=step_attempts+1
         WHERE status=0 AND {ownerless} AND id IN ({slots})",
        param(1),
        param(2),
    )
}

#[cfg(feature = "postgres")]
fn bind_slots(names: &[&str], len: usize) -> String {
    super::notify::values(names, len)
}

#[cfg(feature = "sqlite")]
fn bind_slots(names: &[&str], len: usize) -> String {
    let tuple = format!("({})", vec!["?"; names.len()].join(","));
    vec![tuple; len].join(",")
}

#[cfg(feature = "postgres")]
fn param(i: usize) -> String {
    format!("${i}")
}
#[cfg(feature = "sqlite")]
fn param(_: usize) -> String {
    "?".into()
}

/// Applies release/retention to the same row version as the fenced terminal write.
pub(super) fn finish(
    rows: &mut [TaskRow],
    conf: &TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    for row in rows {
        if row.idempotency_key.is_none()
            || !matches!(row.status, 3 | 4)
            || row.share_role == crate::tasks::models::ShareRole::Subscriber as i16
        {
            continue;
        }
        match conf.idempotency_retention(&row.name, &row.lane_name)? {
            IdempotencyRetention::ActiveOnly => {
                row.idempotency_held = false;
                row.idempotency_expires_at = None;
            }
            IdempotencyRetention::RetainFor(duration) => {
                row.idempotency_expires_at = Some(add_time(
                    now,
                    chrono_duration(duration)?,
                    "idempotency retention",
                )?);
            }
        }
    }
    Ok(())
}

/// Releases only due terminal holds; selection and recheck use the existing store clock.
pub(super) async fn sweep(
    tx: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<(), TaskRuntimeError> {
    tx.execute(
        db::Statement::raw(&sweep_sql())
            .bind(now)
            .bind(limit as i64)
            .bind(now),
    )
    .await?;
    Ok(())
}

/// Builds the bounded expiry pick and the guarded write in one statement.
pub(super) fn sweep_sql() -> String {
    let pick = param(1);
    let bound = param(2);
    let recheck = param(3);
    // Evaluate the bounded ID array once. Keep the write's eligibility check
    // a filter so PostgreSQL cannot drive the write through the expiry index.
    #[cfg(feature = "postgres")]
    let (ids, end, guard, guard_end) = (
        "id = ANY(ARRAY(",
        "))",
        "CASE WHEN ",
        " THEN TRUE ELSE FALSE END",
    );
    #[cfg(feature = "sqlite")]
    let (ids, end, guard, guard_end) = ("id IN (", ")", "", "");
    format!(
        "UPDATE vyuh_tasks SET idempotency_held = FALSE WHERE {ids}
            SELECT id FROM vyuh_tasks WHERE {EXPIRY_PREDICATE} AND status IN (3,4)
                AND idempotency_expires_at <= {pick} ORDER BY idempotency_expires_at,id LIMIT {bound}
        {end} AND {guard}{EXPIRY_PREDICATE} AND status IN (3,4) AND idempotency_expires_at <= {recheck}{guard_end}"
    )
}
