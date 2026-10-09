//! Commit-atomic PostgreSQL wake hints composed with existing task mutations.

use super::model::TaskRow;
use crate::{db, tasks::TaskRuntimeError};
use db::{DbSession as _, Record as _};

pub(super) const CHANNEL: &str = "vyuh_tasks";

/// An acknowledged local operation already carries wakes for its own affine work.
pub(super) fn needs_hint(
    status: i16,
    owner: Option<uuid::Uuid>,
    origin: Option<uuid::Uuid>,
) -> bool {
    status == 0 && (owner.is_none() || owner != origin)
}

/// Reuses Vyuh's transport and existing bounded control channel, including lag rescans.
pub(super) async fn listen(
    store: &super::DbTaskStore,
    node: uuid::Uuid,
    events: tokio::sync::broadcast::Sender<crate::tasks::client::RunnerEvent>,
    shutdown: crate::notifiers::CancellationNotifier,
) -> Result<tokio::task::JoinHandle<()>, TaskRuntimeError> {
    use crate::tasks::client::RunnerEvent;
    let lanes = store
        .runtime_conf
        .lanes
        .iter()
        .map(|lane| lane.lane())
        .collect::<Vec<_>>();
    let topics = vec![CHANNEL.into(), format!("{CHANNEL}_{}", node.simple())];
    Ok(crate::db_notify::listen_owned(
        store.pool.as_sqlx(),
        topics,
        move |hint| {
            let event = match hint {
                None => Some(RunnerEvent::Wake),
                Some(hint) => match serde_json::from_str::<Option<String>>(&hint.payload) {
                    Ok(None) => Some(RunnerEvent::Wake),
                    Ok(Some(name)) => lanes
                        .iter()
                        .find(|lane| lane.as_str() == name)
                        .copied()
                        .map(RunnerEvent::WakeLane),
                    Err(_) => None,
                },
            };
            event.is_none_or(|event| events.send(event).is_ok())
        },
        shutdown,
    )
    .await?)
}

/// Adds routing from changed rows only; the final aggregate forces notification evaluation.
pub(super) fn notifying(mutation: &str) -> String {
    format!(
        "WITH changed AS ({mutation} RETURNING vyuh_tasks.node_id, vyuh_tasks.lane_name, vyuh_tasks.status),
         targets AS (SELECT DISTINCT
           CASE WHEN node_id IS NULL THEN 'vyuh_tasks'
                ELSE 'vyuh_tasks_' || replace(node_id::text, '-', '') END AS channel,
           CASE WHEN octet_length(to_json(lane_name)::text) < 7900
                THEN to_json(lane_name)::text ELSE 'null' END AS payload
           FROM changed WHERE status = 0),
         notified AS MATERIALIZED (SELECT pg_notify(channel, payload) FROM targets)
         SELECT (SELECT count(*) FROM changed), (SELECT count(*) FROM notified)"
    )
}

/// Inserts bounded VALUES batches with Mool's record bindings, without rereading new tasks.
pub(super) async fn insert(
    tx: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    batch: usize,
    origin: Option<uuid::Uuid>,
) -> Result<(), TaskRuntimeError> {
    if rows
        .iter()
        .all(|row| !needs_hint(row.status, row.node_id, origin))
    {
        db::from(super::DbTaskStore::table())
            .insert_many(rows)
            .batch_size(batch)
            .exec(tx)
            .await?;
        return Ok(());
    }
    let columns = TaskRow::record_insert_column_names();
    let names = columns.iter().map(String::as_str).collect::<Vec<_>>();
    for chunk in rows.chunks(batch.max(1).min(db::backend::PARAMETER_LIMIT / names.len())) {
        let sql = format!(
            "INSERT INTO vyuh_tasks ({}) VALUES {}",
            names.join(","),
            values(&names, chunk.len())
        );
        let mut args = db::Arguments::default();
        for row in chunk {
            row.record_bind_insert_values(&mut args)
                .map_err(db::DbError::from)?;
        }
        let _: (i64, i64) = tx
            .fetch_one(db::Statement::new(&notifying(&sql), args))
            .await?;
    }
    Ok(())
}

/// Updates only supplied task fields using Mool's selected-column binding contract.
pub(super) async fn update<T: db::Record>(
    tx: &mut db::DbTransaction<'_>,
    rows: &[T],
    columns: &[&str],
    batch: usize,
) -> Result<u64, TaskRuntimeError> {
    let mut names = columns.to_vec();
    names.push("id");
    let assignments = columns
        .iter()
        .map(|name| format!("{name} = v.{name}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut changed = 0;
    for chunk in rows.chunks(batch.max(1).min(db::backend::PARAMETER_LIMIT / names.len())) {
        let sql = format!(
            "UPDATE vyuh_tasks SET {assignments} FROM (VALUES {}) AS v({}) WHERE vyuh_tasks.id = v.id",
            values(&names, chunk.len()),
            names.join(",")
        );
        let mut args = db::Arguments::default();
        for row in chunk {
            row.record_bind_update_selected(&names, &mut args)
                .map_err(db::DbError::from)?;
        }
        let (count, _): (i64, i64) = tx
            .fetch_one(db::Statement::new(&notifying(&sql), args))
            .await?;
        changed += count as u64;
    }
    Ok(changed)
}

/// Renders schema-owned task column casts; no application strings enter SQL syntax.
fn values(columns: &[&str], rows: usize) -> String {
    (0..rows)
        .map(|row| {
            let fields = columns
                .iter()
                .enumerate()
                .map(|(column, name)| {
                    let kind = match *name {
                        "id" | "parent_id" | "root_id" | "node_id" => "uuid",
                        "kind" | "status" => "smallint",
                        "attempts" | "step_attempts" | "remaining_completions" => "integer",
                        "cancelled" => "boolean",
                        "ready_at"
                        | "created_at"
                        | "updated_at"
                        | "completed_at"
                        | "idempotency_expires_at" => "timestamptz",
                        _ => "text",
                    };
                    format!("${}::{kind}", row * columns.len() + column + 1)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("({fields})")
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(all(test, feature = "migrations"))]
#[path = "tests/notify.rs"]
mod tests;
