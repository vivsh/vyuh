//! Task-owned Mool schema contribution for migration planning.

use crate::db;

use super::model::{
    TaskIdempotencyRow, TaskLaneLockRow, TaskNodeRow, TaskRateRow, TaskRow, TaskScheduleRow,
};

/// Builds the desired task, idempotency, rate, and runtime coordination schema.
pub(crate) fn task_schema() -> Result<db::Schema, db::SchemaLoadError> {
    let mut schema = db::schema()
        .model::<TaskRow>()
        .model::<TaskNodeRow>()
        .model::<TaskIdempotencyRow>()
        .model::<TaskRateRow>()
        .model::<TaskLaneLockRow>()
        .model::<TaskScheduleRow>()
        .build()?;
    if let Some(table) = schema.tables.get_mut("vyuh_tasks") {
        append_task_indexes(table);
        table.foreign_keys.push(db::schema::ForeignKey {
            name: "fk_vyuh_tasks_node".into(),
            columns: vec!["node_id".into()],
            to_table: "vyuh_task_nodes".into(),
            to_columns: vec!["id".into()],
            on_delete: Some("set_null".into()),
            on_update: None,
        });
    }
    if let Some(table) = schema.tables.get_mut("vyuh_task_idempotency") {
        append_idempotency_indexes(table);
    }
    if let Some(table) = schema.tables.get_mut("vyuh_task_lane_rates") {
        append_rate_indexes(table);
    }
    schema.prepare_loaded(dialect())
}

/// Adds per-lane readiness, node ownership, and diagnostic lookup indexes.
fn append_task_indexes(table: &mut db::Table) {
    // PostgreSQL must retain readiness order across both eligible statuses.
    #[cfg(feature = "postgres")]
    let ready_columns: &[&str] = &["lane_name", "ready_at", "created_at", "id"];
    #[cfg(not(feature = "postgres"))]
    let ready_columns: &[&str] = &["lane_name", "status", "ready_at", "created_at", "id"];
    table.indexes.extend([
        index("idx_vyuh_tasks_node", &["node_id"], false, None),
        index(
            "idx_vyuh_tasks_lane_pending_ready",
            ready_columns,
            false,
            pending_predicate(),
        ),
        index(
            "idx_vyuh_tasks_idempotency_key",
            &["idempotency_key", "created_at", "id"],
            false,
            None,
        ),
        index("idx_vyuh_tasks_history", &["created_at", "id"], false, None),
    ]);
}

/// Adds unique key ownership and bounded archive cleanup indexes.
fn append_idempotency_indexes(table: &mut db::Table) {
    table.indexes.extend([
        index(
            "idx_vyuh_task_idempotency_owner",
            &["task_name", "key_value"],
            true,
            None,
        ),
        index(
            "idx_vyuh_task_idempotency_expiry",
            &["expires_at", "id"],
            false,
            None,
        ),
        index("idx_vyuh_task_idempotency_task", &["task_id"], false, None),
    ]);
}

fn append_rate_indexes(table: &mut db::Table) {
    table.indexes.push(index(
        "idx_vyuh_task_lane_rates_lane",
        &["lane_name"],
        true,
        None,
    ));
}

fn index(name: &str, columns: &[&str], unique: bool, predicate: Option<&str>) -> db::Index {
    db::Index {
        name: name.into(),
        columns: columns.iter().map(|column| (*column).to_string()).collect(),
        unique,
        predicate: predicate.map(str::to_string),
        ..Default::default()
    }
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
fn pending_predicate() -> Option<&'static str> {
    Some("node_id IS NULL AND status IN (0, 1)")
}

#[cfg(feature = "mysql")]
fn pending_predicate() -> Option<&'static str> {
    None
}

fn dialect() -> db::Dialect {
    #[cfg(feature = "postgres")]
    {
        return db::Dialect::Postgres;
    }
    #[cfg(feature = "mysql")]
    {
        return db::Dialect::Mysql;
    }
    #[cfg(feature = "sqlite")]
    {
        return db::Dialect::Sqlite;
    }
    #[allow(unreachable_code)]
    db::Dialect::Sqlite
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/upgrade.rs"]
mod upgrade;
