//! Task-owned Mool schema contribution for migration planning.

use crate::db;

use super::model::{
    TaskIdempotencyRow, TaskLaneLockRow, TaskRateRow, TaskRow, TaskRuntimeRow, TaskScheduleRow,
};

/// Builds the desired task, idempotency, rate, and runtime coordination schema.
pub(crate) fn task_schema() -> Result<db::Schema, db::SchemaLoadError> {
    let mut schema = db::schema()
        .model::<TaskRow>()
        .model::<TaskIdempotencyRow>()
        .model::<TaskRateRow>()
        .model::<TaskLaneLockRow>()
        .model::<TaskRuntimeRow>()
        .model::<TaskScheduleRow>()
        .build()?;
    if let Some(table) = schema.tables.get_mut("vyuh_tasks") {
        append_task_indexes(table);
    }
    if let Some(table) = schema.tables.get_mut("vyuh_task_idempotency") {
        append_idempotency_indexes(table);
    }
    if let Some(table) = schema.tables.get_mut("vyuh_task_lane_rates") {
        append_rate_indexes(table);
    }
    schema.prepare_loaded(dialect())
}

/// Adds per-lane readiness, lease, and diagnostic lookup indexes.
fn append_task_indexes(table: &mut db::Table) {
    table.indexes.extend([
        index(
            "idx_vyuh_tasks_lane_pending_ready",
            &["lane_name", "status", "ready_at", "created_at", "id"],
            false,
            pending_predicate(),
        ),
        index(
            "idx_vyuh_tasks_lane_running_lease",
            &["lane_name", "status", "leased_until", "id"],
            false,
            running_predicate(),
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
    Some("status = 0")
}

#[cfg(feature = "mysql")]
fn pending_predicate() -> Option<&'static str> {
    None
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
fn running_predicate() -> Option<&'static str> {
    Some("status = 1")
}

#[cfg(feature = "mysql")]
fn running_predicate() -> Option<&'static str> {
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
mod tests {
    use super::*;

    /// Verifies all task-runtime tables and lane indexes are represented in the desired schema.
    #[test]
    fn task_schema_contains_lane_runtime_contract() -> Result<(), String> {
        let schema = task_schema().map_err(|error| error.to_string())?;
        for name in [
            "vyuh_tasks",
            "vyuh_task_idempotency",
            "vyuh_task_lane_rates",
            "vyuh_task_lane_locks",
            "vyuh_task_runtime",
            "vyuh_schedules",
        ] {
            if !schema.tables.contains_key(name) {
                return Err(format!("task schema omitted {name}"));
            }
        }
        let tasks = schema
            .tables
            .get("vyuh_tasks")
            .ok_or("missing task table")?;
        if tasks.columns.iter().any(|column| {
            matches!(
                column.name.as_str(),
                "priority" | "identity" | "output" | "result" | "max_attempts" | "retry_delay_ms"
            )
        }) {
            return Err("task schema retains a removed task column".into());
        }
        require_columns(
            tasks,
            &[
                "parent_id",
                "root_id",
                "kind",
                "lane_name",
                "ready_at",
                "leased_until",
                "idempotency_key",
            ],
        )?;
        let lane_default = tasks
            .columns
            .iter()
            .find(|column| column.name == "lane_name")
            .and_then(|column| column.default.as_deref());
        if lane_default != Some("'default'") {
            return Err("task lane migration does not backfill the default lane".into());
        }
        let kind_default = tasks
            .columns
            .iter()
            .find(|column| column.name == "kind")
            .and_then(|column| column.default.as_deref());
        if kind_default != Some("0") {
            return Err("task kind migration does not backfill work tasks".into());
        }
        require_indexes(
            tasks,
            &[
                "idx_vyuh_tasks_lane_pending_ready",
                "idx_vyuh_tasks_lane_running_lease",
                "idx_vyuh_tasks_idempotency_key",
                "idx_vyuh_tasks_history",
            ],
        )?;
        require_indexes(
            schema
                .tables
                .get("vyuh_task_idempotency")
                .ok_or("missing idempotency table")?,
            &["idx_vyuh_task_idempotency_owner"],
        )?;
        let lane_locks = schema
            .tables
            .get("vyuh_task_lane_locks")
            .ok_or("missing task lane lock table")?;
        require_columns(
            lane_locks,
            &[
                "lane_name",
                "owner_id",
                "owner_token",
                "leased_until",
                "phase",
                "flushing",
                "empty_since",
                "generation",
                "hook_retry_at",
                "last_hook_error",
                "created_at",
                "updated_at",
            ],
        )?;
        if !lane_locks.indexes.is_empty() {
            return Err("task lane lock table must not have secondary indexes".into());
        }
        Ok(())
    }

    fn require_columns(table: &db::Table, names: &[&str]) -> Result<(), String> {
        for name in names {
            if !table.columns.iter().any(|column| column.name == *name) {
                return Err(format!("table '{}' omitted column '{name}'", table.name));
            }
        }
        Ok(())
    }

    fn require_indexes(table: &db::Table, names: &[&str]) -> Result<(), String> {
        for name in names {
            if !table.indexes.iter().any(|index| index.name == *name) {
                return Err(format!("table '{}' omitted index '{name}'", table.name));
            }
        }
        Ok(())
    }
}
