//! Task-owned Mool schema contribution for migration planning.

use crate::db;

use super::model::{TaskIdempotencyRow, TaskRateRow, TaskRow, TaskRuntimeRow};

/// Builds the desired task, idempotency, rate, and runtime coordination schema.
pub(crate) fn task_schema() -> Result<db::Schema, db::SchemaLoadError> {
    let mut schema = db::schema()
        .model::<TaskRow>()
        .model::<TaskIdempotencyRow>()
        .model::<TaskRateRow>()
        .model::<TaskRuntimeRow>()
        .build()?;
    if let Some(table) = schema.tables.get_mut("vyuh_tasks") {
        append_task_indexes(table);
    }
    if let Some(table) = schema.tables.get_mut("vyuh_task_idempotency") {
        append_idempotency_indexes(table);
    }
    if let Some(table) = schema.tables.get_mut("vyuh_task_group_rates") {
        append_rate_indexes(table);
    }
    schema.prepare_loaded(dialect())
}

/// Adds grouped readiness, lease, and diagnostic lookup indexes.
fn append_task_indexes(table: &mut db::Table) {
    table.indexes.extend([
        index(
            "idx_vyuh_tasks_group_pending_ready",
            &["group_name", "status", "ready_at", "created_at", "id"],
            false,
            pending_predicate(),
        ),
        index(
            "idx_vyuh_tasks_group_running_lease",
            &["group_name", "status", "leased_until", "id"],
            false,
            running_predicate(),
        ),
        index(
            "idx_vyuh_tasks_idempotency_key",
            &["name", "idempotency_key"],
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
        "idx_vyuh_task_group_rates_group",
        &["group_name"],
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

    /// Verifies all task-runtime tables and group indexes are represented in the desired schema.
    #[test]
    fn task_schema_contains_grouped_runtime_contract() -> Result<(), String> {
        let schema = task_schema().map_err(|error| error.to_string())?;
        for name in [
            "vyuh_tasks",
            "vyuh_task_idempotency",
            "vyuh_task_group_rates",
            "vyuh_task_runtime",
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
            &["group_name", "ready_at", "leased_until", "idempotency_key"],
        )?;
        let group_default = tasks
            .columns
            .iter()
            .find(|column| column.name == "group_name")
            .and_then(|column| column.default.as_deref());
        if group_default != Some("'default'") {
            return Err("task group migration does not backfill the default group".into());
        }
        require_indexes(
            tasks,
            &[
                "idx_vyuh_tasks_group_pending_ready",
                "idx_vyuh_tasks_group_running_lease",
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
