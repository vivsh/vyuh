use super::*;

/// Verifies all task-runtime tables and lane indexes are represented in the desired schema.
#[test]
fn task_schema_contains_lane_runtime_contract() -> Result<(), String> {
    let schema = task_schema().map_err(|error| error.to_string())?;
    for name in [
        "vyuh_tasks",
        "vyuh_task_nodes",
        "vyuh_task_idempotency",
        "vyuh_task_lane_rates",
        "vyuh_task_lane_locks",
        "vyuh_schedules",
    ] {
        if !schema.tables.contains_key(name) {
            return Err(format!("task schema omitted {name}"));
        }
    }
    assert!(!schema.tables.contains_key("vyuh_task_runtime"));
    let rates = schema
        .tables
        .get("vyuh_task_lane_rates")
        .ok_or("missing rates")?;
    assert!(
        !rates
            .columns
            .iter()
            .any(|column| column.name == "policy_fingerprint")
    );
    let tasks = schema
        .tables
        .get("vyuh_tasks")
        .ok_or("missing task table")?;
    if tasks.columns.iter().any(|column| {
        matches!(
            column.name.as_str(),
            "priority"
                | "identity"
                | "output"
                | "result"
                | "max_attempts"
                | "retry_delay_ms"
                | "locked_by"
                | "leased_until"
                | "lease_duration_ms"
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
            "node_id",
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
            "idx_vyuh_tasks_idempotency_key",
            "idx_vyuh_tasks_history",
            "idx_vyuh_tasks_node",
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

/// Retirement has one nullable FK and readiness indexes include abandoned running work.
#[test]
fn retirement_schema() -> Result<(), String> {
    let schema = task_schema().map_err(|e| e.to_string())?;
    let tasks = schema.tables.get("vyuh_tasks").ok_or("missing tasks")?;
    let fk = tasks
        .foreign_keys
        .iter()
        .find(|fk| fk.name == "fk_vyuh_tasks_node")
        .ok_or("missing ownership FK")?;
    assert_eq!(fk.columns, vec!["node_id"]);
    assert_eq!(fk.to_table, "vyuh_task_nodes");
    assert_eq!(fk.on_delete.as_deref(), Some("set_null"));
    assert_eq!(fk.on_update, None); // The schema representation of default NO ACTION.
    assert!(
        tasks
            .columns
            .iter()
            .any(|column| column.name == "node_id" && column.nullable)
    );
    let ready = tasks
        .indexes
        .iter()
        .find(|index| index.name == "idx_vyuh_tasks_lane_pending_ready")
        .ok_or("missing readiness index")?;
    assert_eq!(ready.predicate.as_deref(), pending_predicate());
    Ok(())
}

/// PostgreSQL orders both eligible statuses by readiness; other backend keys stay unchanged.
#[test]
fn readiness_index_columns() -> Result<(), String> {
    let schema = task_schema().map_err(|error| error.to_string())?;
    let tasks = schema.tables.get("vyuh_tasks").ok_or("missing tasks")?;
    let ready = tasks
        .indexes
        .iter()
        .find(|index| index.name == "idx_vyuh_tasks_lane_pending_ready")
        .ok_or("missing readiness index")?;
    #[cfg(feature = "postgres")]
    let expected = ["lane_name", "ready_at", "created_at", "id"];
    #[cfg(not(feature = "postgres"))]
    let expected = ["lane_name", "status", "ready_at", "created_at", "id"];
    assert_eq!(ready.columns, expected);
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    assert_eq!(
        ready.predicate.as_deref(),
        Some("node_id IS NULL AND status IN (0, 1)")
    );
    #[cfg(feature = "mysql")]
    assert_eq!(ready.predicate, None);
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
