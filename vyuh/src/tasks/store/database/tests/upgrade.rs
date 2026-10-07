use super::*;
use db::gaman::{Migration, OfflinePlanner};

/// Reconstructs only the predecessor schema; no legacy runtime adapter is retained.
fn predecessor() -> Result<db::Schema, String> {
    let mut schema = task_schema().map_err(|e| e.to_string())?;
    let mut runtime = schema
        .tables
        .remove("vyuh_task_nodes")
        .ok_or("missing nodes")?;
    runtime.name = "vyuh_task_runtime".into();
    runtime
        .columns
        .retain(|column| column.name != "heartbeat_at");
    let clock = runtime
        .columns
        .iter_mut()
        .find(|c| c.name == "started_at")
        .ok_or("missing clock")?;
    clock.name = "updated_at".into();
    runtime.primary_key = None;
    runtime
        .columns
        .push(column("policy_fingerprint", "varchar(64)", false));
    schema.tables.insert(runtime.name.clone(), runtime);
    restore_task_columns(&mut schema)?;
    schema
        .tables
        .get_mut("vyuh_task_lane_rates")
        .ok_or("missing rates")?
        .columns
        .push(column("policy_fingerprint", "varchar(64)", false));
    schema.prepare_loaded(dialect()).map_err(|e| e.to_string())
}

/// Restores the old ownership columns and readiness indexes in test-only schema scratch.
fn restore_task_columns(schema: &mut db::Schema) -> Result<(), String> {
    let tasks = schema.tables.get_mut("vyuh_tasks").ok_or("missing tasks")?;
    tasks.columns.retain(|column| column.name != "node_id");
    tasks.foreign_keys.clear();
    tasks
        .indexes
        .retain(|index| index.name != "idx_vyuh_tasks_node");
    for index in &mut tasks.indexes {
        if index.name == "idx_vyuh_tasks_lane_pending_ready" {
            index.columns = ["lane_name", "status", "ready_at", "created_at", "id"]
                .map(str::to_owned)
                .to_vec();
            index.predicate = if cfg!(feature = "mysql") {
                None
            } else {
                Some("status = 0".into())
            };
        }
    }
    let mut lease = tasks
        .columns
        .iter()
        .find(|c| c.name == "ready_at")
        .cloned()
        .ok_or("missing readiness")?;
    lease.name = "leased_until".into();
    tasks.columns.extend([
        lease,
        column("locked_by", "text", true),
        column("lease_duration_ms", "bigint", true),
    ]);
    tasks.indexes.push(index(
        "idx_vyuh_tasks_lane_running_lease",
        &["lane_name", "status", "leased_until", "id"],
        false,
        if cfg!(feature = "mysql") {
            None
        } else {
            Some("status = 1")
        },
    ));
    Ok(())
}

fn column(name: &str, kind: &str, nullable: bool) -> db::schema::Column {
    db::schema::Column {
        name: name.into(),
        col_type: kind.into(),
        nullable,
        ..Default::default()
    }
}

/// Loads immutable appended migration identities using the selected backend only.
fn templates() -> Result<Vec<Migration>, String> {
    #[cfg(feature = "postgres")]
    let source = [
        include_str!("../../../../../migrations/task_protocol/postgres/0013_reset_tasks.yaml"),
        include_str!("../../../../../migrations/task_protocol/postgres/0014_node_pipeline.yaml"),
    ];
    #[cfg(feature = "sqlite")]
    let source = [
        include_str!("../../../../../migrations/task_protocol/sqlite/0013_reset_tasks.yaml"),
        include_str!("../../../../../migrations/task_protocol/sqlite/0014_node_pipeline.yaml"),
    ];
    #[cfg(feature = "mysql")]
    let source = [
        include_str!("../../../../../migrations/task_protocol/mysql/0013_reset_tasks.yaml"),
        include_str!("../../../../../migrations/task_protocol/mysql/0014_node_pipeline.yaml"),
    ];
    source
        .into_iter()
        .zip(["0013_reset_tasks", "0014_node_pipeline"])
        .map(|(yaml, id)| {
            let mut migration = Migration::from_yaml_str(yaml).map_err(|e| e.to_string())?;
            migration.id = id.into();
            Ok(migration)
        })
        .collect()
}

/// A synthetic predecessor ledger gives SQLite its required complete rebuild context.
fn history() -> Result<Vec<Migration>, String> {
    let planner = OfflinePlanner::new(dialect());
    let mut base = planner
        .make_migration(predecessor()?, &[])
        .map_err(|e| e.to_string())?
        .ok_or("missing predecessor")?;
    base.id = "0012_flow_factories_protocol".into();
    base.dependencies.clear();
    let mut history = vec![base];
    history.extend(templates()?);
    Ok(history)
}

/// Appended ledger operations exactly reach the desired node-owned schema on each backend.
#[test]
fn pipeline_upgrade_matches_schema() -> Result<(), String> {
    let history = history()?;
    let planner = OfflinePlanner::new(dialect()).from_migrations(history.clone());
    let diff = planner
        .make_migration(task_schema().map_err(|e| e.to_string())?, &[])
        .map_err(|e| format!("{e:?}"))?;
    if let Some(diff) = diff {
        return Err(diff.to_yaml_string().map_err(|e| e.to_string())?);
    }
    let mut prior = Vec::new();
    for migration in history {
        let sql = OfflinePlanner::new(dialect())
            .from_migrations(prior.clone())
            .sql_migrate(std::slice::from_ref(&migration))
            .map_err(|e| e.to_string())?;
        assert!(!sql.is_empty());
        prior.push(migration);
    }
    Ok(())
}

/// SQLite applies a destructive task reset while preserving unrelated durable coordination.
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn sqlite_pipeline_upgrade() -> Result<(), String> {
    let pool = db::sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .map_err(|e| e.to_string())?;
    let history = history()?;
    let base = history.first().ok_or("missing predecessor")?;
    apply(&pool, &[], base).await?;
    seed(&pool).await?;
    let mut prior = vec![base.clone()];
    for migration in history.iter().skip(1) {
        apply(&pool, &prior, migration).await?;
        prior.push(migration.clone());
    }
    verify_reset(&pool).await?;
    verify_ownership(&pool).await?;
    Ok(())
}

/// Executes one migration atomically; a failed statement cannot leave an acknowledged reset.
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn sqlite_reset_rolls_back_on_failure() -> Result<(), String> {
    let pool = db::sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .map_err(|e| e.to_string())?;
    let history = history()?;
    let base = history.first().ok_or("missing predecessor")?;
    apply(&pool, &[], base).await?;
    seed(&pool).await?;
    let mut reset = history.get(1).cloned().ok_or("missing reset")?;
    let failure = Migration::from_yaml_str(
        "operations:\n  - type: statement\n    up: INSERT INTO missing_upgrade_guard VALUES (1)\n",
    )
    .map_err(|e| e.to_string())?;
    reset.operations.extend(failure.operations);
    assert!(
        apply(&pool, std::slice::from_ref(base), &reset)
            .await
            .is_err()
    );
    for (table, expected) in [("vyuh_tasks", 5), ("vyuh_task_idempotency", 1)] {
        let count: i64 = db::sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(count, expected);
    }
    Ok(())
}

/// Applies migration SQL with predecessor context and an atomic acknowledgement boundary.
#[cfg(feature = "sqlite")]
async fn apply(
    pool: &db::sqlx::SqlitePool,
    prior: &[Migration],
    migration: &Migration,
) -> Result<(), String> {
    let sql = OfflinePlanner::new(dialect())
        .from_migrations(prior.to_vec())
        .sql_migrate(std::slice::from_ref(migration))
        .map_err(|e| e.to_string())?;
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    for statement in sql {
        db::sqlx::query(&statement)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("{e}: {statement}"))?;
    }
    tx.commit().await.map_err(|e| e.to_string())
}

/// Seeds every legacy task status plus coordination data that must not be reset.
#[cfg(feature = "sqlite")]
async fn seed(pool: &db::sqlx::SqlitePool) -> Result<(), String> {
    for status in 0..5i16 {
        db::sqlx::query("INSERT INTO vyuh_tasks (id,name,input,status,attempts,created_at,updated_at,locked_by,state,resume_input,last_result,waiting_children) VALUES (?,'old','null',?,2,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'old-owner','42','{\"Ok\":1}','{\"Ok\":2}','[]')")
            .bind(uuid::Uuid::now_v7()).bind(status).execute(pool).await.map_err(|e| e.to_string())?;
    }
    for statement in [
        "INSERT INTO vyuh_task_lane_rates (id,lane_name,policy_fingerprint,tokens_micros,updated_at) VALUES (randomblob(16),'default','old',1234,'2000-01-01 00:00:00')",
        "INSERT INTO vyuh_task_runtime (id,policy_fingerprint,updated_at) VALUES (randomblob(16),'old',CURRENT_TIMESTAMP)",
        "INSERT INTO vyuh_schedules (name,last_submitted_at,updated_at) VALUES ('daily','2000-01-01 00:00:00','2000-01-01 00:00:00')",
        "INSERT INTO vyuh_task_idempotency (id,task_name,key_value,task_id,fingerprint,created_at,updated_at) SELECT randomblob(16),'old','key',id,'old',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM vyuh_tasks LIMIT 1",
        "INSERT INTO vyuh_task_lane_locks (lane_name,phase,flushing,generation,created_at,updated_at) VALUES ('gpu',2,false,7,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db::sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Old tasks and reservations are gone; schedules, rate balances and lane phases are retained.
#[cfg(feature = "sqlite")]
async fn verify_reset(pool: &db::sqlx::SqlitePool) -> Result<(), String> {
    for table in ["vyuh_tasks", "vyuh_task_idempotency", "vyuh_task_nodes"] {
        let count: i64 = db::sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(count, 0);
    }
    let tokens: i64 = db::sqlx::query_scalar("SELECT tokens_micros FROM vyuh_task_lane_rates")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(tokens, 1234);
    let phase: i16 = db::sqlx::query_scalar("SELECT phase FROM vyuh_task_lane_locks")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(phase, 2);
    let cursor: String = db::sqlx::query_scalar("SELECT last_submitted_at FROM vyuh_schedules")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(cursor, "2000-01-01 00:00:00");
    let integrity: Option<String> = db::sqlx::query_scalar("PRAGMA foreign_key_check")
        .fetch_optional(pool)
        .await
        .map_err(|e| e.to_string())?;
    assert!(integrity.is_none());
    Ok(())
}

/// The upgraded schema rejects absent owners and clears ownership when a node is retired.
#[cfg(feature = "sqlite")]
async fn verify_ownership(pool: &db::sqlx::SqlitePool) -> Result<(), String> {
    let node = uuid::Uuid::now_v7();
    let task = uuid::Uuid::now_v7();
    let insert = "INSERT INTO vyuh_tasks (id,name,input,status,attempts,node_id,created_at,updated_at) VALUES (?,'new','null',1,1,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)";
    assert!(
        db::sqlx::query(insert)
            .bind(task)
            .bind(node)
            .execute(pool)
            .await
            .is_err()
    );
    db::sqlx::query("INSERT INTO vyuh_task_nodes VALUES (?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
        .bind(node)
        .execute(pool)
        .await
        .map_err(|e| e.to_string())?;
    db::sqlx::query(insert)
        .bind(task)
        .bind(node)
        .execute(pool)
        .await
        .map_err(|e| e.to_string())?;
    db::sqlx::query("DELETE FROM vyuh_task_nodes WHERE id = ?")
        .bind(node)
        .execute(pool)
        .await
        .map_err(|e| e.to_string())?;
    let owner: Option<uuid::Uuid> =
        db::sqlx::query_scalar("SELECT node_id FROM vyuh_tasks WHERE id = ?")
            .bind(task)
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())?;
    assert!(owner.is_none());
    Ok(())
}
