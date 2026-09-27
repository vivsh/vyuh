use super::upgrade::{History, LegacyRate, LegacyRuntime, executor};
use crate::db;
use db::engine::Executor as _;

#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct LegacyTask {
    #[column(primary_key)]
    id: i32,
    state: Option<String>,
    resume_input: Option<String>,
}

/// Ledger replay adds null/zero waits without changing checkpoints or existing resume bytes.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn all_upgrade() -> Result<(), String> {
    let (mut executor, dialect) = executor().await?;
    for table in [
        "vyuh_tasks",
        "vyuh_task_runtime",
        "vyuh_task_lane_rates",
        gaman_core::TRACKING_TABLE,
    ] {
        executor
            .execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await
            .map_err(|e| e.to_string())?;
    }
    let history = history(dialect)?;
    let tracking = gaman_core::DatabaseTrackingStore;
    let base = history.0.first().ok_or("missing base")?.clone();
    gaman_core::MigrationEngine::new(dialect, &History(vec![base]), &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    executor.execute("INSERT INTO vyuh_tasks (id,state,resume_input) VALUES (1,' { \"step\": 1 } ','{\"Ok\":42}')").await.map_err(|e| e.to_string())?;
    for table in ["vyuh_task_runtime", "vyuh_task_lane_rates"] {
        executor.execute(&format!("INSERT INTO {table} (id,policy_fingerprint) VALUES (1,'tr-v5:current'),(2,'tr-t0:legacy'),(3,'tr-t4:old'),(4,'tr-v4:unmigrated')")).await.map_err(|e| e.to_string())?;
    }
    for expected in [2, 0] {
        let result = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
            .apply(None, false)
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(result.applied, expected);
        assert_eq!(executor.fetch_strings("SELECT state FROM vyuh_tasks WHERE remaining_completions=0 AND waiting_children IS NULL AND resume_input='{\"Ok\":42}'").await.map_err(|e| e.to_string())?, [" { \"step\": 1 } "]);
        assert_eq!(
            executor
                .fetch_strings("SELECT policy_fingerprint FROM vyuh_task_runtime ORDER BY id")
                .await
                .map_err(|e| e.to_string())?,
            [
                "tr-a5:current",
                "tr-a0:legacy",
                "tr-a4:old",
                "tr-v4:unmigrated"
            ]
        );
    }
    Ok(())
}

/// Starts at the previous ledger identity and appends rather than rewriting migrations.
fn history(dialect: db::Dialect) -> Result<History, String> {
    let schema = db::schema()
        .model::<LegacyTask>()
        .model::<LegacyRuntime>()
        .model::<LegacyRate>()
        .build()
        .map_err(|e| e.to_string())?;
    let mut base = gaman_core::OfflinePlanner::new(dialect)
        .make_migration(schema, &[])
        .map_err(|e| e.to_string())?
        .ok_or("missing base")?;
    base.id = "0009_typed_returns_protocol".into();
    base.atomic = !matches!(dialect, db::Dialect::Mysql | db::Dialect::Mariadb);
    let (columns, protocol) = templates();
    let mut columns = gaman_core::Migration::from_yaml_str(columns).map_err(|e| e.to_string())?;
    columns.id = "0010_all_columns".into();
    let mut protocol = gaman_core::Migration::from_yaml_str(protocol).map_err(|e| e.to_string())?;
    protocol.id = "0011_all_protocol".into();
    Ok(History(vec![base, columns, protocol]))
}

fn templates() -> (&'static str, &'static str) {
    #[cfg(feature = "postgres")]
    {
        (
            include_str!("../../../../../migrations/task_protocol/postgres/0010_all_columns.yaml"),
            include_str!("../../../../../migrations/task_protocol/postgres/0011_all_protocol.yaml"),
        )
    }
    #[cfg(feature = "mysql")]
    {
        (
            include_str!("../../../../../migrations/task_protocol/mysql/0010_all_columns.yaml"),
            include_str!("../../../../../migrations/task_protocol/mysql/0011_all_protocol.yaml"),
        )
    }
    #[cfg(feature = "sqlite")]
    {
        (
            include_str!("../../../../../migrations/task_protocol/sqlite/0010_all_columns.yaml"),
            include_str!("../../../../../migrations/task_protocol/sqlite/0011_all_protocol.yaml"),
        )
    }
}
