use super::upgrade::{History, LegacyRate, LegacyRuntime, executor};
use crate::db;
use db::engine::Executor as _;

/// The factory upgrade marks only the exact predecessor and is ledger-idempotent.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn factory_protocol_upgrade() -> Result<(), String> {
    let (mut executor, dialect) = executor().await?;
    for table in [
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
    for table in ["vyuh_task_runtime", "vyuh_task_lane_rates"] {
        executor.execute(&format!("INSERT INTO {table} (id,policy_fingerprint) VALUES (1,'tr-v6:accepted'),(2,'tr-v5:rejected'),(3,'tr-a5:prior-ledger')"))
            .await.map_err(|e| e.to_string())?;
    }
    for expected in [1, 0] {
        let applied = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
            .apply(None, false)
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(applied.applied, expected);
        for table in ["vyuh_task_runtime", "vyuh_task_lane_rates"] {
            let actual = executor
                .fetch_strings(&format!(
                    "SELECT policy_fingerprint FROM {table} ORDER BY id"
                ))
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(
                actual,
                ["tr-a6:accepted", "tr-v5:rejected", "tr-a5:prior-ledger"]
            );
        }
    }
    Ok(())
}

/// Adds a protocol-only migration to the existing predecessor ledger identity.
fn history(dialect: db::Dialect) -> Result<History, String> {
    let schema = db::schema()
        .model::<LegacyRuntime>()
        .model::<LegacyRate>()
        .build()
        .map_err(|e| e.to_string())?;
    let mut base = gaman_core::OfflinePlanner::new(dialect)
        .make_migration(schema, &[])
        .map_err(|e| e.to_string())?
        .ok_or("missing base")?;
    base.id = "0011_all_protocol".into();
    base.atomic = !matches!(dialect, db::Dialect::Mysql | db::Dialect::Mariadb);
    let mut protocol =
        gaman_core::Migration::from_yaml_str(template()).map_err(|e| e.to_string())?;
    protocol.id = "0012_flow_factories_protocol".into();
    Ok(History(vec![base, protocol]))
}

fn template() -> &'static str {
    #[cfg(feature = "postgres")]
    {
        include_str!(
            "../../../../../migrations/task_protocol/postgres/0012_flow_factories_protocol.yaml"
        )
    }
    #[cfg(feature = "mysql")]
    {
        include_str!(
            "../../../../../migrations/task_protocol/mysql/0012_flow_factories_protocol.yaml"
        )
    }
    #[cfg(feature = "sqlite")]
    {
        include_str!(
            "../../../../../migrations/task_protocol/sqlite/0012_flow_factories_protocol.yaml"
        )
    }
}
