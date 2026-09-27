use super::super::upgrade::{History, LegacyRate, LegacyRuntime, LegacyTask, executor};
use crate::db;
use db::engine::Executor as _;

/// Cancellation migration preserves payloads, defaults old rows, and is ledger-idempotent.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target; run alone"
)]
async fn cancellation_upgrade() -> Result<(), String> {
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
    let base = history.0.first().ok_or("missing base")?.clone();
    let tracking = gaman_core::DatabaseTrackingStore;
    gaman_core::MigrationEngine::new(dialect, &History(vec![base]), &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    executor.execute("INSERT INTO vyuh_tasks (id, status, attempts, resume_input) VALUES (1, 2, 9, '{\"Ok\":{\"Err\":[1,null]}}')").await.map_err(|e| e.to_string())?;
    for table in ["vyuh_task_runtime", "vyuh_task_lane_rates"] {
        executor.execute(&format!("INSERT INTO {table} (id, policy_fingerprint) VALUES (1, 'tr-v2:current'), (2, 'tr-r0:legacy'), (3, 'tr-r1:resume')")).await.map_err(|e| e.to_string())?;
    }
    let applied = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(applied.applied, 2);
    let defaults = executor
        .fetch_strings("SELECT resume_input FROM vyuh_tasks WHERE cancelled = FALSE")
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(defaults, ["{\"Ok\":{\"Err\":[1,null]}}"]);
    executor
        .execute("UPDATE vyuh_tasks SET cancelled = TRUE")
        .await
        .map_err(|e| e.to_string())?;
    let repeated = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(repeated.applied, 0);
    verify(&mut executor).await
}

/// Ledger replay preserves intent and migrated fingerprints without wrapping existing results.
async fn verify(executor: &mut impl db::engine::Executor) -> Result<(), String> {
    let rows = executor.fetch_strings("SELECT resume_input FROM vyuh_tasks WHERE cancelled = TRUE AND status = 2 AND attempts = 9").await.map_err(|e| e.to_string())?;
    assert_eq!(rows, ["{\"Ok\":{\"Err\":[1,null]}}"]);
    for table in ["vyuh_task_runtime", "vyuh_task_lane_rates"] {
        let markers = executor
            .fetch_strings(&format!(
                "SELECT policy_fingerprint FROM {table} ORDER BY id"
            ))
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(markers, ["tr-c2:current", "tr-c0:legacy", "tr-c1:resume"]);
    }
    Ok(())
}

/// A minimal predecessor schema exercises the shipped migrations without rewriting old identities.
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
    base.id = "0005_persisted_results".into();
    base.atomic = !matches!(dialect, db::Dialect::Mysql | db::Dialect::Mariadb);
    let mut migrations = vec![base];
    for (id, yaml) in templates() {
        let mut migration =
            gaman_core::Migration::from_yaml_str(yaml).map_err(|e| e.to_string())?;
        migration.id = id.into();
        migrations.push(migration);
    }
    Ok(History(migrations))
}

fn templates() -> [(&'static str, &'static str); 2] {
    macro_rules! templates {
        ($backend:literal) => {
            [
                (
                    "0006_cancellation_column",
                    include_str!(concat!(
                        "../../../../../migrations/task_protocol/",
                        $backend,
                        "/0006_cancellation_column.yaml"
                    )),
                ),
                (
                    "0007_cancellation_protocol",
                    include_str!(concat!(
                        "../../../../../migrations/task_protocol/",
                        $backend,
                        "/0007_cancellation_protocol.yaml"
                    )),
                ),
            ]
        };
    }
    #[cfg(feature = "postgres")]
    {
        templates!("postgres")
    }
    #[cfg(feature = "mysql")]
    {
        templates!("mysql")
    }
    #[cfg(feature = "sqlite")]
    {
        templates!("sqlite")
    }
}
