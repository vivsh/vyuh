use super::upgrade::{History, LegacyRate, LegacyRuntime, executor};
use crate::db;
use db::engine::Executor as _;

#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct LegacyTask {
    #[column(primary_key)]
    id: i32,
    name: String,
    kind: i16,
    status: i16,
    state: Option<String>,
    last_result: Option<String>,
}

/// Explicit name reclassification preserves stored bytes and replays through the ledger once.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target; run alone"
)]
async fn work_flow_upgrade() -> Result<(), String> {
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
    gaman_core::MigrationEngine::new(
        dialect,
        &History(vec![history.0[0].clone()]),
        &tracking,
        &mut executor,
    )
    .apply(None, false)
    .await
    .map_err(|e| e.to_string())?;
    executor.execute("INSERT INTO vyuh_tasks (id,name,kind,status,state,last_result) VALUES (1,'converted',0,0,' { \"step\": 1 } ','{\"Ok\":42}'), (2,'converted',0,1,NULL,NULL), (3,'unchanged',0,0,NULL,NULL)").await.map_err(|e| e.to_string())?;
    for table in ["vyuh_task_runtime", "vyuh_task_lane_rates"] {
        executor.execute(&format!("INSERT INTO {table} (id,policy_fingerprint) VALUES (1,'tr-v3:current'),(2,'tr-c0:legacy'),(3,'tr-c1:resume'),(4,'tr-c2:results'),(5,'tr-v4:typed-predecessor')")).await.map_err(|e| e.to_string())?;
    }
    let applied = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(applied.applied, 3);
    verify_rows(&mut executor).await?;
    let repeated = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(repeated.applied, 0);
    verify_markers(&mut executor).await?;
    Ok(())
}

/// The application-owned conversion precedes the framework's protocol marker template.
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
    base.id = "0007_cancellation_protocol".into();
    base.atomic = !matches!(dialect, db::Dialect::Mysql | db::Dialect::Mariadb);
    let mut reclassify = gaman_core::Migration::from_yaml_str(
        "dependencies: [0007_cancellation_protocol]\natomic: true\noperations:\n  - type: statement\n    up: UPDATE vyuh_tasks SET kind=1 WHERE name IN ('converted')"
    ).map_err(|e| e.to_string())?;
    reclassify.id = "application_flow_names".into();
    let mut protocol =
        gaman_core::Migration::from_yaml_str(template()).map_err(|e| e.to_string())?;
    protocol.id = "0008_work_flow_protocol".into();
    protocol.dependencies = vec!["application_flow_names".into()];
    let mut returns =
        gaman_core::Migration::from_yaml_str(returns_template()).map_err(|e| e.to_string())?;
    returns.id = "0009_typed_returns_protocol".into();
    Ok(History(vec![base, reclassify, protocol, returns]))
}

fn template() -> &'static str {
    #[cfg(feature = "postgres")]
    {
        include_str!(
            "../../../../../migrations/task_protocol/postgres/0008_work_flow_protocol.yaml"
        )
    }
    #[cfg(feature = "mysql")]
    {
        include_str!("../../../../../migrations/task_protocol/mysql/0008_work_flow_protocol.yaml")
    }
    #[cfg(feature = "sqlite")]
    {
        include_str!("../../../../../migrations/task_protocol/sqlite/0008_work_flow_protocol.yaml")
    }
}

/// Conversion touches only classification, including non-suspended retained rows.
async fn verify_rows(executor: &mut impl db::engine::Executor) -> Result<(), String> {
    assert_eq!(
        executor
            .fetch_strings(
                "SELECT state FROM vyuh_tasks WHERE id=1 AND kind=1 AND last_result='{\"Ok\":42}'"
            )
            .await
            .map_err(|e| e.to_string())?,
        [" { \"step\": 1 } "]
    );
    assert_eq!(
        executor
            .fetch_strings("SELECT name FROM vyuh_tasks WHERE id=2 AND kind=1 AND status=1")
            .await
            .map_err(|e| e.to_string())?,
        ["converted"]
    );
    assert_eq!(
        executor
            .fetch_strings("SELECT name FROM vyuh_tasks WHERE kind=0")
            .await
            .map_err(|e| e.to_string())?,
        ["unchanged"]
    );
    Ok(())
}

/// Protocol markers retain predecessor hashes through ledger replay.
async fn verify_markers(executor: &mut impl db::engine::Executor) -> Result<(), String> {
    for table in ["vyuh_task_runtime", "vyuh_task_lane_rates"] {
        assert_eq!(
            executor
                .fetch_strings(&format!(
                    "SELECT policy_fingerprint FROM {table} ORDER BY id"
                ))
                .await
                .map_err(|e| e.to_string())?,
            [
                "tr-t3:current",
                "tr-t0:legacy",
                "tr-t1:resume",
                "tr-t2:results",
                "tr-t4:typed-predecessor"
            ]
        );
    }
    Ok(())
}

fn returns_template() -> &'static str {
    #[cfg(feature = "postgres")]
    {
        include_str!(
            "../../../../../migrations/task_protocol/postgres/0009_typed_returns_protocol.yaml"
        )
    }
    #[cfg(feature = "mysql")]
    {
        include_str!(
            "../../../../../migrations/task_protocol/mysql/0009_typed_returns_protocol.yaml"
        )
    }
    #[cfg(feature = "sqlite")]
    {
        include_str!(
            "../../../../../migrations/task_protocol/sqlite/0009_typed_returns_protocol.yaml"
        )
    }
}
