use super::upgrade::{History, LegacyRate, LegacyRuntime, executor};
use crate::{
    db,
    tasks::{TaskFailure, TaskId},
};
use db::engine::Executor as _;

#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct LegacyResultTask {
    #[column(primary_key)]
    id: uuid::Uuid,
    resume_input: Option<String>,
    last_error: Option<String>,
}

/// The ledger upgrade preserves resume bytes and errors, rejects bad data, and never wraps twice.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target; run alone"
)]
async fn persisted_result_upgrade() -> Result<(), String> {
    let (mut executor, dialect) = executor().await?;
    clean_tables(&mut executor).await?;
    let history = history(dialect)?;
    let base = history.0.first().ok_or("missing base")?.clone();
    let tracking = gaman_core::DatabaseTrackingStore;
    gaman_core::MigrationEngine::new(dialect, &History(vec![base]), &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    executor
        .execute(
            "INSERT INTO vyuh_task_runtime (id, policy_fingerprint) VALUES (1, 'tr-v1:previous')",
        )
        .await
        .map_err(|e| e.to_string())?;
    seed(&mut executor, dialect).await?;
    for invalid in [
        "42",
        "{\"Err\":null}",
        "{\"Err\":{\"message\":7}}",
        "{\"Err\":{\"message\":\"x\",\"task_id\":\"bad\"}}",
        "{\"Ok\":1,\"extra\":2}",
        "{",
    ] {
        reject(&mut executor, dialect, &history, invalid).await?;
    }
    reject(
        &mut executor,
        dialect,
        &history,
        &format!("{{\"Ok\":\"{}\"}}", "x".repeat(32_760)),
    )
    .await?;
    executor
        .execute("UPDATE vyuh_tasks SET resume_input = '{\"Ok\":{\"Err\":[1,null]}}'")
        .await
        .map_err(|e| e.to_string())?;
    let applied = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(applied.applied, 3);
    let repeated = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(repeated.applied, 0);
    verify(&mut executor).await
}

/// Clears only the explicit disposable migration fixtures before the ledger test.
async fn clean_tables(executor: &mut impl db::engine::Executor) -> Result<(), String> {
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
    Ok(())
}

/// Failed preflight does not rename the source column or convert any error.
async fn reject(
    executor: &mut impl db::engine::Executor,
    dialect: db::Dialect,
    history: &History,
    invalid: &str,
) -> Result<(), String> {
    executor
        .execute(&format!("UPDATE vyuh_tasks SET resume_input = '{invalid}'"))
        .await
        .map_err(|e| e.to_string())?;
    let result = gaman_core::MigrationEngine::new(
        dialect,
        history,
        &gaman_core::DatabaseTrackingStore,
        &mut *executor,
    )
    .apply(None, false)
    .await;
    assert!(result.is_err());
    assert_eq!(
        executor
            .fetch_strings("SELECT last_error FROM vyuh_tasks WHERE last_error IS NOT NULL")
            .await
            .map_err(|e| e.to_string())?,
        ["quoted \"error\" é"]
    );
    // MySQL temporary-table creation is not rolled back with the failed INSERT.
    executor
        .execute("DROP TABLE IF EXISTS vyuh_result_preflight")
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Seeds actual UUID storage on each dialect rather than integer stand-ins.
async fn seed(
    executor: &mut impl db::engine::Executor,
    dialect: db::Dialect,
) -> Result<(), String> {
    let id = match dialect {
        db::Dialect::Postgres => "'00000000-0000-0000-0000-000000000001'",
        _ => "X'00000000000000000000000000000001'",
    };
    executor.execute(&format!("INSERT INTO vyuh_tasks (id, resume_input, last_error) VALUES ({id}, NULL, 'quoted \"error\" é')"))
        .await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Converted errors retain identity, and existing nested result-shaped success values are unchanged.
async fn verify(executor: &mut impl db::engine::Executor) -> Result<(), String> {
    let rows = executor
        .fetch_strings("SELECT last_result FROM vyuh_tasks")
        .await
        .map_err(|e| e.to_string())?;
    let result: Result<(), TaskFailure> =
        serde_json::from_str(rows.first().ok_or("missing result")?).map_err(|e| e.to_string())?;
    let failure = result.err().ok_or("expected error")?;
    assert_eq!(
        failure.task_id(),
        Some(&TaskId::new(uuid::Uuid::from_u128(1)))
    );
    assert_eq!(failure.message(), "quoted \"error\" é");
    assert_eq!(
        executor
            .fetch_strings("SELECT resume_input FROM vyuh_tasks")
            .await
            .map_err(|e| e.to_string())?,
        ["{\"Ok\":{\"Err\":[1,null]}}"]
    );
    assert_eq!(
        executor
            .fetch_strings("SELECT policy_fingerprint FROM vyuh_task_runtime")
            .await
            .map_err(|e| e.to_string())?,
        ["tr-r1:previous"]
    );
    Ok(())
}

/// Loads the unchanged predecessor schema followed by the three new ledger migrations.
fn history(dialect: db::Dialect) -> Result<History, String> {
    let schema = db::schema()
        .model::<LegacyResultTask>()
        .model::<LegacyRuntime>()
        .model::<LegacyRate>()
        .build()
        .map_err(|e| e.to_string())?;
    let mut base = gaman_core::OfflinePlanner::new(dialect)
        .make_migration(schema, &[])
        .map_err(|e| e.to_string())?
        .ok_or("missing base")?;
    base.id = "0002_resume_results".into();
    base.atomic = !matches!(dialect, db::Dialect::Mysql | db::Dialect::Mariadb);
    let mut migrations = vec![base];
    for (name, yaml) in templates() {
        let mut migration =
            gaman_core::Migration::from_yaml_str(yaml).map_err(|e| e.to_string())?;
        migration.id = name.into();
        migrations.push(migration);
    }
    Ok(History(migrations))
}

fn templates() -> [(&'static str, &'static str); 3] {
    macro_rules! templates {
        ($backend:literal) => {
            [
                (
                    "0003_result_preflight",
                    include_str!(concat!(
                        "../../../../../migrations/task_protocol/",
                        $backend,
                        "/0003_result_preflight.yaml"
                    )),
                ),
                (
                    "0004_result_columns",
                    include_str!(concat!(
                        "../../../../../migrations/task_protocol/",
                        $backend,
                        "/0004_result_columns.yaml"
                    )),
                ),
                (
                    "0005_persisted_results",
                    include_str!(concat!(
                        "../../../../../migrations/task_protocol/",
                        $backend,
                        "/0005_persisted_results.yaml"
                    )),
                ),
            ]
        };
    }
    #[cfg(feature = "sqlite")]
    {
        templates!("sqlite")
    }
    #[cfg(feature = "postgres")]
    {
        templates!("postgres")
    }
    #[cfg(feature = "mysql")]
    {
        templates!("mysql")
    }
}
