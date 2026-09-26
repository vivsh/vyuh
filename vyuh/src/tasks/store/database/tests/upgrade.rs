use crate::db;
use db::engine::Executor as _;
use gaman_core::{BoxFuture, Migration, MigrationStore, StoreError};

#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct LegacyTask {
    #[column(primary_key)]
    id: i32,
    status: i16,
    attempts: i32,
    resume_input: Option<String>,
}

#[derive(db::Model)]
#[table(name = "vyuh_task_runtime")]
pub(super) struct LegacyRuntime {
    #[column(primary_key)]
    id: i32,
    #[column(type = "varchar(64)")]
    policy_fingerprint: String,
}

#[derive(db::Model)]
#[table(name = "vyuh_task_lane_rates")]
pub(super) struct LegacyRate {
    #[column(primary_key)]
    id: i32,
    #[column(type = "varchar(64)")]
    policy_fingerprint: String,
}

pub(super) struct History(pub(super) Vec<Migration>);

impl MigrationStore for &History {
    fn load_all<'a>(&'a self) -> BoxFuture<'a, Result<Vec<Migration>, StoreError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn save<'a>(&'a self, _: &'a Migration) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError::unavailable("read-only test history")) })
    }
}

/// Executes real ledger-backed schema/data upgrades twice without double-wrapping values.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target; run alone"
)]
async fn legacy_resume_upgrade_is_exactly_once() -> Result<(), String> {
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
    let fixture = History(vec![history.0[0].clone()]);
    let tracking = gaman_core::DatabaseTrackingStore;
    gaman_core::MigrationEngine::new(dialect, &fixture, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    executor
        .execute("INSERT INTO vyuh_task_runtime (id, policy_fingerprint) VALUES (1, 'legacy')")
        .await
        .map_err(|e| e.to_string())?;
    executor.execute("INSERT INTO vyuh_tasks (id, status, attempts, resume_input) VALUES (1, 2, 9, '{\"Ok\":{\"Err\":[1,null]}}'), (2, 0, 4, 'null'), (3, 2, 8, NULL)").await.map_err(|e| e.to_string())?;
    let applied = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(applied.applied, 2);
    let repeated = gaman_core::MigrationEngine::new(dialect, &history, &tracking, &mut executor)
        .apply(None, false)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(repeated.applied, 0);
    let values = executor
        .fetch_strings(
            "SELECT resume_input FROM vyuh_tasks WHERE resume_input IS NOT NULL ORDER BY id",
        )
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        values,
        ["{\"Ok\":{\"Ok\":{\"Err\":[1,null]}}}", "{\"Ok\":null}"]
    );
    let counts = executor
        .fetch_strings("SELECT CAST(step_attempts AS CHAR) FROM vyuh_tasks ORDER BY id")
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(counts, ["0", "4", "0"]);
    Ok(())
}

/// Adapts the shipped templates to one small legacy schema without changing their SQL.
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
    base.id = "fixture_base".into();
    base.atomic = dialect != db::Dialect::Mysql && dialect != db::Dialect::Mariadb;
    #[cfg(feature = "postgres")]
    let templates = (
        include_str!("../../../../../migrations/task_protocol/postgres/0001_step_attempts.yaml"),
        include_str!("../../../../../migrations/task_protocol/postgres/0002_resume_results.yaml"),
    );
    #[cfg(feature = "sqlite")]
    let templates = (
        include_str!("../../../../../migrations/task_protocol/sqlite/0001_step_attempts.yaml"),
        include_str!("../../../../../migrations/task_protocol/sqlite/0002_resume_results.yaml"),
    );
    #[cfg(feature = "mysql")]
    let templates = (
        include_str!("../../../../../migrations/task_protocol/mysql/0001_step_attempts.yaml"),
        include_str!("../../../../../migrations/task_protocol/mysql/0002_resume_results.yaml"),
    );
    let mut schema = Migration::from_yaml_str(templates.0).map_err(|e| e.to_string())?;
    schema.id = "0001_step_attempts".into();
    schema.dependencies = vec![base.id.clone()];
    let mut data = Migration::from_yaml_str(templates.1).map_err(|e| e.to_string())?;
    data.id = "0002_resume_results".into();
    Ok(History(vec![base, schema, data]))
}

/// Opens a migration executor only against in-memory SQLite or the opt-in test URL.
pub(super) async fn executor() -> Result<(impl db::engine::Executor, db::Dialect), String> {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    use db::sqlx::Connection as _;
    #[cfg(feature = "sqlite")]
    {
        Ok((
            db::engine::SqliteExecutor::new(
                db::sqlx::SqliteConnection::connect("sqlite::memory:")
                    .await
                    .map_err(|e| e.to_string())?,
            ),
            db::Dialect::Sqlite,
        ))
    }
    #[cfg(feature = "postgres")]
    {
        let url = std::env::var("VYUH_TASK_TEST_URL").map_err(|e| e.to_string())?;
        Ok((
            db::engine::PostgresExecutor::new(
                db::sqlx::PgConnection::connect(&url)
                    .await
                    .map_err(|e| e.to_string())?,
            ),
            db::Dialect::Postgres,
        ))
    }
    #[cfg(feature = "mysql")]
    {
        let url = std::env::var("VYUH_TASK_TEST_URL").map_err(|e| e.to_string())?;
        let dialect = if std::env::var("VYUH_TASK_TEST_DIALECT").as_deref() == Ok("mariadb") {
            db::Dialect::Mariadb
        } else {
            db::Dialect::Mysql
        };
        Ok((
            db::engine::MysqlFamilyExecutor::connect(&url, dialect)
                .await
                .map_err(|e| e.to_string())?,
            dialect,
        ))
    }
}
