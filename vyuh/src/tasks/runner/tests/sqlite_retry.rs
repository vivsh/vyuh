use super::*;
use crate::db::{self, DbSession as _, Statement};
#[cfg(feature = "migrations")]
use crate::tasks::TaskId;
use crate::tasks::{TaskConf, TaskRegistry, store::SqliteTaskStore};
use std::time::Duration;

fn text_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

/// Opens a real WAL database with room for a stale reader and a separate writer.
async fn wal_pool() -> Result<(tempfile::TempDir, db::DbPool), String> {
    use db::sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    let dir = tempfile::tempdir().map_err(text_error)?;
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(dir.path().join("retry.sqlite3"))
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .map_err(text_error)?;
    Ok((dir, db::DbPool::from_pool(pool)))
}

/// Produces SQLite's real extended error without a timing-dependent race.
async fn stale_snapshot(pool: &db::DbPool) -> Result<db::DbError, String> {
    let mut writer = pool.clone();
    for sql in [
        "CREATE TABLE snapshot_probe (n INTEGER NOT NULL)",
        "INSERT INTO snapshot_probe VALUES (0)",
    ] {
        writer
            .execute(Statement::raw(sql))
            .await
            .map_err(text_error)?;
    }
    let mut reader = pool.begin().await.map_err(text_error)?;
    let observed: i64 = reader
        .fetch_scalar(Statement::raw("SELECT n FROM snapshot_probe"))
        .await
        .map_err(text_error)?;
    assert_eq!(observed, 0);
    let write = Statement::raw("UPDATE snapshot_probe SET n = n + 1");
    writer.execute(write).await.map_err(text_error)?;
    let error = reader
        .execute(Statement::raw("UPDATE snapshot_probe SET n = n + 1"))
        .await
        .err()
        .ok_or("stale SQLite transaction unexpectedly wrote")?;
    reader.rollback().await.map_err(text_error)?;
    assert!(matches!(
        &error,
        db::DbError::Database { code: Some(code), .. } if code == "517"
    ));
    Ok(error)
}

/// Only native BUSY_SNAPSHOT takes the SQLite fallback, not ordinary SQL errors.
#[tokio::test]
async fn native_retry_classification() -> Result<(), String> {
    let (_dir, mut pool) = wal_pool().await?;
    let conf = TaskRegistry::new()
        .with_config(TaskConf::default())
        .and_then(|registry| registry.store_conf())
        .map_err(text_error)?;
    let store = SqliteTaskStore::new(pool.as_sqlx().clone(), 32, Duration::from_secs(30), conf);
    let stale = stale_snapshot(&pool).await?;
    assert!(store.retryable_conflict(&stale));
    assert!(retryable(&store, &stale.into()));
    let other = pool
        .execute(Statement::raw("UPDATE missing_snapshot_table SET n = 1"))
        .await
        .err()
        .ok_or("invalid SQLite statement unexpectedly succeeded")?;
    assert_eq!(other.code(), "database_error");
    assert!(!store.retryable_conflict(&other));
    assert!(!retryable(&store, &other.into()));
    Ok(())
}

#[cfg(feature = "migrations")]
type Runner = TaskRunner<SqliteTaskStore>;

#[cfg(feature = "migrations")]
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct RetryJob;

/// A real SQLite pre-commit conflict preserves the publisher request, then delivers once.
#[cfg(feature = "migrations")]
#[tokio::test]
async fn snapshot_retry_preserves_flush() -> Result<(), String> {
    use crate::tasks::TaskStatus;
    let (_dir, pool) = wal_pool().await?;
    install_schema(&pool).await?;
    let mut runner = sqlite_runner(&pool).await?;
    let (parent, child, publisher) = suspend_parent(&runner).await?;
    let commit = TaskCommit::Outcome {
        publisher: true,
        node_id: runner.node_id,
        attempts: 1,
        task_id: publisher,
        lane: crate::tasks::DEFAULT_TASK_LANE,
        outcome: crate::tasks::TaskOutcome::CompleteWith {
            output: "42".into(),
        },
        owner_token: None,
    };
    let request = runner.delivery_request(vec![commit.clone()]);
    let error = stale_snapshot(&pool).await?;
    assert_eq!(row(&runner, parent).await?.status, TaskStatus::Suspended);
    assert_eq!(row(&runner, child).await?.status, TaskStatus::Suspended);
    retry_flush(&mut runner, request, error).await?;
    let completed = row(&runner, publisher).await?;
    assert_eq!(
        (completed.status, completed.attempts),
        (TaskStatus::Succeeded, 1)
    );
    let subscriber = row(&runner, child).await?;
    assert_eq!(
        (subscriber.status, subscriber.attempts),
        (TaskStatus::Succeeded, 0)
    );
    assert_eq!(subscriber.last_result, completed.last_result);
    let pending = row(&runner, parent).await?;
    assert_eq!(pending.status, TaskStatus::Pending);
    assert_eq!(pending.resume_input.as_deref(), Some(r#"{"Ok":42}"#));
    let duplicate = runner
        .store
        .commit_tasks(&[commit])
        .await
        .map_err(text_error)?;
    assert_eq!(duplicate.rejected, vec![publisher]);
    assert_eq!(row(&runner, parent).await?.updated_at, pending.updated_at);
    Ok(())
}

/// Installs the real desired task schema in the disposable SQLite file.
#[cfg(feature = "migrations")]
async fn install_schema(pool: &db::DbPool) -> Result<(), String> {
    let planner = db::gaman::OfflinePlanner::new(db::Dialect::Sqlite);
    let schema = crate::tasks::store::database::schema::task_schema().map_err(text_error)?;
    let migration = planner
        .make_migration(schema, &[])
        .map_err(text_error)?
        .ok_or("missing SQLite task migration")?;
    let mut sql = pool.as_sqlx().acquire().await.map_err(text_error)?;
    for statement in planner.sql_migrate(&[migration]).map_err(text_error)? {
        db::sqlx::query(db::sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&mut *sql)
            .await
            .map_err(|error| format!("{error}: {statement}"))?;
    }
    Ok(())
}

/// Uses the actual SQLite store and a configured Work registration.
#[cfg(feature = "migrations")]
async fn sqlite_runner(pool: &db::DbPool) -> Result<Runner, String> {
    use crate::tasks::{RegisteredTask, TaskDefinition, TaskIdempotency, TaskKind};
    let mut registry = TaskRegistry::new()
        .with_config(TaskConf::default().poll_interval(Duration::from_millis(10)))
        .map_err(text_error)?;
    registry
        .register(RegisteredTask::new(
            TaskDefinition::new("snapshot-work")
                .idempotency(TaskIdempotency::new("1", |_: &RetryJob| "snapshot".into())),
            |_: crate::Data<RetryJob>| async {},
        ))
        .map_err(text_error)?;
    let mut conf = registry.store_conf().map_err(text_error)?;
    conf.handlers.push(("snapshot-flow".into(), TaskKind::Flow));
    let store = Arc::new(SqliteTaskStore::new(
        pool.as_sqlx().clone(),
        32,
        Duration::from_secs(30),
        conf,
    ));
    let mut runner =
        TaskRunner::new(Arc::new(registry).client(store, Vec::new())).map_err(text_error)?;
    runner.initialize().await.map_err(text_error)?;
    Ok(runner)
}

/// Builds an actual suspended parent with a private shared subscriber as its child.
#[cfg(feature = "migrations")]
async fn suspend_parent(runner: &Runner) -> Result<(TaskId, TaskId, TaskId), String> {
    use crate::tasks::{DEFAULT_TASK_LANE, TaskOptions, TaskOutcome};
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    let publisher = client
        .submit_with(RetryJob, TaskOptions::new().shared())
        .await
        .map_err(text_error)?
        .queued_id()
        .map_err(text_error)?;
    let spawn = client
        .prepare_child(
            &crate::callables::DataBox::new_data(RetryJob),
            "null",
            &TaskOptions::new().shared(),
        )
        .map_err(text_error)?;
    let TaskOutcome::Spawn { child, .. } = &spawn else {
        return Err("missing shared subscriber in spawn outcome".into());
    };
    let child_id = child.record.id;
    let parent = claim_parent(runner).await?;
    let checkpoint = TaskCommit::Outcome {
        publisher: false,
        node_id: runner.node_id,
        attempts: parent.attempts,
        task_id: parent.id,
        lane: DEFAULT_TASK_LANE,
        outcome: spawn,
        owner_token: None,
    };
    let flushed = runner
        .store
        .commit_tasks(&[checkpoint])
        .await
        .map_err(text_error)?;
    assert!(flushed.rejected.is_empty());
    Ok((parent.id, child_id, publisher))
}

/// Claims the waiting parent and the already queued publisher in the same operation.
#[cfg(feature = "migrations")]
async fn claim_parent(runner: &Runner) -> Result<crate::tasks::TaskRecord, String> {
    use crate::tasks::{
        DEFAULT_TASK_LANE, LaneClaim, TaskKind, TaskWrite, submission::build_record,
    };
    let parent = build_record(
        "snapshot-flow",
        "null".into(),
        None,
        None,
        DEFAULT_TASK_LANE,
        TaskKind::Flow,
    )
    .map_err(text_error)?;
    let parent_id = parent.id;
    runner
        .store
        .store_tasks(vec![TaskWrite {
            record: parent,
            initial_delay: None,
        }])
        .await
        .map_err(text_error)?;
    let poll = runner
        .store
        .claim_tasks(
            &runner.node_id,
            &[LaneClaim {
                lane: DEFAULT_TASK_LANE,
                limit: 2,
                owner: None,
            }],
        )
        .await
        .map_err(text_error)?;
    poll.lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .find(|task| task.id == parent_id)
        .ok_or("parent was not claimed".into())
}

/// Feeds a native failed acknowledgement to the runner, retaining and retrying the actual store request.
#[cfg(feature = "migrations")]
async fn retry_flush(
    runner: &mut Runner,
    request: DbRequest,
    error: db::DbError,
) -> Result<(), String> {
    let site = Site::build(
        crate::SiteConf::default()
            .log_init(false)
            .database(db::DbConf::from_url("sqlite::memory:").map_err(text_error)?),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(text_error)?;
    let (hooks, _) = mpsc::channel(1);
    let (id, earliest) = report_failed_flush(runner, &site, &hooks, request, error)?;
    tokio::time::sleep_until(earliest).await;
    assert_eq!(runner.retry_due(), Some(id));
    let permit = runner
        .db_slots
        .clone()
        .try_acquire_owned()
        .map_err(text_error)?;
    runner.retry_database(id, permit);
    use futures::StreamExt as _;
    let result = runner
        .operations
        .next()
        .await
        .ok_or("missing retry response")?;
    assert!(result.result.is_ok());
    runner.apply_database(&site, &hooks, result);
    assert!(runner.in_flight.is_empty());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Applies the failed response with a frozen clock to assert the exact retry boundary.
#[cfg(feature = "migrations")]
fn report_failed_flush(
    runner: &mut Runner,
    site: &Site,
    hooks: &mpsc::Sender<HookCompletion>,
    request: DbRequest,
    error: db::DbError,
) -> Result<(uuid::Uuid, tokio::time::Instant), String> {
    let id = uuid::Uuid::now_v7();
    let request = Arc::new(request);
    runner.in_flight.insert(
        id,
        InFlight {
            request: request.clone(),
            phase: DbPhase::Running,
        },
    );
    let permit = runner
        .db_slots
        .clone()
        .try_acquire_owned()
        .map_err(text_error)?;
    tokio::time::pause();
    let earliest = tokio::time::Instant::now() + runner.poll_interval;
    runner.apply_database(
        site,
        hooks,
        DbCompletion {
            id,
            result: Err(StoreFailure::BeforeCommit(error.into())),
            permit,
            started: std::time::Instant::now(),
        },
    );
    assert!(runner.mode == RunMode::Running);
    assert!(runner.retry_due().is_none());
    assert_eq!(runner.db_slots.available_permits(), 1);
    let entry = runner
        .in_flight
        .get(&id)
        .ok_or("failed request was discarded")?;
    assert!(Arc::ptr_eq(&entry.request, &request));
    assert!(
        matches!(entry.phase, DbPhase::Retry { prepared: None, not_before } if not_before == earliest)
    );
    tokio::time::resume();
    Ok((id, earliest))
}

#[cfg(feature = "migrations")]
async fn row(runner: &Runner, id: TaskId) -> Result<crate::tasks::TaskRecord, String> {
    runner
        .store
        .get_task(id)
        .await
        .map_err(text_error)?
        .ok_or("missing task row".into())
}
