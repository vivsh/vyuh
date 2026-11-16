use super::super::model::TaskNodeRow;
use super::super::submit::tx_tests::fixture;
use super::*;
use crate::tasks::{DEFAULT_TASK_LANE, TaskKind, submission::build_record};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

fn future_record(status: TaskStatus) -> Result<TaskRecord, String> {
    let mut row = build_record(
        "tx_plain",
        "42".into(),
        None,
        None,
        DEFAULT_TASK_LANE,
        TaskKind::Work,
    )
    .map_err(error)?;
    row.status = status;
    row.ready_at = Some(Utc::now() + chrono::Duration::minutes(5));
    row.state = Some("7".into());
    row.resume_input = Some(r#"{"Ok":8}"#.into());
    row.last_result = Some(r#"{"Ok":9}"#.into());
    row.attempts = 3;
    row.step_attempts = 2;
    Ok(row)
}

async fn insert(pool: &db::DbPool, row: TaskRecord) -> Result<TaskId, String> {
    let id = row.id;
    db::from(&DbTaskStore::table())
        .insert_many(&[TaskRow::from(row)])
        .exec(&mut pool.clone())
        .await
        .map_err(error)?;
    Ok(id)
}

/// Concurrent and repeated wakes preserve all persisted data other than readiness.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn wake_moves_only_readiness() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let id = insert(database.pool(), future_record(TaskStatus::Pending)?).await?;
    let before = tasks.get(id).await.map_err(error)?.ok_or("missing task")?;
    let (first, second) = tokio::join!(tasks.wake(id), tasks.wake(id));
    let (first, second) = (first.map_err(error)?, second.map_err(error)?);
    assert!(first || second);
    let after = tasks.get(id).await.map_err(error)?.ok_or("missing task")?;
    assert!(after.ready_at() < before.ready_at());
    assert_eq!(after.status(), TaskStatus::Pending);
    assert_eq!(after.node_id(), None);
    assert_eq!(after.state::<i32>().map_err(error)?, Some(7));
    assert_eq!(after.resume_input::<i32>().map_err(error)?, Some(Ok(8)));
    assert_eq!(after.last_result::<i32>().map_err(error)?, Some(Ok(9)));
    assert_eq!((after.attempts(), after.step_attempts()), (3, 2));
    assert_eq!(after.updated_at(), before.updated_at());
    assert_eq!(after.created_at(), before.created_at());
    assert_eq!(after.completed_at(), before.completed_at());
    assert!(!tasks.wake(id).await.map_err(error)?);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Wake is a no-op for all ineligible rows, including already-due and null readiness.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn wake_ineligible_rows() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let pool = database.pool();
    assert!(
        !tasks
            .wake(TaskId::new(uuid::Uuid::now_v7()))
            .await
            .map_err(error)?
    );
    for status in [
        TaskStatus::Running,
        TaskStatus::Suspended,
        TaskStatus::Failed,
        TaskStatus::Succeeded,
    ] {
        let id = insert(pool, future_record(status)?).await?;
        assert!(!tasks.wake(id).await.map_err(error)?);
    }
    let mut cancelled = future_record(TaskStatus::Pending)?;
    cancelled.cancelled = true;
    let id = insert(pool, cancelled).await?;
    assert!(!tasks.wake(id).await.map_err(error)?);
    for readiness in [None, Some(Utc::now() - chrono::Duration::minutes(1))] {
        let mut pending = future_record(TaskStatus::Pending)?;
        pending.ready_at = readiness;
        let id = insert(pool, pending).await?;
        assert!(!tasks.wake(id).await.map_err(error)?);
    }
    let node = uuid::Uuid::now_v7();
    let now = Utc::now();
    db::from(&<TaskNodeRow as db::Model>::table())
        .insert_many(&[TaskNodeRow {
            id: node,
            started_at: now,
            heartbeat_at: now,
        }])
        .exec(&mut pool.clone())
        .await
        .map_err(error)?;
    let mut owned = future_record(TaskStatus::Pending)?;
    owned.node_id = Some(node);
    let id = insert(pool, owned).await?;
    assert!(!tasks.wake(id).await.map_err(error)?);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Cancellation wins durably in either ordering and cannot be overwritten by wake.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn wake_cancel_race() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let id = insert(database.pool(), future_record(TaskStatus::Pending)?).await?;
    let (wake, cancel) = tokio::join!(tasks.wake(id), tasks.cancel(id));
    wake.map_err(error)?;
    cancel.map_err(error)?;
    let task = tasks.get(id).await.map_err(error)?.ok_or("missing task")?;
    assert_eq!(task.status(), TaskStatus::Failed);
    assert!(task.cancelled());
    assert!(!tasks.wake(id).await.map_err(error)?);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

#[derive(Clone, Default)]
struct SqlCount(Arc<AtomicUsize>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SqlCount {
    /// Counts task-store SQL, excluding SQLite's connection initialization.
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "sqlx::query" {
            let connection_setup = cfg!(feature = "sqlite") && sqlite_connection_setup(event);
            if !connection_setup {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn sqlite_connection_setup(event: &tracing::Event<'_>) -> bool {
    let mut setup = false;
    event.record(
        &mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
            setup |= field.name() == "summary"
                && format!("{value:?}").contains("PRAGMA foreign_keys = ON");
        },
    );
    setup
}

/// A wake is one SQL statement, with no extra query for its no-op path.
#[tokio::test]
#[ignore = "run alone: SQL tracing uses a process-global subscriber"]
async fn wake_query_budget() -> Result<(), String> {
    use tracing_subscriber::prelude::*;

    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let id = insert(database.pool(), future_record(TaskStatus::Pending)?).await?;
    let count = SqlCount::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(count.clone()))
        .map_err(error)?;
    for changed in [true, false] {
        let before = count.0.load(Ordering::Relaxed);
        assert_eq!(tasks.wake(id).await.map_err(error)?, changed);
        assert_eq!(count.0.load(Ordering::Relaxed) - before, 1);
    }
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// A changed row emits one lane hint visible after wake returns; a no-op emits none.
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires an isolated dbharness admin URL"]
async fn wake_notifies_lane() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let pool = database.pool();
    let id = insert(pool, future_record(TaskStatus::Pending)?).await?;
    let mut listener = db::sqlx::postgres::PgListener::connect_with(pool.as_sqlx())
        .await
        .map_err(error)?;
    listener
        .listen(super::super::notify::CHANNEL)
        .await
        .map_err(error)?;
    assert!(tasks.wake(id).await.map_err(error)?);
    let hint = tokio::time::timeout(Duration::from_secs(2), listener.recv())
        .await
        .map_err(error)?
        .map_err(error)?;
    assert_eq!(
        serde_json::from_str::<String>(hint.payload()).map_err(error)?,
        DEFAULT_TASK_LANE.as_str()
    );
    assert!(!tasks.wake(id).await.map_err(error)?);
    assert!(
        tokio::time::timeout(Duration::from_millis(40), listener.recv())
            .await
            .is_err()
    );
    drop(listener);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}
