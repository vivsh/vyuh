use super::super::submit::tx_tests::fixture;
use super::*;
use crate::tasks::{DEFAULT_TASK_LANE, TaskInfo, TaskKind, Tasks, submission::build_record};

/// Child cancellation and parent delivery are invisible outside the transaction until commit.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn commits_child_and_parent() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let (parent, child) = waiting_pair(database.pool()).await?;
    let mut tx = database.pool().begin().await.map_err(error)?;
    tasks.cancel_tx(&mut tx, child).await.map_err(error)?;
    assert_waiting(&tasks, parent, child).await?;
    tx.commit().await.map_err(error)?;

    let child_info = info(&tasks, child).await?;
    let parent_info = info(&tasks, parent).await?;
    assert_eq!(child_info.status(), TaskStatus::Failed);
    assert!(child_info.cancelled());
    assert_eq!(parent_info.status(), TaskStatus::Pending);
    assert_eq!(parent_info.state::<u32>().map_err(error)?, Some(42));
    let Some(Err(failure)) = child_info.last_result::<()>().map_err(error)? else {
        return Err("missing cancellation result".into());
    };
    assert_eq!(failure.task_id(), Some(&child));
    assert_eq!(failure.message(), "Task cancelled");
    let Some(Err(delivered)) = parent_info.resume_input::<()>().map_err(error)? else {
        return Err("missing parent resume failure".into());
    };
    assert_eq!(delivered, failure);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Rolling back restores both rows, including the parent's untouched checkpoint and resume input.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn rolls_back_child_and_parent() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let (parent, child) = waiting_pair(database.pool()).await?;
    let mut tx = database.pool().begin().await.map_err(error)?;
    tasks.cancel_tx(&mut tx, child).await.map_err(error)?;
    let table = DbTaskStore::table();
    let staged = db::from(&table)
        .filter(
            table
                .id
                .in_values(vec![parent.into_uuid(), child.into_uuid()]),
        )
        .all::<TaskRow>()
        .exec(&mut tx)
        .await
        .map_err(error)?;
    assert_eq!(staged.len(), 2);
    let child_row = staged
        .iter()
        .find(|row| row.id == child.into_uuid())
        .ok_or("missing staged child")?;
    assert_eq!(child_row.status, TaskStatus::Failed.as_i16());
    assert!(child_row.cancelled);
    let parent_row = staged
        .iter()
        .find(|row| row.id == parent.into_uuid())
        .ok_or("missing staged parent")?;
    assert_eq!(parent_row.status, TaskStatus::Pending.as_i16());
    assert!(parent_row.resume_input.is_some());
    tx.rollback().await.map_err(error)?;
    assert_waiting(&tasks, parent, child).await?;
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Seeds the persisted state of a committed scalar spawn without starting a worker.
async fn waiting_pair(pool: &db::DbPool) -> Result<(TaskId, TaskId), String> {
    let mut parent = build_record(
        "tx_parent",
        "null".into(),
        None,
        None,
        DEFAULT_TASK_LANE,
        TaskKind::Flow,
    )
    .map_err(error)?;
    parent.status = TaskStatus::Suspended;
    parent.state = Some("42".into());
    let mut child = build_record(
        "tx_plain",
        "1".into(),
        None,
        None,
        DEFAULT_TASK_LANE,
        TaskKind::Work,
    )
    .map_err(error)?;
    let ids = (parent.id, child.id);
    child.parent_id = Some(parent.id);
    child.root_id = Some(parent.id);
    db::from(&DbTaskStore::table())
        .insert_many(&[TaskRow::from(parent), TaskRow::from(child)])
        .exec(&mut pool.clone())
        .await
        .map_err(error)?;
    Ok(ids)
}

/// Verifies that no cancellation or resume escaped the caller's transaction.
async fn assert_waiting(tasks: &Tasks, parent: TaskId, child: TaskId) -> Result<(), String> {
    let parent = info(tasks, parent).await?;
    assert_eq!(parent.status(), TaskStatus::Suspended);
    assert_eq!(parent.state::<u32>().map_err(error)?, Some(42));
    assert!(parent.resume_input::<()>().map_err(error)?.is_none());
    let child = info(tasks, child).await?;
    assert_eq!(child.status(), TaskStatus::Pending);
    assert!(!child.cancelled());
    assert!(child.last_result_json().is_none());
    Ok(())
}

async fn info(tasks: &Tasks, id: TaskId) -> Result<TaskInfo, String> {
    tasks
        .get(id)
        .await
        .map_err(error)?
        .ok_or("missing task".into())
}

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}
