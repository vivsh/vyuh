use super::*;
use crate::tasks::store::memory::tests::{claim, commit, conf, flow_record, record, write};
use crate::tasks::{AbstractTaskStore, TaskId, TaskOutcome};

#[path = "all_faults.rs"]
mod faults;

/// Concurrent accepted completions serialize on one parent and deliver exactly once.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn all_concurrent_completion() -> Result<(), String> {
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    let (parent, children) = prepare(&store, 8).await.map_err(|e| e.to_string())?;
    let mut handles = Vec::new();
    for child in children {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let outcome = commit(
                child,
                TaskOutcome::CompleteWith {
                    output: "42".into(),
                },
            );
            store
                .commit_outcomes("children", std::slice::from_ref(&outcome))
                .await?;
            store.commit_outcomes("children", &[outcome]).await
        }));
    }
    for handle in handles {
        handle
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
    }
    store
        .claim_tasks("parent", &[claim()])
        .await
        .map_err(|e| e.to_string())?;
    let task = store
        .get_task(parent)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing parent")?;
    let result: Result<Vec<Result<u32, crate::tasks::TaskFailure>>, crate::tasks::TaskFailure> =
        serde_json::from_str(task.resume_input.as_deref().ok_or("missing input")?)
            .map_err(|e| e.to_string())?;
    assert_eq!(result, Ok(vec![Ok(42); 8]));
    Ok(())
}

/// Rollback of the last completion or claim restores the counter and unmaterialized membership.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn all_transaction_rollback() -> Result<(), String> {
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    rollback(&store).await.map_err(|e| e.to_string())
}

/// Exercises existing transaction entrypoints without injecting a second commit protocol.
async fn rollback(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use crate::db::DbSession as _;
    let (parent, ids) = prepare(store, 1).await?;
    let child = *ids.first().ok_or_else(missing)?;
    let commits = [commit(child, TaskOutcome::Complete)];
    let mut tx = store.pool.begin().await?;
    let now = chrono::Utc::now();
    let (children, deliveries, waits, _) = store
        .commit_outcomes_tx(&mut tx, "children", &commits, &conf(), now, None)
        .await?;
    crate::tasks::store::database::writes::finalize_workflow(
        &mut tx,
        &children,
        deliveries,
        &waits,
        &conf(),
        now,
        32,
    )
    .await?;
    tx.rollback().await?;
    assert_eq!(
        store.get_task(parent).await?.ok_or_else(missing)?.status,
        TaskStatus::Suspended
    );
    assert_eq!(
        store.get_task(child).await?.ok_or_else(missing)?.status,
        TaskStatus::Running
    );
    store.commit_outcomes("children", &commits).await?;
    let mut tx = store.pool.begin().await?;
    let now = tx
        .fetch_scalar(db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?;
    let (poll, _) = store
        .claim_tasks_tx(
            &mut tx,
            "parent",
            &[claim()],
            &conf(),
            now,
            &mut Vec::new(),
            None,
        )
        .await?;
    assert!(
        poll.lanes
            .iter()
            .flat_map(|lane| &lane.tasks)
            .any(|task| task.resume_input.is_some())
    );
    tx.rollback().await?;
    assert!(
        store
            .get_task(parent)
            .await?
            .ok_or_else(missing)?
            .resume_input
            .is_none()
    );
    store.claim_tasks("parent", &[claim()]).await?;
    assert_eq!(
        store
            .get_task(parent)
            .await?
            .and_then(|task| task.resume_input)
            .as_deref(),
        Some("{\"Ok\":[{\"Ok\":null}]}")
    );
    Ok(())
}

/// The final claim reads current committed child results even after an older snapshot read.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn all_current_result_read() -> Result<(), String> {
    use crate::db::DbSession as _;
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    let (parent, children) = prepare(&store, 1).await.map_err(|e| e.to_string())?;
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let _: i64 = tx
        .fetch_scalar(db::Statement::raw(
            "SELECT COUNT(*) FROM vyuh_tasks WHERE status = 3",
        ))
        .await
        .map_err(|e| e.to_string())?;
    if cfg!(feature = "sqlite") {
        tx.rollback().await.map_err(|e| e.to_string())?;
    } else {
        store
            .commit_outcomes(
                "children",
                &[commit(
                    children[0],
                    TaskOutcome::CompleteWith {
                        output: "99".into(),
                    },
                )],
            )
            .await
            .map_err(|e| e.to_string())?;
        let waits = vec![(
            parent,
            serde_json::to_string(&children).map_err(|e| e.to_string())?,
            0,
        )];
        let materialized = crate::tasks::store::database::all::materialize(
            &mut tx,
            waits,
            &mut crate::tasks::TaskPoll { lanes: Vec::new() },
            32,
        )
        .await;
        match materialized {
            Ok(()) => tx.commit().await.map_err(|e| e.to_string())?,
            Err(error)
                if error
                    .to_string()
                    .contains("Record has changed since last read") =>
            {
                // MariaDB may reject promotion of an old read view; a fresh turn must recover.
                tx.rollback().await.map_err(|e| e.to_string())?;
                store
                    .claim_tasks("parent", &[claim()])
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Err(error) => return Err(error.to_string()),
        }
        assert_eq!(
            store
                .get_task(parent)
                .await
                .map_err(|e| e.to_string())?
                .and_then(|task| task.resume_input)
                .as_deref(),
            Some("{\"Ok\":[{\"Ok\":99}]}")
        );
    }
    Ok(())
}

/// Creates running children through the same accepted outcome as application flows.
pub(super) async fn prepare(
    store: &DbTaskStore,
    size: usize,
) -> Result<(TaskId, Vec<TaskId>), TaskRuntimeError> {
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("parent", &[claim()]).await?;
    let children = (0..size).map(|_| write(record())).collect::<Vec<_>>();
    let ids = children
        .iter()
        .map(|child| child.record.id)
        .collect::<Vec<_>>();
    store
        .commit_outcomes(
            "parent",
            &[commit(
                id,
                TaskOutcome::All {
                    state: "0".into(),
                    children,
                },
            )],
        )
        .await?;
    let mut left = size;
    while left > 0 {
        let poll = store.claim_tasks("children", &[claim()]).await?;
        let count = poll
            .lanes
            .iter()
            .map(|lane| lane.tasks.len())
            .sum::<usize>();
        if count == 0 {
            return Err(missing());
        }
        left = left.saturating_sub(count);
    }
    Ok((id, ids))
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("Missing join fixture".into())
}
