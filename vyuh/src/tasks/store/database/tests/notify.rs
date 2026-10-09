use super::super::claim::tests::{fresh_store, row};
use super::*;
use std::time::Duration;

/// Mutation CTEs emit once after commit, not rollback, while preserving affected-row counts.
#[tokio::test]
#[ignore = "requires an empty disposable VYUH_TASK_TEST_URL; run alone"]
async fn notifying_mutations() -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let counts = super::super::claim_postgres::tests::Counts::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(counts.clone()))
        .map_err(|e| e.to_string())?;
    let store = fresh_store().await?;
    let url = std::env::var("VYUH_TASK_TEST_URL").map_err(|e| e.to_string())?;
    let mut listener = db::sqlx::postgres::PgListener::connect(&url)
        .await
        .map_err(|e| e.to_string())?;
    listener.listen(CHANNEL).await.map_err(|e| e.to_string())?;
    let rows = vec![
        row(801, chrono::Utc::now(), None),
        row(802, chrono::Utc::now(), None),
    ];
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    counts.0.store(0, std::sync::atomic::Ordering::Relaxed);
    insert(&mut tx, &rows, 32, None)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(counts.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.recv())
            .await
            .is_err()
    );
    tx.commit().await.map_err(|e| e.to_string())?;
    let hint = tokio::time::timeout(Duration::from_secs(2), listener.recv())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    assert_eq!(hint.payload(), "\"default\"");
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.recv())
            .await
            .is_err()
    );
    rollback_notifications(&store, &rows, &mut listener, &counts).await?;
    workflow_wakes(store).await
}

/// A bulk rollback emits nothing and a no-op preserves zero affected rows in one statement.
async fn rollback_notifications(
    store: &super::super::DbTaskStore,
    rows: &[TaskRow],
    listener: &mut db::sqlx::postgres::PgListener,
    counts: &super::super::claim_postgres::tests::Counts,
) -> Result<(), String> {
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    counts.0.store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        update(&mut tx, rows, &["status", "ready_at"], 32)
            .await
            .map_err(|e| e.to_string())?,
        2
    );
    assert_eq!(counts.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    tx.rollback().await.map_err(|e| e.to_string())?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.recv())
            .await
            .is_err()
    );
    let sql = notifying("UPDATE vyuh_tasks SET status = 0 WHERE status = 2 AND NOT cancelled");
    let counts: (i64, i64) = store
        .pool
        .clone()
        .fetch_one(db::Statement::raw(&sql))
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(counts, (0, 0));
    Ok(())
}

/// Foreign children wake normal ownerless polling only after the final All completion.
async fn workflow_wakes(mut store: super::super::DbTaskStore) -> Result<(), String> {
    use crate::tasks::{DEFAULT_TASK_LANE, TaskKind, TaskLaneConf, TaskStore};
    let child_lane = crate::tasks::TaskLane::new("notify-children");
    let url = std::env::var("VYUH_TASK_TEST_URL").map_err(|e| e.to_string())?;
    store.pool = db::DbPool::from_pool(
        db::sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .map_err(|e| e.to_string())?,
    );
    let mut conf = crate::tasks::store::memory::tests::conf();
    conf.handlers.push(("fixture".into(), TaskKind::Work));
    conf.lanes
        .push(TaskLaneConf::new(child_lane, 8).sticky(false));
    store.runtime_conf = std::sync::Arc::new(conf);
    store.initialize().await.map_err(|e| e.to_string())?;
    let owner = uuid::Uuid::now_v7();
    let peer = uuid::Uuid::now_v7();
    store
        .register_node(&owner)
        .await
        .map_err(|e| e.to_string())?;
    store
        .register_node(&peer)
        .await
        .map_err(|e| e.to_string())?;
    let (events, mut received) = tokio::sync::broadcast::channel(2);
    let listener = store
        .start_notifications(owner, events, crate::notifiers::CancellationNotifier::new())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("no listener")?;
    let result = workflow_contract(&store, owner, peer, child_lane, &mut received).await;
    listener.abort();
    let _ = listener.await;
    result?;
    assert_eq!(
        store.runtime_conf.lanes.first().ok_or("lane")?.lane(),
        DEFAULT_TASK_LANE
    );
    Ok(())
}

/// Exercises real store writes and generic readiness hints without sibling-result reads.
async fn workflow_contract(
    store: &super::super::DbTaskStore,
    owner: uuid::Uuid,
    peer: uuid::Uuid,
    child_lane: crate::tasks::TaskLane,
    received: &mut tokio::sync::broadcast::Receiver<crate::tasks::client::RunnerEvent>,
) -> Result<(), String> {
    use crate::tasks::store::memory::tests::{flow_record, write};
    use crate::tasks::{DEFAULT_TASK_LANE, TaskStatus, TaskStore};
    expect_wake(received, None).await?;
    let (_, mut parents) = store
        .store_admitted(vec![write(flow_record())], &owner, 1)
        .await
        .map_err(|e| e.to_string())?;
    let parent = parents.pop().ok_or("parent")?;
    spawn_group(store, &parent, child_lane).await?;
    expect_wake(received, Some(child_lane)).await?;
    finish_group(store, peer, child_lane, received).await?;
    expect_wake(received, Some(DEFAULT_TASK_LANE)).await?;
    let ready = store
        .get_task(parent.id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("ready")?;
    assert_eq!(ready.status, TaskStatus::Pending);
    assert_eq!(ready.node_id, None);
    assert!(ready.resume_input.is_none());
    external_resume(store, owner, received).await?;
    reconnect_wake(store, owner, received).await?;
    retirement_wake(store, owner, peer, received).await
}

/// Inserts the whole child group through the ordinary atomic Flow outcome.
async fn spawn_group(
    store: &super::super::DbTaskStore,
    parent: &crate::tasks::TaskRecord,
    child_lane: crate::tasks::TaskLane,
) -> Result<(), String> {
    use crate::tasks::store::memory::tests::{record, write};
    use crate::tasks::{TaskOutcome, TaskStore};
    let children = (0..3)
        .map(|_| {
            let mut task = record();
            task.lane = child_lane.to_string();
            write(task)
        })
        .collect();
    store
        .commit_tasks(&[output(
            parent,
            TaskOutcome::All {
                state: "42".into(),
                children,
            },
        )?])
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Each terminal transition counts once, and intermediate counters produce no notification.
async fn finish_group(
    store: &super::super::DbTaskStore,
    peer: uuid::Uuid,
    child_lane: crate::tasks::TaskLane,
    received: &mut tokio::sync::broadcast::Receiver<crate::tasks::client::RunnerEvent>,
) -> Result<(), String> {
    use crate::tasks::{TaskOutcome, TaskStore};
    let request = crate::tasks::LaneClaim {
        lane: child_lane,
        limit: 8,
        owner: None,
    };
    let poll = store
        .claim_tasks(&peer, &[request])
        .await
        .map_err(|e| e.to_string())?;
    let children = poll
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .collect::<Vec<_>>();
    assert_eq!(children.len(), 3);
    for (index, child) in children.iter().enumerate() {
        let commit = output(
            child,
            TaskOutcome::CompleteWith {
                output: index.to_string(),
            },
        )?;
        complete_once(store, commit, child.id).await?;
        if index < 2 {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), received.recv())
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

/// A repeated acknowledgement cannot decrement a join counter or emit another readiness hint.
async fn complete_once(
    store: &super::super::DbTaskStore,
    commit: crate::tasks::TaskCommit,
    id: crate::tasks::TaskId,
) -> Result<(), String> {
    use crate::tasks::TaskStore;
    store
        .commit_tasks(std::slice::from_ref(&commit))
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        store
            .commit_tasks(&[commit])
            .await
            .map_err(|e| e.to_string())?
            .rejected,
        vec![id]
    );
    Ok(())
}

/// A disconnected dedicated listener resubscribes and rescans without using the task pool slot.
async fn reconnect_wake(
    store: &super::super::DbTaskStore,
    owner: uuid::Uuid,
    received: &mut tokio::sync::broadcast::Receiver<crate::tasks::client::RunnerEvent>,
) -> Result<(), String> {
    let pattern = format!("%LISTEN%{}%", owner.simple());
    let stopped: Vec<(bool,)> = store.pool.clone().fetch_all(db::Statement::raw(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query LIKE $1"
    ).bind(pattern)).await.map_err(|e| e.to_string())?;
    assert_eq!(stopped, vec![(true,)]);
    expect_wake(received, None).await?;
    external_resume(store, owner, received).await
}

/// External resume routes by UPDATE RETURNING and preserves no-op semantics.
async fn external_resume(
    store: &super::super::DbTaskStore,
    owner: uuid::Uuid,
    received: &mut tokio::sync::broadcast::Receiver<crate::tasks::client::RunnerEvent>,
) -> Result<(), String> {
    use crate::tasks::store::memory::tests::{flow_record, write};
    use crate::tasks::{DEFAULT_TASK_LANE, TaskOutcome, TaskStore};
    let (_, mut tasks) = store
        .store_admitted(vec![write(flow_record())], &owner, 1)
        .await
        .map_err(|e| e.to_string())?;
    let task = tasks.pop().ok_or("resume fixture")?;
    store
        .commit_tasks(&[output(&task, TaskOutcome::Suspend { state: "42".into() })?])
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        store
            .resume(task.id, "{\"Ok\":7}".into())
            .await
            .map_err(|e| e.to_string())?
    );
    expect_wake(received, Some(DEFAULT_TASK_LANE)).await?;
    assert!(
        !store
            .resume(task.id, "{\"Ok\":8}".into())
            .await
            .map_err(|e| e.to_string())?
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), received.recv())
            .await
            .is_err()
    );
    Ok(())
}

/// Only a successful stale-node DELETE emits a broad hint; the missing node cannot retire peers.
async fn retirement_wake(
    store: &super::super::DbTaskStore,
    owner: uuid::Uuid,
    peer: uuid::Uuid,
    received: &mut tokio::sync::broadcast::Receiver<crate::tasks::client::RunnerEvent>,
) -> Result<(), String> {
    use crate::tasks::TaskStore;
    store
        .pool
        .clone()
        .execute(
            db::Statement::raw(
                "UPDATE vyuh_task_nodes SET heartbeat_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
            )
            .bind(owner),
        )
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        store
            .heartbeat(&peer, Duration::from_secs(72))
            .await
            .map_err(|e| e.to_string())?
    );
    expect_wake(received, None).await?;
    assert!(
        store
            .heartbeat(&peer, Duration::from_secs(72))
            .await
            .map_err(|e| e.to_string())?
    );
    assert!(
        !store
            .heartbeat(&owner, Duration::from_secs(72))
            .await
            .map_err(|e| e.to_string())?
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), received.recv())
            .await
            .is_err()
    );
    Ok(())
}

/// Compares the bounded transport hint without assuming notification delivery acknowledges work.
async fn expect_wake(
    received: &mut tokio::sync::broadcast::Receiver<crate::tasks::client::RunnerEvent>,
    lane: Option<crate::tasks::TaskLane>,
) -> Result<(), String> {
    use crate::tasks::client::RunnerEvent;
    let event = tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    match (event, lane) {
        (RunnerEvent::Wake, None) => Ok(()),
        (RunnerEvent::WakeLane(actual), Some(expected)) if actual == expected => Ok(()),
        _ => Err(format!("unexpected wake {event:?}, expected {lane:?}")),
    }
}

/// Builds an output from the immutable claimed ownership evidence.
fn output(
    task: &crate::tasks::TaskRecord,
    outcome: crate::tasks::TaskOutcome,
) -> Result<crate::tasks::TaskCommit, String> {
    let lane = if task.lane == "notify-children" {
        crate::tasks::TaskLane::new("notify-children")
    } else {
        crate::tasks::DEFAULT_TASK_LANE
    };
    Ok(crate::tasks::TaskCommit::Outcome {
        task_id: task.id,
        node_id: task.node_id.ok_or("owner")?,
        attempts: task.attempts,
        lane,
        outcome,
        owner_token: None,
    })
}
