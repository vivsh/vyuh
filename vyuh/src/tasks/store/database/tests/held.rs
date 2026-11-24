use super::super::submit::tx_tests::{fixture_with, store, writes};
use super::*;
use crate::tasks::{
    DEFAULT_TASK_LANE, ScheduledTaskWrite, TaskCommit, TaskConf, TaskId, TaskKind, TaskLane,
    TaskLaneConf, TaskOutcome, TaskStatus, TaskStore as _,
};
use std::time::Duration;

const RETAIN: TaskLane = TaskLane::new("positive-hold");

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn configuration() -> TaskConf {
    TaskConf::default()
        .lane(TaskLaneConf::new(RETAIN, 2).idempotency_retention(Duration::from_secs(1)))
}

/// Skipping a held scheduled input still commits its cursor and prevents a replay.
#[tokio::test]
#[ignore = "isolated integration database"]
async fn held_schedule_advances_cursor() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let store = store(db.pool(), conf).map_err(error)?;
    store
        .store_tasks(writes(true, RETAIN, 500, 1)?)
        .await
        .map_err(error)?;
    let occurrence = DateTime::from_timestamp(Utc::now().timestamp() + 10, 0)
        .ok_or("invalid test occurrence")?;
    let make = |key, occurrence| {
        Ok::<_, String>(ScheduledTaskWrite {
            name: "held_schedule".into(),
            occurrence,
            write: writes(true, RETAIN, key, 1)?
                .pop()
                .ok_or("missing schedule task")?,
        })
    };
    assert_eq!(
        store
            .store_scheduled(make(500, occurrence)?)
            .await
            .map_err(error)?,
        Some(TaskReceipt::KeyHeld)
    );
    let snapshot = store
        .schedule_snapshot(&["held_schedule".into()])
        .await
        .map_err(error)?;
    assert_eq!(snapshot.cursors.get("held_schedule"), Some(&occurrence));
    assert_eq!(
        store
            .store_scheduled(make(501, occurrence)?)
            .await
            .map_err(error)?,
        None
    );
    assert!(matches!(
        store
            .store_scheduled(make(501, occurrence + chrono::Duration::seconds(1))?)
            .await
            .map_err(error)?,
        Some(TaskReceipt::Queued(_))
    ));
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}

/// The task-row partial unique index keys by registered handler, never by lane or key alone.
#[tokio::test]
#[ignore = "isolated integration database"]
async fn held_keys_are_handler_scoped() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let store = store(db.pool(), conf).map_err(error)?;
    store
        .store_tasks(writes(true, RETAIN, 400, 1)?)
        .await
        .map_err(error)?;
    let mut batch = writes(true, DEFAULT_TASK_LANE, 400, 1)?;
    let mut other = writes(true, RETAIN, 400, 1)?
        .pop()
        .ok_or("missing other handler")?;
    other.record.name = "tx_plain".into();
    let id = other.record.id;
    batch.push(other);
    let mut repeat = writes(true, DEFAULT_TASK_LANE, 400, 1)?
        .pop()
        .ok_or("missing duplicate")?;
    repeat.record.name = "tx_plain".into();
    batch.push(repeat);
    assert_eq!(
        store.store_tasks(batch).await.map_err(error)?,
        [
            TaskReceipt::KeyHeld,
            TaskReceipt::Queued(id),
            TaskReceipt::KeyHeld,
        ]
    );
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}

/// A held key leaves its reserved ingress slot to the next eligible inserted task.
#[tokio::test]
#[ignore = "isolated integration database"]
async fn held_key_backfills_ingress() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let store = store(db.pool(), conf).map_err(error)?;
    store
        .store_tasks(writes(true, RETAIN, 100, 1)?)
        .await
        .map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    let mut batch = writes(true, RETAIN, 100, 1)?;
    let plain = writes(false, RETAIN, 101, 1)?
        .pop()
        .ok_or("missing plain task")?;
    let promoted = plain.record.id;
    batch.push(plain);
    let overflow = writes(true, RETAIN, 101, 1)?
        .pop()
        .ok_or("missing keyed task")?;
    let pending = overflow.record.id;
    batch.push(overflow);
    let (receipts, admitted) = store
        .store_admitted_impl(batch, &node, 1)
        .await
        .map_err(error)?;
    assert_eq!(
        receipts,
        [
            TaskReceipt::KeyHeld,
            TaskReceipt::Queued(promoted),
            TaskReceipt::Queued(pending)
        ]
    );
    assert!(
        matches!(admitted.as_slice(), [task] if task.id == promoted && task.node_id == Some(node) && task.attempts == 1)
    );
    let stored = store
        .get_task(promoted)
        .await
        .map_err(error)?
        .ok_or("missing admitted task")?;
    assert_eq!(
        (stored.status, stored.node_id, stored.attempts),
        (TaskStatus::Running, Some(node), 1)
    );
    let stored = store
        .get_task(pending)
        .await
        .map_err(error)?
        .ok_or("missing pending task")?;
    assert_eq!(
        (stored.status, stored.node_id, stored.attempts),
        (TaskStatus::Pending, None, 0)
    );
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}

/// A held child rolls back its entire group, not a different valid outcome in the flush.
#[tokio::test]
#[ignore = "isolated integration database"]
async fn held_child_rejects_group() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let mut store = store(db.pool(), conf).map_err(error)?;
    std::sync::Arc::get_mut(&mut store.runtime_conf)
        .ok_or("shared fixture config")?
        .handlers
        .push(("held_test_flow".into(), TaskKind::Flow));
    store
        .store_tasks(writes(true, RETAIN, 200, 1)?)
        .await
        .map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    let parents = workflow_parents(&store, node).await?;
    let mut rejected = writes(true, DEFAULT_TASK_LANE, 200, 2)?;
    rejected.extend(writes(false, DEFAULT_TASK_LANE, 202, 1)?);
    let rejected_ids = rejected
        .iter()
        .map(|write| write.record.id)
        .collect::<Vec<_>>();
    let accepted = writes(true, RETAIN, 203, 1)?;
    let survivor = accepted.first().ok_or("missing accepted child")?.record.id;
    let [first, second] = parents.as_slice() else {
        return Err("wrong parent count".into());
    };
    #[cfg(feature = "postgres")]
    let mut listener = workflow_listener(db.pool()).await?;
    let flush = store
        .commit_tasks(&[
            all_commit(first, node, rejected),
            all_commit(second, node, accepted),
        ])
        .await
        .map_err(error)?;
    assert!(flush.rejected.is_empty());
    #[cfg(feature = "postgres")]
    {
        assert_only_survivor_woke(&mut listener).await?;
        drop(listener);
    }
    assert_eq!(
        store
            .get_task(first.id)
            .await
            .map_err(error)?
            .ok_or("missing failed parent")?
            .status,
        TaskStatus::Failed
    );
    assert_eq!(
        store
            .get_task(second.id)
            .await
            .map_err(error)?
            .ok_or("missing waiting parent")?
            .status,
        TaskStatus::Suspended
    );
    for id in rejected_ids {
        assert!(store.get_task(id).await.map_err(error)?.is_none());
    }
    let child = store
        .get_task(survivor)
        .await
        .map_err(error)?
        .ok_or("missing accepted child")?;
    assert_eq!(
        (child.parent_id, child.root_id),
        (Some(second.id), Some(second.id))
    );
    let reused = store
        .store_tasks(writes(true, RETAIN, 201, 1)?)
        .await
        .map_err(error)?;
    assert!(matches!(reused.as_slice(), [TaskReceipt::Queued(_)]));
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}

#[cfg(feature = "postgres")]
async fn workflow_listener(pool: &db::DbPool) -> Result<db::sqlx::postgres::PgListener, String> {
    let mut listener = db::sqlx::postgres::PgListener::connect_with(pool.as_sqlx())
        .await
        .map_err(error)?;
    listener
        .listen(super::super::notify::CHANNEL)
        .await
        .map_err(error)?;
    Ok(listener)
}

#[cfg(feature = "postgres")]
/// Provisional children of the rejected group may not notify its distinct lane.
async fn assert_only_survivor_woke(
    listener: &mut db::sqlx::postgres::PgListener,
) -> Result<(), String> {
    let notice = tokio::time::timeout(Duration::from_secs(2), listener.recv())
        .await
        .map_err(error)?
        .map_err(error)?;
    assert_eq!(
        notice.payload(),
        serde_json::to_string(RETAIN.as_str()).map_err(error)?
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.recv())
            .await
            .is_err()
    );
    Ok(())
}

async fn workflow_parents(
    store: &DbTaskStore,
    node: Uuid,
) -> Result<Vec<crate::tasks::TaskRecord>, String> {
    let mut writes = writes(false, RETAIN, 300, 2)?;
    for write in &mut writes {
        write.record.kind = TaskKind::Flow;
        write.record.name = "held_test_flow".into();
    }
    store
        .store_admitted_impl(writes, &node, 2)
        .await
        .map(|(_, tasks)| tasks)
        .map_err(error)
}

fn all_commit(
    parent: &crate::tasks::TaskRecord,
    node: Uuid,
    children: Vec<TaskWrite>,
) -> TaskCommit {
    TaskCommit::Outcome {
        publisher: false,
        node_id: node,
        attempts: parent.attempts,
        task_id: parent.id,
        lane: RETAIN,
        outcome: TaskOutcome::All {
            state: "null".into(),
            children,
        },
        owner_token: None,
    }
}

/// Creates and cancels a task through the real fenced store transaction.
async fn terminal(
    store: &super::super::DbTaskStore,
    lane: TaskLane,
    key: u32,
) -> Result<TaskId, String> {
    let created = store
        .store_tasks(writes(true, lane, key, 1)?)
        .await
        .map_err(error)?;
    let id = created
        .first()
        .and_then(|receipt| receipt.id())
        .ok_or("missing queued receipt")?;
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: id }])
        .await
        .map_err(error)?;
    Ok(id)
}

/// Zero retention releases at terminal commit; a due positive hold requires sweep commit.
#[tokio::test]
#[ignore = "isolated prototype database"]
async fn held_lifetimes() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let store = store(db.pool(), conf).map_err(error)?;
    let zero = terminal(&store, DEFAULT_TASK_LANE, 1).await?;
    let replacement = store
        .store_tasks(writes(true, RETAIN, 1, 1)?)
        .await
        .map_err(error)?;
    assert!(matches!(replacement.first(), Some(TaskReceipt::Queued(new)) if *new != zero));
    let positive = terminal(&store, RETAIN, 2).await?;
    let expiry = store
        .get_task(positive)
        .await
        .map_err(error)?
        .and_then(|task| task.idempotency_expires_at)
        .ok_or("missing expiry")?;
    let mut tx = db.pool().begin().await.map_err(error)?;
    sweep(&mut tx, expiry - chrono::Duration::seconds(1), 250)
        .await
        .map_err(error)?;
    tx.commit().await.map_err(error)?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let duplicate = store
        .store_tasks(writes(true, DEFAULT_TASK_LANE, 2, 1)?)
        .await
        .map_err(error)?;
    assert_eq!(duplicate, vec![TaskReceipt::KeyHeld]);
    let mut tx = db.pool().begin().await.map_err(error)?;
    sweep(&mut tx, expiry, 250).await.map_err(error)?;
    tx.commit().await.map_err(error)?;
    let replacement = store
        .store_tasks(writes(true, DEFAULT_TASK_LANE, 2, 1)?)
        .await
        .map_err(error)?;
    assert!(matches!(replacement.first(), Some(TaskReceipt::Queued(new)) if *new != positive));
    let historic = store
        .get_task(positive)
        .await
        .map_err(error)?
        .ok_or("missing historic row")?;
    assert_eq!(historic.idempotency_expires_at, Some(expiry));
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}

/// Conflicting, repeated and unkeyed inputs retain receipt order and real rollback semantics.
#[tokio::test]
#[ignore = "isolated prototype database"]
async fn mixed_key_held_batch() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let store = store(db.pool(), conf).map_err(error)?;
    let held = store
        .store_tasks(writes(true, RETAIN, 20, 1)?)
        .await
        .map_err(error)?;
    let mut mixed = writes(true, DEFAULT_TASK_LANE, 20, 1)?;
    let mut plain = writes(false, DEFAULT_TASK_LANE, 21, 1)?
        .pop()
        .ok_or("missing plain row")?;
    let plain_id = plain.record.id;
    mixed.push(plain.clone());
    mixed.extend(writes(true, DEFAULT_TASK_LANE, 21, 1)?);
    let new_id = mixed.last().ok_or("missing new row")?.record.id;
    mixed.extend(writes(true, RETAIN, 21, 1)?);
    let result = store.store_tasks(mixed).await.map_err(error)?;
    assert_eq!(
        result,
        vec![
            TaskReceipt::KeyHeld,
            TaskReceipt::Queued(plain_id),
            TaskReceipt::Queued(new_id),
            TaskReceipt::KeyHeld
        ]
    );
    let count: i64 = db::sqlx::query_scalar("SELECT count(*) FROM vyuh_tasks")
        .fetch_one(db.pool().as_sqlx())
        .await
        .map_err(error)?;
    assert_eq!(count, 3);
    assert!(held.first().and_then(|receipt| receipt.id()).is_some());

    // A different uniqueness violation isn't treated as KeyHeld.
    plain.record.input = "88".into();
    let mut bad = writes(true, RETAIN, 22, 1)?;
    let rejected_id = bad.first().ok_or("missing rollback row")?.record.id;
    bad.push(plain);
    assert!(store.store_tasks(bad).await.is_err());
    assert!(store.get_task(rejected_id).await.map_err(error)?.is_none());
    let next = store
        .store_tasks(writes(true, RETAIN, 22, 1)?)
        .await
        .map_err(error)?;
    assert!(matches!(next.first(), Some(TaskReceipt::Queued(_))));
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}

/// Releases a conflicting hold between INSERT and receipt classification.
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "isolated PostgreSQL 17 conflict gate"]
async fn sweep_keeps_key_held_receipt() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let store = store(db.pool(), conf).map_err(error)?;
    let id = terminal(&store, RETAIN, 9).await?;
    let expiry = store
        .get_task(id)
        .await
        .map_err(error)?
        .and_then(|task| task.idempotency_expires_at)
        .ok_or("missing expiry")?;
    let mut submit_tx = db.pool().begin().await.map_err(error)?;
    let mut rows = writes(true, RETAIN, 9, 1)?;
    let write = rows.pop().ok_or("missing candidate")?;
    let row = TaskRow::from(write.record.clone());
    let returned = insert(&mut submit_tx, &[row], true).await.map_err(error)?;
    assert!(returned.is_empty());
    let mut sweep_tx = db.pool().begin().await.map_err(error)?;
    tokio::time::timeout(Duration::from_secs(2), sweep(&mut sweep_tx, expiry, 250))
        .await
        .map_err(error)?
        .map_err(error)?;
    sweep_tx.commit().await.map_err(error)?;
    assert_eq!(
        receipt(write.record.id, true, &returned).map_err(error)?,
        TaskReceipt::KeyHeld
    );
    submit_tx.commit().await.map_err(error)?;
    let after = store
        .store_tasks(writes(true, RETAIN, 9, 1)?)
        .await
        .map_err(error)?;
    assert!(matches!(after.first(), Some(TaskReceipt::Queued(new)) if *new != id));
    println!(
        "HELD_GATE: PostgreSQL returned KeyHeld after concurrent release, with no lookup or retry"
    );
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}

/// A concurrent zero-retention commit doesn't change an already-skipped receipt.
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "isolated PostgreSQL 17 release race"]
async fn completion_keeps_key_held_receipt() -> Result<(), String> {
    let conf = configuration();
    let (db, runtime, _) = fixture_with(conf.clone()).await?;
    let store = store(db.pool(), conf).map_err(error)?;
    let active = store
        .store_tasks(writes(true, DEFAULT_TASK_LANE, 31, 1)?)
        .await
        .map_err(error)?;
    let id = active
        .first()
        .and_then(|receipt| receipt.id())
        .ok_or("missing original task")?;
    let mut submit_tx = db.pool().begin().await.map_err(error)?;
    let incoming = writes(true, DEFAULT_TASK_LANE, 31, 1)?
        .pop()
        .ok_or("missing candidate")?;
    let row = TaskRow::from(incoming.record.clone());
    let returned = insert(&mut submit_tx, &[row], true).await.map_err(error)?;
    assert!(returned.is_empty());
    tokio::time::timeout(
        Duration::from_secs(2),
        store.commit_tasks(&[TaskCommit::Cancel { task_id: id }]),
    )
    .await
    .map_err(error)?
    .map_err(error)?;
    assert_eq!(
        receipt(incoming.record.id, true, &returned).map_err(error)?,
        TaskReceipt::KeyHeld
    );
    submit_tx.commit().await.map_err(error)?;
    let next = store
        .store_tasks(writes(true, RETAIN, 31, 1)?)
        .await
        .map_err(error)?;
    assert!(matches!(next.first(), Some(TaskReceipt::Queued(new)) if *new != id));
    runtime.shutdown_and_wait().await;
    db.teardown().await.map_err(error)
}
