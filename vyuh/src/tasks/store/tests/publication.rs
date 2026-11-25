use crate::tasks::{
    DEFAULT_TASK_LANE, LaneClaim, PublishResult, TaskCommit, TaskConf, TaskId, TaskKind, TaskLane,
    TaskLaneConf, TaskOutcome, TaskReceipt, TaskRecord, TaskRegistry, TaskRuntimeError, TaskStatus,
    TaskStore, TaskStoreConf, TaskWrite, submission::build_record,
};
use uuid::Uuid;

pub(super) const OTHER: TaskLane = TaskLane::new("unrelated");
const INPUT: &str = r#"{"Ok":42}"#;
const PUBLISH_CAP: usize = 128;

pub(super) fn configuration() -> Result<TaskStoreConf, TaskRuntimeError> {
    let config = TaskConf::default().lane(TaskLaneConf::new(OTHER, 8));
    let mut conf = TaskRegistry::new().with_config(config)?.store_conf()?;
    conf.handlers.extend([
        ("topic_work".into(), TaskKind::Work),
        ("topic_flow".into(), TaskKind::Flow),
    ]);
    Ok(conf)
}

pub(super) fn request(lane: TaskLane, limit: usize) -> LaneClaim {
    LaneClaim {
        lane,
        limit,
        owner: None,
    }
}

pub(super) fn write(lane: TaskLane, index: usize) -> Result<TaskWrite, TaskRuntimeError> {
    let kind = if index % 2 == 0 {
        TaskKind::Work
    } else {
        TaskKind::Flow
    };
    Ok(TaskWrite {
        record: build_record(
            if kind == TaskKind::Work {
                "topic_work"
            } else {
                "topic_flow"
            },
            "null".into(),
            None,
            None,
            lane,
            kind,
        )?,
        initial_delay: None,
    })
}

pub(super) fn output(
    row: &TaskRecord,
    lane: TaskLane,
    outcome: TaskOutcome,
) -> Result<TaskCommit, String> {
    Ok(TaskCommit::Outcome {
        fanout_bytes: super::sharing::estimate(row.share_role, &outcome),
        node_id: row.node_id.ok_or("fixture was not claimed")?,
        task_id: row.id,
        attempts: row.attempts,
        lane,
        owner_token: None,
        outcome,
    })
}

pub(super) fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

pub(super) fn shared_conf() -> Result<TaskStoreConf, String> {
    let mut conf = configuration().map_err(error)?;
    conf.idempotency.push(super::TaskIdempotencyConf {
        handler: "topic_work".into(),
        revision: "1".into(),
    });
    conf.lanes.retain(|lane| lane.lane() != DEFAULT_TASK_LANE);
    conf.lanes.push(
        TaskLaneConf::new(DEFAULT_TASK_LANE, 32)
            .idempotency_retention(std::time::Duration::from_secs(300)),
    );
    Ok(conf)
}

pub(super) fn shared(key: &str) -> Result<TaskWrite, String> {
    let mut write = write(DEFAULT_TASK_LANE, 0).map_err(error)?;
    write.record.idempotency_key = Some(key.into());
    write.record.idempotency_fingerprint = Some("matching".into());
    write.record.share_role = crate::tasks::models::ShareRole::Publisher;
    Ok(write)
}

async fn one(store: &impl TaskStore, write: TaskWrite) -> Result<TaskId, String> {
    store
        .store_tasks(vec![write])
        .await
        .map_err(error)?
        .first()
        .ok_or("missing receipt")?
        .queued_id()
        .map_err(error)
}

async fn record(store: &impl TaskStore, id: TaskId) -> Result<TaskRecord, String> {
    store
        .get_task(id)
        .await
        .map_err(error)?
        .ok_or("missing task".into())
}

/// Covers success, failure, cancellation and a mixed late/live/ordinary all group.
pub(super) async fn shared_contract(store: &impl TaskStore) -> Result<(), String> {
    store.initialize().await.map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    for (key, outcome) in [
        (
            "ok",
            Some(TaskOutcome::CompleteWith {
                output: "21".into(),
            }),
        ),
        ("failed", Some(TaskOutcome::fail("failure"))),
        ("cancelled", None),
    ] {
        let winner = one(store, shared(key)?).await?;
        let subscriber = one(store, shared(key)?).await?;
        assert_eq!(
            record(store, subscriber).await?.status,
            TaskStatus::Suspended
        );
        let task = next_page(store, node, 8)
            .await?
            .into_iter()
            .next()
            .ok_or("winner was not claimed")?;
        assert_eq!(task.id, winner);
        let commit = if let Some(outcome) = outcome {
            output(&task, DEFAULT_TASK_LANE, outcome)?
        } else {
            TaskCommit::Cancel { task_id: winner }
        };
        store.commit_tasks(&[commit]).await.map_err(error)?;
        let late = one(store, shared(key)?).await?;
        let expected = record(store, winner).await?;
        for id in [subscriber, late] {
            let actual = record(store, id).await?;
            assert_eq!(actual.status, expected.status);
            assert_eq!(actual.last_result, expected.last_result);
            assert_eq!(actual.attempts, 0);
        }
    }
    mixed_shared_all(store, node).await?;
    same_turn_spawn(store, node).await?;
    late_shared_spawn(store, node).await?;
    paged_join(store, node).await?;
    shared_pages(store, node).await?;
    stopped_drainer(store, node, false).await?;
    stopped_drainer(store, node, true).await
}

/// A spawn joining an already-terminal publisher delivers without a wait or claim.
async fn late_shared_spawn(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    let parent = one(store, write(DEFAULT_TASK_LANE, 1).map_err(error)?).await?;
    let rows = next_page(store, node, 8).await?;
    let flow = rows
        .iter()
        .find(|row| row.id == parent)
        .ok_or("missing late flow")?;
    let child = shared("ok")?;
    let child_id = child.record.id;
    store
        .commit_tasks(&[output(
            flow,
            DEFAULT_TASK_LANE,
            TaskOutcome::Spawn {
                child,
                state: "null".into(),
            },
        )?])
        .await
        .map_err(error)?;
    let actual = record(store, child_id).await?;
    assert_eq!((actual.status, actual.attempts), (TaskStatus::Succeeded, 0));
    let actual = record(store, parent).await?;
    assert_eq!(actual.status, TaskStatus::Pending);
    assert_eq!(actual.resume_input.as_deref(), Some(r#"{"Ok":21}"#));
    Ok(())
}

/// A shared child in the second page decrements its mixed all parent once, even after replay.
async fn paged_join(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    let (parent, winner) = stage_paged_join(store, node).await?;
    let ordinary = next_page(store, node, 8)
        .await?
        .into_iter()
        .next()
        .ok_or("missing ordinary child")?;
    let value = TaskOutcome::CompleteWith {
        output: "91".into(),
    };
    store
        .commit_tasks(&[
            output(&ordinary, DEFAULT_TASK_LANE, TaskOutcome::Complete)?,
            output(&winner, DEFAULT_TASK_LANE, value)?,
        ])
        .await
        .map_err(error)?;
    assert_eq!(record(store, parent).await?.status, TaskStatus::Suspended);
    let drain = next_page(store, node, 8)
        .await?
        .into_iter()
        .next()
        .ok_or("missing drainer")?;
    let commit = output(&drain, DEFAULT_TASK_LANE, TaskOutcome::Complete)?;
    store
        .commit_tasks(std::slice::from_ref(&commit))
        .await
        .map_err(error)?;
    assert_eq!(
        store.commit_tasks(&[commit]).await.map_err(error)?.rejected,
        vec![drain.id]
    );
    let ready = next_page(store, node, 8)
        .await?
        .into_iter()
        .next()
        .ok_or("missing resumed parent")?;
    assert_eq!(ready.id, parent);
    assert_eq!(
        ready.resume_input.as_deref(),
        Some(r#"{"Ok":[{"Ok":null},{"Ok":91}]}"#)
    );
    store
        .commit_tasks(&[output(&ready, DEFAULT_TASK_LANE, TaskOutcome::Complete)?])
        .await
        .map_err(error)?;
    Ok(())
}

/// Fills the inline page and installs a shared child as the overflow member of a mixed join.
async fn stage_paged_join(
    store: &impl TaskStore,
    node: Uuid,
) -> Result<(TaskId, TaskRecord), String> {
    let key = "paged-all";
    let writes = (0..=super::sharing::PAGE_SIZE)
        .map(|_| shared(key))
        .collect::<Result<Vec<_>, _>>()?;
    let publisher = store
        .store_tasks(writes)
        .await
        .map_err(error)?
        .first()
        .and_then(|r| r.id())
        .ok_or("publisher missing")?;
    let parent = one(store, write(DEFAULT_TASK_LANE, 1).map_err(error)?).await?;
    let claims = next_page(store, node, 8).await?;
    let flow = claims
        .iter()
        .find(|row| row.id == parent)
        .ok_or("missing flow")?;
    let winner = claims
        .iter()
        .find(|row| row.id == publisher)
        .ok_or("missing publisher")?;
    let group = TaskOutcome::All {
        state: "42".into(),
        children: vec![write(DEFAULT_TASK_LANE, 0).map_err(error)?, shared(key)?],
    };
    store
        .commit_tasks(&[output(flow, DEFAULT_TASK_LANE, group)?])
        .await
        .map_err(error)?;
    Ok((parent, winner.clone()))
}

/// Each subscriber gets a receipt, and each fenced drainer step completes only one page.
async fn shared_pages(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    let page = super::sharing::PAGE_SIZE;
    let inputs = (0..2 * page + 2)
        .map(|i| {
            let mut write = shared("bulk-cap")?;
            write.record.input = i.to_string();
            write.record.idempotency_fingerprint = Some(i.to_string());
            Ok::<_, String>(write)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let winner = inputs.first().ok_or("missing winner")?.record.id;
    let receipts = store.store_tasks(inputs).await.map_err(error)?;
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.id().is_some())
            .count(),
        2 * page + 2
    );
    let claims = next_page(store, node, page + 2).await?;
    let publisher = claims
        .iter()
        .find(|row| row.id == winner)
        .ok_or("publisher was not claimed")?;
    assert!(
        !claims
            .iter()
            .any(|row| row.share_role == crate::tasks::models::ShareRole::Subscriber)
    );
    store
        .commit_tasks(&[output(publisher, DEFAULT_TASK_LANE, TaskOutcome::Complete)?])
        .await
        .map_err(error)?;
    replace_generation(store, winner).await?;
    replay_pages(store, node, &receipts, page).await?;
    for id in receipts
        .into_iter()
        .filter_map(crate::tasks::TaskReceipt::id)
        .filter(|id| *id != winner)
    {
        let row = record(store, id).await?;
        assert_eq!((row.status, row.attempts), (TaskStatus::Succeeded, 0));
    }
    Ok(())
}

/// Verifies exact pending counts, one drainer identity and rejection of every duplicate output.
async fn replay_pages(
    store: &impl TaskStore,
    node: Uuid,
    receipts: &[TaskReceipt],
    page: usize,
) -> Result<(), String> {
    let mut drainer_id = None;
    for remaining in [page + 1, 1] {
        let mut suspended = 0;
        for id in receipts.iter().filter_map(|receipt| receipt.id()) {
            suspended += usize::from(record(store, id).await?.status == TaskStatus::Suspended);
        }
        assert_eq!(suspended, remaining);
        let tasks = next_page(store, node, 8).await?;
        let drain = tasks
            .iter()
            .find(|r| r.share_role == crate::tasks::models::ShareRole::Drainer)
            .ok_or("missing drainer")?;
        assert_eq!(drain.step_attempts, 1);
        assert_eq!(*drainer_id.get_or_insert(drain.id), drain.id);
        let commit = output(drain, DEFAULT_TASK_LANE, TaskOutcome::Complete)?;
        let ack = store
            .commit_tasks(std::slice::from_ref(&commit))
            .await
            .map_err(error)?;
        if remaining > page {
            assert!(ack.wake_lanes.contains(&DEFAULT_TASK_LANE));
        }
        assert_eq!(
            store.commit_tasks(&[commit]).await.map_err(error)?.rejected,
            vec![drain.id]
        );
    }
    Ok(())
}

/// Releasing a terminal key during an active drain starts an independent publisher generation.
async fn replace_generation(store: &impl TaskStore, winner: TaskId) -> Result<(), String> {
    let late = one(store, shared("bulk-cap")?).await?;
    assert_eq!(
        record(store, late).await?.last_result,
        record(store, winner).await?.last_result
    );
    assert_eq!(
        store.release_idempotency(&[winner]).await.map_err(error)?,
        1
    );
    let fresh = one(store, shared("bulk-cap")?).await?;
    assert_eq!(record(store, fresh).await?.status, TaskStatus::Pending);
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: fresh }])
        .await
        .map_err(error)?;
    Ok(())
}

/// Explicit cancellation or crash exhaustion stops only uncommitted pages, even after node takeover.
async fn stopped_drainer(store: &impl TaskStore, node: Uuid, exhaust: bool) -> Result<(), String> {
    let key = if exhaust {
        "drain-exhaust"
    } else {
        "drain-cancel"
    };
    let page = super::sharing::PAGE_SIZE;
    let writes = (0..page + 2)
        .map(|_| shared(key))
        .collect::<Result<Vec<_>, _>>()?;
    let receipts = store.store_tasks(writes).await.map_err(error)?;
    let winner = receipts
        .first()
        .and_then(|r| r.id())
        .ok_or("missing publisher")?;
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: winner }])
        .await
        .map_err(error)?;
    // Initialization must accept the internal name without registering an application handler.
    store.initialize().await.map_err(error)?;
    let peer = Uuid::now_v7();
    store.register_node(&peer).await.map_err(error)?;
    let drainer = next_page(store, peer, 8)
        .await?
        .into_iter()
        .find(|r| r.share_role == crate::tasks::models::ShareRole::Drainer)
        .ok_or("missing drainer")?;
    let stale = output(&drainer, DEFAULT_TASK_LANE, TaskOutcome::Complete)?;
    stop_page(store, peer, drainer.id, exhaust).await?;
    assert_eq!(
        store.commit_tasks(&[stale]).await.map_err(error)?.rejected,
        vec![drainer.id]
    );
    assert_eq!(record(store, drainer.id).await?.status, TaskStatus::Failed);
    check_stopped_waits(store, node, receipts, winner).await
}

/// A new wait is installed before a publisher in the same flush delivers it.
async fn same_turn_spawn(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    let winner = one(store, shared("same-turn")?).await?;
    let parent = one(store, write(DEFAULT_TASK_LANE, 1).map_err(error)?).await?;
    let rows = next_page(store, node, 8).await?;
    let publisher = rows
        .iter()
        .find(|row| row.id == winner)
        .ok_or("missing winner")?;
    let flow = rows
        .iter()
        .find(|row| row.id == parent)
        .ok_or("missing flow")?;
    let child = shared("same-turn")?;
    let child_id = child.record.id;
    let complete = TaskOutcome::CompleteWith {
        output: "34".into(),
    };
    let spawn = TaskOutcome::Spawn {
        child,
        state: "null".into(),
    };
    store
        .commit_tasks(&[
            output(publisher, DEFAULT_TASK_LANE, complete)?,
            output(flow, DEFAULT_TASK_LANE, spawn)?,
        ])
        .await
        .map_err(error)?;
    let actual = record(store, child_id).await?;
    assert_eq!(actual.status, TaskStatus::Succeeded);
    assert_eq!(actual.attempts, 0);
    let actual = record(store, parent).await?;
    assert_eq!(actual.status, TaskStatus::Pending);
    assert_eq!(actual.resume_input.as_deref(), Some(r#"{"Ok":34}"#));
    Ok(())
}

/// Stops the claimed page using operator cancellation or actual claim exhaustion.
async fn stop_page(
    store: &impl TaskStore,
    peer: Uuid,
    id: TaskId,
    exhaust: bool,
) -> Result<(), String> {
    if exhaust {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(12);
        while record(store, id).await?.status != TaskStatus::Failed {
            if tokio::time::Instant::now() >= deadline {
                return Err("drainer did not exhaust before deadline".into());
            }
            store.return_claims(Some(peer)).await.map_err(error)?;
            store
                .claim_tasks(&peer, &[request(DEFAULT_TASK_LANE, 1)])
                .await
                .map_err(error)?;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    } else {
        store
            .commit_tasks(&[TaskCommit::Cancel { task_id: id }])
            .await
            .map_err(error)?;
    }
    Ok(())
}

/// The first page was atomically committed and exactly one waiter is left behind.
async fn check_stopped_waits(
    store: &impl TaskStore,
    node: Uuid,
    receipts: Vec<TaskReceipt>,
    winner: TaskId,
) -> Result<(), String> {
    let mut waiting = 0;
    for id in receipts
        .into_iter()
        .filter_map(TaskReceipt::id)
        .filter(|id| *id != winner)
    {
        let row = record(store, id).await?;
        if row.status == TaskStatus::Suspended {
            waiting += 1;
        } else {
            assert_eq!(row.status, TaskStatus::Failed);
        }
        assert_eq!(row.attempts, 0);
    }
    assert_eq!(waiting, 1);
    assert!(
        store
            .claim_tasks(&node, &[request(DEFAULT_TASK_LANE, 8)])
            .await
            .map_err(error)?
            .lanes
            .into_iter()
            .all(|l| l.tasks.is_empty())
    );
    Ok(())
}

/// Keeps a terminal subscriber in its ordered slot without prematurely waking its parent.
async fn mixed_shared_all(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    let live = one(store, shared("live")?).await?;
    let parent = one(store, write(DEFAULT_TASK_LANE, 1).map_err(error)?).await?;
    let rows = next_page(store, node, 8).await?;
    let parent_row = rows
        .iter()
        .find(|row| row.id == parent)
        .ok_or("missing parent claim")?;
    let live_row = rows
        .iter()
        .find(|row| row.id == live)
        .ok_or("missing publisher claim")?;
    let plain = write(DEFAULT_TASK_LANE, 2).map_err(error)?;
    let plain_id = plain.record.id;
    let group = TaskOutcome::All {
        state: "42".into(),
        children: vec![shared("ok")?, shared("live")?, plain],
    };
    store
        .commit_tasks(&[output(parent_row, DEFAULT_TASK_LANE, group)?])
        .await
        .map_err(error)?;
    let candidates = next_page(store, node, 8).await?;
    assert_eq!(candidates.len(), 1);
    let plain_row = candidates.first().ok_or("missing ordinary child")?;
    assert_eq!(plain_row.id, plain_id);
    store
        .commit_tasks(&[output(
            plain_row,
            DEFAULT_TASK_LANE,
            TaskOutcome::CompleteWith {
                output: "23".into(),
            },
        )?])
        .await
        .map_err(error)?;
    assert_eq!(record(store, parent).await?.status, TaskStatus::Suspended);
    store
        .commit_tasks(&[output(
            live_row,
            DEFAULT_TASK_LANE,
            TaskOutcome::CompleteWith {
                output: "22".into(),
            },
        )?])
        .await
        .map_err(error)?;
    let resumed = next_page(store, node, 8)
        .await?
        .into_iter()
        .next()
        .ok_or("missing resumed parent")?;
    assert_eq!(resumed.id, parent);
    assert_eq!(
        resumed.resume_input.as_deref(),
        Some(r#"{"Ok":[{"Ok":21},{"Ok":22},{"Ok":23}]}"#)
    );
    Ok(())
}

/// Exact page multiples finish without a spare drainer step, even beyond the IPC batch size.
pub(super) async fn boundary_contract(store: &impl TaskStore) -> Result<(), String> {
    store.initialize().await.map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    for n in [0usize, 1, 31, 32, 33, 64, 127, 128, 129, 512, 1024] {
        let key = format!("boundary-{n}");
        let publisher = one(store, shared(&key)?).await?;
        let subscribers = (0..n)
            .map(|_| shared(&key))
            .collect::<Result<Vec<_>, _>>()?;
        if n > 0 {
            let receipts = store.store_tasks(subscribers).await.map_err(error)?;
            assert!(receipts.iter().all(|r| r.id().is_some()));
        }
        let mut drainer = None;
        let pages = n.div_ceil(super::sharing::PAGE_SIZE).max(1);
        for page in 0..pages {
            let rows = next_page(store, node, 2).await?;
            let [row] = rows.as_slice() else {
                return Err("expected exactly one runnable task".into());
            };
            if page == 0 {
                assert_eq!(row.id, publisher);
            } else {
                assert_eq!(*drainer.get_or_insert(row.id), row.id);
                assert_eq!(row.share_role, crate::tasks::models::ShareRole::Drainer);
                assert_eq!((row.step_attempts, row.attempts as usize), (1, page));
            }
            store
                .commit_tasks(&[output(row, DEFAULT_TASK_LANE, TaskOutcome::Complete)?])
                .await
                .map_err(error)?;
            let waiting = crate::tasks::TaskFilter::new()
                .idempotency_key(&key)
                .status(TaskStatus::Suspended);
            let waiting = store.list_tasks(waiting).await.map_err(error)?;
            assert_eq!(
                waiting.total as usize,
                n.saturating_sub((page + 1) * super::sharing::PAGE_SIZE)
            );
        }
        let poll = store
            .claim_tasks(&node, &[request(DEFAULT_TASK_LANE, 2)])
            .await
            .map_err(error)?;
        assert!(poll.lanes.iter().all(|l| l.tasks.is_empty()));
    }
    Ok(())
}

/// Exercises all overflow boundaries without database-specific plumbing.
#[tokio::test]
async fn memory_shared_page_boundaries() -> Result<(), String> {
    boundary_contract(&super::MemoryTaskStore::new(1024, shared_conf()?)).await
}

/// Exercises the same shared contract without database-specific plumbing.
#[tokio::test]
async fn memory_shared_result_contract() -> Result<(), String> {
    let store = super::MemoryTaskStore::new(1024, shared_conf()?);
    shared_contract(&store).await
}

/// Releases only retained terminal owners, preserving result history and active holds.
pub(super) async fn release_idempotency_contract(store: &impl TaskStore) -> Result<(), String> {
    store.initialize().await.map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    let (publisher, subscriber, failed, running, sleeping) = stage_release(store, node).await?;
    let pending = one(store, ordinary_key("release-pending")?).await?;
    let unkeyed = one(store, write(DEFAULT_TASK_LANE, 0).map_err(error)?).await?;
    let late = one(store, shared("release-publisher")?).await?;
    let before = record(store, publisher).await?;
    assert_eq!(before.status, TaskStatus::Succeeded);
    assert!(before.idempotency_expires_at.is_some());
    assert_eq!(record(store, late).await?.last_result, before.last_result);
    let mut ids = vec![
        publisher, failed, pending, running, sleeping, subscriber, late, unkeyed, publisher,
    ];
    ids.resize_with(crate::tasks::config::MAX_BATCH_SIZE, || {
        TaskId::new(Uuid::now_v7())
    });
    assert_eq!(store.release_idempotency(&[]).await.map_err(error)?, 0);
    assert_eq!(store.release_idempotency(&ids).await.map_err(error)?, 2);
    unchanged_release_history(store, &before, [subscriber, late]).await?;
    assert_eq!(store.release_idempotency(&ids).await.map_err(error)?, 0);
    let fresh = one(store, shared("release-publisher")?).await?;
    assert_eq!(record(store, fresh).await?.status, TaskStatus::Pending);
    assert_eq!(
        store
            .release_idempotency(&[publisher])
            .await
            .map_err(error)?,
        0
    );
    for key in [
        "release-publisher",
        "release-pending",
        "release-running",
        "release-sleeping",
    ] {
        assert_eq!(
            store
                .store_tasks(vec![ordinary_key(key)?])
                .await
                .map_err(error)?,
            [TaskReceipt::KeyHeld],
        );
    }
    release_races(store, node).await
}

/// Releasing the hold leaves the publisher's history and subscriber results intact.
async fn unchanged_release_history(
    store: &impl TaskStore,
    before: &TaskRecord,
    subscribers: [TaskId; 2],
) -> Result<(), String> {
    let after = record(store, before.id).await?;
    assert_eq!(after.status, before.status);
    assert_eq!(after.last_result, before.last_result);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.idempotency_key, before.idempotency_key);
    assert_eq!(after.idempotency_expires_at, before.idempotency_expires_at);
    for id in subscribers {
        assert_eq!(record(store, id).await?.last_result, before.last_result);
    }
    Ok(())
}

fn ordinary_key(key: &str) -> Result<TaskWrite, String> {
    let mut write = shared(key)?;
    write.record.share_role = crate::tasks::models::ShareRole::Unshared;
    Ok(write)
}

/// Creates successful and cancelled terminal owners alongside claimed and suspended tasks.
async fn stage_release(
    store: &impl TaskStore,
    node: Uuid,
) -> Result<(TaskId, TaskId, TaskId, TaskId, TaskId), String> {
    let publisher = one(store, shared("release-publisher")?).await?;
    let subscriber = one(store, shared("release-publisher")?).await?;
    let failed = one(store, ordinary_key("release-failed")?).await?;
    let running = one(store, ordinary_key("release-running")?).await?;
    let sleeping = one(store, ordinary_key("release-sleeping")?).await?;
    let rows = next_page(store, node, 8).await?;
    let claimed = |id| {
        rows.iter()
            .find(|row| row.id == id)
            .ok_or("missing claimed task")
    };
    let commits = [
        output(
            claimed(publisher)?,
            DEFAULT_TASK_LANE,
            TaskOutcome::CompleteWith {
                output: "42".into(),
            },
        )?,
        TaskCommit::Cancel { task_id: failed },
        output(
            claimed(sleeping)?,
            DEFAULT_TASK_LANE,
            wait_outcome("release-suspended"),
        )?,
    ];
    assert!(
        store
            .commit_tasks(&commits)
            .await
            .map_err(error)?
            .rejected
            .is_empty()
    );
    assert_eq!(record(store, failed).await?.status, TaskStatus::Failed);
    assert_eq!(record(store, running).await?.status, TaskStatus::Running);
    assert_eq!(record(store, sleeping).await?.status, TaskStatus::Suspended);
    Ok((publisher, subscriber, failed, running, sleeping))
}

/// Concurrent resubmission never lets a retried old ID free a replacement owner.
async fn release_races(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    let owner = race_completion(store, node).await?;
    let ids = [owner];
    let writes = vec![ordinary_key("release-race")?];
    let (released, submitted) =
        tokio::join!(store.release_idempotency(&ids), store.store_tasks(writes));
    assert_eq!(released.map_err(error)?, 1);
    let receipts = submitted.map_err(error)?;
    if receipts == [TaskReceipt::KeyHeld] {
        one(store, ordinary_key("release-race")?).await?;
    } else {
        assert!(
            receipts
                .first()
                .is_some_and(|receipt| receipt.id().is_some())
        );
    }
    assert_eq!(store.release_idempotency(&ids).await.map_err(error)?, 0);
    assert_eq!(
        store
            .store_tasks(vec![ordinary_key("release-race")?])
            .await
            .map_err(error)?,
        [TaskReceipt::KeyHeld]
    );
    race_shared_join(store, node).await
}

/// Races release against completion, leaving a retained terminal owner for the next test.
async fn race_completion(store: &impl TaskStore, node: Uuid) -> Result<TaskId, String> {
    let old = one(store, ordinary_key("release-race")?).await?;
    let rows = next_page(store, node, 8).await?;
    let row = rows
        .iter()
        .find(|row| row.id == old)
        .ok_or("racing task was not claimed")?;
    let ids = [old];
    let commits = [output(row, DEFAULT_TASK_LANE, TaskOutcome::Complete)?];
    let (released, flushed) = tokio::join!(
        store.release_idempotency(&ids),
        store.commit_tasks(&commits)
    );
    assert!(flushed.map_err(error)?.rejected.is_empty());
    let released = released.map_err(error)?;
    // If the release wins before the terminal commit, the hold is still present.
    if released == 1 {
        let replacement = one(store, ordinary_key("release-race")?).await?;
        let rows = next_page(store, node, 8).await?;
        let row = rows
            .iter()
            .find(|row| row.id == replacement)
            .ok_or("replacement was not claimed")?;
        assert!(
            store
                .commit_tasks(&[output(row, DEFAULT_TASK_LANE, TaskOutcome::Complete)?])
                .await
                .map_err(error)?
                .rejected
                .is_empty()
        );
        Ok(replacement)
    } else {
        assert_eq!(released, 0);
        Ok(old)
    }
}

/// A simultaneous shared submission can see the old result or win a fresh hold.
async fn race_shared_join(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    let key = "release-join-race";
    let old = one(store, shared(key)?).await?;
    let rows = next_page(store, node, 8).await?;
    let row = rows
        .iter()
        .find(|row| row.id == old)
        .ok_or("publisher was not claimed")?;
    store
        .commit_tasks(&[output(row, DEFAULT_TASK_LANE, TaskOutcome::Complete)?])
        .await
        .map_err(error)?;
    let ids = [old];
    let (released, joined) = tokio::join!(
        store.release_idempotency(&ids),
        store.store_tasks(vec![shared(key)?])
    );
    assert_eq!(released.map_err(error)?, 1);
    if !joined_fresh(store, old, joined).await? {
        let fresh = one(store, shared(key)?).await?;
        assert_eq!(record(store, fresh).await?.status, TaskStatus::Pending);
    }
    assert_eq!(store.release_idempotency(&ids).await.map_err(error)?, 0);
    assert_eq!(
        store
            .store_tasks(vec![ordinary_key(key)?])
            .await
            .map_err(error)?,
        [TaskReceipt::KeyHeld]
    );
    Ok(())
}

/// Recognizes a new publisher; a late reply or known transient conflict can be retried.
async fn joined_fresh(
    store: &impl TaskStore,
    old: TaskId,
    result: Result<Vec<TaskReceipt>, TaskRuntimeError>,
) -> Result<bool, String> {
    match result {
        Ok(receipts) if receipts == [TaskReceipt::KeyHeld] => Ok(false),
        Ok(receipts) => {
            let id = receipts
                .first()
                .and_then(|r| r.id())
                .ok_or("missing shared receipt")?;
            let joined = record(store, id).await?;
            if joined.status == TaskStatus::Succeeded {
                assert_eq!(joined.last_result, record(store, old).await?.last_result);
                Ok(false)
            } else {
                assert_eq!(joined.status, TaskStatus::Pending);
                Ok(true)
            }
        }
        Err(TaskRuntimeError::StoreError(db)) => {
            let transient = matches!(
                db.code(),
                "serialization_failure" | "deadlock" | "database_busy" | "database_locked"
            );
            #[cfg(any(feature = "mysql", feature = "sqlite"))]
            let transient = transient || store.retryable_conflict(&db);
            if transient { Ok(false) } else { Err(error(db)) }
        }
        Err(other) => Err(error(other)),
    }
}

/// Release behavior and the public input cap match the SQL implementations.
#[tokio::test]
async fn memory_release_idempotency() -> Result<(), String> {
    let store = std::sync::Arc::new(super::MemoryTaskStore::new(1024, shared_conf()?));
    release_idempotency_contract(&store).await?;
    let registry = std::sync::Arc::new(TaskRegistry::new());
    let client = registry.client(store, Vec::new());
    let ids = vec![TaskId::new(Uuid::now_v7()); crate::tasks::config::MAX_BATCH_SIZE + 1];
    assert!(matches!(
        client.release_idempotency(&ids).await,
        Err(TaskRuntimeError::InvalidOptions(_))
    ));
    assert_eq!(client.release_idempotency(&[]).await.map_err(error)?, 0);
    Ok(())
}

/// Installs waits through the real store claim and fenced commit paths.
pub(super) async fn prepare(
    store: &impl TaskStore,
    node: Uuid,
    count: usize,
    outcome: TaskOutcome,
) -> Result<Vec<TaskId>, String> {
    let ids = store
        .store_tasks(
            (0..count)
                .map(|index| write(DEFAULT_TASK_LANE, index))
                .collect::<Result<Vec<_>, _>>()
                .map_err(error)?,
        )
        .await
        .map_err(error)?
        .into_iter()
        .map(|r| r.queued_id())
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    let mut remaining = count;
    while remaining > 0 {
        let rows = next_page(store, node, remaining).await?;
        let commits = rows
            .iter()
            .map(|row| output(row, DEFAULT_TASK_LANE, outcome.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        store.commit_tasks(&commits).await.map_err(error)?;
        remaining = remaining
            .checked_sub(rows.len())
            .ok_or("unexpected fixture task")?;
    }
    Ok(ids)
}

/// Waits for ordinary polling eligibility during fixture setup, outside timed publication.
async fn next_page(
    store: &impl TaskStore,
    node: Uuid,
    limit: usize,
) -> Result<Vec<TaskRecord>, String> {
    for _ in 0..100 {
        let claims = store
            .claim_tasks(&node, &[request(DEFAULT_TASK_LANE, limit)])
            .await
            .map_err(error)?;
        let rows = claims
            .lanes
            .into_iter()
            .flat_map(|lane| lane.tasks)
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            return Ok(rows);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Err("fixture tasks did not become ready".into())
}

pub(super) fn wait_outcome(topic: &str) -> TaskOutcome {
    TaskOutcome::Wait {
        topic: topic.into(),
        state: "7".into(),
    }
}

/// The same contract drives memory and each SQL backend.
pub(super) async fn contract(store: &impl TaskStore) -> Result<(), String> {
    store.initialize().await.map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?.resumed,
        0
    );
    assert!(store.publish(" \u{2003}", INPUT).await.is_err());
    let ids = prepare(store, node, PUBLISH_CAP + 1, wait_outcome("Topic")).await?;
    let lower = prepare(store, node, 1, wait_outcome("topic")).await?;
    check_caps(store, node).await?;
    let direct = prepare(store, node, 1, TaskOutcome::Suspend { state: "8".into() }).await?;
    let id = *ids.first().ok_or("no wait id")?;
    assert!(!store.resume(id, INPUT.into()).await.map_err(error)?);
    let too_large = format!("{{\"Ok\":\"{}\"}}", "x".repeat(32768));
    assert!(matches!(
        store.publish("Topic", &too_large).await,
        Err(TaskRuntimeError::ResultTooLarge { .. })
    ));
    assert_eq!(
        store.publish("Topic ", INPUT).await.map_err(error)?.resumed,
        0
    );
    check_over_cap(store, &ids).await?;
    check_delivery(
        store,
        &ids,
        lower.first().copied().ok_or("no lowercase id")?,
    )
    .await?;
    assert!(
        store
            .resume(*direct.first().ok_or("no direct id")?, INPUT.into())
            .await
            .map_err(error)?
    );
    Ok(())
}

/// The extra waiter is only a hint and remains suspended until the caller publishes again.
async fn check_over_cap(store: &impl TaskStore, ids: &[TaskId]) -> Result<(), String> {
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?,
        PublishResult {
            resumed: PUBLISH_CAP as u64,
            more: true
        }
    );
    let last = *ids.iter().max().ok_or("no extra waiter id")?;
    let task = store
        .get_task(last)
        .await
        .map_err(error)?
        .ok_or("missing extra waiter")?;
    assert_eq!(task.status, TaskStatus::Suspended);
    assert!(task.resume_input.is_none());
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?,
        PublishResult {
            resumed: 1,
            more: false
        }
    );
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?.resumed,
        0
    );
    Ok(())
}

/// Checks that an exactly full result does not advertise a missing sentinel.
async fn check_caps(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    for count in [PUBLISH_CAP - 1, PUBLISH_CAP] {
        let topic = format!("cap:{count}");
        let ids = prepare(store, node, count, wait_outcome(&topic)).await?;
        let result = store.publish(&topic, INPUT).await.map_err(error)?;
        assert_eq!(
            result,
            PublishResult {
                resumed: count as u64,
                more: false
            }
        );
        for batch in ids.chunks(PUBLISH_CAP) {
            let cancel = batch
                .iter()
                .map(|&task_id| TaskCommit::Cancel { task_id })
                .collect::<Vec<_>>();
            store.commit_tasks(&cancel).await.map_err(error)?;
        }
    }
    Ok(())
}

/// Checks that delivery preserves checkpoints and does not affect a different topic.
async fn check_delivery(
    store: &impl TaskStore,
    ids: &[TaskId],
    unrelated: TaskId,
) -> Result<(), String> {
    for &id in ids {
        let row = store
            .get_task(id)
            .await
            .map_err(error)?
            .ok_or("missing resumed task")?;
        assert_eq!(row.status, TaskStatus::Pending);
        assert_eq!(row.resume_input.as_deref(), Some(INPUT));
        assert_eq!(row.state.as_deref(), Some("7"));
        assert_eq!(row.attempts, 1);
        assert_eq!(row.step_attempts, 0);
    }
    assert_eq!(
        store
            .get_task(unrelated)
            .await
            .map_err(error)?
            .ok_or("missing unrelated task")?
            .status,
        TaskStatus::Suspended
    );
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: unrelated }])
        .await
        .map_err(error)?;
    assert_eq!(
        store.publish("topic", INPUT).await.map_err(error)?.resumed,
        0
    );
    Ok(())
}

/// Memory stores use the same topic, cancellation, and bounded resume contract.
#[tokio::test]
async fn memory_publication() -> Result<(), String> {
    let store = super::MemoryTaskStore::new(1024, configuration().map_err(error)?);
    contract(&store).await
}
