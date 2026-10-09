use super::*;
use crate::tasks::store::TestError;

#[path = "commits.rs"]
mod commits;

/// Retirement changes only ownership; normal claims replay continuation and fence old output.
#[tokio::test]
async fn retire_reclaim_complete() -> Result<(), TestError> {
    let (store, old, new, original) = abandoned().await?;
    assert!(store.heartbeat(&new, Duration::from_secs(72)).await?);
    let retired = store.get_task(original.id).await?.ok_or_else(missing)?;
    assert_eq!(retired.node_id, None);
    assert_eq!(retired.status, TaskStatus::Running);
    assert_eq!(retired.updated_at, original.updated_at);
    assert!(!store.heartbeat(&old, Duration::from_secs(72)).await?);
    assert!(store.claim_tasks(&old, &[claim()]).await.is_err());
    let poll = store.claim_tasks(&new, &[claim()]).await?;
    let lane = poll.lanes.first().ok_or_else(missing)?;
    assert_eq!(lane.reclaimed, 1);
    let task = lane.tasks.first().ok_or_else(missing)?;
    assert_eq!((task.attempts, task.step_attempts), (18, 2));
    assert_eq!(task.state, original.state);
    assert_eq!(task.resume_input, original.resume_input);
    let stale = store.commit_tasks(&[commit(&original)?]).await?;
    assert_eq!(stale.rejected, vec![original.id]);
    store.commit_tasks(&[commit(task)?]).await?;
    let done = store.get_task(original.id).await?.ok_or_else(missing)?;
    assert_eq!(done.status, TaskStatus::Succeeded);
    assert_eq!(done.node_id, None);
    Ok(())
}

/// Creates a due running continuation on a stale node, with a live replacement.
async fn abandoned()
-> Result<(MemoryTaskStore, uuid::Uuid, uuid::Uuid, TaskRecord), TaskRuntimeError> {
    let conf = crate::tasks::TaskRegistry::new()
        .with_config(crate::tasks::TaskConf::default())?
        .store_conf()?;
    let store = MemoryTaskStore::with_lease_duration(32, Duration::from_secs(300), conf);
    let old = uuid::Uuid::now_v7();
    let new = uuid::Uuid::now_v7();
    store.register_node(&old).await?;
    store.register_node(&new).await?;
    let mut task = record(old);
    task.ready_at = None;
    task.step_attempts = 1;
    {
        let mut state = store.state.lock().await;
        state.nodes.get_mut(&old).ok_or_else(missing)?.1 -= chrono::Duration::seconds(73);
        state.tasks.push(task.clone());
    }
    Ok((store, old, new, task))
}

fn claim() -> LaneClaim {
    LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit: 32,
        owner: None,
    }
}

/// Only strictly older peers are retired; equality, fresh peers and the caller survive.
#[test]
fn retirement_cutoff() {
    let now = chrono::Utc::now();
    let caller = uuid::Uuid::now_v7();
    let expired = uuid::Uuid::now_v7();
    let equal = uuid::Uuid::now_v7();
    let fresh = uuid::Uuid::now_v7();
    let before = now - chrono::Duration::microseconds(1);
    let mut state = MemoryState::default();
    state.nodes.extend([
        (caller, (before, before)),
        (expired, (before, before)),
        (equal, (now, now)),
        (fresh, (now, now + chrono::Duration::microseconds(1))),
    ]);
    retire_nodes(&mut state, &caller, now);
    assert!(!state.nodes.contains_key(&expired));
    assert!(state.nodes.contains_key(&caller));
    assert!(state.nodes.contains_key(&equal));
    assert!(state.nodes.contains_key(&fresh));
    retire_nodes(&mut state, &caller, now);
    assert_eq!(state.nodes.len(), 3);
}

fn commit(task: &TaskRecord) -> Result<TaskCommit, TaskRuntimeError> {
    Ok(TaskCommit::Outcome {
        node_id: task.node_id.ok_or_else(missing)?,
        task_id: task.id,
        attempts: task.attempts,
        lane: crate::tasks::DEFAULT_TASK_LANE,
        owner_token: None,
        outcome: TaskOutcome::Complete,
    })
}

/// A missing caller cannot retire peers; refreshed peers survive repeated heartbeats.
#[tokio::test]
async fn retirement_authority() -> Result<(), TestError> {
    let (store, old, new, task) = abandoned().await?;
    let missing_node = uuid::Uuid::now_v7();
    assert!(
        !store
            .heartbeat(&missing_node, Duration::from_secs(72))
            .await?
    );
    assert_eq!(
        store.get_task(task.id).await?.ok_or_else(missing)?.node_id,
        Some(old)
    );
    assert!(store.heartbeat(&old, Duration::from_secs(72)).await?);
    for _ in 0..2 {
        assert!(store.heartbeat(&new, Duration::from_secs(72)).await?);
    }
    assert!(store.state.lock().await.nodes.contains_key(&old));
    assert_eq!(
        store.get_task(task.id).await?.ok_or_else(missing)?.node_id,
        Some(old)
    );
    Ok(())
}

/// Retirement preserves future readiness and the normal lane wake hint without charging attempts.
#[tokio::test]
async fn retirement_preserves_readiness() -> Result<(), TestError> {
    let (store, _, new, original) = abandoned().await?;
    let future = chrono::Utc::now() + chrono::Duration::hours(1);
    store
        .state
        .lock()
        .await
        .tasks
        .first_mut()
        .ok_or_else(missing)?
        .ready_at = Some(future);
    store.heartbeat(&new, Duration::from_secs(72)).await?;
    let poll = store.claim_tasks(&new, &[claim()]).await?;
    let lane = poll.lanes.first().ok_or_else(missing)?;
    assert!(lane.tasks.is_empty());
    assert!(
        lane.next_wake_in
            .is_some_and(|delay| delay > Duration::from_secs(3500))
    );
    let row = store.get_task(original.id).await?.ok_or_else(missing)?;
    assert_eq!(row.ready_at, Some(future));
    assert_eq!(row.attempts, original.attempts);
    assert_eq!(row.status, TaskStatus::Running);
    Ok(())
}

/// Cancelled and exhausted abandoned children fail normally, waking parents only for later claims.
#[tokio::test]
async fn recovery_delivers_failure() -> Result<(), TestError> {
    for cancelled in [false, true] {
        let (store, _, new, original) = abandoned().await?;
        let mut parent = record(new);
        parent.status = TaskStatus::Suspended;
        parent.node_id = None;
        parent.step_attempts = 0;
        parent.resume_input = None;
        {
            let mut state = store.state.lock().await;
            let child = state.tasks.first_mut().ok_or_else(missing)?;
            child.parent_id = Some(parent.id);
            child.cancelled = cancelled;
            child.step_attempts = 5;
            state.tasks.push(parent.clone());
        }
        store.heartbeat(&new, Duration::from_secs(72)).await?;
        let poll = store.claim_tasks(&new, &[claim()]).await?;
        assert!(poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
        let child = store.get_task(original.id).await?.ok_or_else(missing)?;
        let resumed = store.get_task(parent.id).await?.ok_or_else(missing)?;
        assert_eq!(child.status, TaskStatus::Failed);
        assert_eq!(child.attempts, original.attempts);
        assert_eq!(resumed.status, TaskStatus::Pending);
        assert_eq!(resumed.state, parent.state);
        assert_eq!(resumed.resume_input, child.last_result);
        assert!(
            child
                .last_result
                .as_deref()
                .is_some_and(|value| value.contains(if cancelled {
                    "Task cancelled"
                } else {
                    "Maximum task attempts"
                }))
        );
        let next = store.claim_tasks(&new, &[claim()]).await?;
        assert_eq!(next.lanes.first().ok_or_else(missing)?.tasks.len(), 1);
    }
    Ok(())
}

/// First claims persist ordered join input; recovery reuses it even without retained child bytes.
#[tokio::test]
async fn reclaimed_join_reuses_input() -> Result<(), TestError> {
    let (store, old, new, original) = abandoned().await?;
    let mut child = record(new);
    child.node_id = None;
    child.status = TaskStatus::Succeeded;
    child.last_result = Some("{\"Ok\":42}".into());
    {
        let mut state = store.state.lock().await;
        let parent = state.tasks.first_mut().ok_or_else(missing)?;
        parent.node_id = None;
        parent.status = TaskStatus::Pending;
        parent.resume_input = None;
        state.waits.insert(
            original.id,
            workflow::TaskWait {
                children: vec![child.id],
                remaining_completions: 0,
            },
        );
        state.tasks.push(child.clone());
    }
    store.claim_tasks(&old, &[claim()]).await?;
    let input = store
        .get_task(original.id)
        .await?
        .ok_or_else(missing)?
        .resume_input;
    assert_eq!(input.as_deref(), Some("{\"Ok\":[{\"Ok\":42}]}"));
    {
        let mut state = store.state.lock().await;
        assert!(!state.waits.contains_key(&original.id));
        state.tasks.retain(|row| row.id != child.id);
    }
    store.heartbeat(&new, Duration::from_secs(72)).await?;
    let poll = store.claim_tasks(&new, &[claim()]).await?;
    assert_eq!(
        poll.lanes
            .first()
            .ok_or_else(missing)?
            .tasks
            .first()
            .ok_or_else(missing)?
            .resume_input,
        input
    );
    Ok(())
}

/// Registration is idempotent, missing heartbeats do not insert, and only the target node changes.
#[tokio::test]
async fn node_contract() -> Result<(), TestError> {
    let store = MemoryTaskStore::with_lease_duration(
        32,
        Duration::from_secs(300),
        crate::tasks::TaskRegistry::new()
            .with_config(crate::tasks::TaskConf::default())?
            .store_conf()?,
    );
    let node = uuid::Uuid::now_v7();
    let other = uuid::Uuid::now_v7();
    assert!(!store.heartbeat(&node, Duration::from_secs(72)).await?);
    assert!(store.state.lock().await.nodes.is_empty());
    store.register_node(&node).await?;
    store.register_node(&other).await?;
    let before = store.state.lock().await.nodes.clone();
    store.register_node(&node).await?;
    assert_eq!(store.state.lock().await.nodes, before);
    assert!(store.heartbeat(&node, Duration::from_secs(72)).await?);
    let after = store.state.lock().await.nodes.clone();
    assert_eq!(after.get(&other), before.get(&other));
    assert_eq!(
        after.get(&node).map(|x| x.0),
        before.get(&node).map(|x| x.0)
    );
    assert!(after.get(&node).map(|x| x.1) >= before.get(&node).map(|x| x.1));
    assert!(store.state.lock().await.tasks.is_empty());
    Ok(())
}

/// Resets only matching running rows and preserves continuation, join, and retry state.
#[tokio::test]
async fn claim_return_contract() -> Result<(), TestError> {
    let conf = crate::tasks::TaskRegistry::new()
        .with_config(crate::tasks::TaskConf::default())?
        .store_conf()?;
    let store = MemoryTaskStore::with_lease_duration(32, Duration::from_secs(300), conf);
    let node = uuid::Uuid::now_v7();
    let mut original = record(node);
    original.cancelled = true;
    let id = original.id;
    let mut other = record(uuid::Uuid::now_v7());
    other.node_id = None;
    let mut pending = record(node);
    pending.status = TaskStatus::Pending;
    let untouched = pending.updated_at;
    {
        let mut state = store.state.lock().await;
        state.tasks.extend([original.clone(), other, pending]);
        state.waits.insert(
            id,
            workflow::TaskWait {
                children: vec![id],
                remaining_completions: 0,
            },
        );
    }
    assert_eq!(store.return_claims(Some(node)).await?, 2);
    assert_eq!(store.return_claims(Some(node)).await?, 0);
    let row = store.get_task(id).await?.ok_or_else(missing)?;
    assert_preserved(&row, &original);
    assert_eq!(store.return_claims(None).await?, 1);
    let state = store.state.lock().await;
    assert_eq!(state.waits.get(&id).ok_or_else(missing)?.children, vec![id]);
    assert_eq!(
        state
            .waits
            .get(&id)
            .ok_or_else(missing)?
            .remaining_completions,
        0
    );
    assert!(state.tasks.last().ok_or_else(missing)?.updated_at >= untouched);
    assert_eq!(state.tasks.last().ok_or_else(missing)?.node_id, None);
    assert!(state.nodes.is_empty());
    Ok(())
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("missing lifecycle fixture".into())
}

/// Creates a claimed continuation with immutable diagnostic and retry evidence.
fn record(node: uuid::Uuid) -> TaskRecord {
    let now = chrono::Utc::now();
    TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind: crate::tasks::TaskKind::Work,
        name: "fixture".into(),
        input: "null".into(),
        state: Some("{\"step\":1}".into()),
        resume_input: Some("{\"Ok\":42}".into()),
        status: TaskStatus::Running,
        cancelled: false,
        attempts: 17,
        step_attempts: 3,
        lane: "default".into(),
        last_result: Some("{\"Err\":{\"task_id\":null,\"message\":\"retry\"}}".into()),
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        node_id: Some(node),
        ready_at: Some(now + chrono::Duration::hours(1)),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

/// Checks that resetting ownership never erases continuation or attempt evidence.
fn assert_preserved(row: &TaskRecord, original: &TaskRecord) {
    assert_eq!(row.status, TaskStatus::Pending);
    assert!(
        row.ready_at
            .is_some_and(|at| at < original.ready_at.unwrap_or(at))
    );
    assert_eq!(row.state, original.state);
    assert_eq!(row.resume_input, original.resume_input);
    assert_eq!(row.last_result, original.last_result);
    assert_eq!((row.attempts, row.step_attempts), (17, 3));
    assert!(row.cancelled);
}
