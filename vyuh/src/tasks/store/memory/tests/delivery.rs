use super::*;
use crate::tasks::store::TestError;

/// Immediate children respect destination opt-out, lane locks and durable Work rate gates.
#[tokio::test]
async fn child_admission_gates() -> Result<(), TestError> {
    let destination = TaskLane::new("destination");
    for mode in 0..4 {
        let mut configuration = conf();
        let lane = TaskLaneConf::new(destination, 8);
        configuration.lanes.push(match mode {
            0 => lane,
            1 => lane.sticky(false),
            2 => lane.lock(crate::tasks::TaskLaneLock::new(1)),
            _ => lane.global_rate_limit(crate::tasks::TaskRate::new(1, Duration::from_secs(60))),
        });
        let store = MemoryTaskStore::new(32, configuration);
        store.initialize().await?;
        let node = uuid::Uuid::now_v7();
        store.register_node(&node).await?;
        let (_, mut parents) = store
            .store_admitted(vec![write(flow_record())], &node, 1)
            .await?;
        let parent = parents.pop().ok_or_else(missing)?;
        let mut child = record();
        child.lane = destination.to_string();
        let id = child.id;
        let result = store
            .commit_admitted(
                &[outcome(
                    &parent,
                    TaskOutcome::Spawn {
                        state: "null".into(),
                        child: write(child),
                    },
                )?],
                &node,
                1,
                &[],
            )
            .await?;
        let saved = store.get_task(id).await?.ok_or_else(missing)?;
        assert_eq!(saved.node_id, (mode == 0).then_some(node));
        assert_eq!(
            saved.status,
            if mode == 0 {
                TaskStatus::Running
            } else {
                TaskStatus::Pending
            }
        );
        assert_eq!(result.tasks.iter().any(|task| task.id == id), mode == 0);
    }
    Ok(())
}
/// Immediate bulk insertion owns only reserved slots; delayed and excess rows remain pending.
#[tokio::test]
async fn bounded_submission() -> Result<(), TestError> {
    let store = MemoryTaskStore::new(32, conf());
    store.initialize().await?;
    let node = uuid::Uuid::now_v7();
    store.register_node(&node).await?;
    let immediate = record();
    let other = record();
    let delayed = record();
    let mut future = write(delayed.clone());
    future.initial_delay = Some(Duration::from_secs(60));
    let (receipts, rows) = store
        .store_admitted(
            vec![future, write(immediate.clone()), write(other.clone())],
            &node,
            1,
        )
        .await?;
    assert_eq!(receipts.len(), 3);
    assert_eq!(rows.len(), 1);
    let running = rows.first().ok_or_else(missing)?;
    assert_eq!(running.id, immediate.id);
    assert_eq!(
        (running.status, running.node_id, running.attempts),
        (TaskStatus::Running, Some(node), 1)
    );
    for id in [other.id, delayed.id] {
        let stored = store.get_task(id).await?.ok_or_else(missing)?;
        assert_eq!(
            (stored.status, stored.node_id, stored.attempts),
            (TaskStatus::Pending, None, 0)
        );
    }
    Ok(())
}

/// Overflow children and cache misses are stealable; a validated scalar cache avoids another claim.
#[tokio::test]
async fn sticky_scalar_delivery() -> Result<(), TestError> {
    let store = MemoryTaskStore::new(32, conf());
    store.initialize().await?;
    let node = uuid::Uuid::now_v7();
    let peer = uuid::Uuid::now_v7();
    store.register_node(&node).await?;
    store.register_node(&peer).await?;
    let (parent, cached) = suspended_parent(&store, node).await?;
    let checkpoint = store.get_task(parent.id).await?.ok_or_else(missing)?;
    assert_eq!(
        (checkpoint.status, checkpoint.node_id),
        (TaskStatus::Suspended, None)
    );
    let child = claimed(&store, peer).await?;
    assert_eq!(child.parent_id, Some(parent.id));
    let fallback = store
        .commit_admitted(
            &[outcome(
                &child,
                TaskOutcome::CompleteWith { output: "7".into() },
            )?],
            &node,
            1,
            &[Arc::new(parent.clone())],
        )
        .await?;
    assert!(fallback.tasks.is_empty());
    let resumed = claimed(&store, peer).await?;
    assert_eq!(resumed.resume_input::<i32>()?, Some(Ok(7)));
    cached_followup(&store, node, resumed, cached).await
}

/// Commits a scalar child wait with a current suspended snapshot and obsolete Running base.
async fn suspended_parent(
    store: &impl TaskStore,
    node: uuid::Uuid,
) -> Result<(TaskRecord, Vec<Arc<TaskRecord>>), TestError> {
    let (_, mut parents) = store
        .store_admitted(vec![write(flow_record())], &node, 1)
        .await?;
    let parent = parents.pop().ok_or_else(missing)?;
    let result = store
        .commit_admitted(
            &[outcome(
                &parent,
                TaskOutcome::Spawn {
                    state: "42".into(),
                    child: write(record()),
                },
            )?],
            &node,
            0,
            &[],
        )
        .await?;
    Ok((parent, result.tasks))
}

/// A committed checkpoint admits its next scalar delivery; obsolete acknowledgements do nothing.
async fn cached_followup(
    store: &impl TaskStore,
    node: uuid::Uuid,
    resumed: TaskRecord,
    cached: Vec<Arc<TaskRecord>>,
) -> Result<(), TestError> {
    let parent_id = resumed.id;
    store
        .commit_tasks(&[outcome(
            &resumed,
            TaskOutcome::Spawn {
                state: "43".into(),
                child: write(record()),
            },
        )?])
        .await?;
    let checkpoint = Arc::new(store.get_task(parent_id).await?.ok_or_else(missing)?);
    let child = claimed(store, node).await?;
    let complete = outcome(&child, TaskOutcome::Complete)?;
    let result = store
        .commit_admitted(&[complete.clone()], &node, 1, &[checkpoint])
        .await?;
    let resumed = result
        .tasks
        .iter()
        .find(|task| task.id == parent_id)
        .ok_or_else(missing)?;
    assert_eq!(resumed.status, TaskStatus::Running);
    assert_eq!(resumed.resume_input::<()>()?, Some(Ok(())));
    assert_eq!(resumed.state.as_deref(), Some("43"));
    assert!(
        store
            .commit_admitted(&[complete], &node, 1, &cached)
            .await?
            .tasks
            .is_empty()
    );
    Ok(())
}

/// Missing cache and an explicit opt-out both leave sleeping Flows available to peers.
#[tokio::test]
async fn sticky_recovery_and_optout() -> Result<(), TestError> {
    for sticky in [true, false] {
        let mut configuration = conf();
        configuration.lanes = vec![TaskLaneConf::new(DEFAULT_TASK_LANE, 8).sticky(sticky)];
        let store = MemoryTaskStore::new(32, configuration);
        store.initialize().await?;
        let node = uuid::Uuid::now_v7();
        let peer = uuid::Uuid::now_v7();
        store.register_node(&node).await?;
        store.register_node(&peer).await?;
        let (_, rows) = store
            .store_admitted(vec![write(flow_record())], &node, 1)
            .await?;
        let parent = rows.first().ok_or_else(missing)?;
        store
            .commit_tasks(&[outcome(
                parent,
                TaskOutcome::Sleep {
                    state: "null".into(),
                    delay: Duration::ZERO,
                },
            )?])
            .await?;
        let recovered = claimed(&store, peer).await?;
        assert_eq!(recovered.id, parent.id);
        assert_eq!(recovered.attempts, 2);
        assert_eq!(recovered.step_attempts, 1);
    }
    Ok(())
}

/// Flow claims bypass an exhausted Work rate bucket even behind more than one claim page of Work.
#[tokio::test]
async fn flow_rate_exemption() -> Result<(), TestError> {
    let mut configuration = conf();
    configuration.lanes = vec![
        TaskLaneConf::new(DEFAULT_TASK_LANE, 8)
            .global_rate_limit(crate::tasks::TaskRate::new(1, Duration::from_secs(3600))),
    ];
    let store = MemoryTaskStore::new(1, configuration);
    store.initialize().await?;
    let node = uuid::Uuid::now_v7();
    store.register_node(&node).await?;
    store.store_tasks(vec![write(record())]).await?;
    claimed(&store, node).await?;
    let flow = flow_record();
    store
        .store_tasks(vec![write(record()), write(record()), write(flow.clone())])
        .await?;
    assert_eq!(claimed(&store, node).await?.id, flow.id);
    let mut delayed = write(flow_record());
    delayed.initial_delay = Some(Duration::from_secs(1));
    store.store_tasks(vec![delayed]).await?;
    let poll = store.claim_tasks(&node, &[claim()]).await?;
    assert!(
        poll.lanes
            .first()
            .ok_or_else(missing)?
            .next_wake_in
            .is_some_and(|wait| wait <= Duration::from_secs(1))
    );
    Ok(())
}

/// Creates the normal fenced output without exposing a task mutation handle.
fn outcome(task: &TaskRecord, outcome: TaskOutcome) -> Result<TaskCommit, TaskRuntimeError> {
    Ok(TaskCommit::Outcome {
        task_id: task.id,
        node_id: task.node_id.ok_or_else(missing)?,
        attempts: task.attempts,
        lane: DEFAULT_TASK_LANE,
        outcome,
        owner_token: None,
    })
}

/// Reads one immutable claim in these store-boundary tests.
async fn claimed(store: &impl TaskStore, node: uuid::Uuid) -> Result<TaskRecord, TestError> {
    store
        .claim_tasks(&node, &[claim()])
        .await?
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .next()
        .ok_or_else(|| missing().into())
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("missing delivery fixture".into())
}

/// Work suspension and retries release ownership without relaxing scheduling or discarding resume input.
#[tokio::test]
async fn sticky_work_continuation() -> Result<(), TestError> {
    let mut configuration = conf();
    configuration
        .handlers
        .retain(|(_, kind)| *kind == TaskKind::Work);
    let store = MemoryTaskStore::new(32, configuration);
    store.initialize().await?;
    let owner = uuid::Uuid::now_v7();
    let peer = uuid::Uuid::now_v7();
    store.register_node(&owner).await?;
    store.register_node(&peer).await?;
    store.store_tasks(vec![write(record())]).await?;
    let task = claimed(&store, owner).await?;
    store
        .commit_tasks(&[outcome(&task, TaskOutcome::Suspend { state: "42".into() })?])
        .await?;
    assert!(store.resume(task.id, "{\"Ok\":7}".into()).await?);
    let resumed = claimed(&store, peer).await?;
    assert_eq!(resumed.state.as_deref(), Some("42"));
    assert_eq!(resumed.resume_input::<i32>()?, Some(Ok(7)));
    store
        .commit_tasks(&[outcome(&resumed, TaskOutcome::retry("later"))?])
        .await?;
    let retry = store.get_task(task.id).await?.ok_or_else(missing)?;
    assert_eq!(retry.node_id, None);
    assert_eq!(retry.resume_input, resumed.resume_input);
    assert_eq!(store.return_claims(Some(owner)).await?, 0);
    let returned = store.get_task(task.id).await?.ok_or_else(missing)?;
    assert_eq!(returned.node_id, None);
    assert_eq!(returned.ready_at, retry.ready_at);
    Ok(())
}

/// Delayed children stay ownerless for sticky, opted-out and locked destination lanes.
#[tokio::test]
async fn child_destination_policy() -> Result<(), TestError> {
    let destination = TaskLane::new("child-destination");
    for mode in 0..3 {
        let mut configuration = conf();
        let lane = TaskLaneConf::new(destination, 8);
        configuration.lanes.push(match mode {
            0 => lane,
            1 => lane.sticky(false),
            _ => lane.lock(crate::tasks::TaskLaneLock::new(4)),
        });
        let store = MemoryTaskStore::new(32, configuration);
        store.initialize().await?;
        let node = uuid::Uuid::now_v7();
        store.register_node(&node).await?;
        let (_, mut rows) = store
            .store_admitted(vec![write(flow_record())], &node, 1)
            .await?;
        let parent = rows.pop().ok_or_else(missing)?;
        let mut child = write(record());
        child.record.lane = destination.to_string();
        child.initial_delay = Some(Duration::from_secs(60));
        let id = child.record.id;
        let result = store
            .commit_admitted(
                &[outcome(
                    &parent,
                    TaskOutcome::Spawn {
                        state: "42".into(),
                        child,
                    },
                )?],
                &node,
                8,
                &[],
            )
            .await?;
        assert!(result.tasks.iter().all(|task| task.id != id));
        let stored = store.get_task(id).await?.ok_or_else(missing)?;
        assert_eq!(stored.node_id, None);
        assert_eq!(stored.attempts, 0);
        assert_eq!(stored.status, TaskStatus::Pending);
        assert!(stored.ready_at.is_some_and(|at| at > chrono::Utc::now()));
    }
    Ok(())
}

/// A partial local All delivery leaves overflow stealable and resumes only after every child.
#[tokio::test]
async fn partial_all_delivery() -> Result<(), TestError> {
    let store = MemoryTaskStore::new(32, conf());
    local_all_contract(&store).await
}

/// Shared backend contract: local ownership and ownerless fallback coexist in one atomic group.
pub(crate) async fn local_all_contract(store: &impl TaskStore) -> Result<(), TestError> {
    store.initialize().await?;
    let node = uuid::Uuid::now_v7();
    let peer = uuid::Uuid::now_v7();
    store.register_node(&node).await?;
    store.register_node(&peer).await?;
    let (_, mut rows) = store
        .store_admitted(vec![write(flow_record())], &node, 1)
        .await?;
    let parent = rows.pop().ok_or_else(missing)?;
    let children: Vec<_> = (0..3).map(|_| write(record())).collect();
    let ids: Vec<_> = children.iter().map(|child| child.record.id).collect();
    let result = store
        .commit_admitted(
            &[outcome(
                &parent,
                TaskOutcome::All {
                    state: "42".into(),
                    children,
                },
            )?],
            &node,
            1,
            &[],
        )
        .await?;
    let checkpoint = result
        .tasks
        .iter()
        .find(|task| task.id == parent.id)
        .ok_or_else(missing)?;
    let local = result
        .tasks
        .iter()
        .find(|task| task.kind == TaskKind::Work)
        .ok_or_else(missing)?;
    assert_eq!((local.node_id, local.attempts), (Some(node), 1));
    for id in ids.iter().filter(|id| **id != local.id) {
        let task = store.get_task(*id).await?.ok_or_else(missing)?;
        assert_eq!(
            (task.status, task.node_id, task.attempts),
            (TaskStatus::Pending, None, 0)
        );
    }
    finish_shared_all(store, peer, checkpoint, local).await
}

/// Peer completion cannot steal a local running child or satisfy a join prematurely.
async fn finish_shared_all(
    store: &impl TaskStore,
    peer: uuid::Uuid,
    checkpoint: &Arc<TaskRecord>,
    local: &TaskRecord,
) -> Result<(), TestError> {
    assert_eq!(checkpoint.status, TaskStatus::Suspended);
    assert!(checkpoint.node_id.is_none());
    let poll = store.claim_tasks(&peer, &[claim()]).await?;
    let children: Vec<_> = poll.lanes.into_iter().flat_map(|lane| lane.tasks).collect();
    assert_eq!(children.len(), 2);
    for child in &children {
        assert_ne!(child.id, local.id);
        store
            .commit_tasks(&[outcome(child, TaskOutcome::Complete)?])
            .await?;
    }
    assert_eq!(
        store
            .get_task(checkpoint.id)
            .await?
            .ok_or_else(missing)?
            .status,
        TaskStatus::Suspended
    );
    let node = local.node_id.ok_or_else(missing)?;
    let result = store
        .commit_admitted(
            &[outcome(local, TaskOutcome::Complete)?],
            &node,
            1,
            std::slice::from_ref(checkpoint),
        )
        .await?;
    assert!(result.tasks.iter().all(|task| task.id != checkpoint.id));
    let pending = store.get_task(checkpoint.id).await?.ok_or_else(missing)?;
    assert_eq!(
        (pending.status, pending.node_id),
        (TaskStatus::Pending, None)
    );
    let resumed = claimed(store, peer).await?;
    assert_eq!(resumed.id, checkpoint.id);
    assert_eq!(
        resumed.resume_input::<Vec<Result<(), crate::tasks::TaskFailure>>>()?,
        Some(Ok(vec![Ok(()), Ok(()), Ok(())]))
    );
    Ok(())
}

/// A current cache never bypasses sticky opt-out or an exhausted delivery budget.
#[tokio::test]
async fn cached_resume_fallback() -> Result<(), TestError> {
    for (sticky, capacity) in [(true, 0), (false, 1)] {
        let mut configuration = conf();
        configuration.lanes = vec![TaskLaneConf::new(DEFAULT_TASK_LANE, 8).sticky(sticky)];
        let store = MemoryTaskStore::new(32, configuration);
        store.initialize().await?;
        let node = uuid::Uuid::now_v7();
        let peer = uuid::Uuid::now_v7();
        store.register_node(&node).await?;
        store.register_node(&peer).await?;
        let (parent, cached) = suspended_parent(&store, node).await?;
        let child = claimed(&store, node).await?;
        let result = store
            .commit_admitted(
                &[outcome(&child, TaskOutcome::Complete)?],
                &node,
                capacity,
                &cached,
            )
            .await?;
        assert!(result.tasks.is_empty());
        let resumed = claimed(&store, peer).await?;
        assert_eq!(resumed.id, parent.id);
        assert_eq!(resumed.resume_input::<()>()?, Some(Ok(())));
    }
    Ok(())
}
