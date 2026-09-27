use super::*;

#[path = "cancellation_lanes.rs"]
pub(crate) mod lanes;
#[path = "cancellation_races.rs"]
mod races;

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("cancellation fixture missing".into())
}

async fn get<S: AbstractTaskStore>(store: &S, id: TaskId) -> Result<TaskRecord, TaskRuntimeError> {
    store.get_task(id).await?.ok_or_else(missing)
}

/// All backends share intent, outcome, renewal, recovery, and rollback acceptance tests.
pub(crate) async fn contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    suspended_contract(store).await?;
    intent_contract(store).await?;
    bounded_contract(store).await?;
    outcome_contract(store).await?;
    renewal_contract(store).await?;
    cancelled_parent(store).await?;
    recovery_contract(store).await?;
    races::contract(store).await?;
    idempotency_contract(store).await?;
    rollback_contract(store).await
}

/// Active-only idempotency stays held until cancellation is finalized, then becomes reusable.
async fn idempotency_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut task = record();
    task.idempotency_key = Some("cancelled-key".into());
    task.idempotency_fingerprint = Some("same-input".into());
    let id = task.id;
    store.store_tasks(vec![write(task.clone())]).await?;
    assert!(store.cancel(id).await?);
    task.id = TaskId::new(uuid::Uuid::now_v7());
    let replacement = task.id;
    let duplicate = store.store_tasks(vec![write(task.clone())]).await?;
    assert_eq!(duplicate, vec![crate::tasks::TaskReceipt::Existing(id)]);
    store.claim_tasks("owner", &[claim()]).await?;
    assert_cancelled(&get(store, id).await?)?;
    let submitted = store.store_tasks(vec![write(task)]).await?;
    assert_eq!(
        submitted,
        vec![crate::tasks::TaskReceipt::Queued(replacement)]
    );
    assert!(!get(store, replacement).await?.cancelled);
    store.claim_tasks("owner", &[claim()]).await?;
    store
        .commit_outcomes("owner", &[commit(replacement, TaskOutcome::Complete)])
        .await?;
    Ok(())
}

/// Cancellation leaves terminal tasks and repeat requests untouched and advances future timers.
async fn intent_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    assert!(!store.cancel(TaskId::new(uuid::Uuid::now_v7())).await?);
    for status in [
        TaskStatus::Pending,
        TaskStatus::Suspended,
        TaskStatus::Succeeded,
        TaskStatus::Failed,
    ] {
        let mut task = record();
        task.status = status;
        task.state = Some("42".into());
        task.resume_input = Some("{\"Ok\":1}".into());
        let id = task.id;
        let mut submission = write(task);
        submission.initial_delay = Some(Duration::from_secs(3600));
        store.store_tasks(vec![submission]).await?;
        let before = get(store, id).await?;
        let active = matches!(status, TaskStatus::Pending | TaskStatus::Suspended);
        assert_eq!(store.cancel(id).await?, active);
        let after = get(store, id).await?;
        assert_eq!(after.cancelled, active);
        assert_eq!(after.state, before.state);
        assert_eq!(after.resume_input, before.resume_input);
        assert_eq!(after.last_result, before.last_result);
        assert_eq!(after.attempts, before.attempts);
        if active {
            assert_eq!(after.status, TaskStatus::Pending);
            assert!(after.ready_at < before.ready_at);
        } else {
            assert_eq!(after.status, before.status);
            assert_eq!(after.updated_at, before.updated_at);
            assert_eq!(after.ready_at, before.ready_at);
        }
        assert!(!store.cancel(id).await?);
        assert_eq!(get(store, id).await?.updated_at, after.updated_at);
    }
    assert!(
        store
            .claim_tasks("owner", &[claim()])
            .await?
            .lanes
            .iter()
            .all(|lane| lane.tasks.is_empty())
    );
    Ok(())
}

/// Cancellation overrides every outcome and prevents child creation before spawn validation.
async fn outcome_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let child = record();
    let child_id = child.id;
    let outcomes = [
        TaskOutcome::Complete,
        TaskOutcome::CompleteWith {
            output: "malformed".into(),
        },
        TaskOutcome::retry("retry"),
        TaskOutcome::fail("different failure"),
        TaskOutcome::Suspend { state: "1".into() },
        TaskOutcome::Sleep {
            state: "2".into(),
            delay: Duration::from_secs(3600),
        },
        TaskOutcome::Spawn {
            state: "3".into(),
            child: write(child),
        },
    ];
    for outcome in outcomes {
        let task = record();
        let id = task.id;
        store.store_tasks(vec![write(task)]).await?;
        store.claim_tasks("owner", &[claim()]).await?;
        let snapshot = get(store, id).await?;
        assert!(store.cancel(id).await?);
        let requested = get(store, id).await?;
        assert_eq!(requested.status, TaskStatus::Running);
        assert_eq!(requested.leased_until, snapshot.leased_until);
        assert_eq!(requested.locked_by, snapshot.locked_by);
        store
            .commit_outcomes("stale", &[commit(id, TaskOutcome::Complete)])
            .await?;
        assert_eq!(get(store, id).await?.status, TaskStatus::Running);
        store
            .commit_outcomes("owner", &[commit(id, outcome)])
            .await?;
        assert_cancelled(&get(store, id).await?)?;
        store
            .commit_outcomes("owner", &[commit(id, TaskOutcome::Complete)])
            .await?;
        assert_cancelled(&get(store, id).await?)?;
        assert!(!store.cancel(id).await?);
        assert!(!snapshot.cancelled);
    }
    assert!(store.get_task(child_id).await?.is_none());
    Ok(())
}

/// Inspects a finalized cancellation through the public result projection.
fn assert_cancelled(task: &TaskRecord) -> Result<(), TaskRuntimeError> {
    assert!(task.cancelled);
    assert_eq!(task.status, TaskStatus::Failed);
    let info = crate::tasks::TaskInfo::from(task.clone());
    assert!(info.cancelled());
    let failure = info
        .last_result::<()>()?
        .and_then(Result::err)
        .ok_or_else(missing)?;
    assert_eq!(failure.message(), "Task cancelled");
    assert_eq!(failure.task_id(), Some(&task.id));
    assert!(task.locked_by.is_none() && task.leased_until.is_none());
    Ok(())
}

fn lease(id: TaskId) -> TaskLease {
    TaskLease {
        task_id: id,
        lane: DEFAULT_TASK_LANE,
        owner_token: None,
    }
}

/// Renewal finalizes only cancelled members, delivers after claims, and rejects stale completions.
async fn renewal_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut parent = record();
    parent.status = TaskStatus::Suspended;
    parent.state = Some("9".into());
    let parent_id = parent.id;
    let mut child = record();
    child.parent_id = Some(parent_id);
    let id = child.id;
    let sibling = record();
    let sibling_id = sibling.id;
    store
        .store_tasks(vec![write(parent), write(child), write(sibling)])
        .await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store.cancel(id).await?;
    let tick = store
        .tick("owner", &[claim()], &[], &[lease(id), lease(sibling_id)])
        .await?;
    assert_eq!(tick.lost, vec![id]);
    assert_eq!(tick.cancelled, vec![id]);
    assert!(tick.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
    assert_eq!(tick.wake_lanes, vec![DEFAULT_TASK_LANE]);
    let child = get(store, id).await?;
    assert_cancelled(&child)?;
    assert_eq!(get(store, parent_id).await?.resume_input, child.last_result);
    assert_eq!(get(store, sibling_id).await?.status, TaskStatus::Running);
    cancellation_replay(store, id, sibling_id).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store
        .commit_outcomes(
            "owner",
            &[
                commit(id, TaskOutcome::Complete),
                commit(parent_id, TaskOutcome::Complete),
                commit(sibling_id, TaskOutcome::Complete),
            ],
        )
        .await?;
    assert_cancelled(&get(store, id).await?)?;
    assert!(!store.cancel(parent_id).await?);
    Ok(())
}

/// Lost acknowledgement replay recognizes terminal cancellation without repeating delivery.
async fn cancellation_replay<S: AbstractTaskStore>(
    store: &S,
    id: TaskId,
    sibling: TaskId,
) -> Result<(), TaskRuntimeError> {
    let replay = store
        .tick("owner", &[], &[], &[lease(id), lease(sibling)])
        .await?;
    assert_eq!(replay.lost, vec![id]);
    assert_eq!(replay.cancelled, vec![id]);
    assert!(replay.wake_lanes.is_empty());
    Ok(())
}

/// Cancelling a parent neither cancels its child nor permits child delivery to revive it.
async fn cancelled_parent<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut parent = record();
    parent.status = TaskStatus::Suspended;
    let id = parent.id;
    let mut child = record();
    child.parent_id = Some(id);
    let child_id = child.id;
    store.store_tasks(vec![write(parent), write(child)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store.cancel(id).await?;
    assert!(!store.resume(id, "{\"Ok\":null}".into()).await?);
    store
        .commit_outcomes("owner", &[commit(child_id, TaskOutcome::Complete)])
        .await?;
    assert!(get(store, id).await?.resume_input.is_none());
    assert!(!get(store, child_id).await?.cancelled);
    store.claim_tasks("owner", &[claim()]).await?;
    assert_cancelled(&get(store, id).await?)?;
    Ok(())
}

/// Recovery prioritizes cancellation over exhausted attempts and legacy missing leases.
async fn recovery_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    for deadline in [None, Some(chrono::Utc::now() - chrono::Duration::hours(1))] {
        let mut task = record();
        task.status = TaskStatus::Running;
        task.locked_by = Some("crashed".into());
        task.leased_until = deadline;
        task.attempts = 100;
        task.step_attempts = 100;
        let id = task.id;
        store.store_tasks(vec![write(task)]).await?;
        store.cancel(id).await?;
        if deadline.is_none() {
            store.initialize(conf()).await?;
        } else {
            store.claim_tasks("owner", &[claim()]).await?;
        }
        let task = get(store, id).await?;
        assert_cancelled(&task)?;
        assert_eq!(task.attempts, 100);
    }
    Ok(())
}

/// A failed renewal rolls back cancellation finalization and parent delivery together.
async fn rollback_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut parent = record();
    parent.status = TaskStatus::Suspended;
    let parent_id = parent.id;
    let mut child = record();
    child.parent_id = Some(parent_id);
    child.status = TaskStatus::Running;
    child.locked_by = Some("owner".into());
    child.leased_until = Some(chrono::Utc::now() + chrono::Duration::hours(1));
    let id = child.id;
    let mut invalid = child.clone();
    invalid.id = TaskId::new(uuid::Uuid::now_v7());
    invalid.parent_id = None;
    invalid.lease_duration_ms = Some(i64::MAX);
    let invalid_id = invalid.id;
    store
        .store_tasks(vec![write(parent), write(child), write(invalid)])
        .await?;
    store.cancel(id).await?;
    assert!(
        store
            .tick("owner", &[], &[], &[lease(id), lease(invalid_id)])
            .await
            .is_err()
    );
    assert_eq!(get(store, id).await?.status, TaskStatus::Running);
    assert!(get(store, id).await?.last_result.is_none());
    assert_eq!(get(store, parent_id).await?.status, TaskStatus::Suspended);
    store.renew_leases("owner", &[lease(id)]).await?;
    assert_cancelled(&get(store, id).await?)?;
    assert_eq!(get(store, parent_id).await?.status, TaskStatus::Pending);
    Ok(())
}

/// The memory backend exercises the same race and recovery contract as SQL.
#[tokio::test]
async fn memory_cancellation_contract() -> Result<(), TaskRuntimeError> {
    contract(&MemoryTaskStore::new(32)).await
}

/// More than one claim chunk of cancellations never leaks cancelled rows to handlers.
async fn bounded_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let records = (0..35).map(|_| record()).collect::<Vec<_>>();
    let ids = records.iter().map(|task| task.id).collect::<Vec<_>>();
    store
        .store_tasks(records.into_iter().map(write).collect())
        .await?;
    for id in &ids {
        let before = get(store, *id).await?;
        assert!(store.cancel(*id).await?);
        assert_eq!(get(store, *id).await?.ready_at, before.ready_at);
    }
    let first = store.claim_tasks("owner", &[claim()]).await?;
    assert!(first.lanes.iter().all(|lane| lane.tasks.is_empty()));
    let mut failed = 0;
    for id in &ids {
        failed += usize::from(get(store, *id).await?.status == TaskStatus::Failed);
    }
    assert_eq!(failed, 32);
    let second = store.claim_tasks("owner", &[claim()]).await?;
    assert!(second.lanes.iter().all(|lane| lane.tasks.is_empty()));
    for id in ids {
        assert_cancelled(&get(store, id).await?)?;
    }
    Ok(())
}

/// Null readiness remains immediately eligible instead of being replaced with an audit timestamp.
#[tokio::test]
async fn null_readiness_is_preserved() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    store.initialize(conf()).await?;
    let task = record();
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    store
        .state
        .lock()
        .await
        .tasks
        .iter_mut()
        .find(|task| task.id == id)
        .ok_or_else(missing)?
        .ready_at = None;
    assert!(store.cancel(id).await?);
    assert_eq!(get(&store, id).await?.ready_at, None);
    store.claim_tasks("owner", &[claim()]).await?;
    assert_cancelled(&get(&store, id).await?)
}

/// Aborted batch siblings remain independently leased and recover after normal expiry.
#[tokio::test]
async fn sibling_recovers_after_cancellation() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::with_lease_duration(32, Duration::from_millis(30));
    store.initialize(conf()).await?;
    let first = record();
    let first_id = first.id;
    let second = record();
    let second_id = second.id;
    store.store_tasks(vec![write(first), write(second)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store.cancel(first_id).await?;
    store
        .renew_leases("owner", &[lease(first_id), lease(second_id)])
        .await?;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let recovered = store.claim_tasks("takeover", &[claim()]).await?;
    let tasks = recovered
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .collect::<Vec<_>>();
    assert_eq!(tasks.len(), 1);
    let task = tasks.first().ok_or_else(missing)?;
    assert_eq!(task.id, second_id);
    assert_eq!(task.attempts, 2);
    assert_cancelled(&get(&store, first_id).await?)
}

/// Suspended child cancellation persists intent, preserves state, and delivers only next poll.
async fn suspend_child<S: AbstractTaskStore>(
    store: &S,
) -> Result<(TaskId, TaskId), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let parent = flow_record();
    let id = parent.id;
    let child = flow_record();
    let child_id = child.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let spawn = TaskOutcome::Spawn {
        state: "7".into(),
        child: write(child),
    };
    store.commit_outcomes("owner", &[commit(id, spawn)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store
        .commit_outcomes(
            "owner",
            &[commit(child_id, TaskOutcome::Suspend { state: "8".into() })],
        )
        .await?;
    Ok((id, child_id))
}

/// Suspension checkpoints survive intent recording, and child failure resumes only next poll.
pub(crate) async fn suspended_contract<S: AbstractTaskStore>(
    store: &S,
) -> Result<(), TaskRuntimeError> {
    let (id, child_id) = suspend_child(store).await?;
    let before = get(store, child_id).await?;
    assert!(store.cancel(child_id).await?);
    let requested = get(store, child_id).await?;
    assert!(requested.cancelled);
    assert_eq!(requested.status, TaskStatus::Pending);
    assert_eq!(requested.state, before.state);
    assert_eq!(requested.attempts, before.attempts);
    assert!(!store.cancel(child_id).await?);
    assert_eq!(get(store, child_id).await?.updated_at, requested.updated_at);
    assert!(!store.resume(child_id, "{\"Ok\":null}".into()).await?);
    let tick = store.tick("owner", &[claim()], &[], &[]).await?;
    assert!(tick.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
    assert_eq!(tick.wake_lanes, vec![DEFAULT_TASK_LANE]);
    let failed = get(store, child_id).await?;
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.attempts, before.attempts);
    let parent = get(store, id).await?;
    assert_eq!(parent.last_result, None);
    assert_eq!(parent.resume_input, failed.last_result);
    assert_eq!(parent.state::<u32>()?, Some(7));
    let failure = parent
        .resume_input::<()>()?
        .and_then(Result::err)
        .ok_or_else(missing)?;
    assert_eq!(failure.message(), "Task cancelled");
    assert_eq!(failure.task_id(), Some(&child_id));
    let claimed = store.claim_tasks("owner", &[claim()]).await?;
    assert_eq!(
        claimed
            .lanes
            .iter()
            .flat_map(|lane| &lane.tasks)
            .map(|task| task.id)
            .collect::<Vec<_>>(),
        vec![id]
    );
    store
        .commit_outcomes("owner", &[commit(id, TaskOutcome::Complete)])
        .await?;
    Ok(())
}

/// The first end-to-end reference path uses only store-owned transitions.
#[tokio::test]
async fn suspended_child_cancellation() -> Result<(), TaskRuntimeError> {
    suspended_contract(&MemoryTaskStore::new(32)).await
}
