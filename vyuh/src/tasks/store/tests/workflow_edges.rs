use super::*;

const CHILD: TaskLane = TaskLane::new("children");

/// Exercises cross-lane delivery, retry input retention, and startup failure delivery.
pub(crate) async fn cross_lane_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskError> {
    let mut config = conf();
    config.lanes = vec![
        TaskLaneConf::new(DEFAULT_TASK_LANE, 8)
            .retry(crate::tasks::TaskRetry::exponential(2, Duration::ZERO)),
        TaskLaneConf::new(CHILD, 8),
    ];
    store.initialize(config.clone()).await?;
    let parent = record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let mut child = record();
    child.lane = CHILD.to_string();
    let child_id = child.id;
    let claims = [
        claim(),
        LaneClaim {
            lane: CHILD,
            ..claim()
        },
    ];
    let spawned = store
        .tick(
            "owner",
            &claims,
            &[commit(
                id,
                TaskOutcome::Spawn {
                    state: "42".into(),
                    child: write(child),
                },
            )],
            &[],
        )
        .await?;
    assert!(spawned.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
    assert_eq!(spawned.wake_lanes, [CHILD]);
    let (left, right) = tokio::join!(
        store.claim_tasks("owner", &claims),
        store.claim_tasks("contender", &claims)
    );
    let left = left?;
    let right = right?;
    let count = |poll: &TaskPoll| {
        poll.lanes
            .iter()
            .map(|lane| lane.tasks.len())
            .sum::<usize>()
    };
    assert_eq!(count(&left) + count(&right), 1);
    let owner = if count(&left) == 1 {
        "owner"
    } else {
        "contender"
    };
    let outcome = TaskCommit {
        lane: CHILD,
        ..commit(child_id, TaskOutcome::CompleteWith { output: "7".into() })
    };
    let delivered = store.tick(owner, &claims, &[outcome], &[]).await?;
    assert!(
        delivered
            .poll
            .lanes
            .iter()
            .all(|lane| lane.tasks.is_empty())
    );
    assert_eq!(delivered.wake_lanes, [DEFAULT_TASK_LANE]);
    retry_parent(store, id).await?;
    scheduled_child(store).await?;
    ineligible_parent(store).await?;
    startup_failure(store, config).await
}

/// Spawned scheduling delays remain database-relative and cannot bypass readiness.
async fn scheduled_child<S: AbstractTaskStore>(store: &S) -> Result<(), TaskError> {
    let parent = record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let mut child = write(record());
    child.initial_delay = Some(Duration::from_secs(120));
    let child_id = child.record.id;
    let turn = store
        .tick(
            "owner",
            &[claim()],
            &[commit(
                id,
                TaskOutcome::Spawn {
                    state: "null".into(),
                    child,
                },
            )],
            &[],
        )
        .await?;
    assert!(turn.poll.lanes[0].tasks.is_empty());
    let next = store.claim_tasks("owner", &[claim()]).await?;
    assert!(next.lanes[0].tasks.is_empty());
    assert!(
        next.lanes[0]
            .next_wake_in
            .is_some_and(|delay| delay > Duration::from_secs(100))
    );
    assert!(
        store
            .get_task(child_id)
            .await?
            .unwrap()
            .ready_at
            .is_some_and(|ready| ready > chrono::Utc::now())
    );
    assert_eq!(
        store.get_task(id).await?.unwrap().status,
        TaskStatus::Suspended
    );
    Ok(())
}

/// Missing or no-longer-suspended parents never prevent child completion or receive input.
async fn ineligible_parent<S: AbstractTaskStore>(store: &S) -> Result<(), TaskError> {
    let mut parent = record();
    parent.status = TaskStatus::Succeeded;
    let id = parent.id;
    let mut missing = record();
    missing.parent_id = Some(record().id);
    let mut completed = record();
    completed.parent_id = Some(id);
    let commits = [
        commit(missing.id, TaskOutcome::Complete),
        commit(completed.id, TaskOutcome::Complete),
    ];
    store
        .store_tasks(vec![write(parent), write(missing), write(completed)])
        .await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store.commit_outcomes("owner", &commits).await?;
    for commit in commits {
        assert_eq!(
            store.get_task(commit.task_id).await?.unwrap().status,
            TaskStatus::Succeeded
        );
    }
    assert_eq!(store.get_task(id).await?.unwrap().resume_input, None);
    Ok(())
}

/// Retrying a resumed step preserves its input and exhausts only its own attempt budget.
async fn retry_parent<S: AbstractTaskStore>(store: &S, id: TaskId) -> Result<(), TaskError> {
    for attempt in 1..=2 {
        let task = store.claim_tasks("owner", &[claim()]).await?.lanes[0]
            .tasks
            .remove(0);
        assert_eq!(task.id, id);
        assert_eq!(task.step_attempts, attempt);
        assert_eq!(task.resume_input::<u32>()?, Some(Ok(7)));
        assert_eq!(task.state::<u32>()?, Some(42));
        store
            .commit_outcomes("owner", &[commit(id, TaskOutcome::retry("again"))])
            .await?;
    }
    assert_eq!(
        store.get_task(id).await?.unwrap().status,
        TaskStatus::Failed
    );
    Ok(())
}

/// Startup recovery delivers an unleased child's terminal error through the same envelope.
async fn startup_failure<S: AbstractTaskStore>(
    store: &S,
    config: TaskStoreConf,
) -> Result<(), TaskError> {
    let mut parent = record();
    parent.status = TaskStatus::Suspended;
    let id = parent.id;
    let mut child = record();
    child.parent_id = Some(id);
    child.status = TaskStatus::Running;
    let child_id = child.id;
    store.store_tasks(vec![write(parent), write(child)]).await?;
    store.initialize(config).await?;
    let parent = store.get_task(id).await?.unwrap();
    assert_eq!(parent.status, TaskStatus::Pending);
    assert_eq!(
        parent.resume_input::<()>()?.unwrap().unwrap_err().task_id(),
        Some(&child_id)
    );
    Ok(())
}

/// Memory and database stores share cross-lane, retry, contention, and startup semantics.
#[tokio::test]
async fn cross_lane_retry_and_startup() -> Result<(), TaskError> {
    cross_lane_contract(&MemoryTaskStore::new(32)).await
}
