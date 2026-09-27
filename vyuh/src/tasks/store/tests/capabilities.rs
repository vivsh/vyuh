use super::*;

/// Both classifications share claims, attempts, renewal, cancellation and fenced completion.
pub(crate) async fn contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    invalid_outcomes(store).await?;
    work_suspend(store).await?;
    recovery(store).await?;
    for fixture in [record(), flow_record()] {
        fenced_cancellation(store, fixture).await?;
    }
    Ok(())
}

/// Crash replay increments the same counters and exhausts the same budget for both kinds.
async fn recovery<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let budget = conf().lanes[0].retry_policy().max_attempts() as i32;
    let tasks = [record(), flow_record()].map(|mut task| {
        task.lease_duration_ms = Some(1);
        task.step_attempts = budget - 1;
        task
    });
    let ids = tasks.iter().map(|task| task.id).collect::<Vec<_>>();
    store
        .store_tasks(tasks.into_iter().map(write).collect())
        .await?;
    store.claim_tasks("crashed", &[claim()]).await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    store.claim_tasks("takeover", &[claim()]).await?;
    for id in ids {
        let task = get(store, id).await?;
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.attempts, 1);
        assert_eq!(task.step_attempts, budget);
    }
    Ok(())
}

/// Invalid capabilities fail only their row and never insert a child or disturb siblings.
async fn invalid_outcomes<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let child = record();
    let child_id = child.id;
    let mut wrong_child = flow_record();
    wrong_child.kind = TaskKind::Work;
    let wrong_id = wrong_child.id;
    let cases = invalid_cases(child, wrong_child);
    let mut commits = Vec::new();
    for (task, outcome) in cases {
        commits.push(commit(task.id, outcome));
        store.store_tasks(vec![write(task)]).await?;
    }
    let valid = record();
    let valid_id = valid.id;
    store.store_tasks(vec![write(valid)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let failed_ids = commits
        .iter()
        .map(|commit| commit.task_id)
        .collect::<Vec<_>>();
    commits.push(commit(valid_id, TaskOutcome::Complete));
    store.commit_outcomes("owner", &commits).await?;
    for id in failed_ids {
        assert_eq!(get(store, id).await?.status, TaskStatus::Failed);
    }
    assert_eq!(get(store, valid_id).await?.status, TaskStatus::Succeeded);
    assert!(store.get_task(child_id).await?.is_none());
    assert!(store.get_task(wrong_id).await?.is_none());
    Ok(())
}

async fn get<S: AbstractTaskStore>(store: &S, id: TaskId) -> Result<TaskRecord, TaskRuntimeError> {
    store
        .get_task(id)
        .await?
        .ok_or_else(|| TaskRuntimeError::TaskNotFound(id.to_string()))
}

/// Memory uses the same defensive capability and cancellation ordering as SQL stores.
#[tokio::test]
async fn memory_capability_contract() -> Result<(), TaskRuntimeError> {
    contract(&MemoryTaskStore::new(32)).await
}

/// Handler classification participates in deployment identity, independently of order.
#[test]
fn kinds_are_fingerprinted() {
    let original = conf();
    let expected = crate::tasks::store::policy_fingerprint(&original);
    let mut changed = original.clone();
    changed.handlers.reverse();
    assert_eq!(crate::tasks::store::policy_fingerprint(&changed), expected);
    changed.handlers[0].1 = TaskKind::Work;
    assert_ne!(crate::tasks::store::policy_fingerprint(&changed), expected);
}

/// Renewal and stale outcomes obey the same cancellation precedence for either kind.
async fn fenced_cancellation<S: AbstractTaskStore>(
    store: &S,
    fixture: TaskRecord,
) -> Result<(), TaskRuntimeError> {
    let id = fixture.id;
    store.store_tasks(vec![write(fixture)]).await?;
    let poll = store.claim_tasks("owner", &[claim()]).await?;
    let task = poll
        .lanes
        .iter()
        .flat_map(|lane| &lane.tasks)
        .find(|task| task.id == id)
        .ok_or_else(|| TaskRuntimeError::TaskNotFound(id.to_string()))?;
    assert_eq!(task.attempts, 1);
    store
        .commit_outcomes("stale", &[commit(id, TaskOutcome::Complete)])
        .await?;
    assert_eq!(get(store, id).await?.status, TaskStatus::Running);
    store
        .tick(
            "owner",
            &[],
            &[],
            &[TaskLease {
                task_id: id,
                lane: DEFAULT_TASK_LANE,
                owner_token: None,
            }],
        )
        .await?;
    assert_eq!(get(store, id).await?.attempts, 1);
    assert!(store.cancel(id).await?);
    store
        .commit_outcomes("owner", &[commit(id, TaskOutcome::Complete)])
        .await?;
    let failed = get(store, id).await?;
    assert_eq!(failed.status, TaskStatus::Failed);
    assert!(failed.cancelled);
    assert_eq!(
        crate::tasks::TaskInfo::from(failed)
            .last_result::<()>()?
            .and_then(Result::err)
            .map(|e| e.message().to_owned()),
        Some("Task cancelled".into())
    );
    Ok(())
}

/// Produces prohibited low-level transitions without exposing them in handler return types.
fn invalid_cases(child: TaskRecord, wrong_child: TaskRecord) -> [(TaskRecord, TaskOutcome); 4] {
    [
        (
            record(),
            TaskOutcome::Sleep {
                state: "null".into(),
                delay: Duration::ZERO,
            },
        ),
        (
            record(),
            TaskOutcome::Spawn {
                state: "null".into(),
                child: write(child),
            },
        ),
        (flow_record(), TaskOutcome::retry("invalid")),
        (
            flow_record(),
            TaskOutcome::Spawn {
                state: "null".into(),
                child: write(wrong_child),
            },
        ),
    ]
}

/// Work suspension uses the unchanged checkpoint, resume, cancellation, and claim rules.
async fn work_suspend<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let task = record();
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let outcome = TaskOutcome::Suspend { state: "7".into() };
    let turn = store
        .tick("owner", &[claim()], &[commit(id, outcome)], &[])
        .await?;
    assert!(
        turn.poll
            .lanes
            .iter()
            .all(|lane| lane.tasks.iter().all(|t| t.id != id))
    );
    let snapshot = get(store, id).await?;
    assert_eq!(snapshot.status, TaskStatus::Suspended);
    assert_eq!(snapshot.state.as_deref(), Some("7"));
    assert_eq!(snapshot.step_attempts, 0);
    assert!(store.resume(id, "{\"Ok\":8}".into()).await?);
    store.claim_tasks("owner", &[claim()]).await?;
    let resumed = get(store, id).await?;
    assert_eq!(resumed.attempts, 2);
    assert_eq!(resumed.resume_input.as_deref(), Some("{\"Ok\":8}"));
    store
        .commit_outcomes(
            "owner",
            &[commit(id, TaskOutcome::Suspend { state: "9".into() })],
        )
        .await?;
    assert!(store.cancel(id).await?);
    store.claim_tasks("owner", &[claim()]).await?;
    assert_eq!(get(store, id).await?.status, TaskStatus::Failed);
    assert!(!store.resume(id, "{\"Ok\":10}".into()).await?);
    Ok(())
}
