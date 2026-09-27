use super::*;
use crate::tasks::{TaskId, TaskOutcome, TaskStatus};

#[path = "runner_batch_cancel.rs"]
mod batch_live;

/// The live scheduler observes intent through renewal, aborts the handler, and frees capacity.
#[tokio::test]
async fn cancellation_live_runner() -> Result<(), String> {
    let (started, mut receiver) = mpsc::channel(1);
    let dispatcher = live_dispatcher(started).map_err(|error| error.to_string())?;
    let runner = AbstractTaskRunner::new(dispatcher.clone()).map_err(|error| error.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let running = tokio::spawn(runner.run(site.clone()));
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let id = dispatcher.submit(BatchJob).await?.id();
        let dropped = receiver.recv().await.ok_or_else(missing)?;
        assert!(dispatcher.cancel(id).await?);
        assert!(
            dropped.await.is_err(),
            "central renewal must abort the handler"
        );
        assert_eq!(
            dispatcher.get(id).await?.ok_or_else(missing)?.status,
            TaskStatus::Failed
        );
        let next = dispatcher.submit(OtherBatchJob).await?.id();
        loop {
            if dispatcher.get(next).await?.ok_or_else(missing)?.status == TaskStatus::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<(), TaskRuntimeError>(())
    })
    .await;
    site.shutdown_and_wait().await;
    running.abort();
    let _ = running.await;
    result
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
}

/// Builds a one-slot runner fixture whose pending handler exposes future destruction.
fn live_dispatcher(
    started: mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
) -> Result<crate::tasks::TaskDispatcher<MemoryTaskStore>, TaskRuntimeError> {
    let lease = Duration::from_millis(150);
    let conf = TaskConf::default()
        .concurrency(1)
        .batch_size(4)
        .poll_interval(Duration::from_millis(10))
        .fallback_poll_interval(Duration::from_millis(20))
        .lease_duration(lease)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1));
    let mut registry = TaskRegistry::new();
    registry.register(RegisteredTask::new(
        crate::tasks::TaskDefinition::new("waiting-job"),
        move |_: Data<BatchJob>| {
            let started = started.clone();
            async move {
                let (held, dropped) = tokio::sync::oneshot::channel();
                let _ = started.send(dropped).await;
                std::future::pending::<()>().await;
                drop(held);
            }
        },
    ))?;
    registry.register(RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("following-job"),
        other_batch_job,
    ))?;
    Ok(Arc::new(registry.with_config(conf)?).dispatcher(
        Arc::new(MemoryTaskStore::with_lease_duration(4, lease)),
        Vec::new(),
    ))
}

/// Every scheduler turn drains only available outcomes without exceeding its flush bound.
#[test]
fn commit_drain_handles_empty_partial_and_full_queues() -> Result<(), TaskRuntimeError> {
    for queued in [0, 1, 4, 7] {
        for already in [0, 2, 4] {
            let mut runner = batch_runner()?;
            let make_commit = || TaskCommit {
                task_id: TaskId::new(uuid::Uuid::now_v7()),
                lane: DEFAULT_TASK_LANE,
                outcome: TaskOutcome::Complete,
                owner_token: None,
            };
            runner
                .pending_commits
                .extend((0..queued).map(|_| make_commit()));
            let expected = runner
                .pending_commits
                .iter()
                .map(|item| item.task_id)
                .collect::<Vec<_>>();
            let mut commits = (0..already).map(|_| make_commit()).collect::<Vec<_>>();
            runner.fill_commits(&mut commits);
            let drained = queued.min(4 - already);
            assert_eq!(commits.len(), already + drained);
            assert_eq!(runner.pending_commits.len(), queued - drained);
            assert_eq!(
                commits
                    .iter()
                    .skip(already)
                    .map(|item| item.task_id)
                    .collect::<Vec<_>>(),
                expected[..drained]
            );
        }
    }
    Ok(())
}

/// Cancellation detaches one member while the shared future and sibling renewal remain live.
#[tokio::test]
async fn cancellation_preserves_batch() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let dispatcher = runner
        .registry
        .clone()
        .dispatcher(runner.store.clone(), Vec::new());
    let first = dispatcher.submit(BatchJob).await?.id();
    let second = dispatcher.submit(BatchJob).await?.id();
    let claims = runner.claims(true);
    let snapshot = runner.store.claim_tasks(&runner.runner_id, &claims).await?;
    let invocation_id = uuid::Uuid::now_v7();
    let future = track_batch(&mut runner, invocation_id, first, second)?;
    assert!(dispatcher.cancel(first).await?);
    assert!(!dispatcher.cancel(first).await?);
    let leases = runner.renewal_leases(&[]);
    let tick = runner
        .store
        .tick(&runner.runner_id, &[], &[], &leases)
        .await?;
    assert_eq!(tick.lost, vec![first]);
    assert_eq!(tick.cancelled, vec![first]);
    runner.record_renewals(&leases, &tick.lost, &tick.cancelled);
    assert!(!future.is_finished());
    assert_eq!(runner.running_invocations.len(), 1);
    assert_eq!(runner.running_tasks.len(), 1);
    assert!(runner.running_tasks.contains_key(&second));
    assert!(
        snapshot
            .lanes
            .iter()
            .flat_map(|lane| &lane.tasks)
            .all(|task| !task.cancelled)
    );
    finish_preserved(&mut runner, invocation_id, first, second).await?;
    future.abort();
    Ok(())
}

/// Only the still-renewed sibling produces a commit, without another handler attempt.
async fn finish_preserved(
    runner: &mut AbstractTaskRunner<MemoryTaskStore>,
    invocation: uuid::Uuid,
    first: TaskId,
    second: TaskId,
) -> Result<(), TaskRuntimeError> {
    let commits = complete_batch(runner, invocation, first, second);
    assert_eq!(commits.len(), 1);
    assert_eq!(commits.first().ok_or_else(missing)?.task_id, second);
    runner
        .store
        .commit_outcomes(&runner.runner_id, &commits)
        .await?;
    let sibling = runner.store.get_task(second).await?.ok_or_else(missing)?;
    assert_eq!(sibling.status, TaskStatus::Succeeded);
    assert_eq!(sibling.attempts, 1);
    assert_eq!(
        runner
            .store
            .get_task(first)
            .await?
            .ok_or_else(missing)?
            .status,
        TaskStatus::Failed
    );
    Ok(())
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("missing cancellation fixture".into())
}

/// Even cancelling every member retains execution capacity until the shared future settles.
#[tokio::test]
async fn all_cancelled_stays_busy() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let first = TaskId::new(uuid::Uuid::now_v7());
    let second = TaskId::new(uuid::Uuid::now_v7());
    let id = uuid::Uuid::now_v7();
    let future = track_batch(&mut runner, id, first, second)?;
    for task in [first, second] {
        runner.record_renewals(&[], &[task], &[task]);
    }
    assert!(!future.is_finished());
    assert!(runner.renewal_leases(&[]).is_empty());
    assert_eq!(runner.running, 1);
    let lane = runner.lane_mut(DEFAULT_TASK_LANE).ok_or_else(missing)?;
    assert!(
        !locked_claim(lane, true)
            .owner
            .ok_or_else(missing)?
            .quiescent
    );
    let mut state = RunState::new(4, Duration::from_millis(10));
    state.shutting_down = true;
    assert!(!runner.finished(&state));
    assert!(complete_batch(&mut runner, id, first, second).is_empty());
    assert_eq!(runner.running, 0);
    assert!(runner.finished(&state));
    future.abort();
    Ok(())
}

/// Actual sibling ownership loss still invalidates a partially cancelled invocation.
#[tokio::test]
async fn sibling_loss_still_aborts() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let first = TaskId::new(uuid::Uuid::now_v7());
    let second = TaskId::new(uuid::Uuid::now_v7());
    let id = uuid::Uuid::now_v7();
    let future = track_batch(&mut runner, id, first, second)?;
    runner.record_renewals(&[], &[first, second], &[first]);
    assert!(future.await.is_err());
    assert!(complete_batch(&mut runner, id, first, second).is_empty());
    Ok(())
}

/// Dropping a middle member preserves ordered per-item outcomes for its unaffected siblings.
#[tokio::test]
async fn middle_result_is_discarded() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let ids = [
        TaskId::new(uuid::Uuid::now_v7()),
        TaskId::new(uuid::Uuid::now_v7()),
        TaskId::new(uuid::Uuid::now_v7()),
    ];
    let first = *ids.first().ok_or_else(missing)?;
    let middle = *ids.get(1).ok_or_else(missing)?;
    let last = *ids.last().ok_or_else(missing)?;
    let id = uuid::Uuid::now_v7();
    let future = track_batch(&mut runner, id, first, middle)?;
    runner.running_tasks.insert(last, id);
    runner
        .running_invocations
        .get_mut(&id)
        .ok_or_else(missing)?
        .task_ids
        .push(last);
    runner.record_renewals(&[], &[middle], &[middle]);
    let commits = numbered_commits(&ids);
    let mut accepted = Vec::new();
    runner.accept_completion(
        Completion {
            invocation_id: id,
            lane: DEFAULT_TASK_LANE,
            commits,
        },
        &mut accepted,
    );
    assert_eq!(
        accepted
            .iter()
            .map(|commit| commit.task_id)
            .collect::<Vec<_>>(),
        vec![first, last]
    );
    for (commit, expected) in accepted.iter().zip(["0", "2"]) {
        assert!(
            matches!(&commit.outcome, TaskOutcome::CompleteWith { output } if output == expected)
        );
    }
    assert_eq!(runner.running, 0);
    future.abort();
    Ok(())
}

/// Distinct output values expose accidental reassociation after a middle result is discarded.
fn numbered_commits(ids: &[TaskId]) -> Vec<TaskCommit> {
    ids.iter()
        .enumerate()
        .map(|(index, task_id)| TaskCommit {
            task_id: *task_id,
            lane: DEFAULT_TASK_LANE,
            owner_token: None,
            outcome: TaskOutcome::CompleteWith {
                output: index.to_string(),
            },
        })
        .collect()
}

/// Renewal removes cancelled prefetched work without editing its immutable queue snapshot.
#[tokio::test]
async fn cancellation_drops_queued_snapshot() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let dispatcher = runner
        .registry
        .clone()
        .dispatcher(runner.store.clone(), Vec::new());
    let id = dispatcher.submit(BatchJob).await?.id();
    let claim = LaneClaim {
        lane: DEFAULT_TASK_LANE,
        limit: 1,
        owner: None,
    };
    let poll = runner
        .store
        .claim_tasks(&runner.runner_id, &[claim])
        .await?;
    let record = poll
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .next()
        .ok_or_else(missing)?;
    let snapshot = Arc::new(record);
    runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(missing)?
        .tasks
        .push_back(snapshot.clone());
    assert!(dispatcher.cancel(id).await?);
    let leases = runner.renewal_leases(&[]);
    let lost = runner
        .store
        .renew_leases(&runner.runner_id, &leases)
        .await?;
    assert_eq!(lost, vec![id]);
    runner.drop_lost(&lost);
    assert_eq!(runner.queued(), 0);
    assert!(!snapshot.cancelled);
    assert_eq!(snapshot.status, TaskStatus::Running);
    assert_eq!(
        dispatcher.get(id).await?.ok_or_else(missing)?.status,
        TaskStatus::Failed
    );
    Ok(())
}

/// Tracks a pending invocation exactly as dispatch would, leaving the durable rows unchanged.
fn track_batch(
    runner: &mut AbstractTaskRunner<MemoryTaskStore>,
    id: uuid::Uuid,
    first: TaskId,
    second: TaskId,
) -> Result<tokio::task::JoinHandle<()>, TaskRuntimeError> {
    let future = tokio::spawn(std::future::pending::<()>());
    runner.running = 1;
    runner.running_tasks.insert(first, id);
    runner.running_tasks.insert(second, id);
    runner.running_invocations.insert(
        id,
        RunningInvocation {
            lane: DEFAULT_TASK_LANE,
            owner_token: None,
            task_ids: vec![first, second],
            abort: future.abort_handle(),
        },
    );
    runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(missing)?
        .running = 1;
    Ok(future)
}

/// Supplies a complete batch result; the runner filters members no longer held locally.
fn complete_batch(
    runner: &mut AbstractTaskRunner<MemoryTaskStore>,
    id: uuid::Uuid,
    first: TaskId,
    second: TaskId,
) -> Vec<TaskCommit> {
    let mut accepted = Vec::new();
    runner.accept_completion(
        Completion {
            invocation_id: id,
            lane: DEFAULT_TASK_LANE,
            commits: [first, second]
                .into_iter()
                .map(|task_id| TaskCommit {
                    task_id,
                    lane: DEFAULT_TASK_LANE,
                    outcome: TaskOutcome::Complete,
                    owner_token: None,
                })
                .collect(),
        },
        &mut accepted,
    );
    assert!(runner.pending_commits.is_empty());
    accepted
}
