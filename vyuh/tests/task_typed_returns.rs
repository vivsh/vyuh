//! Public-runtime proofs for typed Work output, sleep, and external resumption.
#![cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]

use std::time::Duration;
use vyuh::prelude::*;
use vyuh::tasks::{
    DEFAULT_TASK_LANE, TaskConf, TaskDefinition, TaskIdempotency, TaskLane, TaskLaneConf,
    TaskOptions, TaskReceipt, TaskStatus, Tasks,
};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Calculate(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct AwaitResponse;

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct PollWork(u64);

#[bundles::work]
async fn poll_work(
    continuation: Continuation<u32>,
    input: Data<PollWork>,
) -> Result<WorkState<u32>, WorkError> {
    if continuation.resume().is_some() {
        return Err(WorkError::fail(
            "Sleep unexpectedly supplied a resume value",
        ));
    }
    match continuation.state() {
        Some(checkpoint) => Ok(WorkState::complete(*checkpoint)),
        None => Ok(WorkState::sleep(42, Duration::from_millis(input.0.0))?),
    }
}

/// Sleep releases the only Work slot; a wake or the deadline enables a later claim.
#[tokio::test]
async fn work_sleep_and_wake() -> Result<(), TestError> {
    let site = Site::build(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .concurrency(1)
                .poll_interval(Duration::from_millis(10)),
        ),
        bundles::bundle([__bundle_part_poll_work(), __bundle_part_calculate()]),
    )
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let tasks = site.tasks();
    let id =
        tasks.submit(PollWork(60_000)).await?.id().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("expected queued test task".into())
        })?;
    let sleeping = wait_sleep(&site, id).await?;
    assert_eq!(sleeping.state::<u32>()?, Some(42));
    assert_eq!(sleeping.node_id(), None);
    assert_eq!(sleeping.step_attempts(), 0);
    assert_eq!(sleeping.attempts(), 1);
    assert!(!tasks.resume(id, ()).await?);
    let unrelated =
        tasks.submit(Calculate(7)).await?.id().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("expected queued test task".into())
        })?;
    wait_status(&site, unrelated, TaskStatus::Succeeded).await?;
    let fallback =
        tasks.submit(PollWork(100)).await?.id().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("expected queued test task".into())
        })?;
    let zero_delay =
        tasks.submit(PollWork(0)).await?.id().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("expected queued test task".into())
        })?;
    let delayed = tasks
        .submit_with(
            Calculate(9),
            TaskOptions::new().delay(Duration::from_secs(60)),
        )
        .await?
        .id()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("expected queued test task".into()))?;
    assert!(tasks.wake(delayed).await?);
    assert!(tasks.wake(id).await?);
    assert!(!tasks.wake(id).await?);
    for id in [id, fallback, zero_delay] {
        wait_status(&site, id, TaskStatus::Succeeded).await?;
        let task = tasks.get(id).await?.ok_or(TestError::Missing)?;
        assert_eq!(task.last_result::<u32>()?, Some(Ok(42)));
        assert_eq!(task.attempts(), 2);
        assert_eq!(task.step_attempts(), 1);
    }
    wait_status(&site, delayed, TaskStatus::Succeeded).await?;
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Waits for the first invocation to commit its checkpoint and future eligibility.
async fn wait_sleep(site: &Site, id: TaskId) -> Result<vyuh::tasks::TaskInfo, TestError> {
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(task) = site.tasks().get(id).await?
                && task.status() == TaskStatus::Pending
                && task.attempts() == 1
            {
                return Ok::<_, TaskRuntimeError>(task);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(result)
}

#[bundles::work]
async fn calculate(input: Data<Calculate>) -> u32 {
    input.0.0 * 2
}

/// Single and bulk routing execute the original handler and retain keys by the stored lane.
#[tokio::test]
async fn typed_submission_routes_to_lane() -> Result<(), TestError> {
    const BULK: TaskLane = TaskLane::new("bulk");
    let retention = Duration::from_secs(60);
    let site = Site::build(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .lane(TaskLaneConf::new(BULK, 2).idempotency_retention(retention))
                .poll_interval(Duration::from_millis(10)),
        ),
        bundles::bundle([bundles::work(
            calculate,
            TaskDefinition::new("calculate")
                .idempotency(TaskIdempotency::new("1", |input: &Calculate| {
                    input.0.to_string()
                })),
        )]),
    )
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    let tasks = site.tasks();
    let default =
        tasks.submit(Calculate(1)).await?.id().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("expected queued test task".into())
        })?;
    let routed = tasks
        .submit_with(Calculate(2), TaskOptions::new().lane(BULK))
        .await?
        .id()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("expected queued test task".into()))?;
    let batch = tasks
        .submit_many_with(
            [Calculate(3), Calculate(2)],
            TaskOptions::new().lane(BULK).clone(),
        )
        .await?;
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.get(1), Some(&TaskReceipt::KeyHeld));
    let bulk = batch
        .first()
        .and_then(|receipt| receipt.id())
        .ok_or(TestError::Missing)?;
    runtime.start_runtime().await?;
    for (id, lane, value) in [
        (default, DEFAULT_TASK_LANE, 2),
        (routed, BULK, 4),
        (bulk, BULK, 6),
    ] {
        wait_status(&site, id, TaskStatus::Succeeded).await?;
        assert_routed_result(&tasks, id, lane, value, retention).await?;
    }
    assert_eq!(tasks.submit(Calculate(2)).await?, TaskReceipt::KeyHeld);
    let replacement = tasks
        .submit_with(Calculate(1), TaskOptions::new().lane(BULK))
        .await?;
    assert!(matches!(replacement, TaskReceipt::Queued(id) if id != default));
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Checks the persisted routing, result, and lane-specific retention after completion.
async fn assert_routed_result(
    tasks: &Tasks,
    id: TaskId,
    lane: TaskLane,
    value: u32,
    retention: Duration,
) -> Result<(), TestError> {
    let task = tasks.get(id).await?.ok_or(TestError::Missing)?;
    assert_eq!(task.lane(), lane.as_str());
    assert_eq!(task.last_result::<u32>()?, Some(Ok(value)));
    let expires_after = task
        .idempotency_expires_at()
        .zip(task.completed_at())
        .and_then(|(expiry, completion)| (expiry - completion).to_std().ok());
    assert_eq!(
        expires_after,
        (lane != DEFAULT_TASK_LANE).then_some(retention)
    );
    Ok(())
}

async fn await_response(
    key: IdempotencyKey,
    StepAttempt(attempt): StepAttempt,
    continuation: Continuation<u32, u32>,
    _: Data<AwaitResponse>,
) -> Result<WorkState<u32>, WorkError> {
    if key.as_deref() != Some("response:✓") {
        return Err(WorkError::fail("Work did not receive its persisted key"));
    }
    match (attempt, continuation.into_parts()) {
        (1, (None, None)) => Err(WorkError::retry("retry before checkpoint")),
        (2, (None, None)) => Ok(WorkState::suspend(7u32)?),
        (1, (Some(checkpoint), Some(response))) => Ok(WorkState::complete(checkpoint + response?)),
        _ => Err(WorkError::fail("Unexpected step attempt or continuation")),
    }
}

/// Proves a Work handler sees attempts 1, 2, then 1 across retry, checkpoint, and resume.
#[tokio::test]
async fn direct_output_and_external_resume() -> Result<(), TestError> {
    let site = vyuh::Site::build(
        vyuh::SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(10))),
        bundles::bundle([
            __bundle_part_calculate(),
            bundles::work(
                await_response,
                TaskDefinition::new("await_response")
                    .idempotency(TaskIdempotency::new("1", |_| "response:✓".into())),
            ),
        ]),
    )
    .await?;
    let client = vyuh::testing::TestSite::new(site.clone());
    client.start_runtime().await?;
    let direct = site
        .tasks()
        .submit(Calculate(21))
        .await?
        .id()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("expected queued test task".into()))?;
    let suspended = site
        .tasks()
        .submit(AwaitResponse)
        .await?
        .id()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("expected queued test task".into()))?;
    wait_status(&site, direct, TaskStatus::Succeeded).await?;
    wait_status(&site, suspended, TaskStatus::Suspended).await?;
    assert_eq!(
        site.tasks()
            .get(direct)
            .await?
            .ok_or(TestError::Missing)?
            .last_result::<u32>()?,
        Some(Ok(42))
    );
    assert!(site.tasks().resume(suspended, 8u32).await?);
    wait_status(&site, suspended, TaskStatus::Succeeded).await?;
    let task = site
        .tasks()
        .get(suspended)
        .await?
        .ok_or(TestError::Missing)?;
    assert_eq!((task.attempts(), task.step_attempts()), (3, 1));
    assert_eq!(task.last_result::<u32>()?, Some(Ok(15)));
    client.shutdown_and_wait().await;
    Ok(())
}

/// Waits for committed status, not for synchronous handler completion.
async fn wait_status(site: &vyuh::Site, id: TaskId, status: TaskStatus) -> Result<(), TestError> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if site
                .tasks()
                .get(id)
                .await?
                .is_some_and(|task| task.status() == status)
            {
                return Ok::<_, TaskRuntimeError>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Site(#[from] vyuh::SiteError),
    #[error(transparent)]
    Runtime(#[from] TaskRuntimeError),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("missing task")]
    Missing,
}
