use std::{sync::Arc, time::Duration};

use super::*;
use crate::Data;
use crate::tasks::{
    DEFAULT_TASK_LANE, LanePoll, RegisteredTask, TaskConf, TaskLane, TaskRate, TaskRegistry,
    store::MemoryTaskStore,
};

const EMAIL: TaskLane = TaskLane::new("email");

#[path = "runner_cancellation.rs"]
mod cancellation;

#[path = "runner_all.rs"]
mod all_tests;
#[path = "runner_flow.rs"]
mod flow_tests;
static HOOK_GATE: tokio::sync::Notify = tokio::sync::Notify::const_new();

#[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
struct PanicJob;

#[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
struct BatchJob;

#[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
struct OtherBatchJob;

async fn panic_job(_: Data<PanicJob>) {
    panic!("deliberate task panic");
}

async fn batch_job(_: Data<super::super::Batch<BatchJob>>) {}

async fn batch_panic_job(_: Data<super::super::Batch<BatchJob>>) {
    panic!("deliberate batch task panic");
}

async fn other_batch_job(_: Data<super::super::Batch<OtherBatchJob>>) {}

async fn wait_lane_hook() -> Result<(), crate::Error> {
    HOOK_GATE.notified().await;
    Ok(())
}

async fn panic_lane_hook() -> Result<(), crate::Error> {
    panic!("deliberate lane hook panic");
}

fn panic_record() -> Result<Arc<TaskRecord>, TaskRuntimeError> {
    let now = chrono::Utc::now();
    Ok(Arc::new(TaskRecord {
        id: super::super::TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind: super::super::TaskKind::Work,
        name: "panic-job".into(),
        input: serde_json::to_string(&PanicJob)?,
        state: None,
        resume_input: None,
        status: super::super::TaskStatus::Running,
        cancelled: false,
        attempts: 1,
        step_attempts: 1,
        lane: DEFAULT_TASK_LANE.to_string(),
        lease_duration_ms: None,
        last_result: None,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        locked_by: Some("runner".into()),
        leased_until: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }))
}

/// Builds a two-lane runner with a claim batch smaller than global capacity.
fn lane_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(3)
        .batch_size(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 2))
        .lane(
            TaskLaneConf::new(EMAIL, 1)
                .rate_limit(TaskRate::per_minute(1).burst(1))
                .global_rate_limit(TaskRate::per_minute(1).burst(1)),
        );
    let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn prefetch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(4)
        .batch_size(4)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 4));
    let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(4)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn batch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(4)
        .batch_size(4)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 4));
    let mut registry = TaskRegistry::new();
    registry.register(RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("batch-job"),
        batch_job,
    ))?;
    let registry = Arc::new(registry.with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(4)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn locked_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(2)
        .batch_size(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(TaskLaneConf::new(EMAIL, 1).lock(super::super::TaskLaneLock::new(2)));
    let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn locked_batch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(2)
        .batch_size(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(TaskLaneConf::new(EMAIL, 1).lock(super::super::TaskLaneLock::new(2)));
    let mut registry = TaskRegistry::new();
    registry.register(RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("batch-job").lane(EMAIL),
        batch_job,
    ))?;
    let registry = Arc::new(registry.with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn multi_batch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(4)
        .batch_size(4)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 4));
    let mut registry = TaskRegistry::new();
    registry.register(RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("batch-job"),
        batch_job,
    ))?;
    registry.register(RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("other-batch-job"),
        other_batch_job,
    ))?;
    let registry = Arc::new(registry.with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(4)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn locked_rate_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(2)
        .batch_size(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(
            TaskLaneConf::new(EMAIL, 1)
                .rate_limit(TaskRate::per_minute(1).burst(1))
                .lock(super::super::TaskLaneLock::new(2)),
        );
    let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn locked_batch_rate_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(2)
        .batch_size(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(
            TaskLaneConf::new(EMAIL, 1)
                .rate_limit(TaskRate::new(1, Duration::from_millis(10)).burst(1))
                .lock(super::super::TaskLaneLock::new(2)),
        );
    let mut registry = TaskRegistry::new();
    registry.register(RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("batch-job").lane(EMAIL),
        batch_job,
    ))?;
    let registry = Arc::new(registry.with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

fn lane_record(lane: TaskLane) -> Result<Arc<TaskRecord>, TaskRuntimeError> {
    let mut record = panic_record()?.as_ref().clone();
    record.lane = lane.to_string();
    Ok(Arc::new(record))
}

fn named_record(name: &str) -> Result<Arc<TaskRecord>, TaskRuntimeError> {
    let mut record = panic_record()?.as_ref().clone();
    record.name = name.into();
    Ok(Arc::new(record))
}

/// Verifies batch handlers drain only matching work already in the local queue.
#[test]
fn batch_dispatch_groups_matching_queue_rows() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.tasks.push_back(named_record("batch-job")?);
    lane.tasks.push_back(named_record("ordinary-job")?);
    lane.tasks.push_back(named_record("batch-job")?);

    let (_, _, records) = runner
        .pop_ready()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("batch was not dispatched".into()))?;
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| record.name() == "batch-job"));
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    assert_eq!(lane.running, 1);
    assert_eq!(
        lane.tasks.front().map(|record| record.name()),
        Some("ordinary-job")
    );
    Ok(())
}

/// Verifies interleaved batch-handler names each retain their own stable local ordering.
#[test]
fn batch_dispatch_keeps_handler_groups_separate() -> Result<(), TaskRuntimeError> {
    let mut runner = multi_batch_runner()?;
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.tasks.push_back(named_record("batch-job")?);
    lane.tasks.push_back(named_record("other-batch-job")?);
    lane.tasks.push_back(named_record("ordinary-job")?);
    lane.tasks.push_back(named_record("batch-job")?);
    lane.tasks.push_back(named_record("other-batch-job")?);

    let (_, _, first) = runner.pop_ready().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("first batch was not dispatched".into())
    })?;
    assert_eq!(first.len(), 2);
    assert!(first.iter().all(|record| record.name() == "batch-job"));
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.running = 0;

    let (_, _, second) = runner.pop_ready().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("second batch was not dispatched".into())
    })?;
    assert_eq!(second.len(), 2);
    assert!(
        second
            .iter()
            .all(|record| record.name() == "other-batch-job")
    );
    assert_eq!(
        runner
            .lane_mut(DEFAULT_TASK_LANE)
            .and_then(|lane| lane.tasks.front())
            .map(|record| record.name()),
        Some("ordinary-job")
    );
    Ok(())
}

/// Verifies local handler grouping is unchanged when its lane has durable ownership.
#[test]
fn locked_lane_batching_remains_local() -> Result<(), TaskRuntimeError> {
    let mut runner = locked_batch_runner()?;
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    lane.owner_token = Some("owner".into());
    lane.tasks.push_back(lane_record(EMAIL).map(|record| {
        let mut record = record.as_ref().clone();
        record.name = "batch-job".into();
        Arc::new(record)
    })?);
    lane.tasks.push_back(lane_record(EMAIL).map(|record| {
        let mut record = record.as_ref().clone();
        record.name = "batch-job".into();
        Arc::new(record)
    })?);

    let (claimed_lane, owner_token, records) = runner.pop_ready().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("locked batch was not dispatched".into())
    })?;
    assert_eq!(claimed_lane, EMAIL);
    assert_eq!(owner_token.as_deref(), Some("owner"));
    assert_eq!(records.len(), 2);
    assert_eq!(runner.lane_mut(EMAIL).map(|lane| lane.running), Some(1));
    Ok(())
}

/// Verifies one local batch invocation consumes one global slot while retaining both commits.
#[tokio::test]
async fn batch_invocation_uses_one_global_slot_and_commits_every_member() -> Result<(), String> {
    let mut runner = batch_runner().map_err(|error| error.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| "default lane is missing".to_string())?;
    lane.tasks
        .push_back(named_record("batch-job").map_err(|error| error.to_string())?);
    lane.tasks
        .push_back(named_record("batch-job").map_err(|error| error.to_string())?);
    let (sender, mut receiver) = mpsc::channel(1);
    runner.dispatch_ready(&site, &sender);
    assert_eq!(runner.running, 1);
    assert_eq!(runner.running_tasks.len(), 2);
    let completion = receiver
        .recv()
        .await
        .ok_or_else(|| "batch completion is missing".to_string())?;
    assert_eq!(completion.commits.len(), 2);
    let mut commits = Vec::new();
    runner.accept_completion(completion, &mut commits);
    assert_eq!(runner.running, 0);
    assert_eq!(commits.len(), 2);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Verifies losing one constituent lease aborts and removes the whole batch future.
#[tokio::test]
async fn batch_lease_loss_aborts_invocation() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let first = named_record("batch-job")?;
    let second = named_record("batch-job")?;
    let invocation_id = uuid::Uuid::now_v7();
    let future = tokio::spawn(std::future::pending::<()>());
    let invocation = RunningInvocation {
        lane: DEFAULT_TASK_LANE,
        owner_token: None,
        task_ids: vec![first.id(), second.id()],
        abort: future.abort_handle(),
    };
    runner.running = 1;
    runner.running_tasks.insert(first.id(), invocation_id);
    runner.running_tasks.insert(second.id(), invocation_id);
    runner.running_invocations.insert(invocation_id, invocation);
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.running = 1;

    runner.drop_lost(&[first.id()]);

    assert!(runner.running_invocations.is_empty());
    assert!(runner.running_tasks.is_empty());
    assert_eq!(runner.running, 0);
    assert!(future.await.is_err());
    Ok(())
}

/// Verifies a completion sent after batch lease loss cannot create stale commits.
#[tokio::test]
async fn stale_batch_completion_is_ignored_after_lease_loss() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let first = named_record("batch-job")?;
    let second = named_record("batch-job")?;
    let invocation_id = uuid::Uuid::now_v7();
    let future = tokio::spawn(std::future::pending::<()>());
    runner.running = 1;
    runner.running_tasks.insert(first.id(), invocation_id);
    runner.running_tasks.insert(second.id(), invocation_id);
    runner.running_invocations.insert(
        invocation_id,
        RunningInvocation {
            lane: DEFAULT_TASK_LANE,
            owner_token: None,
            task_ids: vec![first.id(), second.id()],
            abort: future.abort_handle(),
        },
    );
    runner.drop_lost(&[first.id()]);
    let mut commits = Vec::new();
    runner.accept_completion(
        Completion {
            invocation_id,
            lane: DEFAULT_TASK_LANE,
            commits: vec![
                TaskCommit {
                    task_id: first.id(),
                    lane: DEFAULT_TASK_LANE,
                    outcome: super::super::TaskOutcome::Complete,
                    owner_token: None,
                },
                TaskCommit {
                    task_id: second.id(),
                    lane: DEFAULT_TASK_LANE,
                    outcome: super::super::TaskOutcome::Complete,
                    owner_token: None,
                },
            ],
        },
        &mut commits,
    );
    assert!(commits.is_empty());
    assert!(runner.pending_commits.is_empty());
    assert!(future.await.is_err());
    Ok(())
}

/// Verifies a fenced lane-owner loss aborts every local batch member and clears queued work.
#[tokio::test]
async fn owner_loss_aborts_the_entire_batch_invocation() -> Result<(), String> {
    let mut runner = locked_batch_runner().map_err(|error| error.to_string())?;
    let first = named_record("batch-job").map_err(|error| error.to_string())?;
    let second = named_record("batch-job").map_err(|error| error.to_string())?;
    let invocation_id = uuid::Uuid::now_v7();
    let future = tokio::spawn(std::future::pending::<()>());
    runner.running = 1;
    runner.running_tasks.insert(first.id(), invocation_id);
    runner.running_tasks.insert(second.id(), invocation_id);
    runner.running_invocations.insert(
        invocation_id,
        RunningInvocation {
            lane: EMAIL,
            owner_token: Some("owner".into()),
            task_ids: vec![first.id(), second.id()],
            abort: future.abort_handle(),
        },
    );
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| "locked lane is missing".to_string())?;
    lane.owner_token = Some("owner".into());
    lane.running = 1;
    lane.tasks.push_back(first.clone());
    runner.pending_commits.push_back(TaskCommit {
        task_id: second.id(),
        lane: EMAIL,
        outcome: super::super::TaskOutcome::Complete,
        owner_token: Some("owner".into()),
    });
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let (hook_sender, _hook_receiver) = mpsc::channel(1);
    runner.apply_poll_with_hooks(
        &site,
        &hook_sender,
        TaskPoll {
            lanes: vec![super::super::LanePoll {
                lane: EMAIL,
                tasks: Vec::new(),
                reclaimed: 0,
                saturated: false,
                next_wake_in: None,
                owner: Some(super::super::LaneOwnerPoll {
                    token: None,
                    generation: 2,
                    phase: super::super::LaneOwnerPhase::Active,
                    action: None,
                    takeover: false,
                }),
            }],
        },
        false,
    );
    assert!(runner.running_invocations.is_empty());
    assert!(runner.running_tasks.is_empty());
    assert!(runner.pending_commits.is_empty());
    assert_eq!(runner.lane_mut(EMAIL).map(|lane| lane.tasks.len()), Some(0));
    assert!(future.await.is_err());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Verifies completed rows awaiting a bounded commit remain centrally renewable.
#[test]
fn pending_batch_commits_retain_leases() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    let record = named_record("batch-job")?;
    runner.pending_commits.push_back(TaskCommit {
        task_id: record.id(),
        lane: DEFAULT_TASK_LANE,
        outcome: super::super::TaskOutcome::Complete,
        owner_token: None,
    });
    let leases = runner.renewal_leases(&[]);
    assert_eq!(leases.len(), 1);
    assert_eq!(leases.first().map(|lease| lease.task_id), Some(record.id()));
    Ok(())
}

fn hooked_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let lane_lock = super::super::TaskLaneLock::new(2)
        .on_idle(wait_lane_hook)
        .on_busy(wait_lane_hook);
    let conf = TaskConf::default()
        .concurrency(2)
        .batch_size(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(TaskLaneConf::new(EMAIL, 1).lock(lane_lock));
    let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
    AbstractTaskRunner::new(dispatcher)
}

/// Verifies lane leases join central paced polling only at work or renewal deadlines.
#[test]
fn lane_lease_renewal_uses_the_central_claim_turn() -> Result<(), TaskRuntimeError> {
    let mut runner = locked_runner()?;
    let now = tokio::time::Instant::now();
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    lane.owner_token = Some("owner".into());
    lane.owner_renew_at = Some(now + Duration::from_secs(30));
    lane.poll_after = now + Duration::from_secs(60);
    assert!(runner.claims(true).iter().all(|claim| claim.lane != EMAIL));

    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    lane.owner_renew_at = Some(now);
    let claims = runner.claims(false);
    let renewal = claims
        .iter()
        .find(|claim| claim.lane == EMAIL)
        .and_then(|claim| claim.owner.as_ref())
        .ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("lane renewal claim is missing".into())
        })?;
    assert!(!renewal.allow_claim);
    Ok(())
}

/// Verifies a running hook leaves its owner renewal on the central scheduler turn.
#[tokio::test]
async fn running_hook_keeps_lane_renewal_centrally_scheduled() -> Result<(), String> {
    let mut runner = hooked_runner().map_err(|error| error.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let now = tokio::time::Instant::now();
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| "locked lane is missing".to_string())?;
    lane.owner_token = Some("owner".into());
    lane.owner_renew_at = Some(now);
    let (sender, _receiver) = mpsc::channel(1);
    runner.spawn_hook(
        site.clone(),
        sender,
        EMAIL,
        "owner".into(),
        1,
        super::super::LaneHookAction::Idle,
    );
    let renewal = runner
        .claims(false)
        .into_iter()
        .find(|claim| claim.lane == EMAIL)
        .and_then(|claim| claim.owner)
        .ok_or_else(|| "hook owner renewal claim is missing".to_string())?;
    assert_eq!(renewal.token.as_deref(), Some("owner"));
    assert!(!renewal.allow_claim);
    runner.abort_hooks();
    site.shutdown_and_wait().await;
    Ok(())
}

/// Verifies locked cohorts apply local start limits while retaining claimed task leases.
#[test]
fn locked_lane_rates_tasks_at_dispatch() -> Result<(), TaskRuntimeError> {
    let mut runner = locked_rate_runner()?;
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    lane.owner_token = Some("owner".into());
    lane.tasks.push_back(lane_record(EMAIL)?);
    lane.tasks.push_back(lane_record(EMAIL)?);

    assert!(runner.pop_ready().is_some());
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    lane.running = 0;
    let before = tokio::time::Instant::now();
    assert!(runner.pop_ready().is_none());
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    assert!(lane.poll_after.duration_since(before) >= Duration::from_secs(59));
    Ok(())
}

/// Verifies local rate availability may split one locked lane's batch-handler queue.
#[tokio::test]
async fn locked_lane_rate_limit_splits_a_local_batch() -> Result<(), TaskRuntimeError> {
    let mut runner = locked_batch_rate_runner()?;
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    lane.owner_token = Some("owner".into());
    lane.tasks.push_back(lane_record(EMAIL).map(|record| {
        let mut record = record.as_ref().clone();
        record.name = "batch-job".into();
        Arc::new(record)
    })?);
    lane.tasks.push_back(lane_record(EMAIL).map(|record| {
        let mut record = record.as_ref().clone();
        record.name = "batch-job".into();
        Arc::new(record)
    })?);
    let (_, _, first) = runner.pop_ready().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("first batch member was not dispatched".into())
    })?;
    assert_eq!(first.len(), 1);
    runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?
        .running = 0;
    tokio::time::sleep(Duration::from_millis(12)).await;
    let (_, _, second) = runner.pop_ready().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("second batch member was not dispatched".into())
    })?;
    assert_eq!(second.len(), 1);
    Ok(())
}

/// Verifies a spawned lane hook neither consumes task concurrency nor blocks other futures.
#[tokio::test]
async fn lane_hook_runs_outside_task_concurrency() -> Result<(), String> {
    let mut runner = hooked_runner().map_err(|error| error.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::Bundle::default(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let (sender, _receiver) = mpsc::channel(1);
    runner.spawn_hook(
        site.clone(),
        sender,
        EMAIL,
        "owner".into(),
        1,
        super::super::LaneHookAction::Idle,
    );
    runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| "default lane is missing".to_string())?
        .tasks
        .push_back(panic_record().map_err(|error| error.to_string())?);
    let (task_sender, mut task_receiver) = mpsc::channel(1);
    runner.dispatch_ready(&site, &task_sender);
    let completion = tokio::time::timeout(Duration::from_millis(50), task_receiver.recv())
        .await
        .map_err(|_| "task dispatch was blocked by lane hook".to_string())?
        .ok_or_else(|| "task completion channel closed".to_string())?;
    assert_eq!(completion.commits.len(), 1);
    let mut commits = Vec::new();
    runner.accept_completion(completion, &mut commits);
    assert_eq!(commits.len(), 1);
    assert_eq!(runner.running, 0);
    assert!(runner.running_tasks.is_empty());
    assert_eq!(runner.running_hooks.len(), 1);
    runner.drop_lane(EMAIL);
    assert!(runner.running_hooks.is_empty());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Verifies a hook completion arriving after ownership loss cannot alter lane state.
#[tokio::test]
async fn late_hook_completion_is_rejected_after_ownership_loss() -> Result<(), String> {
    let mut runner = hooked_runner().map_err(|error| error.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let (sender, _receiver) = mpsc::channel(1);
    runner.spawn_hook(
        site.clone(),
        sender,
        EMAIL,
        "old-owner".into(),
        3,
        super::super::LaneHookAction::Idle,
    );
    runner.drop_lane(EMAIL);
    runner
        .lane_mut(EMAIL)
        .ok_or_else(|| "locked lane is missing".to_string())?
        .owner_token = None;
    runner.accept_hook(HookCompletion {
        lane: EMAIL,
        token: "old-owner".into(),
        result: super::super::LaneHookResult {
            generation: 3,
            action: super::super::LaneHookAction::Idle,
            result: Ok(()),
        },
    });
    assert!(runner.running_hooks.is_empty());
    assert!(
        runner
            .lane_mut(EMAIL)
            .and_then(|lane| lane.hook_result.as_ref())
            .is_none()
    );
    site.shutdown_and_wait().await;
    Ok(())
}

/// Verifies a panicking lifecycle hook becomes a fenced failure completion.
#[tokio::test]
async fn lane_hook_panics_are_contained() -> Result<(), String> {
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::Bundle::default(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let lane_lock = super::super::TaskLaneLock::new(1)
        .on_idle(panic_lane_hook)
        .on_busy(panic_lane_hook);
    let hook = lane_lock
        .idle_hook()
        .cloned()
        .ok_or_else(|| "idle hook is missing".to_string())?;
    let (sender, mut receiver) = mpsc::channel(1);
    let call = HookCall {
        hook,
        site: site.clone(),
        lane: EMAIL,
        token: "owner".into(),
        generation: 1,
        action: super::super::LaneHookAction::Idle,
        error_limit: 1024,
    };
    execute_hook(call, sender).await;
    let completion = receiver
        .recv()
        .await
        .ok_or_else(|| "hook completion is missing".to_string())?;
    assert!(completion.result.result.is_err());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Verifies per-lane claims never reserve more than one global persistence batch.
#[test]
fn claims_share_global_capacity_and_batch_budget() -> Result<(), TaskRuntimeError> {
    let mut runner = lane_runner()?;
    let first = runner.claims(true);
    assert_eq!(first.iter().map(|claim| claim.limit).sum::<usize>(), 2);
    assert_eq!(
        first.first().map(|claim| claim.lane),
        Some(DEFAULT_TASK_LANE)
    );

    runner.rotate();
    let second = runner.claims(true);
    assert_eq!(second.iter().map(|claim| claim.limit).sum::<usize>(), 2);
    assert_eq!(second.first().map(|claim| claim.lane), Some(EMAIL));
    Ok(())
}

/// Verifies a local rate bucket bounds claims before any task lease is acquired.
#[test]
fn local_rate_limits_lane_claim_budget() -> Result<(), TaskRuntimeError> {
    let mut runner = lane_runner()?;
    runner.rotate();
    let claims = runner.claims(true);
    assert_eq!(
        claims
            .iter()
            .find(|claim| claim.lane == EMAIL)
            .map(|claim| claim.limit),
        Some(1)
    );

    let now = tokio::time::Instant::now();
    let email = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    email.consume_local_rate(1, now);
    assert_eq!(email.claim_limit(1, now), 0);
    assert!(email.local_rate_wake(now).is_some());
    Ok(())
}

/// Verifies a lane refills only after queued work falls below half its capacity.
#[test]
fn lane_prefetch_uses_a_half_capacity_low_watermark() -> Result<(), TaskRuntimeError> {
    let mut runner = prefetch_runner()?;
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.tasks.push_back(panic_record()?);
    lane.tasks.push_back(panic_record()?);

    assert!(runner.claims(true).is_empty());

    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.tasks.pop_back();
    let claims = runner.claims(true);
    assert_eq!(claims.first().map(|claim| claim.limit), Some(3));
    Ok(())
}

/// Verifies local wakeups move a fallback tick forward without breaking poll pacing.
#[test]
fn local_wake_waits_for_the_next_legal_tick() -> Result<(), TaskRuntimeError> {
    let runner = prefetch_runner()?;
    let now = tokio::time::Instant::now();
    let mut state = RunState {
        last_tick: now,
        next_poll: now + runner.fallback_interval,
        poll_error: runner.poll_interval,
        commits: Vec::new(),
        shutting_down: false,
    };

    runner.schedule_tick(&mut state);

    assert!(state.next_poll >= now + runner.poll_interval);
    assert!(state.next_poll < now + runner.fallback_interval);
    Ok(())
}

/// Committed workflow wakes advance fallback polling without same-turn dispatch.
#[tokio::test]
async fn workflow_wake_obeys_poll_gate() -> Result<(), String> {
    let mut runner = prefetch_runner().map_err(|e| e.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|e| e.to_string())?;
    let now = tokio::time::Instant::now();
    let mut state = RunState {
        last_tick: now,
        next_poll: now + runner.fallback_interval,
        poll_error: runner.poll_interval,
        commits: Vec::new(),
        shutting_down: false,
    };
    let (sender, mut receiver) = mpsc::channel(1);
    let (hooks, _) = mpsc::channel(1);
    runner.apply_tick(
        &site,
        &sender,
        &hooks,
        &mut state,
        TickResult {
            claims: Vec::new(),
            renewals: Vec::new(),
            started: std::time::Instant::now(),
            tick: super::super::TaskTick {
                poll: super::super::TaskPoll { lanes: Vec::new() },
                lost: Vec::new(),
                cancelled: Vec::new(),
                wake_lanes: vec![DEFAULT_TASK_LANE],
            },
        },
    );
    let expected = now + runner.poll_interval;
    assert_eq!(
        runner.lane_mut(DEFAULT_TASK_LANE).unwrap().poll_after,
        expected
    );
    assert_eq!(state.next_poll, now + runner.poll_interval);
    assert!(receiver.try_recv().is_err());
    Ok(())
}

/// Verifies an empty local permit budget preserves its next-token wake deadline.
#[test]
fn local_rate_wait_does_not_fall_back_to_idle_polling() -> Result<(), TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(1)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1).rate_limit(TaskRate::per_minute(1).burst(1)));
    let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
    let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(1)), Vec::new());
    let mut runner = AbstractTaskRunner::new(dispatcher)?;
    let now = tokio::time::Instant::now();
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.consume_local_rate(1, now);

    assert!(runner.claims(true).is_empty());
    let deadline = runner.next_lane_deadline(now + runner.fallback_interval);
    assert!(deadline.duration_since(now) >= tokio::time::Duration::from_secs(59));
    assert!(deadline.duration_since(now) < runner.fallback_interval);
    Ok(())
}

/// Verifies saturated work uses the short interval while idle work uses the fallback.
#[test]
fn adaptive_deadlines_distinguish_backlog_and_idle() -> Result<(), TaskRuntimeError> {
    let mut runner = lane_runner()?;
    let before_idle = tokio::time::Instant::now();
    let idle = runner.apply_poll(TaskPoll::empty());
    assert!(idle.duration_since(before_idle) >= tokio::time::Duration::from_secs(299));

    let before_hot = tokio::time::Instant::now();
    let hot = runner.apply_poll(TaskPoll {
        lanes: vec![LanePoll {
            lane: DEFAULT_TASK_LANE,
            tasks: Vec::new(),
            reclaimed: 0,
            saturated: true,
            next_wake_in: None,
            owner: None,
        }],
    });
    assert!(hot.duration_since(before_hot) <= tokio::time::Duration::from_millis(1_100));
    Ok(())
}

/// Verifies each lane retains its own rate deadline while another lane stays hot.
#[test]
fn adaptive_deadlines_are_isolated_by_lane() -> Result<(), TaskRuntimeError> {
    let mut runner = lane_runner()?;
    let now = tokio::time::Instant::now();
    let earliest = runner.apply_poll(TaskPoll {
        lanes: vec![
            LanePoll {
                lane: DEFAULT_TASK_LANE,
                tasks: Vec::new(),
                reclaimed: 0,
                saturated: true,
                next_wake_in: None,
                owner: None,
            },
            LanePoll {
                lane: EMAIL,
                tasks: Vec::new(),
                reclaimed: 0,
                saturated: true,
                next_wake_in: Some(std::time::Duration::from_secs(60)),
                owner: None,
            },
        ],
    });
    assert!(earliest.duration_since(now) <= tokio::time::Duration::from_millis(1_100));
    let email = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    assert!(email.poll_after.duration_since(now) >= tokio::time::Duration::from_secs(60));
    Ok(())
}

/// Verifies a capacity-blocked lane cannot keep the scheduler in an expired poll loop.
#[test]
fn unavailable_lanes_do_not_control_next_poll() -> Result<(), TaskRuntimeError> {
    let mut runner = lane_runner()?;
    let now = tokio::time::Instant::now();
    let default = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    default.running = default.conf.concurrency();
    default.poll_after = now;
    let email = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    email.poll_after = now + tokio::time::Duration::from_secs(60);
    let deadline = runner.next_lane_deadline(now + runner.fallback_interval);
    assert!(deadline.duration_since(now) >= tokio::time::Duration::from_secs(60));
    Ok(())
}

/// Verifies malformed custom-store polling evidence fails before queue mutation.
#[test]
fn lane_poll_evidence_must_match_claims() {
    let claims = [LaneClaim {
        lane: DEFAULT_TASK_LANE,
        limit: 1,
        owner: None,
    }];
    assert!(matches!(
        validate_poll(&claims, &TaskPoll::empty()),
        Err(TaskRuntimeError::TaskExecutionError(_))
    ));
}

/// Verifies a panicking handler becomes one generic terminal failure completion.
#[tokio::test]
async fn handler_panics_are_contained_as_terminal_failures() -> Result<(), String> {
    let mut registry = TaskRegistry::new()
        .with_config(TaskConf::default())
        .map_err(|error| error.to_string())?;
    registry
        .register(RegisteredTask::new(
            crate::tasks::TaskDefinition::new("panic-job"),
            panic_job,
        ))
        .map_err(|error| error.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let (sender, mut receiver) = mpsc::channel(1);
    execute_task(TaskExecution {
        invocation_id: uuid::Uuid::now_v7(),
        engine: Arc::new(registry),
        site,
        records: vec![panic_record().map_err(|error| error.to_string())?],
        sender,
        lane: DEFAULT_TASK_LANE,
        metrics: Arc::new(super::super::TaskMetrics::new(
            ["panic-job".into()],
            [DEFAULT_TASK_LANE.to_string()],
        )),
        payload_limit: 1024,
        error_limit: 1024,
        owner_token: None,
    })
    .await;
    let completion = receiver.recv().await.ok_or("missing panic completion")?;
    let commit = completion.commits.first().ok_or("missing panic commit")?;
    assert!(matches!(
        commit.outcome,
        super::super::TaskOutcome::Fail { ref error } if error == "Task handler panicked"
    ));
    Ok(())
}

/// Verifies a panicking batch handler fails every constituent task without escaping the runner.
#[tokio::test]
async fn batch_handler_panics_are_contained_for_every_member() -> Result<(), String> {
    let mut registry = TaskRegistry::new()
        .with_config(TaskConf::default())
        .map_err(|error| error.to_string())?;
    registry
        .register(RegisteredTask::new_batch(
            crate::tasks::TaskDefinition::new("batch-panic-job"),
            batch_panic_job,
        ))
        .map_err(|error| error.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let first = named_record("batch-panic-job").map_err(|error| error.to_string())?;
    let second = named_record("batch-panic-job").map_err(|error| error.to_string())?;
    let (sender, mut receiver) = mpsc::channel(1);
    execute_task(TaskExecution {
        invocation_id: uuid::Uuid::now_v7(),
        engine: Arc::new(registry),
        site,
        records: vec![first, second],
        sender,
        lane: DEFAULT_TASK_LANE,
        metrics: Arc::new(super::super::TaskMetrics::new(
            ["batch-panic-job".into()],
            [DEFAULT_TASK_LANE.to_string()],
        )),
        payload_limit: 1024,
        error_limit: 1024,
        owner_token: None,
    })
    .await;
    let completion = receiver
        .recv()
        .await
        .ok_or("missing batch panic completion")?;
    assert_eq!(completion.commits.len(), 2);
    assert!(completion.commits.iter().all(|commit| matches!(
        &commit.outcome,
        super::super::TaskOutcome::Fail { error } if error == "Task handler panicked"
    )));
    Ok(())
}
