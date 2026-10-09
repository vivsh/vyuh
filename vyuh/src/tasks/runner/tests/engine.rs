use std::{sync::Arc, time::Duration};

use super::*;
use crate::Data;
use crate::tasks::runner::execution::execute_task;
use crate::tasks::runner::lanes::{HookCall, execute_hook, validate_poll};
use crate::tasks::{
    DEFAULT_TASK_LANE, RegisteredTask, TaskConf, TaskLane, TaskRegistry, store::MemoryTaskStore,
};

const EMAIL: TaskLane = TaskLane::new("email");

/// Per-lane claim limits do not inherit either channel capacity or output batch size.
#[tokio::test]
async fn independent_claim_and_ingress() -> Result<(), TaskRuntimeError> {
    let config = TaskConf::default()
        .concurrency(10)
        .commit_batch_size(1)
        .ingress_capacity(7)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 5).claim_batch_size(3))
        .lane(TaskLaneConf::new(EMAIL, 5).claim_batch_size(2));
    let registry = Arc::new(TaskRegistry::new().with_config(config)?);
    let store = Arc::new(MemoryTaskStore::new(1, registry.store_conf()?));
    let mut runner = TaskRunner::new(registry.client(store, Vec::new()))?;
    assert_eq!(runner.ingress_tx.max_capacity(), 7);
    assert_eq!(runner.commit_batch_size, 1);
    for _ in 0..3 {
        runner.output_buffer.push_back(TaskCommit::Cancel {
            task_id: crate::tasks::TaskId::new(uuid::Uuid::now_v7()),
        });
    }
    let Some(DbRequest::Commit { commits, .. }) = runner.flush_request() else {
        return Err(TaskRuntimeError::TaskExecutionError("missing flush".into()));
    };
    assert_eq!(commits.len(), 1);
    assert_eq!(runner.output_buffer.len(), 2);
    for expected in [3, 2] {
        let Some(DbRequest::Claim { claims }) = runner.claim_request() else {
            return Err(TaskRuntimeError::TaskExecutionError("missing claim".into()));
        };
        assert_eq!(claims.first().map(|claim| claim.limit), Some(expected));
    }
    Ok(())
}

/// Admission reuses capacity within a pass but re-derives it after each reserved claim.
#[tokio::test]
async fn claim_capacity_between_admissions() -> Result<(), String> {
    let config = TaskConf::default()
        .concurrency(4)
        .db_concurrency(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 2))
        .lane(TaskLaneConf::new(EMAIL, 2));
    let registry = Arc::new(
        TaskRegistry::new()
            .with_config(config)
            .map_err(|e| e.to_string())?,
    );
    let store = Arc::new(MemoryTaskStore::new(
        32,
        registry.store_conf().map_err(|e| e.to_string())?,
    ));
    let mut runner =
        TaskRunner::new(registry.client(store, Vec::new())).map_err(|e| e.to_string())?;
    for (expected, capacity) in [(DEFAULT_TASK_LANE, 2), (EMAIL, 0)] {
        let request = runner.claim_request().ok_or("missing claim")?;
        let DbRequest::Claim { ref claims } = request else {
            return Err("not a claim".into());
        };
        let claim = claims.first().ok_or("missing lane")?;
        assert_eq!((claim.lane, claim.limit), (expected, 2));
        let permit = runner
            .db_slots
            .clone()
            .try_acquire_owned()
            .map_err(|e| e.to_string())?;
        runner.start_database(request, permit);
        assert_eq!(runner.claim_capacity(), capacity);
    }
    assert!(runner.claim_request().is_none());
    assert!(runner.deadline().is_none());
    Ok(())
}

#[path = "runner_flow.rs"]
mod flow_tests;
static HOOK_GATE: tokio::sync::Notify = tokio::sync::Notify::const_new();

#[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
struct PanicJob;

#[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
struct BatchJob;

async fn panic_job(_: Data<PanicJob>) {
    panic!("deliberate task panic");
}

async fn batch_panic_job(_: Data<crate::tasks::Batch<BatchJob>>) {
    panic!("deliberate batch task panic");
}

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
        id: crate::tasks::TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind: crate::tasks::TaskKind::Work,
        name: "panic-job".into(),
        input: serde_json::to_string(&PanicJob)?,
        state: None,
        resume_input: None,
        status: crate::tasks::TaskStatus::Running,
        cancelled: false,
        attempts: 1,
        step_attempts: 1,
        lane: DEFAULT_TASK_LANE.to_string(),
        last_result: None,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        node_id: Some(uuid::Uuid::from_u128(1)),
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }))
}

fn hooked_runner() -> Result<TaskRunner<MemoryTaskStore>, TaskRuntimeError> {
    let lane_lock = crate::tasks::TaskLaneLock::new(2)
        .on_idle(wait_lane_hook)
        .on_busy(wait_lane_hook);
    let conf = TaskConf::default()
        .concurrency(2)
        .commit_batch_size(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(TaskLaneConf::new(EMAIL, 1).lock(lane_lock));
    let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
    let store = Arc::new(MemoryTaskStore::new(2, registry.store_conf()?));
    let client = registry.client(store, Vec::new());
    TaskRunner::new(client)
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
        crate::tasks::LaneHookAction::Idle,
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
    runner.accept_completion(completion);
    assert_eq!(runner.output_buffer.len(), 1);
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency
    );
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
        crate::tasks::LaneHookAction::Idle,
    );
    runner.drop_lane(EMAIL);
    runner
        .lane_mut(EMAIL)
        .ok_or_else(|| "locked lane is missing".to_string())?
        .owner_token = None;
    runner.accept_hook(HookCompletion {
        lane: EMAIL,
        token: "old-owner".into(),
        result: crate::tasks::LaneHookResult {
            generation: 3,
            action: crate::tasks::LaneHookAction::Idle,
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
    let lane_lock = crate::tasks::TaskLaneLock::new(1)
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
        action: crate::tasks::LaneHookAction::Idle,
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
        node_id: uuid::Uuid::from_u128(1),
        global_permit: Some(permit()?),
        lane_permit: Some(permit()?),
        invocation_id: uuid::Uuid::now_v7(),
        engine: Arc::new(registry),
        site,
        records: vec![panic_record().map_err(|error| error.to_string())?],
        sender,
        lane: DEFAULT_TASK_LANE,
        metrics: Arc::new(crate::tasks::TaskMetrics::new(
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
        commit.outcome(),
        Some(crate::tasks::TaskOutcome::Fail { error }) if error == "Task handler panicked"
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
        node_id: uuid::Uuid::from_u128(1),
        global_permit: Some(permit()?),
        lane_permit: Some(permit()?),
        invocation_id: uuid::Uuid::now_v7(),
        engine: Arc::new(registry),
        site,
        records: vec![first, second],
        sender,
        lane: DEFAULT_TASK_LANE,
        metrics: Arc::new(crate::tasks::TaskMetrics::new(
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
        &commit.outcome(),
        Some(crate::tasks::TaskOutcome::Fail { error }) if error == "Task handler panicked"
    )));
    Ok(())
}

/// Supplies an owned execution permit for direct invocation-containment tests.
fn permit() -> Result<OwnedSemaphorePermit, String> {
    Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .map_err(|error| error.to_string())
}

/// Supplies an immutable row for the named batch handler.
fn named_record(name: &str) -> Result<Arc<TaskRecord>, TaskRuntimeError> {
    let mut record = panic_record()?.as_ref().clone();
    record.name = name.into();
    Ok(Arc::new(record))
}
