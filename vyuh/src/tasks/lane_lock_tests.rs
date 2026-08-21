//! Locked-lane coordination contract tests for the reference memory store.

use std::time::Duration;

use super::{
    AbstractTaskStore, LaneClaim, LaneHookAction, LaneHookResult, LaneOwnerPhase, LaneOwnerRequest,
    LanePoll, TaskCommit, TaskError, TaskLane, TaskLaneConf, TaskLaneLock, TaskLease, TaskOutcome,
    TaskRate, TaskRecord, TaskStatus, TaskStoreConf, TaskWrite, store::MemoryTaskStore,
};

const GPU: TaskLane = TaskLane::new("gpu");

async fn lifecycle_hook() -> Result<(), crate::Error> {
    Ok(())
}

fn locked_conf(size: usize, deadline: Option<Duration>, hooks: bool) -> TaskStoreConf {
    let mut lane_lock = TaskLaneLock::new(size).idle_after(Duration::ZERO);
    if let Some(deadline) = deadline {
        lane_lock = lane_lock.deadline(deadline);
    }
    if hooks {
        lane_lock = lane_lock.on_idle(lifecycle_hook).on_busy(lifecycle_hook);
    }
    TaskStoreConf {
        handlers: vec!["job".into()],
        lanes: vec![TaskLaneConf::new(GPU, 1).lock(lane_lock)],
        idempotency: Vec::new(),
        schedules: Vec::new(),
        poll_interval: Duration::from_millis(10),
    }
}

fn debounced_conf(delay: Duration) -> TaskStoreConf {
    TaskStoreConf {
        handlers: vec!["job".into()],
        lanes: vec![
            TaskLaneConf::new(GPU, 1).lock(
                TaskLaneLock::new(1)
                    .idle_after(delay)
                    .on_idle(lifecycle_hook)
                    .on_busy(lifecycle_hook),
            ),
        ],
        idempotency: Vec::new(),
        schedules: Vec::new(),
        poll_interval: Duration::from_millis(10),
    }
}

fn task(created_at: chrono::DateTime<chrono::Utc>) -> TaskWrite {
    TaskWrite {
        record: TaskRecord {
            id: super::TaskId::new(uuid::Uuid::now_v7()),
            parent_id: None,
            root_id: None,
            kind: super::TaskKind::Work,
            name: "job".into(),
            input: "{}".into(),
            state: None,
            resume_input: None,
            status: TaskStatus::Pending,
            attempts: 0,
            lane: GPU.to_string(),
            lease_duration_ms: None,
            last_error: None,
            idempotency_key: None,
            idempotency_fingerprint: None,
            idempotency_expires_at: None,
            locked_by: None,
            leased_until: None,
            ready_at: Some(created_at),
            created_at,
            updated_at: created_at,
            completed_at: None,
        },
        ignore_conflicts: false,
        initial_delay: None,
    }
}

fn claim(token: Option<String>, hook: Option<LaneHookResult>) -> LaneClaim {
    LaneClaim {
        lane: GPU,
        limit: 64,
        owner: Some(LaneOwnerRequest {
            token,
            quiescent: true,
            allow_claim: true,
            completed_work: false,
            hook,
        }),
    }
}

fn completed_claim(token: String) -> LaneClaim {
    let mut claim = claim(Some(token), None);
    if let Some(owner) = claim.owner.as_mut() {
        owner.completed_work = true;
    }
    claim
}

async fn poll(
    store: &MemoryTaskStore,
    runner: &str,
    claim: LaneClaim,
) -> Result<LanePoll, TaskError> {
    store
        .claim_tasks(runner, &[claim])
        .await?
        .lanes
        .into_iter()
        .next()
        .ok_or_else(|| TaskError::TaskExecutionError("locked lane poll is missing".into()))
}

fn owner(poll: &LanePoll) -> Result<&super::LaneOwnerPoll, TaskError> {
    poll.owner
        .as_ref()
        .ok_or_else(|| TaskError::TaskExecutionError("lane owner evidence is missing".into()))
}

fn token(poll: &LanePoll) -> Result<String, TaskError> {
    owner(poll)?
        .token
        .clone()
        .ok_or_else(|| TaskError::TaskExecutionError("lane owner token is missing".into()))
}

fn hook_result(poll: &LanePoll, result: Result<(), &str>) -> Result<LaneHookResult, TaskError> {
    let owner = owner(poll)?;
    let action = owner
        .action
        .ok_or_else(|| TaskError::TaskExecutionError("lane hook action is missing".into()))?;
    Ok(LaneHookResult {
        generation: owner.generation,
        action,
        result: result.map_err(str::to_string),
    })
}

/// Verifies a locked lane leaves candidates pending until its exact threshold is ready.
#[tokio::test]
async fn locked_lane_claims_only_a_complete_cohort() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store.initialize(locked_conf(2, None, false)).await?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;

    let accumulating = poll(&store, "runner-a", claim(None, None)).await?;
    assert!(accumulating.tasks.is_empty());
    let owner_token = token(&accumulating)?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;

    let ready = poll(&store, "runner-a", claim(Some(owner_token), None)).await?;
    assert_eq!(ready.tasks.len(), 2);
    Ok(())
}

/// Verifies an expired batch deadline releases a bounded partial cohort.
#[tokio::test]
async fn locked_lane_deadline_claims_a_partial_cohort() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store
        .initialize(locked_conf(4, Some(Duration::from_millis(1)), false))
        .await?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;
    tokio::time::sleep(Duration::from_millis(2)).await;

    let ready = poll(&store, "runner-a", claim(None, None)).await?;
    assert_eq!(ready.tasks.len(), 1);
    Ok(())
}

/// Verifies a rate-limited partial flush remains open until its ready cohort drains.
#[tokio::test]
async fn locked_lane_continues_a_rate_limited_flush() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    let mut conf = locked_conf(2, None, false);
    conf.lanes = vec![
        TaskLaneConf::new(GPU, 1)
            .global_rate_limit(TaskRate::new(1, Duration::from_millis(10)).burst(1))
            .lock(TaskLaneLock::new(2)),
    ];
    store.initialize(conf).await?;
    store
        .store_tasks(vec![task(chrono::Utc::now()), task(chrono::Utc::now())])
        .await?;

    let first = poll(&store, "runner-a", claim(None, None)).await?;
    assert_eq!(first.tasks.len(), 1);
    let owner_token = token(&first)?;
    let task_id =
        first.tasks.first().map(TaskRecord::id).ok_or_else(|| {
            TaskError::TaskExecutionError("first rate permit claimed no task".into())
        })?;
    store
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id,
                lane: GPU,
                outcome: TaskOutcome::complete(),
                owner_token: Some(owner_token.clone()),
            }],
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(12)).await;

    let second = poll(&store, "runner-a", completed_claim(owner_token)).await?;
    assert_eq!(second.tasks.len(), 1);
    Ok(())
}

/// Verifies contenders cannot claim while a live owner retains the durable token.
#[tokio::test]
async fn locked_lane_allows_only_one_live_owner() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store.initialize(locked_conf(2, None, false)).await?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;

    let first = poll(&store, "runner-a", claim(None, None)).await?;
    assert!(owner(&first)?.token.is_some());
    let contender = poll(&store, "runner-b", claim(None, None)).await?;
    assert!(contender.tasks.is_empty());
    assert!(owner(&contender)?.token.is_none());
    Ok(())
}

/// Verifies simultaneous contenders can produce only one durable owner and one claimed task.
#[tokio::test]
async fn concurrent_locked_lane_contenders_have_one_winner() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store.initialize(locked_conf(1, None, false)).await?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;

    let (left, right) = tokio::join!(
        poll(&store, "runner-a", claim(None, None)),
        poll(&store, "runner-b", claim(None, None)),
    );
    let polls = [left?, right?];
    let owner_count = polls
        .iter()
        .filter(|poll| {
            owner(poll)
                .ok()
                .and_then(|owner| owner.token.as_ref())
                .is_some()
        })
        .count();
    let task_count = polls.iter().map(|poll| poll.tasks.len()).sum::<usize>();
    assert_eq!(owner_count, 1);
    assert_eq!(task_count, 1);
    Ok(())
}

/// Verifies takeover rejects stale task commits and task-lease renewals.
#[tokio::test]
async fn takeover_fences_stale_task_mutations() -> Result<(), TaskError> {
    let store = MemoryTaskStore::with_lease_duration(64, Duration::from_millis(10));
    store.initialize(locked_conf(1, None, false)).await?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;
    let first = poll(&store, "runner-a", claim(None, None)).await?;
    let stale_token = token(&first)?;
    let task_id = first
        .tasks
        .first()
        .map(TaskRecord::id)
        .ok_or_else(|| TaskError::TaskExecutionError("first owner claimed no task".into()))?;

    tokio::time::sleep(Duration::from_millis(20)).await;
    let takeover = poll(&store, "runner-b", claim(None, None)).await?;
    assert_ne!(token(&takeover)?, stale_token);
    assert_eq!(takeover.tasks.len(), 1);

    store
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id,
                lane: GPU,
                outcome: TaskOutcome::complete(),
                owner_token: Some(stale_token.clone()),
            }],
        )
        .await?;
    let lost = store
        .renew_leases(
            "runner-a",
            &[TaskLease {
                task_id,
                lane: GPU,
                owner_token: Some(stale_token),
            }],
        )
        .await?;
    assert_eq!(lost, vec![task_id]);
    assert_eq!(
        store.get_task(task_id).await?.map(|task| task.status),
        Some(TaskStatus::Running)
    );
    Ok(())
}

/// Verifies idle and busy hooks serialize and busy success gates task claims.
#[tokio::test]
async fn lifecycle_hooks_gate_idle_and_busy_edges() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store.initialize(locked_conf(1, None, true)).await?;

    let idling = poll(&store, "runner-a", claim(None, None)).await?;
    assert_eq!(owner(&idling)?.action, Some(LaneHookAction::Idle));
    let idle_token = token(&idling)?;
    let idle = poll(
        &store,
        "runner-a",
        claim(Some(idle_token), Some(hook_result(&idling, Ok(()))?)),
    )
    .await?;
    assert_eq!(owner(&idle)?.phase, LaneOwnerPhase::Idle);
    assert!(owner(&idle)?.token.is_none());

    store.store_tasks(vec![task(chrono::Utc::now())]).await?;
    let busying = poll(&store, "runner-b", claim(None, None)).await?;
    assert_eq!(owner(&busying)?.action, Some(LaneHookAction::Busy));
    assert!(busying.tasks.is_empty());
    let busy_token = token(&busying)?;
    let active = poll(
        &store,
        "runner-b",
        claim(Some(busy_token), Some(hook_result(&busying, Ok(()))?)),
    )
    .await?;
    assert_eq!(owner(&active)?.phase, LaneOwnerPhase::Active);
    assert_eq!(active.tasks.len(), 1);
    Ok(())
}

/// Verifies work appearing during idle reconciliation forces a serialized busy hook.
#[tokio::test]
async fn work_during_idle_hook_transitions_directly_to_busying() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store.initialize(locked_conf(1, None, true)).await?;
    let idling = poll(&store, "runner-a", claim(None, None)).await?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;

    let next = poll(
        &store,
        "runner-a",
        claim(Some(token(&idling)?), Some(hook_result(&idling, Ok(()))?)),
    )
    .await?;
    assert_eq!(owner(&next)?.phase, LaneOwnerPhase::Busying);
    assert_eq!(owner(&next)?.action, Some(LaneHookAction::Busy));
    assert!(next.tasks.is_empty());
    Ok(())
}

/// Verifies idle failures open after activity while busy failures remain closed until retry.
#[tokio::test]
async fn lifecycle_failures_apply_asymmetric_safety() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store.initialize(locked_conf(1, None, true)).await?;
    let idling = poll(&store, "runner-a", claim(None, None)).await?;
    let failed_idle = poll(
        &store,
        "runner-a",
        claim(
            Some(token(&idling)?),
            Some(hook_result(&idling, Err("stop failed"))?),
        ),
    )
    .await?;
    assert_eq!(owner(&failed_idle)?.phase, LaneOwnerPhase::IdleFailed);
    assert!(owner(&failed_idle)?.token.is_none());

    store.store_tasks(vec![task(chrono::Utc::now())]).await?;
    let active = poll(&store, "runner-b", claim(None, None)).await?;
    assert_eq!(active.tasks.len(), 1);
    let active_token = token(&active)?;
    let task_id = active
        .tasks
        .first()
        .map(TaskRecord::id)
        .ok_or_else(|| TaskError::TaskExecutionError("active owner claimed no task".into()))?;
    store
        .commit_outcomes(
            "runner-b",
            &[TaskCommit {
                task_id,
                lane: GPU,
                outcome: TaskOutcome::complete(),
                owner_token: Some(active_token.clone()),
            }],
        )
        .await?;
    let idling_again = poll(&store, "runner-b", completed_claim(active_token)).await?;
    assert_eq!(owner(&idling_again)?.action, Some(LaneHookAction::Idle));

    let idle = poll(
        &store,
        "runner-b",
        claim(
            Some(token(&idling_again)?),
            Some(hook_result(&idling_again, Ok(()))?),
        ),
    )
    .await?;
    store.store_tasks(vec![task(chrono::Utc::now())]).await?;
    let busying = poll(&store, "runner-c", claim(None, None)).await?;
    let failed_busy = poll(
        &store,
        "runner-c",
        claim(
            Some(token(&busying)?),
            Some(hook_result(&busying, Err("start failed"))?),
        ),
    )
    .await?;
    assert_eq!(owner(&failed_busy)?.phase, LaneOwnerPhase::BusyFailed);
    assert!(failed_busy.tasks.is_empty());
    let closed = poll(&store, "runner-d", claim(None, None)).await?;
    assert!(closed.tasks.is_empty());
    assert!(owner(&closed)?.action.is_none());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let retry = poll(&store, "runner-d", claim(None, None)).await?;
    assert_eq!(owner(&retry)?.action, Some(LaneHookAction::Busy));
    assert!(owner(&idle)?.token.is_none());
    Ok(())
}

/// Verifies idle debounce requires one continuous empty interval and resets on ready work.
#[tokio::test]
async fn idle_debounce_resets_when_work_arrives() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store
        .initialize(debounced_conf(Duration::from_millis(20)))
        .await?;
    let empty = poll(&store, "runner-a", claim(None, None)).await?;
    assert!(owner(&empty)?.action.is_none());
    let owner_token = token(&empty)?;

    store.store_tasks(vec![task(chrono::Utc::now())]).await?;
    let active = poll(&store, "runner-a", claim(Some(owner_token.clone()), None)).await?;
    assert_eq!(active.tasks.len(), 1);
    let task_id =
        active.tasks.first().map(TaskRecord::id).ok_or_else(|| {
            TaskError::TaskExecutionError("debounced lane claimed no task".into())
        })?;
    store
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id,
                lane: GPU,
                outcome: TaskOutcome::complete(),
                owner_token: Some(owner_token.clone()),
            }],
        )
        .await?;
    let drained = poll(&store, "runner-a", claim(Some(owner_token.clone()), None)).await?;
    assert!(owner(&drained)?.action.is_none());
    tokio::time::sleep(Duration::from_millis(25)).await;
    let idling = poll(&store, "runner-a", claim(Some(owner_token), None)).await?;
    assert_eq!(owner(&idling)?.action, Some(LaneHookAction::Idle));
    Ok(())
}

/// Verifies future scheduled tasks supply a wake deadline but do not prevent idle.
#[tokio::test]
async fn future_tasks_do_not_prevent_idle_transition() -> Result<(), TaskError> {
    let store = MemoryTaskStore::new(64);
    store.initialize(locked_conf(1, None, true)).await?;
    let mut scheduled = task(chrono::Utc::now());
    scheduled.initial_delay = Some(Duration::from_secs(60));
    store.store_tasks(vec![scheduled]).await?;

    let idling = poll(&store, "runner-a", claim(None, None)).await?;
    assert!(idling.tasks.is_empty());
    assert_eq!(owner(&idling)?.action, Some(LaneHookAction::Idle));
    Ok(())
}
