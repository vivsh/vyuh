use super::*;
use crate::tasks::{DEFAULT_TASK_LANE, TaskKind, TaskLaneConf};
#[path = "all.rs"]
pub(crate) mod all;

#[path = "cancellation.rs"]
pub(crate) mod cancellation;

#[path = "capabilities.rs"]
pub(crate) mod capabilities;

#[path = "results.rs"]
mod results;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) use results::result_contract;

#[path = "workflow_edges.rs"]
mod workflow_edges;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) use workflow_edges::cross_lane_contract;

pub(crate) fn record() -> TaskRecord {
    let now = chrono::Utc::now();
    TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind: TaskKind::Work,
        name: "workflow".into(),
        input: "null".into(),
        state: None,
        resume_input: None,
        status: TaskStatus::Pending,
        cancelled: false,
        attempts: 0,
        step_attempts: 0,
        lane: DEFAULT_TASK_LANE.to_string(),
        lease_duration_ms: None,
        last_result: None,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        locked_by: None,
        leased_until: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

pub(crate) fn write(record: TaskRecord) -> TaskWrite {
    TaskWrite {
        record,
        ignore_conflicts: false,
        initial_delay: None,
    }
}

/// Builds orchestration fixtures without changing ordinary work fixture defaults.
pub(crate) fn flow_record() -> TaskRecord {
    TaskRecord {
        name: "flow".into(),
        kind: TaskKind::Flow,
        ..record()
    }
}

pub(crate) fn conf() -> TaskStoreConf {
    TaskStoreConf {
        max_all_children: 256,
        handlers: vec![
            ("workflow".into(), TaskKind::Work),
            ("flow".into(), TaskKind::Flow),
        ],
        lanes: vec![TaskLaneConf::new(DEFAULT_TASK_LANE, 8)],
        idempotency: vec![crate::tasks::store::TaskIdempotencyConf {
            handler: "workflow".into(),
            lane: DEFAULT_TASK_LANE.to_string(),
            revision: "v1".into(),
            retention: crate::tasks::IdempotencyRetention::ActiveOnly,
        }],
        schedules: Vec::new(),
        poll_interval: Duration::from_millis(10),
    }
}

pub(crate) fn claim() -> LaneClaim {
    LaneClaim {
        lane: DEFAULT_TASK_LANE,
        limit: 32,
        owner: None,
    }
}

pub(crate) fn commit(id: TaskId, outcome: TaskOutcome) -> TaskCommit {
    TaskCommit {
        task_id: id,
        lane: DEFAULT_TASK_LANE,
        outcome,
        owner_token: None,
    }
}

/// Runs the same spawn/checkpoint/next-poll contract against every store backend.
pub(crate) async fn workflow_contract<S: AbstractTaskStore>(
    store: &S,
) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    assert_eq!(
        store.tick("owner", &[claim()], &[], &[]).await?.poll.lanes[0]
            .tasks
            .len(),
        1
    );
    for step in 0..8 {
        let child = record();
        let child_id = child.id;
        let outcome = TaskOutcome::Spawn {
            state: step.to_string(),
            child: write(child),
        };
        let spawned = store
            .tick("owner", &[claim()], &[commit(id, outcome)], &[])
            .await?;
        assert!(spawned.poll.lanes[0].tasks.is_empty());
        assert_eq!(spawned.wake_lanes, vec![DEFAULT_TASK_LANE]);
        let child = store.tick("owner", &[claim()], &[], &[]).await?.poll.lanes[0]
            .tasks
            .remove(0);
        assert_eq!(child.id, child_id);
        assert_eq!(child.parent_id, Some(id));
        assert_eq!(child.root_id, Some(id));
        let completed = store
            .tick(
                "owner",
                &[claim()],
                &[commit(
                    child_id,
                    TaskOutcome::CompleteWith {
                        output: step.to_string(),
                    },
                )],
                &[],
            )
            .await?;
        assert!(completed.poll.lanes[0].tasks.is_empty());
        let resumed = store.tick("owner", &[claim()], &[], &[]).await?.poll.lanes[0]
            .tasks
            .remove(0);
        assert_eq!(resumed.id, id);
        assert_eq!(resumed.state::<u32>()?, Some(step));
        assert_eq!(resumed.resume_input::<u32>()?, Some(Ok(step)));
        assert_eq!(resumed.step_attempts, 1);
        assert_eq!(resumed.attempts, step as i32 + 2);
    }
    store
        .commit_outcomes("owner", &[commit(id, TaskOutcome::Complete)])
        .await?;
    Ok(())
}

/// Sequential checkpoints exceed the lifetime retry bound without same-turn claims.
#[tokio::test]
async fn sequential_children_resume_only_next_poll() -> Result<(), TaskRuntimeError> {
    workflow_contract(&MemoryTaskStore::new(32)).await
}

/// Terminal child failures become structured resume errors, not parent failures.
#[tokio::test]
async fn child_failure_resumes_parent_with_identity() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    store.initialize(conf()).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let child = record();
    let child_id = child.id;
    store
        .commit_outcomes(
            "owner",
            &[commit(
                id,
                TaskOutcome::Spawn {
                    state: "7".into(),
                    child: write(child),
                },
            )],
        )
        .await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store
        .commit_outcomes(
            "owner",
            &[commit(child_id, TaskOutcome::fail("safe error"))],
        )
        .await?;
    let parent = store.get_task(id).await?.unwrap();
    assert_eq!(parent.state::<u32>()?, Some(7));
    let failure = parent.resume_input::<()>()?.unwrap().unwrap_err();
    assert_eq!(failure.task_id(), Some(&child_id));
    assert_eq!(failure.message(), "safe error");
    Ok(())
}

/// Shared recovery, retry, stale completion, and nested lineage behavior.
pub(crate) async fn edge_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let child = flow_record();
    let child_id = child.id;
    let spawn = commit(
        id,
        TaskOutcome::Spawn {
            state: "1".into(),
            child: write(child),
        },
    );
    store
        .commit_outcomes("stale-owner", std::slice::from_ref(&spawn))
        .await?;
    assert!(store.get_task(child_id).await?.is_none());
    store
        .commit_outcomes("owner", std::slice::from_ref(&spawn))
        .await?;
    store.commit_outcomes("owner", &[spawn]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let grandchild = record();
    let grandchild_id = grandchild.id;
    store
        .commit_outcomes(
            "owner",
            &[commit(
                child_id,
                TaskOutcome::Spawn {
                    state: "2".into(),
                    child: write(grandchild),
                },
            )],
        )
        .await?;
    assert_eq!(
        store.get_task(grandchild_id).await?.unwrap().root_id,
        Some(id)
    );
    store.claim_tasks("owner", &[claim()]).await?;
    let terminal = commit(grandchild_id, TaskOutcome::fail("child failed"));
    store
        .commit_outcomes("owner", std::slice::from_ref(&terminal))
        .await?;
    let resumed = store.claim_tasks("owner", &[claim()]).await?.lanes[0]
        .tasks
        .remove(0);
    assert_eq!(resumed.id, child_id);
    assert!(resumed.resume_input::<()>()?.unwrap().is_err());
    store.commit_outcomes("owner", &[terminal]).await?;
    assert_eq!(
        store.get_task(child_id).await?.unwrap().status,
        TaskStatus::Running
    );
    store
        .commit_outcomes("owner", &[commit(child_id, TaskOutcome::Complete)])
        .await?;
    let root = store.claim_tasks("owner", &[claim()]).await?.lanes[0]
        .tasks
        .remove(0);
    assert_eq!(root.resume_input::<()>()?, Some(Ok(())));
    store
        .commit_outcomes("owner", &[commit(id, TaskOutcome::Complete)])
        .await?;
    recovery_contract(store).await?;
    conflict_contract(store).await
}

/// Child-key collisions fail their parent and do not discard unrelated outcomes.
async fn conflict_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut existing = record();
    existing.status = TaskStatus::Suspended;
    existing.idempotency_key = Some("child-key".into());
    existing.idempotency_fingerprint = Some("same".into());
    let existing_id = existing.id;
    let parent = flow_record();
    let parent_id = parent.id;
    let sibling = record();
    let sibling_id = sibling.id;
    store
        .store_tasks(vec![write(existing), write(parent), write(sibling)])
        .await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let mut child = record();
    let child_id = child.id;
    child.idempotency_key = Some("child-key".into());
    child.idempotency_fingerprint = Some("same".into());
    store
        .commit_outcomes(
            "owner",
            &[
                commit(
                    parent_id,
                    TaskOutcome::Spawn {
                        state: "null".into(),
                        child: write(child),
                    },
                ),
                commit(sibling_id, TaskOutcome::Complete),
            ],
        )
        .await?;
    assert!(store.get_task(child_id).await?.is_none());
    assert_eq!(
        store.get_task(parent_id).await?.unwrap().status,
        TaskStatus::Failed
    );
    assert_eq!(
        store.get_task(sibling_id).await?.unwrap().status,
        TaskStatus::Succeeded
    );
    assert_eq!(store.get_task(existing_id).await?.unwrap().parent_id, None);
    Ok(())
}

/// Lease exhaustion delivers the result after selection, never during the same claim.
async fn recovery_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut parent = flow_record();
    parent.status = TaskStatus::Suspended;
    parent.state = Some("9".into());
    let id = parent.id;
    let mut child = record();
    child.parent_id = Some(id);
    child.root_id = Some(id);
    child.status = TaskStatus::Running;
    child.attempts = 8;
    child.step_attempts = 5;
    child.locked_by = Some("crashed".into());
    child.leased_until = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
    store.store_tasks(vec![write(parent), write(child)]).await?;
    let tick = store.tick("takeover", &[claim()], &[], &[]).await?;
    assert!(tick.poll.lanes[0].tasks.is_empty());
    assert_eq!(tick.wake_lanes, vec![DEFAULT_TASK_LANE]);
    let parent = store.get_task(id).await?.unwrap();
    assert_eq!(parent.status, TaskStatus::Pending);
    assert_eq!(parent.state::<u32>()?, Some(9));
    assert!(parent.resume_input::<()>()?.unwrap().is_err());
    store.claim_tasks("takeover", &[claim()]).await?;
    store
        .commit_outcomes("takeover", &[commit(id, TaskOutcome::Complete)])
        .await?;
    Ok(())
}

/// Nested workflows and crash exhaustion deliver exactly one result per accepted child.
#[tokio::test]
async fn nested_and_recovery_contract() -> Result<(), TaskRuntimeError> {
    edge_contract(&MemoryTaskStore::new(32)).await
}

/// An invalid sibling outcome cannot leave the accepted spawn partially applied.
#[tokio::test]
async fn invalid_flush_preserves_parent_and_has_no_child() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    store.initialize(conf()).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let child = record();
    let child_id = child.id;
    let spawn = commit(
        id,
        TaskOutcome::Spawn {
            state: "null".into(),
            child: write(child),
        },
    );
    assert!(
        store
            .commit_outcomes("owner", &[spawn, commit(id, TaskOutcome::Complete)])
            .await
            .is_err()
    );
    assert_eq!(
        store.get_task(id).await?.unwrap().status,
        TaskStatus::Running
    );
    assert!(store.get_task(child_id).await?.is_none());
    Ok(())
}

/// A claim failure after outcome staging restores every task and workflow write.
#[tokio::test]
async fn later_claim_failure_rolls_back_spawn() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    store.initialize(conf()).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let child = record();
    let child_id = child.id;
    let spawn = commit(
        id,
        TaskOutcome::Spawn {
            state: "null".into(),
            child: write(child),
        },
    );
    let mut bad = claim();
    bad.lane = TaskLane::new("missing");
    assert!(
        store
            .tick("owner", &[claim(), bad], std::slice::from_ref(&spawn), &[])
            .await
            .is_err()
    );
    assert_eq!(
        store.get_task(id).await?.unwrap().status,
        TaskStatus::Running
    );
    assert!(store.get_task(child_id).await?.is_none());
    store.tick("owner", &[claim()], &[spawn], &[]).await?;
    assert_eq!(
        store.get_task(id).await?.unwrap().status,
        TaskStatus::Suspended
    );
    assert!(store.get_task(child_id).await?.is_some());
    Ok(())
}

async fn lifecycle() -> Result<(), crate::Error> {
    Ok(())
}

/// Newly generated same-lane work cancels an idle edge before the hook is exposed.
pub(crate) async fn locked_workflow_contract<S: AbstractTaskStore>(
    store: &S,
) -> Result<(), TaskRuntimeError> {
    locked_child_contract(store, false).await
}

/// All finalization follows the same owner and idle-hook gates as scalar child spawning.
pub(crate) async fn locked_all_contract<S: AbstractTaskStore>(
    store: &S,
) -> Result<(), TaskRuntimeError> {
    locked_child_contract(store, true).await
}

/// Runs both child-wait forms through the shared leased-lane lifecycle.
async fn locked_child_contract<S: AbstractTaskStore>(
    store: &S,
    all: bool,
) -> Result<(), TaskRuntimeError> {
    use crate::tasks::{LaneOwnerRequest, TaskLaneLock};
    let mut config = conf();
    config.lanes = vec![
        TaskLaneConf::new(DEFAULT_TASK_LANE, 8).lock(
            TaskLaneLock::new(1)
                .idle_after(Duration::ZERO)
                .on_idle(lifecycle)
                .on_busy(lifecycle),
        ),
    ];
    store.initialize(config).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    let mut lane = claim();
    lane.owner = Some(LaneOwnerRequest {
        token: None,
        quiescent: true,
        allow_claim: true,
        completed_work: false,
        hook: None,
    });
    let initial = store
        .tick("owner", std::slice::from_ref(&lane), &[], &[])
        .await?;
    let token = initial.poll.lanes[0].owner.as_ref().unwrap().token.clone();
    lane.owner.as_mut().unwrap().token = token.clone();
    let child = record();
    let child_id = child.id;
    let outcome = if all {
        TaskOutcome::All {
            state: "null".into(),
            children: vec![write(child)],
        }
    } else {
        TaskOutcome::Spawn {
            state: "null".into(),
            child: write(child),
        }
    };
    let mut spawn = commit(id, outcome);
    spawn.owner_token = token.clone();
    let spawned = store
        .tick("owner", std::slice::from_ref(&lane), &[spawn], &[])
        .await?;
    assert!(spawned.poll.lanes[0].tasks.is_empty());
    assert_eq!(spawned.poll.lanes[0].owner.as_ref().unwrap().action, None);
    assert_eq!(
        spawned.poll.lanes[0].owner.as_ref().unwrap().phase,
        LaneOwnerPhase::Active
    );
    let next = store
        .tick("owner", std::slice::from_ref(&lane), &[], &[])
        .await?;
    assert_eq!(next.poll.lanes[0].tasks[0].id, child_id);
    let mut complete = commit(child_id, TaskOutcome::Complete);
    complete.owner_token = token;
    let delivered = store
        .tick("owner", std::slice::from_ref(&lane), &[complete], &[])
        .await?;
    assert!(delivered.poll.lanes[0].tasks.is_empty());
    assert_eq!(delivered.poll.lanes[0].owner.as_ref().unwrap().action, None);
    assert_eq!(
        store.tick("owner", &[lane], &[], &[]).await?.poll.lanes[0].tasks[0].id,
        id
    );
    Ok(())
}

/// Durable lane ownership remains independent of workflow next-poll delivery.
#[tokio::test]
async fn locked_lane_finalization_defers_idle() -> Result<(), TaskRuntimeError> {
    locked_workflow_contract(&MemoryTaskStore::new(32)).await
}
