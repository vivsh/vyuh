use super::*;
use crate::tasks::{LaneHookAction, LaneHookResult, LaneOwnerPoll, LaneOwnerRequest, TaskLaneLock};

async fn hook() -> Result<(), crate::Error> {
    Ok(())
}

fn owned(token: Option<String>, hook: Option<LaneHookResult>) -> LaneClaim {
    LaneClaim {
        owner: Some(LaneOwnerRequest {
            token,
            hook,
            quiescent: true,
            allow_claim: true,
            completed_work: false,
        }),
        ..claim()
    }
}

async fn poll<S: AbstractTaskStore>(
    store: &S,
    claim: LaneClaim,
) -> Result<LanePoll, TaskRuntimeError> {
    store
        .claim_tasks("owner", &[claim])
        .await?
        .lanes
        .into_iter()
        .next()
        .ok_or_else(missing)
}

fn owner(poll: &LanePoll) -> Result<LaneOwnerPoll, TaskRuntimeError> {
    poll.owner.clone().ok_or_else(missing)
}

/// Cancellation follows hook and batch gates, and running finalization remains owner-fenced.
pub(crate) async fn contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut config = conf();
    config.lanes = vec![
        TaskLaneConf::new(DEFAULT_TASK_LANE, 8).lock(
            TaskLaneLock::new(2)
                .idle_after(Duration::ZERO)
                .on_idle(hook)
                .on_busy(hook),
        ),
    ];
    store.initialize(config).await?;
    let idle = owner(&poll(store, owned(None, None)).await?)?;
    assert_eq!(idle.action, Some(LaneHookAction::Idle));
    let task = record();
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    store.cancel(id).await?;
    let blocked = poll(store, owned(idle.token.clone(), None)).await?;
    assert!(blocked.tasks.is_empty());
    assert_eq!(get(store, id).await?.status, TaskStatus::Pending);
    let busy = resume_hook(store, idle, id).await?;
    finish_cohort(store, id, busy.token).await
}

/// Both lifecycle hooks and the partial-batch threshold remain in the normal claim path.
async fn resume_hook<S: AbstractTaskStore>(
    store: &S,
    idle: LaneOwnerPoll,
    id: TaskId,
) -> Result<LaneOwnerPoll, TaskRuntimeError> {
    let busy = owner(
        &poll(
            store,
            owned(
                idle.token.clone(),
                Some(LaneHookResult {
                    generation: idle.generation,
                    action: LaneHookAction::Idle,
                    result: Ok(()),
                }),
            ),
        )
        .await?,
    )?;
    assert_eq!(busy.action, Some(LaneHookAction::Busy));
    let partial = poll(
        store,
        owned(
            busy.token.clone(),
            Some(LaneHookResult {
                generation: busy.generation,
                action: LaneHookAction::Busy,
                result: Ok(()),
            }),
        ),
    )
    .await?;
    assert!(partial.tasks.is_empty());
    assert_eq!(get(store, id).await?.status, TaskStatus::Pending);
    Ok(busy)
}

/// Filling the cohort finalizes cancellation without executing it and fences stale owners.
async fn finish_cohort<S: AbstractTaskStore>(
    store: &S,
    id: TaskId,
    token: Option<String>,
) -> Result<(), TaskRuntimeError> {
    let task = record();
    let sibling = task.id;
    store.store_tasks(vec![write(task)]).await?;
    let claimed = poll(store, owned(token.clone(), None)).await?;
    assert_eq!(
        claimed.tasks.iter().map(|task| task.id).collect::<Vec<_>>(),
        vec![sibling]
    );
    assert_cancelled(&get(store, id).await?)?;
    store.cancel(sibling).await?;
    let stale = TaskLease {
        owner_token: Some("stale-token".into()),
        ..lease(sibling)
    };
    assert_eq!(store.renew_leases("owner", &[stale]).await?, vec![sibling]);
    assert_eq!(get(store, sibling).await?.status, TaskStatus::Running);
    let stale = TaskCommit {
        owner_token: Some("stale-token".into()),
        ..commit(sibling, TaskOutcome::Complete)
    };
    store.commit_outcomes("owner", &[stale]).await?;
    assert_eq!(get(store, sibling).await?.status, TaskStatus::Running);
    let current = TaskLease {
        owner_token: token,
        ..lease(sibling)
    };
    assert_eq!(
        store.renew_leases("owner", &[current]).await?,
        vec![sibling]
    );
    assert_cancelled(&get(store, sibling).await?)?;
    Ok(())
}

/// Reference store observes the same lane gates and fencing as persistent stores.
#[tokio::test]
async fn locked_cancellation_contract() -> Result<(), TaskRuntimeError> {
    contract(&MemoryTaskStore::new(32)).await
}

/// A crashed lane owner is replaced after expiry without running its cancelled task.
pub(crate) async fn takeover<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut config = conf();
    config.lanes = vec![TaskLaneConf::new(DEFAULT_TASK_LANE, 8).lock(TaskLaneLock::new(1))];
    store.initialize(config).await?;
    let task = record();
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    let initial = poll(store, owned(None, None)).await?;
    let token = owner(&initial)?.token;
    store.cancel(id).await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let next = store.claim_tasks("takeover", &[owned(None, None)]).await?;
    let lane = next.lanes.first().ok_or_else(missing)?;
    assert!(lane.tasks.is_empty());
    assert_ne!(owner(lane)?.token, token);
    assert_cancelled(&get(store, id).await?)?;
    let stale = TaskCommit {
        owner_token: token,
        ..commit(id, TaskOutcome::Complete)
    };
    store.commit_outcomes("owner", &[stale]).await?;
    assert_cancelled(&get(store, id).await?)
}

/// A global rate gate cannot turn cancelled candidates into handler work or consume a start.
pub(crate) async fn rate<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut config = conf();
    config.lanes = vec![
        TaskLaneConf::new(DEFAULT_TASK_LANE, 8)
            .global_rate_limit(crate::tasks::TaskRate::per_minute(1).burst(1)),
    ];
    store.initialize(config).await?;
    let cancelled = record();
    let id = cancelled.id;
    let live = record();
    let live_id = live.id;
    store
        .store_tasks(vec![write(cancelled), write(live)])
        .await?;
    store.cancel(id).await?;
    let claimed = store.claim_tasks("owner", &[claim()]).await?;
    let ids = claimed
        .lanes
        .iter()
        .flat_map(|lane| &lane.tasks)
        .map(|task| task.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![live_id]);
    assert_cancelled(&get(store, id).await?)?;
    let blocked = record();
    let blocked_id = blocked.id;
    store.store_tasks(vec![write(blocked)]).await?;
    assert!(
        store
            .claim_tasks("owner", &[claim()])
            .await?
            .lanes
            .iter()
            .all(|lane| lane.tasks.is_empty())
    );
    assert_eq!(get(store, blocked_id).await?.status, TaskStatus::Pending);
    Ok(())
}

/// The memory store recovers cancelled owned work after a crashed owner loses its lease.
#[tokio::test]
async fn cancellation_crash_takeover() -> Result<(), TaskRuntimeError> {
    takeover(&MemoryTaskStore::with_lease_duration(
        32,
        Duration::from_millis(50),
    ))
    .await
}

/// Cancellation finalization preserves global rate permits for actual invocations.
#[tokio::test]
async fn cancellation_preserves_rate() -> Result<(), TaskRuntimeError> {
    rate(&MemoryTaskStore::new(32)).await
}
