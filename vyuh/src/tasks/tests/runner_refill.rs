use super::*;
use crate::tasks::TaskOutcome;
use crate::tasks::store::fixtures::{commit, record, write};

/// Records one finished invocation through normal runner completion accounting.
fn finish(
    runner: &mut AbstractTaskRunner<MemoryTaskStore>,
    outcomes: Vec<TaskOutcome>,
    commits: &mut Vec<TaskCommit>,
) -> Result<(), TaskRuntimeError> {
    let invocation_id = uuid::Uuid::now_v7();
    let values = outcomes
        .into_iter()
        .map(|outcome| commit(record().id, outcome))
        .collect::<Vec<_>>();
    let task_ids = values.iter().map(|value| value.task_id).collect::<Vec<_>>();
    let handle = tokio::spawn(async {});
    runner.running += 1;
    runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?
        .running += 1;
    for id in &task_ids {
        runner.running_tasks.insert(*id, invocation_id);
    }
    runner.running_invocations.insert(
        invocation_id,
        RunningInvocation {
            lane: DEFAULT_TASK_LANE,
            owner_token: None,
            task_ids,
            abort: handle.abort_handle(),
        },
    );
    runner.accept_completion(
        Completion {
            invocation_id,
            lane: DEFAULT_TASK_LANE,
            commits: values,
        },
        commits,
    );
    Ok(())
}

/// Covers every durable outcome without interpreting its store transition.
fn outcomes() -> [TaskOutcome; 8] {
    [
        TaskOutcome::Complete,
        TaskOutcome::CompleteWith { output: "1".into() },
        TaskOutcome::Retry {
            error: "retry".into(),
        },
        TaskOutcome::Fail {
            error: "failed".into(),
        },
        TaskOutcome::Suspend {
            state: "null".into(),
        },
        TaskOutcome::Sleep {
            state: "null".into(),
            delay: Duration::from_secs(1),
        },
        TaskOutcome::Spawn {
            state: "null".into(),
            child: write(record()),
        },
        TaskOutcome::All {
            state: "null".into(),
            children: Vec::new(),
        },
    ]
}

/// Every outcome holds refill capacity even when all outcomes fit in the current turn.
#[tokio::test]
async fn every_outcome_backpressures() -> Result<(), TaskRuntimeError> {
    for outcome in outcomes() {
        let mut runner = prefetch_runner()?;
        let mut commits = Vec::new();
        for _ in 0..4 {
            finish(&mut runner, vec![outcome.clone()], &mut commits)?;
        }
        assert!(runner.pending_commits.is_empty());
        assert!(runner.claims(true).is_empty(), "{outcome:?}");
        assert_eq!(runner.renewal_leases(&commits).len(), 4);
        let (first, rest) = commits
            .split_first()
            .ok_or_else(|| TaskRuntimeError::TaskExecutionError("missing completion".into()))?;
        runner.acknowledge_commits(std::slice::from_ref(first));
        assert_eq!(
            runner.claims(true).first().map(|claim| claim.limit),
            Some(1)
        );
        runner.acknowledge_commits(rest);
        assert_eq!(
            runner.claims(true).first().map(|claim| claim.limit),
            Some(4)
        );
    }
    Ok(())
}

/// Executing and queued work combine with uncommitted outcomes, saturating at zero.
#[test]
fn mixed_occupancy_limits_refill() -> Result<(), TaskRuntimeError> {
    let mut runner = prefetch_runner()?;
    let lane = runner
        .lane_mut(DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
    lane.running = 1;
    lane.tasks.push_back(panic_record()?);
    lane.uncommitted = 1;
    assert_eq!(lane.available(4), 1);
    lane.uncommitted = usize::MAX;
    assert_eq!(lane.available(4), 0);
    lane.uncommitted = 0;
    assert_eq!(lane.available(1), 1);
    Ok(())
}

/// Locked lanes keep coordinating their owner while uncommitted work blocks another cohort.
#[test]
fn locked_refill_waits_for_ack() -> Result<(), TaskRuntimeError> {
    let mut runner = locked_runner()?;
    runner.cursor = 1;
    let lane = runner
        .lane_mut(EMAIL)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    lane.owner_token = Some("owner".into());
    lane.uncommitted = 1;
    let claims = runner.claims(true);
    let owner = claims
        .iter()
        .find(|claim| claim.lane == EMAIL)
        .and_then(|claim| claim.owner.as_ref())
        .ok_or_else(|| TaskRuntimeError::UnknownLane(EMAIL.to_string()))?;
    assert_eq!(owner.token.as_deref(), Some("owner"));
    assert!(!owner.allow_claim);
    assert!(!owner.quiescent);
    let mut value = commit(record().id, TaskOutcome::Complete);
    value.lane = EMAIL;
    runner.acknowledge_commits(&[value]);
    assert!(runner.claims(true).iter().any(|claim| {
        claim.lane == EMAIL && claim.owner.as_ref().is_some_and(|owner| owner.allow_claim)
    }));
    Ok(())
}

/// Ordinary buffered outcomes leave another lane's rate-limited claim capacity available.
#[tokio::test]
async fn pressure_is_lane_local() -> Result<(), TaskRuntimeError> {
    let mut runner = lane_runner()?;
    let mut commits = Vec::new();
    for _ in 0..2 {
        finish(&mut runner, vec![TaskOutcome::Complete], &mut commits)?;
    }
    let claims = runner.claims(true);
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims.first().map(|claim| (claim.lane, claim.limit)),
        Some((EMAIL, 1))
    );
    runner.acknowledge_commits(&commits);
    assert!(
        runner
            .claims(true)
            .iter()
            .any(|claim| claim.lane == DEFAULT_TASK_LANE)
    );
    Ok(())
}

/// A batch releases one handler slot but reserves refill capacity for every buffered item.
#[tokio::test]
async fn batch_pressure_counts_items() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    runner.batch_size = 2;
    let mut commits = Vec::new();
    finish(&mut runner, vec![TaskOutcome::Complete; 4], &mut commits)?;
    assert_eq!(runner.running, 0);
    assert_eq!(commits.len(), 2);
    assert_eq!(runner.pending_commits.len(), 2);
    assert!(runner.claims(true).is_empty());
    assert_eq!(runner.renewal_leases(&commits).len(), 4);
    runner.acknowledge_commits(&commits);
    commits.clear();
    runner.fill_commits(&mut commits);
    assert_eq!(
        runner.claims(true).first().map(|claim| claim.limit),
        Some(2)
    );
    runner.acknowledge_commits(&commits);
    assert!(runner.pending_commits.is_empty());
    assert_eq!(runner.lanes.first().map(|lane| lane.uncommitted), Some(0));
    Ok(())
}

/// Failed turns keep pressure and renewals; loss of a deferred lease releases only its slot.
#[tokio::test]
async fn failed_turn_retains_pressure() -> Result<(), TaskRuntimeError> {
    let mut runner = batch_runner()?;
    runner.batch_size = 2;
    let mut state = RunState::new(2, runner.poll_interval);
    finish(
        &mut runner,
        vec![TaskOutcome::Complete; 4],
        &mut state.commits,
    )?;
    let before = runner.renewal_leases(&state.commits).len();
    runner.fail_tick(
        &mut state,
        TaskRuntimeError::TaskExecutionError("test failure".into()),
    );
    assert!(runner.claims(true).is_empty());
    assert_eq!(runner.renewal_leases(&state.commits).len(), before);
    let lost = runner
        .pending_commits
        .front()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("missing deferred outcome".into()))?
        .task_id;
    runner.drop_lost(&[lost]);
    assert_eq!(
        runner.claims(true).first().map(|claim| claim.limit),
        Some(1)
    );
    assert_eq!(runner.renewal_leases(&state.commits).len(), before - 1);
    Ok(())
}

/// Slow ordinary flushes cannot grow buffered outcomes without bound, and all work drains.
#[tokio::test]
async fn sustained_pressure_drains() -> Result<(), TaskRuntimeError> {
    let mut runner = prefetch_runner()?;
    runner.batch_size = 1;
    let mut commits = Vec::new();
    finish(&mut runner, vec![TaskOutcome::Complete; 4], &mut commits)?;
    let mut remaining = 28;
    let mut committed = 0;
    for _ in 0..64 {
        runner.fill_commits(&mut commits);
        let fetched = runner
            .claims(true)
            .iter()
            .map(|claim| claim.limit)
            .sum::<usize>()
            .min(remaining);
        committed += commits.len();
        runner.acknowledge_commits(&commits);
        commits.clear();
        remaining -= fetched;
        for _ in 0..fetched {
            finish(&mut runner, vec![TaskOutcome::Complete], &mut commits)?;
        }
        assert!(runner.renewal_leases(&commits).len() <= 4);
        if committed == 32 {
            break;
        }
    }
    assert_eq!(committed, 32);
    assert_eq!(remaining, 0);
    assert!(runner.pending_commits.is_empty());
    assert_eq!(runner.lanes.first().map(|lane| lane.uncommitted), Some(0));
    Ok(())
}

/// Fully occupied fast lanes alternate claim and commit turns instead of anticipating acknowledgement.
#[tokio::test]
async fn saturated_turns_are_paced() -> Result<(), TaskRuntimeError> {
    let mut runner = prefetch_runner()?;
    let mut commits = Vec::new();
    let mut remaining = 32;
    let mut committed = 0;
    let mut turns = 0;
    while committed < 32 && turns < 32 {
        let fetched = runner
            .claims(true)
            .iter()
            .map(|claim| claim.limit)
            .sum::<usize>()
            .min(remaining);
        committed += commits.len();
        runner.acknowledge_commits(&commits);
        commits.clear();
        remaining -= fetched;
        for _ in 0..fetched {
            finish(&mut runner, vec![TaskOutcome::Complete], &mut commits)?;
        }
        turns += 1;
    }
    assert_eq!(committed, 32);
    assert_eq!(turns, 16);
    Ok(())
}

/// A commit-only turn releases pressure at the normal poll gate, never the idle fallback.
#[tokio::test(start_paused = true)]
async fn acknowledgement_wakes_refill() -> Result<(), String> {
    let mut runner = prefetch_runner().map_err(|error| error.to_string())?;
    let mut state = RunState::new(4, runner.poll_interval);
    finish(
        &mut runner,
        vec![TaskOutcome::Complete; 4],
        &mut state.commits,
    )
    .map_err(|error| error.to_string())?;
    assert!(runner.claims(true).is_empty());
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let (sender, _) = mpsc::channel(4);
    let (hooks, _) = mpsc::channel(1);
    state.last_tick = tokio::time::Instant::now();
    runner.apply_tick(
        &site,
        &sender,
        &hooks,
        &mut state,
        TickResult {
            claims: Vec::new(),
            renewals: Vec::new(),
            started: std::time::Instant::now(),
            tick: TaskTick {
                poll: TaskPoll { lanes: Vec::new() },
                lost: Vec::new(),
                cancelled: Vec::new(),
                wake_lanes: Vec::new(),
            },
        },
    );
    assert!(state.commits.is_empty());
    assert_eq!(state.next_poll, state.last_tick + runner.poll_interval);
    assert_eq!(
        runner.claims(true).first().map(|claim| claim.limit),
        Some(4)
    );
    Ok(())
}
