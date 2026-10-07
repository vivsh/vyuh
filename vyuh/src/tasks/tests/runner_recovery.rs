use super::*;
use crate::tasks::runner::database::DbPhase;

/// Repeated failed reads retain the same claim identities and never release its reservation early.
#[tokio::test(start_paused = true)]
async fn recovery_retry_retains_evidence() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    runner
        .registry
        .clone()
        .dispatcher(runner.store.clone(), Vec::new())
        .submit(Job)
        .await?;
    let site = site().await;
    probe.failure.store(12, Ordering::SeqCst);
    runner.admit();
    apply(&mut runner, &site).await;
    let original = *runner.in_flight.keys().next().ok_or_else(failure)?;
    probe.failure.store(16, Ordering::SeqCst);
    for _ in 0..2 {
        tokio::time::advance(runner.poll_interval).await;
        runner.admit();
        apply(&mut runner, &site).await;
        assert!(runner.in_flight.contains_key(&original));
        assert_eq!(runner.queued(), 0);
        assert!(matches!(
            runner.in_flight.get(&original).ok_or_else(failure)?.phase,
            DbPhase::Retry {
                prepared: Some(DbResponse::Poll(_)),
                ..
            }
        ));
    }
    probe.failure.store(0, Ordering::SeqCst);
    tokio::time::advance(runner.poll_interval).await;
    runner.admit();
    apply(&mut runner, &site).await;
    assert_eq!(runner.queued(), 1);
    assert_eq!(probe.claims.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// The event loop itself exits a hung store call without waiting for its DB permit.
#[tokio::test(start_paused = true)]
async fn watchdog_select_exit() -> Result<(), TestError> {
    let (runner, probe) = runner(TaskMode::Clustered, 1)?;
    runner.initialize().await?;
    probe.failure.store(5, Ordering::SeqCst);
    let health = runner.health.clone();
    let site = site().await;
    tokio::time::timeout(Duration::from_millis(150), runner.run(site.clone()))
        .await
        .map_err(|_| failure())?;
    assert_eq!(probe.beats.load(Ordering::SeqCst), 1);
    assert_eq!(probe.returns.load(Ordering::SeqCst), 0);
    assert_eq!(
        health.snapshot().state,
        crate::tasks::health::TaskHealthState::Unhealthy
    );
    site.shutdown_and_wait().await;
    Ok(())
}

/// An uncertain claim waits one poll, recovers the same attempt, and never claims twice.
#[tokio::test(start_paused = true)]
async fn claim_ack_recovery() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    let dispatcher = runner
        .registry
        .clone()
        .dispatcher(runner.store.clone(), Vec::new());
    let id = dispatcher.submit(Job).await?.id();
    probe.failure.store(12, Ordering::SeqCst);
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    assert_eq!(runner.queued(), 0);
    assert!(runner.death_at.is_some());
    assert_eq!(runner.db_slots.available_permits(), 1);
    let original = *runner.in_flight.keys().next().ok_or_else(failure)?;
    runner.admit();
    assert!(runner.operations.is_empty());
    tokio::time::advance(runner.poll_interval).await;
    probe.failure.store(0, Ordering::SeqCst);
    runner.admit();
    assert!(runner.in_flight.contains_key(&original));
    apply(&mut runner, &site).await;
    assert_eq!(runner.queued(), 1);
    assert_eq!(probe.claims.load(Ordering::SeqCst), 1);
    assert_eq!(
        runner
            .store
            .get_task(id)
            .await?
            .ok_or_else(failure)?
            .attempts,
        1
    );
    assert!(runner.death_at.is_none());
    site.shutdown_and_wait().await;
    Ok(())
}

/// A lost flush acknowledgement is resolved without executing or claiming the task again.
#[tokio::test(start_paused = true)]
async fn flush_ack_recovery() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    let dispatcher = runner
        .registry
        .clone()
        .dispatcher(runner.store.clone(), Vec::new());
    let id = dispatcher.submit(Job).await?.id();
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    let (tx, mut rx) = mpsc::channel(1);
    runner.dispatch_ready(&site, &tx);
    runner.accept_completion(rx.recv().await.ok_or_else(failure)?);
    probe.failure.store(13, Ordering::SeqCst);
    runner.admit();
    apply(&mut runner, &site).await;
    assert_eq!(runner.lanes.front().ok_or_else(failure)?.uncommitted, 1);
    assert!(runner.pending_commits.is_empty());
    tokio::time::advance(runner.poll_interval).await;
    probe.failure.store(0, Ordering::SeqCst);
    runner.admit();
    apply(&mut runner, &site).await;
    assert_eq!(runner.lanes.front().ok_or_else(failure)?.uncommitted, 0);
    let task = runner.store.get_task(id).await?.ok_or_else(failure)?;
    assert_eq!(task.status, crate::tasks::TaskStatus::Succeeded);
    assert_eq!(task.node_id, None);
    assert_eq!(task.attempts, 1);
    assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Before-commit errors retry normally but cannot spin before their next deadline.
#[tokio::test(start_paused = true)]
async fn before_commit_pacing() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    probe.failure.store(14, Ordering::SeqCst);
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    assert!(
        runner
            .in_flight
            .values()
            .all(|entry| matches!(entry.phase, DbPhase::Retry { prepared: None, .. }))
    );
    for _ in 0..10 {
        runner.admit();
    }
    assert_eq!(probe.claims.load(Ordering::SeqCst), 1);
    tokio::time::advance(runner.poll_interval).await;
    probe.failure.store(0, Ordering::SeqCst);
    runner.admit();
    apply(&mut runner, &site).await;
    assert_eq!(probe.claims.load(Ordering::SeqCst), 2);
    assert!(runner.in_flight.is_empty());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Idle standalone has no watchdog; an outstanding hung call expires even without a DB permit.
#[tokio::test(start_paused = true)]
async fn watchdog_hung_and_idle() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(!runner.expire());
    probe.failure.store(5, Ordering::SeqCst);
    runner.start_database(
        DbRequest::Heartbeat,
        runner
            .db_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure())?,
    );
    assert!(runner.operations.next().now_or_never().is_none());
    assert_eq!(runner.db_slots.available_permits(), 0);
    tokio::time::advance(Duration::from_millis(60)).await;
    assert!(runner.expire());
    assert!(runner.mode == RunMode::Dead);
    assert_eq!(runner.db_slots.available_permits(), 1);
    assert!(runner.in_flight.is_empty());
    assert_eq!(probe.returns.load(Ordering::SeqCst), 0);
    Ok(())
}

/// A successful heartbeat extends contact liveness without acknowledging a failed claim.
#[tokio::test(start_paused = true)]
async fn heartbeat_preserves_failure() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 2)?;
    runner.initialize().await?;
    probe.failure.store(14, Ordering::SeqCst);
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    let prior = runner.death_at.ok_or_else(failure)?;
    tokio::time::advance(Duration::from_millis(5)).await;
    probe.failure.store(0, Ordering::SeqCst);
    runner.start_database(
        DbRequest::Heartbeat,
        runner
            .db_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure())?,
    );
    apply(&mut runner, &site).await;
    assert!(runner.death_at.ok_or_else(failure)? > prior);
    assert_eq!(runner.in_flight.len(), 1);
    assert_ne!(
        runner.health.snapshot().state,
        crate::tasks::health::TaskHealthState::Healthy
    );
    site.shutdown_and_wait().await;
    Ok(())
}

/// A ready response processed at the death deadline cannot revive or enqueue work.
#[tokio::test(start_paused = true)]
async fn expired_response_ignored() -> Result<(), TestError> {
    let (mut runner, _) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    runner.admit();
    let response = runner.operations.next().await.ok_or_else(failure)?;
    tokio::time::advance(Duration::from_millis(60)).await;
    let site = site().await;
    let (hooks, _) = mpsc::channel(1);
    runner.apply_database(&site, &hooks, response);
    assert!(runner.mode == RunMode::Dead);
    assert_eq!(runner.queued(), 0);
    assert_eq!(runner.db_slots.available_permits(), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Shutdown give-back retries on a poll boundary and only then finishes the runner.
#[tokio::test(start_paused = true)]
async fn giveback_retries() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    let site = site().await;
    runner.mode = RunMode::Draining;
    probe.failure.store(15, Ordering::SeqCst);
    runner.start_database(
        DbRequest::GiveBack,
        runner
            .db_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure())?,
    );
    apply(&mut runner, &site).await;
    assert!(runner.mode == RunMode::Draining);
    assert!(!runner.finished());
    runner.heartbeat_after = Some(tokio::time::Instant::now());
    assert_eq!(runner.deadline(), runner.retry_deadline());
    tokio::time::advance(runner.poll_interval).await;
    probe.failure.store(0, Ordering::SeqCst);
    runner.admit();
    apply(&mut runner, &site).await;
    assert!(runner.mode == RunMode::Finished);
    assert!(runner.death_at.is_none());
    site.shutdown_and_wait().await;
    Ok(())
}

/// An older recovered lane response cannot erase a hook completion collected during the DB call.
#[tokio::test]
async fn recovered_poll_keeps_newer_hook() -> Result<(), TestError> {
    use crate::tasks::{LaneHookAction, LaneHookResult};
    let (mut runner, _) = runner(TaskMode::Standalone, 1)?;
    let lane = runner.lanes.front_mut().ok_or_else(failure)?;
    let name = lane.conf.lane();
    lane.owner_token = Some("retained".into());
    lane.owner_generation = 7;
    lane.hook_result = Some(LaneHookResult {
        generation: 7,
        action: LaneHookAction::Busy,
        result: Ok(()),
    });
    let (request, poll) = hook_evidence(name);
    let site = site().await;
    let (hooks, _) = mpsc::channel(1);
    runner.apply_poll(&site, &hooks, &[request], poll);
    assert!(
        runner
            .lanes
            .front()
            .ok_or_else(failure)?
            .hook_result
            .is_some()
    );
    assert!(runner.running_hooks.is_empty());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Supplies the older coordination snapshot, before a local hook completion was collected.
fn hook_evidence(name: TaskLane) -> (LaneClaim, TaskPoll) {
    use crate::tasks::{LaneHookAction, LaneOwnerPhase, LaneOwnerPoll, LaneOwnerRequest, LanePoll};
    let request = LaneClaim {
        lane: name,
        limit: 0,
        owner: Some(LaneOwnerRequest {
            token: Some("retained".into()),
            quiescent: false,
            allow_claim: false,
            completed_work: false,
            hook: None,
        }),
    };
    let poll = TaskPoll {
        lanes: vec![LanePoll {
            lane: name,
            tasks: Vec::new(),
            reclaimed: 0,
            saturated: false,
            next_wake_in: None,
            owner: Some(LaneOwnerPoll {
                token: Some("retained".into()),
                generation: 7,
                phase: LaneOwnerPhase::Busying,
                action: Some(LaneHookAction::Busy),
                takeover: false,
            }),
        }],
        wake_lanes: Vec::new(),
    };
    (request, poll)
}
