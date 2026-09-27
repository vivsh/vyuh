use super::*;
use crate::tasks::TaskOutcome;
use crate::tasks::store::fixtures::{commit, flow_record, record, write};

/// Expanded flushing preserves group boundaries, FIFO, and deferred task renewal.
#[test]
fn all_flush_fifo_and_renewal() -> Result<(), TaskRuntimeError> {
    let mut runner = prefetch_runner()?;
    let first = commit(
        flow_record().id,
        TaskOutcome::All {
            state: "0".into(),
            children: (0..256).map(|_| write(record())).collect(),
        },
    );
    let second = commit(
        flow_record().id,
        TaskOutcome::All {
            state: "1".into(),
            children: vec![write(record())],
        },
    );
    let last = commit(record().id, TaskOutcome::Complete);
    let ids = [first.task_id, second.task_id, last.task_id];
    let mut turn = Vec::new();
    runner.queue_commits(vec![first, second, last], &mut turn);
    runner.limit_commits(&mut turn);
    assert_eq!(
        turn.iter().map(|commit| commit.task_id).collect::<Vec<_>>(),
        [ids[0]]
    );
    assert_eq!(
        runner
            .pending_commits
            .iter()
            .map(|commit| commit.task_id)
            .collect::<Vec<_>>(),
        [ids[1], ids[2]]
    );
    let leases = runner.renewal_leases(&turn);
    for id in ids {
        assert!(leases.iter().any(|lease| lease.task_id == id));
    }
    turn.clear();
    runner.fill_commits(&mut turn);
    runner.limit_commits(&mut turn);
    assert_eq!(
        turn.iter().map(|commit| commit.task_id).collect::<Vec<_>>(),
        [ids[1], ids[2]]
    );
    assert!(runner.pending_commits.is_empty());
    Ok(())
}

/// Ordinary buffering still fills the same count without introducing a pending-queue hop.
#[test]
fn ordinary_flush_capacity() -> Result<(), TaskRuntimeError> {
    let mut runner = prefetch_runner()?;
    let values = (0..4)
        .map(|_| commit(record().id, TaskOutcome::Complete))
        .collect();
    let mut turn = Vec::new();
    runner.queue_commits(values, &mut turn);
    runner.limit_commits(&mut turn);
    assert_eq!(turn.len(), 4);
    assert!(runner.pending_commits.is_empty());
    Ok(())
}

/// Deferred fan-out cannot grow its lane's queue indefinitely or consume other lanes' admission slots.
#[test]
fn deferred_join_backpressure_is_lane_local() -> Result<(), TaskRuntimeError> {
    let mut runner = prefetch_runner()?;
    let group = commit(
        flow_record().id,
        TaskOutcome::All {
            state: "0".into(),
            children: vec![write(record())],
        },
    );
    runner.pending_commits.push_back(group);
    let lane = runner
        .lanes
        .first_mut()
        .ok_or_else(|| TaskRuntimeError::InvalidConfig("Missing lane".into()))?;
    lane.uncommitted = lane.conf.concurrency();
    assert_eq!(
        super::super::all::claim_capacity(lane, &runner.pending_commits, 4),
        0
    );
    lane.uncommitted -= 1;
    assert_eq!(
        super::super::all::claim_capacity(lane, &runner.pending_commits, 4),
        1
    );
    lane.conf = TaskLaneConf::new(TaskLane::new("unrelated"), 4);
    assert_eq!(
        super::super::all::claim_capacity(lane, &runner.pending_commits, 4),
        4
    );
    runner.pending_commits.clear();
    assert_eq!(
        super::super::all::claim_capacity(lane, &runner.pending_commits, 4),
        4
    );
    Ok(())
}
