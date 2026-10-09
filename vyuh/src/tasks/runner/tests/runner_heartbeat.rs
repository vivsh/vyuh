use super::*;
use crate::tasks::store::{StoreFailure, TestError};

#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct Job;

/// A stalled heartbeat does not hold execution permits or prevent completion collection.
#[tokio::test]
async fn slow_heartbeat_collection() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    client.submit(Job).await?;
    assert_eq!(probe.registrations.load(Ordering::SeqCst), 0);
    runner.initialize().await?;
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    probe.failure.store(5, Ordering::SeqCst);
    runner.heartbeat_after = Some(tokio::time::Instant::now());
    runner.admit();
    assert!(runner.operations.next().now_or_never().is_none());
    let (tx, mut rx) = mpsc::channel(runner.concurrency);
    runner.dispatch_ready(&site, &tx);
    let completion = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency - 1
    );
    runner.accept_completion(completion);
    assert_eq!(runner.output_buffer.len(), 1);
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency
    );
    assert_eq!(runner.db_slots.available_permits(), 0);
    assert!(runner.heartbeat_request().is_none());
    probe.gate.notify_one();
    apply(&mut runner, &site).await;
    assert_eq!(runner.db_slots.available_permits(), 1);
    assert_eq!(runner.output_buffer.len(), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Returned claims reject late outputs both before and after a new claim increments the attempt.
#[tokio::test]
async fn returned_claim_rejects_stale() -> Result<(), TestError> {
    let (mut runner, _) = runner(TaskMode::Clustered, 1)?;
    runner.initialize().await?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    let id = client.submit(Job).await?.id();
    let claims = [LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit: 1,
        owner: None,
    }];
    runner
        .store
        .memory
        .claim_tasks(&runner.node_id, &claims)
        .await?;
    let commits = [TaskCommit::Outcome {
        node_id: runner.node_id,
        attempts: 1,
        task_id: id,
        lane: crate::tasks::DEFAULT_TASK_LANE,
        outcome: crate::tasks::TaskOutcome::Complete,
        owner_token: None,
    }];
    assert_eq!(runner.store.return_claims(Some(runner.node_id)).await?, 1);
    let rejected = runner.store.commit_tasks(&commits).await?.rejected;
    assert_eq!(rejected, vec![id]);
    runner
        .store
        .memory
        .claim_tasks(&runner.node_id, &claims)
        .await?;
    let rejected = runner.store.commit_tasks(&commits).await?.rejected;
    assert_eq!(rejected, vec![id]);
    let row = runner.store.get_task(id).await?.ok_or_else(failure)?;
    assert_eq!(row.status, crate::tasks::TaskStatus::Running);
    assert_eq!((row.attempts, row.step_attempts), (2, 2));
    assert_eq!(row.last_result, None);
    Ok(())
}

/// Standalone startup resets old-incarnation claims; clustered startup never steals them.
#[tokio::test]
async fn startup_reclaim_scope() -> Result<(), TestError> {
    for mode in [TaskMode::Standalone, TaskMode::Clustered] {
        let (mut runner, probe) = runner(mode, 1)?;
        let client = runner
            .registry
            .clone()
            .client(runner.store.clone(), Vec::new());
        let id = client.submit(Job).await?.id();
        let old = uuid::Uuid::now_v7();
        runner.store.memory.register_node(&old).await?;
        runner
            .store
            .memory
            .claim_tasks(
                &old,
                &[LaneClaim {
                    lane: crate::tasks::DEFAULT_TASK_LANE,
                    limit: 1,
                    owner: None,
                }],
            )
            .await?;
        runner.initialize().await?;
        let row = runner.store.get_task(id).await?.ok_or_else(failure)?;
        if mode == TaskMode::Standalone {
            assert_eq!(row.status, crate::tasks::TaskStatus::Pending);
            assert_eq!(row.node_id, None);
            assert_eq!(probe.reclaims.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(row.status, crate::tasks::TaskStatus::Running);
            assert_eq!(row.node_id, Some(old));
            assert_eq!(probe.reclaims.load(Ordering::SeqCst), 0);
        }
        assert_eq!(row.attempts, 1);
        assert_eq!(row.step_attempts, 1);
    }
    Ok(())
}

/// A delayed claim acknowledgement must settle before give-back; its queued work never runs.
#[tokio::test]
async fn late_claim_giveback() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    let id = client.submit(Job).await?.id();
    runner.initialize().await?;
    probe.failure.store(11, Ordering::SeqCst);
    runner.admit();
    assert!(runner.operations.next().now_or_never().is_none());
    runner.mode = RunMode::Draining;
    assert!(!runner.finished());
    assert!(!runner.give_back());
    assert_eq!(probe.returns.load(Ordering::SeqCst), 0);
    probe.gate.notify_one();
    let site = site().await;
    apply(&mut runner, &site).await;
    assert_eq!(runner.queued(), 1);
    let (sender, _) = mpsc::channel(1);
    runner.dispatch_ready(&site, &sender);
    assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
    assert!(runner.finished());
    assert!(runner.give_back());
    apply(&mut runner, &site).await;
    assert_eq!(runner.queued(), 0);
    let row = runner.store.get_task(id).await?.ok_or_else(failure)?;
    assert_eq!(row.status, crate::tasks::TaskStatus::Pending);
    assert_eq!(row.node_id, None);
    assert_eq!(row.attempts, 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// A permanent reset error faults shutdown without acknowledging or clearing durable ownership.
#[tokio::test]
async fn failed_giveback_retains_queue() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    let id = client.submit(Job).await?.id();
    runner.initialize().await?;
    runner.admit();
    let site = site().await;
    apply(&mut runner, &site).await;
    runner.mode = RunMode::Draining;
    probe.failure.store(10, Ordering::SeqCst);
    assert!(runner.give_back());
    apply(&mut runner, &site).await;
    assert!(runner.mode == RunMode::Faulted);
    assert_eq!(runner.queued(), 0);
    assert_eq!(
        runner
            .store
            .get_task(id)
            .await?
            .ok_or_else(failure)?
            .node_id,
        Some(runner.node_id)
    );
    assert!(!runner.give_back());
    assert_eq!(probe.returns.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Reclaim errors prevent standalone startup and cannot create a clustered node.
#[tokio::test]
async fn failed_reclaim_blocks_start() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    probe.failure.store(10, Ordering::SeqCst);
    assert!(runner.initialize().await.is_err());
    assert_eq!(probe.reclaims.load(Ordering::SeqCst), 1);
    assert_eq!(probe.registrations.load(Ordering::SeqCst), 0);
    assert_eq!(probe.claims.load(Ordering::SeqCst), 0);
    assert_eq!(runner.db_slots.available_permits(), 1);
    Ok(())
}

/// An unrelated success cannot acknowledge a heartbeat request awaiting retry.
#[tokio::test]
async fn late_success_keeps_fault() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 2)?;
    runner.initialize().await?;
    probe.failure.store(2, Ordering::SeqCst);
    runner.heartbeat_after = Some(tokio::time::Instant::now());
    runner.admit();
    assert_eq!(runner.in_flight.len(), 2);
    let site = site().await;
    apply(&mut runner, &site).await;
    apply(&mut runner, &site).await;
    assert!(runner.mode == RunMode::Running);
    assert_eq!(runner.in_flight.len(), 1);
    assert!(runner.deadline().is_some());
    site.shutdown_and_wait().await;
    Ok(())
}
use crate::tasks::{
    ScheduledTaskWrite, TaskConf, TaskFilter, TaskId, TaskMode, TaskReceipt, TaskScheduleSnapshot,
    TaskWrite, store::MemoryTaskStore,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[derive(Default)]
struct Probe {
    setups: AtomicUsize,
    reclaims: AtomicUsize,
    returns: AtomicUsize,
    registrations: AtomicUsize,
    beats: AtomicUsize,
    claims: AtomicUsize,
    executions: AtomicUsize,
    flushes: AtomicUsize,
    failure: AtomicUsize,
    gate: tokio::sync::Notify,
}

struct Store {
    memory: MemoryTaskStore,
    probe: Arc<Probe>,
}

macro_rules! forward {
    ($name:ident($($arg:ident: $ty:ty),*) -> $out:ty) => {
        async fn $name(&self, $($arg: $ty),*) -> Result<$out, TaskRuntimeError> {
            self.memory.$name($($arg),*).await
        }
    };
}

impl TaskStore for Store {
    async fn register_node(&self, node: &uuid::Uuid) -> Result<(), TaskRuntimeError> {
        self.probe.registrations.fetch_add(1, Ordering::SeqCst);
        if self.probe.failure.load(Ordering::SeqCst) == 6 {
            panic!("deliberate registration panic");
        }
        if self.probe.failure.load(Ordering::SeqCst) == 4 {
            return Err(failure());
        }
        self.memory.register_node(node).await
    }

    async fn heartbeat(
        &self,
        node: &uuid::Uuid,
        stale_after: Duration,
    ) -> Result<bool, TaskRuntimeError> {
        assert_eq!(stale_after, Duration::from_millis(60));
        self.probe.beats.fetch_add(1, Ordering::SeqCst);
        match self.probe.failure.load(Ordering::SeqCst) {
            1 => return Ok(false),
            2 => return Err(db_failure()),
            3 => panic!("deliberate store panic"),
            5 => self.probe.gate.notified().await,
            _ => (),
        }
        self.memory.heartbeat(node, stale_after).await
    }

    async fn claim_tasks(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        assert!(self.probe.registrations.load(Ordering::SeqCst) > 0);
        self.probe.claims.fetch_add(1, Ordering::SeqCst);
        if self.probe.failure.load(Ordering::SeqCst) == 14 {
            return Err(db_failure().into());
        }
        let result = self.memory.claim_tasks(node, claims).await;
        if self.probe.failure.load(Ordering::SeqCst) == 12 {
            return Err(StoreFailure::CommitUnknown {
                error: db_failure(),
                prepared: result?,
            });
        }
        if self.probe.failure.load(Ordering::SeqCst) == 11 {
            self.probe.gate.notified().await;
        }
        result
    }

    async fn return_claims(&self, node: Option<uuid::Uuid>) -> Result<u64, TaskRuntimeError> {
        if node.is_some() {
            self.probe.returns.fetch_add(1, Ordering::SeqCst);
        } else {
            self.probe.reclaims.fetch_add(1, Ordering::SeqCst);
        }
        if self.probe.failure.load(Ordering::SeqCst) == 10 {
            return Err(failure());
        }
        if self.probe.failure.load(Ordering::SeqCst) == 15 {
            return Err(db_failure());
        }
        self.memory.return_claims(node).await
    }

    async fn initialize(&self) -> Result<(), TaskRuntimeError> {
        self.probe.setups.fetch_add(1, Ordering::SeqCst);
        if self.probe.failure.load(Ordering::SeqCst) == 9 {
            return Err(failure());
        }
        self.memory.initialize().await
    }
    async fn commit_tasks(
        &self,
        commits: &[TaskCommit],
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        self.probe.flushes.fetch_add(1, Ordering::SeqCst);
        if self.probe.failure.load(Ordering::SeqCst) == 18 {
            return Err(failure().into());
        }
        let result = self.memory.commit_tasks(commits).await?;
        if self.probe.failure.load(Ordering::SeqCst) == 13 {
            return Err(StoreFailure::CommitUnknown {
                error: db_failure(),
                prepared: result,
            });
        }
        Ok(result)
    }
    async fn coordinate_lanes(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        self.memory.coordinate_lanes(node, claims).await
    }
    async fn recover_claims(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
        prepared: TaskPoll,
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        if self.probe.failure.load(Ordering::SeqCst) == 16 {
            return Err(StoreFailure::CommitUnknown {
                error: db_failure(),
                prepared,
            });
        }
        self.memory.recover_claims(node, claims, prepared).await
    }
    async fn recover_outputs(
        &self,
        node: &uuid::Uuid,
        commits: &[TaskCommit],
        prepared: CommitResult,
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        self.memory.recover_outputs(node, commits, prepared).await
    }
    forward!(store_tasks(writes: Vec<TaskWrite>) -> Vec<TaskReceipt>);
    forward!(schedule_snapshot(names: &[String]) -> TaskScheduleSnapshot);
    forward!(store_scheduled(write: ScheduledTaskWrite) -> Option<TaskReceipt>);
    forward!(reassign_lane(from: &str, to: &str) -> u64);
    forward!(resume(id: TaskId, input: String) -> bool);
    forward!(list_tasks(filter: TaskFilter) -> crate::routes::Page<TaskRecord>);
    forward!(get_task(id: TaskId) -> Option<TaskRecord>);
}

fn failure() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("deliberate heartbeat test failure".into())
}

/// Builds a real runner over a reference store with controllable heartbeat failures.
fn runner(
    mode: TaskMode,
    slots: usize,
) -> Result<(TaskRunner<Store>, Arc<Probe>), TaskRuntimeError> {
    let probe = Arc::new(Probe::default());
    let conf = TaskConf::default()
        .poll_interval(Duration::from_millis(2))
        .mode(mode)
        .db_concurrency(slots)
        .heartbeat_interval(Duration::from_millis(10));
    let mut registry = TaskRegistry::new();
    let execution = probe.clone();
    registry.register(crate::tasks::RegisteredTask::new(
        crate::tasks::TaskDefinition::new("heartbeat-job"),
        move |_: crate::Data<Job>| {
            let probe = execution.clone();
            async move {
                probe.executions.fetch_add(1, Ordering::SeqCst);
                if probe.failure.load(Ordering::SeqCst) == 8 {
                    probe.gate.notified().await;
                }
            }
        },
    ))?;
    let registry = Arc::new(registry.with_config(conf)?);
    let store = Store {
        memory: MemoryTaskStore::with_lease_duration(
            4,
            Duration::from_secs(300),
            registry.store_conf()?,
        ),
        probe: probe.clone(),
    };
    Ok((
        TaskRunner::new(registry.client(Arc::new(store), Vec::new()))?,
        probe,
    ))
}

async fn site() -> Site {
    Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .unwrap()
}

async fn apply(runner: &mut TaskRunner<Store>, site: &Site) {
    let completion = runner.operations.next().await.unwrap();
    let (hooks, _) = mpsc::channel(1);
    runner.apply_database(site, &hooks, completion);
}

/// Defaults and interval bounds are independent of polling and valid in either mode.
#[test]
fn configuration_bounds() {
    assert_eq!(TaskConf::default().mode_value(), TaskMode::Clustered);
    assert_eq!(
        TaskConf::default().heartbeat_interval_value(),
        Duration::from_secs(12)
    );
    for mode in [TaskMode::Standalone, TaskMode::Clustered] {
        for duration in [Duration::from_millis(1), Duration::from_secs(604800)] {
            assert!(
                TaskConf::default()
                    .mode(mode)
                    .heartbeat_interval(duration)
                    .validate()
                    .is_ok()
            );
        }
        for duration in [
            Duration::ZERO,
            Duration::from_micros(999),
            Duration::from_secs(604801),
        ] {
            assert!(
                TaskConf::default()
                    .mode(mode)
                    .heartbeat_interval(duration)
                    .validate()
                    .is_err()
            );
        }
    }
}

/// Each incarnation has a distinct UUID; standalone registers once without heartbeat scheduling.
#[tokio::test]
async fn identities_and_standalone() -> Result<(), TestError> {
    let (mut first, probe) = runner(TaskMode::Standalone, 1)?;
    let (second, _) = runner(TaskMode::Standalone, 1)?;
    assert_ne!(first.node_id, second.node_id);
    first.initialize().await?;
    assert_eq!(probe.registrations.load(Ordering::SeqCst), 1);
    assert!(first.heartbeat_deadline().is_none());
    assert!(first.heartbeat_request().is_none());
    Ok(())
}

/// Producer submission skips setup; worker setup and registration precede claims.
#[tokio::test]
async fn startup_registration_boundary() -> Result<(), TestError> {
    for mode in [TaskMode::Standalone, TaskMode::Clustered] {
        let (mut runner, probe) = runner(mode, 1)?;
        let client = runner
            .registry
            .clone()
            .client(runner.store.clone(), Vec::new());
        client.submit(Job).await?;
        assert_eq!(probe.setups.load(Ordering::SeqCst), 0);
        assert_eq!(probe.registrations.load(Ordering::SeqCst), 0);
        runner.initialize().await?;
        assert_eq!(probe.setups.load(Ordering::SeqCst), 1);
        runner.admit();
        let site = site().await;
        apply(&mut runner, &site).await;
        assert_eq!(probe.registrations.load(Ordering::SeqCst), 1);
        assert_eq!(probe.claims.load(Ordering::SeqCst), 1);
        site.shutdown_and_wait().await;
    }
    Ok(())
}

/// Failed registration leaves all claims unstarted and releases the startup database permit.
#[tokio::test]
async fn registration_failure() -> Result<(), TestError> {
    for mode in [TaskMode::Standalone, TaskMode::Clustered] {
        for failure in [4, 6] {
            let (mut runner, probe) = runner(mode, 1)?;
            probe.failure.store(failure, Ordering::SeqCst);
            assert!(runner.initialize().await.is_err());
            assert_eq!(probe.registrations.load(Ordering::SeqCst), 1);
            assert_eq!(probe.claims.load(Ordering::SeqCst), 0);
            assert_eq!(runner.db_slots.available_permits(), 1);
        }
    }
    Ok(())
}

/// Saturation creates no permit waiters; the next free slot goes to the due heartbeat.
#[tokio::test]
async fn heartbeat_budget_priority() -> Result<(), TestError> {
    for slots in [1, 2, 4] {
        let (mut runner, _) = runner(TaskMode::Clustered, slots)?;
        runner.initialize().await?;
        runner.heartbeat_after = Some(tokio::time::Instant::now());
        let mut permits = Vec::new();
        for _ in 0..slots {
            permits.push(runner.db_slots.clone().try_acquire_owned().unwrap());
        }
        runner.admit();
        assert!(runner.in_flight.is_empty());
        assert!(runner.deadline().is_none());
        drop(permits.pop());
        runner.admit();
        assert_eq!(runner.in_flight.len(), 1);
        assert!(
            runner
                .in_flight
                .values()
                .all(|r| matches!(&*r.request, DbRequest::Heartbeat))
        );
        assert_eq!(runner.db_slots.available_permits(), 0);
    }
    Ok(())
}

/// Standalone registers only at startup and never heartbeats across configured intervals.
#[tokio::test]
async fn standalone_event_loop() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    runner.initialize().await?;
    for lane in &mut runner.lanes {
        lane.poll_after = tokio::time::Instant::now() + Duration::from_secs(300);
    }
    let site = site().await;
    let worker = tokio::spawn(runner.run(site.clone()));
    tokio::time::sleep(Duration::from_millis(40)).await;
    site.shutdown();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(probe.registrations.load(Ordering::SeqCst), 1);
    assert_eq!(probe.beats.load(Ordering::SeqCst), 0);
    site.shutdown_and_wait().await;
    Ok(())
}

/// An idle heartbeat uses one DB slot, does not rotate lanes, and schedules from acknowledgement.
#[tokio::test]
async fn priority_and_cadence() -> Result<(), TestError> {
    let site = site().await;
    for slots in [1, 2, 4] {
        let (mut runner, probe) = runner(TaskMode::Clustered, slots)?;
        runner.initialize().await?;
        for lane in &mut runner.lanes {
            lane.poll_after = tokio::time::Instant::now() + Duration::from_secs(300);
        }
        let order: Vec<_> = runner.lanes.iter().map(|lane| lane.conf.lane()).collect();
        runner.heartbeat_after = Some(tokio::time::Instant::now());
        runner.admit();
        runner.admit();
        assert_eq!(runner.in_flight.len(), 1);
        assert_eq!(runner.db_slots.available_permits(), slots - 1);
        assert!(runner.heartbeat_deadline().is_none());
        assert_eq!(
            order,
            runner
                .lanes
                .iter()
                .map(|lane| lane.conf.lane())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
        apply(&mut runner, &site).await;
        assert_eq!(probe.beats.load(Ordering::SeqCst), 1);
        assert!(runner.heartbeat_request().is_none());
        assert_eq!(runner.deadline(), runner.heartbeat_after);
        assert_eq!(runner.db_slots.available_permits(), slots);
        assert_eq!(
            runner.execution_slots.available_permits(),
            runner.concurrency
        );
    }
    site.shutdown_and_wait().await;
    Ok(())
}

/// Failed and panicking heartbeats fault without acknowledging or replaying the request.
#[tokio::test]
async fn heartbeat_failures() -> Result<(), TestError> {
    let site = site().await;
    for failure in [2, 3] {
        let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
        runner.initialize().await?;
        probe.failure.store(failure, Ordering::SeqCst);
        runner.start_database(
            DbRequest::Heartbeat,
            runner.db_slots.clone().try_acquire_owned().unwrap(),
        );
        apply(&mut runner, &site).await;
        if failure == 2 {
            assert!(runner.mode == RunMode::Running);
            assert_eq!(runner.in_flight.len(), 1);
        } else {
            assert!(runner.mode == RunMode::Faulted);
            assert!(runner.in_flight.is_empty());
        }
        assert_eq!(runner.deadline().is_some(), failure == 2);
        assert_eq!(runner.db_slots.available_permits(), 1);
        assert_eq!(probe.registrations.load(Ordering::SeqCst), 1);
        assert!(
            runner
                .metrics
                .render(runner.health.snapshot())
                .contains("result=\"failure\"} 1")
        );
    }
    site.shutdown_and_wait().await;
    Ok(())
}

/// Drain waits for admitted heartbeats but not future deadlines, including an overdue deadline.
#[tokio::test]
async fn drain_boundary() -> Result<(), TestError> {
    let (mut runner, _) = runner(TaskMode::Clustered, 1)?;
    runner.initialize().await?;
    runner.mode = RunMode::Draining;
    runner.heartbeat_after = Some(tokio::time::Instant::now());
    assert!(runner.finished());
    runner.start_database(
        DbRequest::Heartbeat,
        runner.db_slots.clone().try_acquire_owned().unwrap(),
    );
    assert!(!runner.finished());
    let site = site().await;
    apply(&mut runner, &site).await;
    assert!(runner.finished());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Idle event-loop heartbeats progress without claims or notifications and stop on graceful shutdown.
#[tokio::test]
async fn idle_event_loop() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
    runner.initialize().await?;
    for lane in &mut runner.lanes {
        lane.poll_after = tokio::time::Instant::now() + Duration::from_secs(300);
    }
    let site = site().await;
    let worker = tokio::spawn(runner.run(site.clone()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while probe.beats.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(probe.claims.load(Ordering::SeqCst), 0);
    site.shutdown();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap();
    site.shutdown_and_wait().await;
    Ok(())
}

/// Graceful shutdown keeps heartbeats running until an active handler and its output settle.
#[tokio::test]
async fn draining_execution_heartbeats() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    let receipt = client.submit(Job).await?;
    runner.initialize().await?;
    probe.failure.store(8, Ordering::SeqCst);
    let site = site().await;
    let worker = tokio::spawn(runner.run(site.clone()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while probe.executions.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    site.shutdown();
    let prior = probe.beats.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while probe.beats.load(Ordering::SeqCst) < prior + 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    probe.gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(probe.registrations.load(Ordering::SeqCst), 1);
    let task = client.store.get_task(receipt.id()).await?.unwrap();
    assert_eq!(task.status, crate::tasks::TaskStatus::Succeeded);
    assert_eq!(probe.returns.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Failed worker setup prevents registration; producers never trigger or retry setup.
#[tokio::test]
async fn failed_setup_blocks_registration() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
    probe.failure.store(9, Ordering::SeqCst);
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    client.submit(Job).await?;
    assert_eq!(probe.setups.load(Ordering::SeqCst), 0);
    assert!(runner.initialize().await.is_err());
    assert_eq!(probe.setups.load(Ordering::SeqCst), 1);
    assert_eq!(probe.registrations.load(Ordering::SeqCst), 0);
    assert_eq!(probe.claims.load(Ordering::SeqCst), 0);
    client.submit(Job).await?;
    assert_eq!(probe.setups.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A missing node exits without shutdown notification, new flushes, or graceful give-back.
#[tokio::test]
async fn missing_node_exits() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 2)?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    client.submit(Job).await?;
    runner.initialize().await?;
    probe.failure.store(8, Ordering::SeqCst);
    let site = site().await;
    let worker = tokio::spawn(runner.run(site.clone()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while probe.executions.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| failure())?;
    probe.failure.store(1, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .map_err(|_| failure())?
        .map_err(|_| failure())?;
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.returns.load(Ordering::SeqCst), 0);
    assert_eq!(probe.registrations.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Retirement aborts invocations and hooks, drops DB requests, and cannot be reversed by late responses.
#[tokio::test]
async fn retirement_cleanup() -> Result<(), TestError> {
    let site = site().await;
    for mode in [RunMode::Running, RunMode::Draining] {
        let (mut runner, probe) = runner(TaskMode::Clustered, 2)?;
        runner.initialize().await?;
        let invocation = tokio::spawn(std::future::pending::<()>());
        let hook = tokio::spawn(std::future::pending::<()>());
        let lane = crate::tasks::DEFAULT_TASK_LANE;
        runner.running_invocations.insert(
            uuid::Uuid::now_v7(),
            RunningInvocation {
                lane,
                task_ids: Vec::new(),
                abort: invocation.abort_handle(),
            },
        );
        runner.running_hooks.insert(
            lane,
            RunningLaneHook {
                token: "token".into(),
                generation: 1,
                action: crate::tasks::LaneHookAction::Idle,
                abort: hook.abort_handle(),
                started: std::time::Instant::now(),
            },
        );
        probe.failure.store(1, Ordering::SeqCst);
        runner.heartbeat_after = Some(tokio::time::Instant::now());
        runner.admit();
        runner.mode = mode;
        apply(&mut runner, &site).await;
        assert!(runner.mode == RunMode::Retired);
        assert_eq!(
            runner.health.snapshot().state,
            crate::tasks::health::TaskHealthState::Unhealthy
        );
        assert!(invocation.await.is_err());
        assert!(hook.await.is_err());
        assert_eq!(runner.db_slots.available_permits(), 2);
        assert!(runner.in_flight.is_empty() && runner.operations.is_empty());
        runner.admit();
        assert!(runner.in_flight.is_empty());
        assert!(!runner.give_back());
        assert_eq!(probe.returns.load(Ordering::SeqCst), 0);
    }
    site.shutdown_and_wait().await;
    Ok(())
}

/// A completion holding execution permits is discarded after retirement, not buffered or persisted.
#[tokio::test]
async fn retired_late_completion() -> Result<(), TestError> {
    let (mut runner, probe) = runner(TaskMode::Clustered, 1)?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    client.submit(Job).await?;
    runner.initialize().await?;
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    let (tx, mut rx) = mpsc::channel(1);
    runner.dispatch_ready(&site, &tx);
    let completion = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .map_err(|_| failure())?
        .ok_or_else(failure)?;
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency - 1
    );
    runner.retire();
    runner.accept_completion(completion);
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency
    );
    assert!(runner.output_buffer.is_empty());
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
    site.shutdown_and_wait().await;
    Ok(())
}

fn db_failure() -> TaskRuntimeError {
    TaskRuntimeError::DatabaseError(crate::db::sqlx::Error::PoolTimedOut)
}
#[path = "runner_recovery.rs"]
mod recovery;

#[path = "runner_control.rs"]
mod control;
