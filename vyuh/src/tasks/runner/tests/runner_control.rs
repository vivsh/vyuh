use super::*;
use crate::tasks::TaskStatus;

/// Connects a facade to the runner's bounded control channel, as normal construction does.
fn connected() -> Result<(TaskRunner<Store>, TaskClient<Store>, Arc<Probe>), TaskRuntimeError> {
    let (mut runner, probe) = runner(TaskMode::Standalone, 1)?;
    let client = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    runner.events = client.events.subscribe();
    Ok((runner, client, probe))
}

/// Terminal cancellation is idempotent; repeated acknowledgements may safely repeat local cleanup.
#[tokio::test]
async fn request_is_idempotent() -> Result<(), TestError> {
    let (mut runner, client, _) = connected()?;
    let id = client.submit(Job).await?.id();
    runner.collect_events();
    let (): () = client.cancel(id).await?;
    assert!(matches!(runner.events.try_recv(), Ok(RunnerEvent::Cancel(value)) if value == id));
    client.cancel(id).await?;
    client.cancel(TaskId::new(uuid::Uuid::new_v4())).await?;
    runner.collect_events();
    assert!(runner.events.try_recv().is_err());
    assert!(
        runner
            .store
            .get_task(id)
            .await?
            .ok_or_else(failure)?
            .cancelled
    );
    drop(runner);
    client.cancel(id).await?;
    Ok(())
}

/// A failed durable request cannot remove queued work or emit a cancellation notification.
#[tokio::test]
async fn failed_request_does_not_notify() -> Result<(), TestError> {
    let (mut runner, client, probe) = connected()?;
    let id = client.submit(Job).await?.id();
    runner.collect_events();
    probe.failure.store(18, Ordering::SeqCst);
    assert!(client.cancel(id).await.is_err());
    assert!(runner.events.try_recv().is_err());
    assert!(
        !runner
            .store
            .get_task(id)
            .await?
            .ok_or_else(failure)?
            .cancelled
    );
    Ok(())
}

/// Uses the same roster/configuration with a batch handler to exercise invocation-level permits.
fn batch_connected() -> Result<(TaskRunner<Store>, TaskClient<Store>, Arc<Probe>), TaskRuntimeError>
{
    let (original, _, probe) = connected()?;
    let mut registry = TaskRegistry::new();
    let execution = probe.clone();
    registry.register(crate::tasks::RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("heartbeat-job"),
        move |input: crate::Data<crate::tasks::Batch<Job>>| {
            let probe = execution.clone();
            async move {
                probe.executions.fetch_add(input.len(), Ordering::SeqCst);
                if probe.failure.load(Ordering::SeqCst) == 8 {
                    probe.gate.notified().await;
                }
            }
        },
    ))?;
    let registry = Arc::new(registry.with_config(original.registry.config.clone())?);
    let client = registry.clone().client(original.store.clone(), Vec::new());
    let mut runner = TaskRunner::new(registry.client(original.store.clone(), Vec::new()))?;
    runner.events = client.events.subscribe();
    Ok((runner, client, probe))
}

/// Queued cancellation excludes only that member before the remaining records form a batch.
#[tokio::test]
async fn queued_batch_member_is_skipped() -> Result<(), TestError> {
    batch_cancellation(false).await
}

/// Running batch cancellation never aborts or replays siblings and retains its single permit.
#[tokio::test]
async fn running_batch_member_does_not_abort_siblings() -> Result<(), TestError> {
    batch_cancellation(true).await
}

/// Collects one bounded handler completion through the actual permit-release boundary.
async fn collect_one(
    runner: &mut TaskRunner<Store>,
    rx: &mut mpsc::Receiver<Completion>,
) -> Result<(), TestError> {
    let completion = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .map_err(|_| failure())?
        .ok_or_else(failure)?;
    runner.accept_completion(completion);
    Ok(())
}

fn queued_ids(runner: &TaskRunner<Store>) -> Vec<TaskId> {
    runner
        .lanes
        .iter()
        .flat_map(|lane| lane.tasks.iter().map(|row| row.id()))
        .collect()
}

/// Verifies independent per-row persistence and unchanged sibling attempt counts after a batch flush.
async fn assert_batch_results(runner: &TaskRunner<Store>, ids: &[TaskId]) -> Result<(), TestError> {
    for (index, id) in ids.iter().enumerate() {
        let task = runner.store.get_task(*id).await?.ok_or_else(failure)?;
        let expected = if index == 1 {
            TaskStatus::Failed
        } else {
            TaskStatus::Succeeded
        };
        assert_eq!(task.status, expected);
        assert_eq!(task.attempts, 1);
    }
    Ok(())
}

/// Drives cancellation on either side of batch dispatch through the real shared collector and flush.
async fn batch_cancellation(running: bool) -> Result<(), TestError> {
    let (mut runner, client, probe) = batch_connected()?;
    runner.initialize().await?;
    let ids = client
        .submit_many(vec![Job, Job, Job])
        .await?
        .into_iter()
        .map(|r| r.id())
        .collect::<Vec<_>>();
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    let (tx, mut rx) = mpsc::channel(1);
    probe.failure.store(8, Ordering::SeqCst);
    if running {
        runner.dispatch_ready(&site, &tx);
    }
    client.cancel(ids[1]).await?;
    runner.collect_events();
    if !running {
        assert_eq!(queued_ids(&runner), vec![ids[0], ids[2]]);
        runner.dispatch_ready(&site, &tx);
    }
    assert_eq!(runner.running_invocations.len(), 1);
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency - 1
    );
    probe.gate.notify_one();
    collect_one(&mut runner, &mut rx).await?;
    assert_eq!(runner.output_buffer.len(), if running { 3 } else { 2 });
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency
    );
    runner.flush_after = tokio::time::Instant::now();
    runner.admit();
    apply(&mut runner, &site).await;
    assert_batch_results(&runner, &ids).await?;
    assert_eq!(
        probe.executions.load(Ordering::SeqCst),
        if running { 3 } else { 2 }
    );
    site.shutdown_and_wait().await;
    Ok(())
}

/// Cancellation commits before local queue removal, with no second output, permit or pressure change.
#[tokio::test(start_paused = true)]
async fn queued_cancel_needs_no_flush() -> Result<(), TestError> {
    let (mut runner, client, probe) = connected()?;
    runner.initialize().await?;
    let id = client.submit(Job).await?.id();
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    let snapshot = runner.lanes.front().ok_or_else(failure)?.tasks[0].clone();
    let slots = runner.execution_slots.available_permits();
    client.cancel(id).await?;
    assert_eq!(
        runner.store.get_task(id).await?.ok_or_else(failure)?.status,
        TaskStatus::Failed
    );
    runner.collect_events();
    runner.accept_event(Ok(RunnerEvent::Cancel(id)));
    assert_eq!(runner.queued(), 0);
    assert!(!snapshot.cancelled);
    assert_eq!(runner.execution_slots.available_permits(), slots);
    assert!(runner.output_buffer.is_empty());
    assert_eq!(runner.lanes.front().ok_or_else(failure)?.uncommitted, 0);
    let (tx, _) = mpsc::channel(1);
    runner.dispatch_ready(&site, &tx);
    assert_eq!(probe.executions.load(Ordering::SeqCst), 0);
    assert!(runner.flush_request().is_none());
    let task = runner.store.get_task(id).await?.ok_or_else(failure)?;
    assert_eq!(task.status, TaskStatus::Failed);
    assert!(task.cancelled);
    assert_eq!(task.node_id, None);
    assert_eq!(task.attempts, snapshot.attempts);
    assert_eq!(runner.lanes.front().ok_or_else(failure)?.uncommitted, 0);
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Running work is not interrupted; cancellation commits now and its later output is obsolete.
#[tokio::test]
async fn running_cancel_never_aborts() -> Result<(), TestError> {
    let (mut runner, client, probe) = connected()?;
    runner.initialize().await?;
    let id = client.submit(Job).await?.id();
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    probe.failure.store(8, Ordering::SeqCst);
    let (tx, mut rx) = mpsc::channel(1);
    let slots = runner.execution_slots.available_permits();
    runner.dispatch_ready(&site, &tx);
    client.cancel(id).await?;
    runner.collect_events();
    assert_eq!(runner.running_invocations.len(), 1);
    assert!(runner.output_buffer.is_empty());
    assert_eq!(runner.execution_slots.available_permits(), slots - 1);
    assert_eq!(
        runner.store.get_task(id).await?.ok_or_else(failure)?.status,
        TaskStatus::Failed
    );
    probe.gate.notify_one();
    collect_one(&mut runner, &mut rx).await?;
    assert_eq!(runner.execution_slots.available_permits(), slots);
    runner.accept_event(Ok(RunnerEvent::Cancel(id)));
    assert_eq!(runner.output_buffer.len(), 1);
    runner.flush_after = tokio::time::Instant::now();
    runner.admit();
    apply(&mut runner, &site).await;
    assert_eq!(
        runner.store.get_task(id).await?.ok_or_else(failure)?.status,
        TaskStatus::Failed
    );
    assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// An independent producer cannot alter this runner's local queues; durable cancellation still wins.
#[tokio::test]
async fn separate_channel_is_best_effort() -> Result<(), TestError> {
    let (mut runner, client, probe) = connected()?;
    runner.initialize().await?;
    let id = client.submit(Job).await?.id();
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    runner.collect_events();
    let remote = runner
        .registry
        .clone()
        .client(runner.store.clone(), Vec::new());
    remote.cancel(id).await?;
    runner.collect_events();
    assert_eq!(runner.queued(), 1);
    let (tx, mut rx) = mpsc::channel(1);
    runner.dispatch_ready(&site, &tx);
    runner.accept_completion(rx.recv().await.ok_or_else(failure)?);
    runner.flush_after = tokio::time::Instant::now();
    runner.admit();
    apply(&mut runner, &site).await;
    assert_eq!(
        runner.store.get_task(id).await?.ok_or_else(failure)?.status,
        TaskStatus::Failed
    );
    assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
    Ok(())
}

/// Notifications do not retain tombstones for a claim response which has not reached the lane yet.
#[tokio::test]
async fn cancellation_before_claim_response_can_miss_queue() -> Result<(), TestError> {
    let (mut runner, client, _) = connected()?;
    runner.initialize().await?;
    let id = client.submit(Job).await?.id();
    let site = site().await;
    runner.admit();
    let response = runner.operations.next().await.ok_or_else(failure)?;
    client.cancel(id).await?;
    runner.collect_events();
    assert_eq!(runner.queued(), 0);
    let (hooks, _) = mpsc::channel(1);
    runner.apply_database(&site, &hooks, response);
    assert_eq!(runner.queued(), 1);
    assert!(runner.output_buffer.is_empty());
    assert!(
        runner
            .store
            .get_task(id)
            .await?
            .ok_or_else(failure)?
            .cancelled
    );
    site.shutdown_and_wait().await;
    Ok(())
}

/// A bounded lag or closed notification stream does not lose durable intent or cause DB work.
#[tokio::test(start_paused = true)]
async fn lag_and_closure_are_notification_only() -> Result<(), TestError> {
    let (mut runner, client, probe) = connected()?;
    let id = client.submit(Job).await?.id();
    client.cancel(id).await?;
    for _ in 0..SCHEDULER_BUDGET * 4 {
        let _ = client.events.send(RunnerEvent::Wake);
    }
    assert!(matches!(
        runner.events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
    runner.collect_events();
    assert!(
        runner
            .store
            .get_task(id)
            .await?
            .ok_or_else(failure)?
            .cancelled
    );
    assert_eq!(probe.claims.load(Ordering::SeqCst), 0);
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 1);
    drop(client);
    runner.collect_events();
    assert!(runner.events.is_closed() && runner.events.is_empty());
    runner.accept_event(Err(tokio::sync::broadcast::error::RecvError::Closed));
    assert!(runner.mode == RunMode::Running);
    Ok(())
}

/// Wake shortens fallback polling only to the normal interval, and cancellation does not accelerate flushes.
#[tokio::test(start_paused = true)]
async fn wake_preserves_poll_gates() -> Result<(), TestError> {
    let (mut runner, _, _) = connected()?;
    let now = tokio::time::Instant::now();
    runner.lanes.front_mut().ok_or_else(failure)?.poll_after = now + Duration::from_secs(60);
    let flush = runner.flush_after;
    runner.accept_event(Ok(RunnerEvent::Wake));
    assert_eq!(
        runner.lanes.front().ok_or_else(failure)?.poll_after,
        now + runner.poll_interval
    );
    runner.accept_event(Ok(RunnerEvent::Cancel(TaskId::new(uuid::Uuid::new_v4()))));
    assert_eq!(runner.flush_after, flush);
    assert!(runner.in_flight.is_empty());
    Ok(())
}

/// Draining cancellation requires no pending output and remains safe around final give-back.
#[tokio::test]
async fn draining_and_give_back_boundary() -> Result<(), TestError> {
    let (mut runner, client, _) = connected()?;
    runner.initialize().await?;
    let id = client.submit(Job).await?.id();
    let site = site().await;
    runner.admit();
    apply(&mut runner, &site).await;
    runner.mode = RunMode::Draining;
    client.cancel(id).await?;
    runner.collect_events();
    assert!(runner.output_buffer.is_empty());
    runner.give_back();
    assert!(
        runner
            .in_flight
            .values()
            .any(|entry| matches!(&*entry.request, DbRequest::GiveBack))
    );
    runner.accept_event(Ok(RunnerEvent::Cancel(id)));
    assert!(runner.output_buffer.is_empty());
    apply(&mut runner, &site).await;
    assert!(runner.mode == RunMode::Finished);
    site.shutdown_and_wait().await;
    Ok(())
}
