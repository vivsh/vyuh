use super::*;

/// Full ingress suppresses local flush delivery even when execution capacity remains free.
#[tokio::test]
async fn flush_respects_ingress_space() -> Result<(), String> {
    let (runner, _, _) = delivery_fixture().await?;
    let commit = delivery_spawn(runner.node_id)?;
    let mut permits = Vec::new();
    for expected in [1, 1, 1, 0] {
        let DbRequest::Commit { capacity, .. } = runner.delivery_request(vec![commit.clone()])
        else {
            return Err("missing flush".into());
        };
        assert_eq!(capacity, expected);
        if expected > 0 {
            permits.push(
                runner
                    .ingress_tx
                    .clone()
                    .try_reserve_owned()
                    .map_err(|e| e.to_string())?,
            );
        }
    }
    drop(permits);
    let DbRequest::Commit { capacity, .. } = runner.delivery_request(vec![commit]) else {
        return Err("missing flush".into());
    };
    assert_eq!(capacity, 1);
    Ok(())
}
/// Lane hints survive an older empty claim response and never bypass normal poll pacing.
#[tokio::test]
async fn notification_wake_survives_empty_claim() -> Result<(), String> {
    let (mut runner, client, _) = delivery_fixture().await?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|e| e.to_string())?;
    let now = tokio::time::Instant::now();
    runner.lanes.front_mut().ok_or("lane")?.poll_after = now + Duration::from_secs(60);
    client
        .events
        .send(RunnerEvent::WakeLane(DEFAULT_TASK_LANE))
        .map_err(|e| e.to_string())?;
    runner.collect_events();
    let deadline = runner.lanes.front().ok_or("lane")?.poll_after;
    assert!(deadline >= now + runner.poll_interval);
    assert!(deadline < now + runner.poll_interval + Duration::from_secs(1));
    let (hooks, _) = mpsc::channel(1);
    runner.apply_poll(
        &site,
        &hooks,
        &[],
        TaskPoll {
            lanes: vec![crate::tasks::LanePoll {
                lane: DEFAULT_TASK_LANE,
                tasks: vec![],
                reclaimed: 0,
                saturated: false,
                next_wake_in: None,
                owner: None,
            }],
            wake_lanes: vec![],
        },
    );
    assert!(
        runner.lanes.front().ok_or("lane")?.poll_after
            < now + runner.poll_interval + Duration::from_secs(1)
    );
    Ok(())
}

/// Overflow in the existing bounded event channel requests a broad recheck rather than losing work.
#[tokio::test]
async fn notification_lag_rescans() -> Result<(), String> {
    let (mut runner, client, _) = delivery_fixture().await?;
    let now = tokio::time::Instant::now();
    runner.lanes.front_mut().ok_or("lane")?.poll_after = now + Duration::from_secs(60);
    for _ in 0..4096 {
        client
            .events
            .send(RunnerEvent::WakeLane(DEFAULT_TASK_LANE))
            .map_err(|e| e.to_string())?;
    }
    runner.collect_events();
    assert!(
        runner.lanes.front().ok_or("lane")?.poll_after
            < now + runner.poll_interval + Duration::from_secs(1)
    );
    Ok(())
}

/// Committed channel input follows execution, collection and fenced flush without a claim query.
#[tokio::test]
async fn delivered_input_uses_pipeline() -> Result<(), String> {
    let (mut runner, client, store) = delivery_fixture().await?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|e| e.to_string())?;
    client.submit(PanicJob).await.map_err(|e| e.to_string())?;
    let record = runner.ingress_rx.try_recv().map_err(|e| e.to_string())?;
    let id = record.id;
    runner.accept_input(record);
    let (sender, mut receiver) = mpsc::channel(1);
    runner.dispatch_ready(&site, &sender);
    runner.accept_completion(receiver.recv().await.ok_or("missing outcome")?);
    assert_eq!(runner.output_buffer.len(), 1);
    assert_eq!(
        runner.execution_slots.available_permits(),
        runner.concurrency
    );
    runner.flush_after = tokio::time::Instant::now();
    let request = runner.flush_request().ok_or("missing flush")?;
    let permit = runner
        .db_slots
        .clone()
        .try_acquire_owned()
        .map_err(|e| e.to_string())?;
    runner.start_database(request, permit);
    let completion = runner
        .operations
        .next()
        .await
        .ok_or("missing acknowledgement")?;
    let (hooks, _) = mpsc::channel(1);
    runner.apply_database(&site, &hooks, completion);
    let task = store
        .get_task(id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing task")?;
    assert_eq!(task.status, crate::tasks::TaskStatus::Failed);
    assert_eq!((task.node_id, task.attempts), (None, 1));
    assert_eq!(runner.lanes.front().ok_or("missing lane")?.uncommitted, 0);
    Ok(())
}

/// Public bulk submission publishes only committed records, falling back on full/closed input channels.
#[tokio::test]
async fn submission_channel_fallback() -> Result<(), String> {
    let (mut runner, client, store) = delivery_fixture().await?;
    let _first = runner
        .ingress_tx
        .clone()
        .try_reserve_owned()
        .map_err(|e| e.to_string())?;
    let _second = runner
        .ingress_tx
        .clone()
        .try_reserve_owned()
        .map_err(|e| e.to_string())?;
    let receipts = client
        .submit_many([PanicJob, PanicJob, PanicJob])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(receipts.len(), 3);
    let delivered = runner.ingress_rx.try_recv().map_err(|e| e.to_string())?;
    let persisted = store
        .get_task(delivered.id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing committed task")?;
    assert_eq!(
        (persisted.status, persisted.node_id, persisted.attempts),
        (crate::tasks::TaskStatus::Running, Some(runner.node_id), 1)
    );
    assert_fallbacks(&store, &receipts[1..]).await?;
    assert_closed_or_delayed(&mut runner, &client, &store).await
}

/// Bulk submissions exceed commit/ingress capacities and persist overflow as ordinary pending work.
#[tokio::test]
async fn submission_capacity_independent() -> Result<(), String> {
    let (runner, client, store) = delivery_fixture().await?;
    let receipts = client
        .submit_many((0..5).map(|_| PanicJob))
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(receipts.len(), 5);
    assert_eq!(runner.ingress_rx.len(), 3);
    assert_fallbacks(&store, &receipts[3..]).await?;
    let overflow = client
        .submit_many((0..5).map(|_| PanicJob))
        .await
        .map_err(|e| e.to_string())?;
    assert_fallbacks(&store, &overflow).await?;
    assert_eq!(store.task_count().await, 10);
    assert_eq!(runner.ingress_rx.len(), 3);
    Ok(())
}

/// The fixed submission ceiling accepts exactly 10,000 inputs and rejects overflow before mutation.
#[tokio::test]
async fn submission_safety_ceiling() -> Result<(), String> {
    let (runner, client, store) = delivery_fixture().await?;
    let error = client
        .submit_many((0..10_001).map(|_| PanicJob))
        .await
        .err()
        .ok_or("oversized submission accepted")?;
    assert!(matches!(
        error,
        crate::tasks::TaskRuntimeError::InvalidOptions(_)
    ));
    assert_eq!(store.task_count().await, 0);
    assert_eq!(runner.ingress_rx.len(), 0);
    let receipts = client
        .submit_many((0..10_000).map(|_| PanicJob))
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(receipts.len(), 10_000);
    assert_eq!(store.task_count().await, 10_000);
    assert_eq!(runner.ingress_rx.len(), 3);
    Ok(())
}

/// Fallback records have not acquired execution ownership or consumed an attempt.
async fn assert_fallbacks(
    store: &MemoryTaskStore,
    receipts: &[crate::tasks::TaskReceipt],
) -> Result<(), String> {
    for receipt in receipts {
        let crate::tasks::TaskReceipt::Queued(id) = receipt else {
            return Err("not queued".into());
        };
        assert_eq!(
            store
                .get_task(*id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or("missing fallback")?
                .status,
            crate::tasks::TaskStatus::Pending
        );
    }
    Ok(())
}

/// Delayed and closed-channel submissions retain the ordinary Pending path.
async fn assert_closed_or_delayed(
    runner: &mut TaskRunner<MemoryTaskStore>,
    client: &TaskClient<MemoryTaskStore>,
    store: &MemoryTaskStore,
) -> Result<(), String> {
    client
        .submit_with(
            PanicJob,
            crate::tasks::TaskOptions::new().delay(Duration::from_secs(60)),
        )
        .await
        .map_err(|e| e.to_string())?;
    assert!(runner.ingress_rx.try_recv().is_err());
    runner.ingress_rx.close();
    let receipt = client.submit(PanicJob).await.map_err(|e| e.to_string())?;
    let crate::tasks::TaskReceipt::Queued(id) = receipt else {
        return Err("not queued".into());
    };
    assert_eq!(
        store
            .get_task(id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing closed fallback")?
            .status,
        crate::tasks::TaskStatus::Pending
    );
    Ok(())
}

/// Supplies a real client and bounded runner endpoint after registration succeeds.
async fn delivery_fixture() -> Result<
    (
        TaskRunner<MemoryTaskStore>,
        TaskClient<MemoryTaskStore>,
        Arc<MemoryTaskStore>,
    ),
    String,
> {
    let mut registry = TaskRegistry::new()
        .with_config(TaskConf::default().commit_batch_size(3).ingress_capacity(3))
        .map_err(|e| e.to_string())?;
    registry
        .register(RegisteredTask::new(
            crate::tasks::TaskDefinition::new("panic-job"),
            panic_job,
        ))
        .map_err(|e| e.to_string())?;
    let registry = Arc::new(registry);
    let store = Arc::new(MemoryTaskStore::new(
        3,
        registry.store_conf().map_err(|e| e.to_string())?,
    ));
    let mut client = registry.client(store.clone(), Vec::new());
    let mut runner = TaskRunner::new(client.clone()).map_err(|e| e.to_string())?;
    runner.initialize().await.map_err(|e| e.to_string())?;
    client.ingress = Some(runner.ingress());
    Ok((runner, client, store))
}

/// Flows neither consume Work permits nor wait behind an exhausted local Work rate.
#[tokio::test]
async fn flow_bypasses_work_capacity() -> Result<(), String> {
    let registry = Arc::new(
        TaskRegistry::new()
            .with_config(
                TaskConf::default().concurrency(1).lane(
                    TaskLaneConf::new(DEFAULT_TASK_LANE, 1)
                        .rate_limit(crate::tasks::TaskRate::new(1, Duration::from_secs(60))),
                ),
            )
            .map_err(|e| e.to_string())?,
    );
    let store = Arc::new(MemoryTaskStore::new(
        32,
        registry.store_conf().map_err(|e| e.to_string())?,
    ));
    let mut runner =
        TaskRunner::new(registry.client(store, Vec::new())).map_err(|e| e.to_string())?;
    let held = runner
        .execution_slots
        .clone()
        .try_acquire_owned()
        .map_err(|e| e.to_string())?;
    let work = panic_record().map_err(|e| e.to_string())?;
    let mut flow = (*work).clone();
    flow.id = crate::tasks::TaskId::new(uuid::Uuid::now_v7());
    flow.kind = crate::tasks::TaskKind::Flow;
    let flow_id = flow.id;
    let lane = runner.lanes.front_mut().ok_or("missing lane")?;
    lane.consume_local_rate(1, tokio::time::Instant::now());
    lane.tasks.extend([work.clone(), Arc::new(flow)]);
    let (_, _, records, global, local) = runner.pop_lane(0).ok_or("flow blocked")?;
    assert_eq!(records.first().ok_or("missing flow")?.id, flow_id);
    assert!(global.is_none() && local.is_none());
    assert_eq!(
        runner
            .lanes
            .front()
            .ok_or("missing lane")?
            .tasks
            .front()
            .ok_or("missing Work")?
            .id,
        work.id
    );
    assert_eq!(runner.execution_slots.available_permits(), 0);
    drop(held);
    Ok(())
}

/// The byte LRU evicts snapshots without changing task data and terminal evidence removes them.
#[test]
fn cache_is_disposable() -> Result<(), String> {
    let mut first = (*panic_record().map_err(|e| e.to_string())?).clone();
    first.kind = crate::tasks::TaskKind::Flow;
    let first = Arc::new(first);
    let budget = crate::tasks::runner::lanes::flow_bytes(&first);
    let mut queue =
        LaneState::new(TaskLaneConf::new(DEFAULT_TASK_LANE, 1).flow_cache_bytes(budget));
    queue.remember_flow(first.clone());
    let mut second = (*first).clone();
    second.id = crate::tasks::TaskId::new(uuid::Uuid::now_v7());
    let second = Arc::new(second);
    queue.remember_flow(second.clone());
    assert!(!queue.flow_cache.contains_key(&first.id));
    assert!(queue.flow_cache.contains_key(&second.id));
    assert!(queue.cached_bytes <= budget);
    let mut terminal = (*second).clone();
    terminal.status = crate::tasks::TaskStatus::Succeeded;
    queue.remember_flow(Arc::new(terminal));
    assert_eq!(queue.cached_bytes, 0);
    assert_eq!(second.status, first.status);
    Ok(())
}

/// Flow panics use the same spawned invocation containment and terminal failure as Work.
#[tokio::test]
async fn synchronous_panic_is_contained() -> Result<(), String> {
    struct PanicFlow;
    impl crate::tasks::Flow for PanicFlow {
        type Input = PanicJob;
        type Output = ();
        type Checkpoint = ();
        type Resume = ();
        fn advance(
            &self,
            _: crate::tasks::TaskId,
            _: Data<PanicJob>,
            _: crate::tasks::Continuation<(), ()>,
        ) -> Result<crate::tasks::FlowState, crate::tasks::FlowError> {
            panic!("deliberate synchronous panic");
        }
    }
    let mut registry = TaskRegistry::new()
        .with_config(TaskConf::default())
        .map_err(|e| e.to_string())?;
    registry
        .register(RegisteredTask::new_flow(
            crate::tasks::FlowConf::new("panic-job"),
            || PanicFlow,
        ))
        .map_err(|e| e.to_string())?;
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|e| e.to_string())?;
    let registry = registry
        .prepare_flows(&crate::PartialSite::new(site.db()))
        .map_err(|e| e.to_string())?;
    let mut record = (*panic_record().map_err(|e| e.to_string())?).clone();
    record.kind = crate::tasks::TaskKind::Flow;
    let (sender, mut receiver) = mpsc::channel(1);
    execute_task(TaskExecution {
        node_id: uuid::Uuid::from_u128(1),
        global_permit: None,
        lane_permit: None,
        invocation_id: uuid::Uuid::now_v7(),
        engine: Arc::new(registry),
        site,
        records: vec![Arc::new(record)],
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
    let completion = receiver.recv().await.ok_or("missing completion")?;
    let commit = completion.commits.first().ok_or("missing commit")?;
    assert!(
        matches!(commit.outcome(), Some(crate::tasks::TaskOutcome::Fail { error }) if error == "Task handler panicked")
    );
    Ok(())
}

/// Constructs only operation-local spawn evidence for runner admission tests.
fn delivery_spawn(node: uuid::Uuid) -> Result<TaskCommit, String> {
    let record = (*panic_record().map_err(|error| error.to_string())?).clone();
    Ok(TaskCommit::Outcome {
        task_id: record.id,
        node_id: node,
        attempts: 1,
        lane: DEFAULT_TASK_LANE,
        owner_token: None,
        outcome: crate::tasks::TaskOutcome::Spawn {
            state: "null".into(),
            child: crate::tasks::TaskWrite {
                record,
                ignore_conflicts: false,
                initial_delay: None,
            },
        },
    })
}
