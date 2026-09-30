use super::*;
use crate::tasks::{Flow, FlowConf, FlowError, FlowState, TaskKind};

struct CountFlow(Arc<std::sync::atomic::AtomicUsize>);
impl Flow for CountFlow {
    type Input = DirectJob;
    type Output = ();
    type Checkpoint = ();
    type Resume = ();
    fn advance(
        &self,
        _: TaskId,
        _: Data<DirectJob>,
        _: Continuation<(), ()>,
    ) -> Result<FlowState, FlowError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(FlowState::complete(()))
    }
}

struct ErrorFlow;
impl Flow for ErrorFlow {
    type Input = DirectJob;
    type Output = ();
    type Checkpoint = ();
    type Resume = ();
    fn advance(
        &self,
        _: TaskId,
        _: Data<DirectJob>,
        _: Continuation<(), ()>,
    ) -> Result<FlowState, FlowError> {
        Err(crate::Error::invalid("secret").into())
    }
}

fn flow_record() -> Result<Arc<TaskRecord>, TaskRuntimeError> {
    let mut record = (*record("flow", &DirectJob { id: 7 })?).clone();
    record.kind = TaskKind::Flow;
    Ok(Arc::new(record))
}

/// Kind mismatch is rejected before input decoding or application side effects.
#[tokio::test]
async fn kind_mismatch_is_not_invoked() -> Result<(), TaskRuntimeError> {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    let mut flow =
        RegisteredTask::new_flow(FlowConf::new("flow"), move || CountFlow(observed.clone()));
    let site = test_site()
        .await
        .map_err(|e| TaskRuntimeError::TaskExecutionError(e.to_string()))?;
    flow.prepare_flow(&crate::PartialSite::new(site.db()), &mut Default::default())?;
    assert!(matches!(
        flow.execute(site.clone(), record("flow", &DirectJob { id: 1 })?)
            .await,
        TaskOutcome::Fail { .. }
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(matches!(
        flow.execute(site.clone(), flow_record()?).await,
        TaskOutcome::Complete
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let work = RegisteredTask::new(TaskDefinition::new("flow"), direct_job);
    assert!(matches!(
        work.execute(site, flow_record()?).await,
        TaskOutcome::Fail { .. }
    ));
    Ok(())
}

/// Flow errors remain terminal and safe; malformed payloads never invoke user code.
#[tokio::test]
async fn flow_errors_are_contained() -> Result<(), TaskRuntimeError> {
    let site = test_site()
        .await
        .map_err(|e| TaskRuntimeError::TaskExecutionError(e.to_string()))?;
    let mut flow = RegisteredTask::new_flow(FlowConf::new("flow"), || ErrorFlow);
    flow.prepare_flow(&crate::PartialSite::new(site.db()), &mut Default::default())?;
    assert!(matches!(flow.execute(site.clone(), flow_record()?).await,
        TaskOutcome::Fail { error } if error == "Task handler failed"));
    let mut invalid = (*flow_record()?).clone();
    invalid.input = "{".into();
    assert!(matches!(flow.execute(site, Arc::new(invalid)).await,
        TaskOutcome::Fail { error } if error == "Task input is invalid"));
    Ok(())
}

/// Batch kind validation isolates corrupt rows and preserves valid member ordering.
#[tokio::test]
async fn batch_kind_mismatch_is_isolated() -> Result<(), TaskRuntimeError> {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = seen.clone();
    let batch = RegisteredTask::new_batch(
        TaskDefinition::new("batch"),
        move |data: Data<crate::tasks::Batch<DirectJob>>| {
            let seen = observed.clone();
            async move {
                *seen.lock().expect("test mutex") =
                    data.iter().map(|job| job.id).collect::<Vec<_>>();
            }
        },
    );
    let site = test_site()
        .await
        .map_err(|e| TaskRuntimeError::TaskExecutionError(e.to_string()))?;
    let results = batch
        .execute_many(
            site.clone(),
            vec![
                record("batch", &DirectJob { id: 1 })?,
                flow_record()?,
                record("batch", &DirectJob { id: 2 })?,
            ],
        )
        .await;
    assert!(matches!(results[0].outcome, TaskOutcome::Complete));
    assert!(matches!(results[1].outcome, TaskOutcome::Fail { .. }));
    assert!(matches!(results[2].outcome, TaskOutcome::Complete));
    assert_eq!(*seen.lock().expect("test mutex"), [1, 2]);
    batch.execute_many(site, vec![flow_record()?]).await;
    assert_eq!(*seen.lock().expect("test mutex"), [1, 2]);
    Ok(())
}

/// Registration preserves original metadata identity and scheduled submission infers Flow.
#[tokio::test]
async fn flow_metadata_and_scheduled_kind() -> Result<(), TaskRuntimeError> {
    fn unit() -> CountFlow {
        CountFlow(Arc::new(std::sync::atomic::AtomicUsize::new(0)))
    }
    let mut registered = RegisteredTask::new_flow(FlowConf::new("flow"), unit);
    let site = test_site()
        .await
        .map_err(|e| TaskRuntimeError::TaskExecutionError(e.to_string()))?;
    registered.prepare_flow(&crate::PartialSite::new(site.db()), &mut Default::default())?;
    assert_eq!(registered.kind(), TaskKind::Flow);
    let RegisteredHandler::Flow { handler, .. } = &registered.handler else {
        return Err(TaskRuntimeError::TaskExecutionError(
            "wrong registration mode".into(),
        ));
    };
    assert_eq!(handler.inspect().name, std::any::type_name_of_val(&unit));
    let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
    registry.register(registered)?;
    let store = Arc::new(MemoryTaskStore::new(10));
    let dispatcher = Arc::new(registry).dispatcher(store.clone(), Vec::new());
    let receipt = dispatcher
        .submit_with(
            DirectJob { id: 1 },
            TaskOptions::new().delay(std::time::Duration::from_secs(60)),
        )
        .await?;
    let task = store
        .get_task(receipt.id())
        .await?
        .ok_or_else(|| TaskRuntimeError::TaskNotFound("fixture".into()))?;
    assert_eq!(task.kind, TaskKind::Flow);
    assert!(
        task.ready_at
            .is_some_and(|ready| ready > chrono::Utc::now())
    );
    assert!(
        store
            .claim_tasks(
                "owner",
                &[LaneClaim {
                    lane: DEFAULT_TASK_LANE,
                    limit: 10,
                    owner: None
                }]
            )
            .await?
            .lanes
            .iter()
            .all(|lane| lane.tasks.is_empty())
    );
    Ok(())
}
