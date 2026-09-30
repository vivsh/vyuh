use super::*;

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
        matches!(&commit.outcome, crate::tasks::TaskOutcome::Fail { error } if error == "Task handler panicked")
    );
    Ok(())
}
