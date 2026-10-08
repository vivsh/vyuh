use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

/// An incompatible worker registry returns a durable agent failure, never implicit callbacks.
#[tokio::test]
async fn wrong_registry_fails() -> Result<(), TestError> {
    let site = site::<Dispatcher>(worker(pravah::AgentExecutor::new(
        pravah::Context::default(),
    )))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input("question".into())).await?.id();
    let result = terminal(&site, id).await?;
    runtime.shutdown_and_wait().await;
    assert_eq!(result.status(), TaskStatus::Failed);
    let children = site
        .tasks()
        .list(vyuh::tasks::TaskFilter::new().name("agent_work"))
        .await?;
    assert_eq!(children.total, 1);
    assert!(
        children
            .items
            .iter()
            .all(|child| child.status() == TaskStatus::Succeeded)
    );
    Ok(())
}

/// Invalid or oversized successful Work values and explicit task failure cannot advance the graph.
#[tokio::test]
async fn invalid_work_outputs() -> Result<(), TestError> {
    for mode in ["malformed", "oversized", "failure", "panic", "wrong-id"] {
        let work = bundles::work(
            move |input: Data<AgentJob>| async move {
                match mode {
                    "failure" => Err(WorkError::fail("worker unavailable")),
                    "panic" => panic!("private worker data"),
                    "oversized" => Ok(serde_json::json!("x".repeat(32768))),
                    "wrong-id" => serde_json::to_value(pravah::AgentResponse::new(
                        uuid::Uuid::now_v7(),
                        Err(pravah::AgentError::new("test", "bad identity")),
                    ))
                    .map_err(|_| WorkError::fail("encoding failed")),
                    _ => Ok(serde_json::json!({"request": input.request.id()})),
                }
            },
            TaskDefinition::new("agent_work"),
        );
        let site = site::<Dispatcher>(work).await?;
        let runtime = vyuh::testing::TestSite::new(site.clone());
        runtime.start_runtime().await?;
        let id = site.tasks().submit(Input("question".into())).await?.id();
        let result = terminal(&site, id).await?;
        runtime.shutdown_and_wait().await;
        assert_eq!(result.status(), TaskStatus::Failed, "{mode}");
        assert!(
            !result
                .last_result_json()
                .unwrap_or_default()
                .contains("private worker")
        );
    }
    Ok(())
}

/// Explicit Work retry keeps the same durable request and invokes the provider only after retry.
#[tokio::test]
async fn retry_preserves_request() -> Result<(), TestError> {
    let script = pravah::testing::ScriptedFactory::new().then_output(serde_json::json!(42));
    let compiled = graph(pravah::Flow::root()).finish::<Input>()?;
    let context =
        pravah::Context::default().with_providers(pravah::testing::providers(script.clone())?);
    let executor = Arc::new(compiled.prepared().executor(context));
    let worker = bundles::work(
        move |id: TaskId, site: Site, input: Data<AgentJob>| {
            let executor = executor.clone();
            async move {
                if site
                    .tasks()
                    .get(id)
                    .await?
                    .is_some_and(|task| task.attempts() == 1)
                {
                    return Err(WorkError::retry("retry before execution"));
                }
                Ok(executor.execute(&input.request).await)
            }
        },
        TaskDefinition::new("agent_work"),
    );
    let site = site::<Dispatcher>(worker).await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input("question".into())).await?.id();
    let result = terminal(&site, id).await?;
    runtime.shutdown_and_wait().await;
    assert_eq!(result.last_result::<u32>()?, Some(Ok(42)));
    assert_eq!(script.calls().len(), 1);
    let children = site
        .tasks()
        .list(vyuh::tasks::TaskFilter::new().name("agent_work"))
        .await?;
    assert!(children.items.iter().all(|task| task.attempts() == 2));
    Ok(())
}

/// Cancelling child or parent preserves terminal state without interrupting an executing worker.
#[tokio::test]
async fn cancellation_rejects_late_response() -> Result<(), TestError> {
    for cancel_parent in [false, true] {
        let finished = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let work = delayed_worker(started.clone(), release.clone(), finished.clone())?;
        let site = site::<Dispatcher>(work).await?;
        let runtime = vyuh::testing::TestSite::new(site.clone());
        runtime.start_runtime().await?;
        let id = site.tasks().submit(Input("question".into())).await?.id();
        let child = running_child(&site).await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !started.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        site.tasks()
            .cancel(if cancel_parent { id } else { child })
            .await?;
        let result = terminal(&site, id).await?;
        assert_eq!(result.status(), TaskStatus::Failed);
        assert!(!finished.load(Ordering::SeqCst));
        release.store(true, Ordering::SeqCst);
        runtime.shutdown_and_wait().await;
        assert!(finished.load(Ordering::SeqCst));
        assert_eq!(
            terminal(&site, id).await?.last_result_json(),
            result.last_result_json()
        );
    }
    Ok(())
}

/// Keeps an invocation running until the test has observed committed cancellation.
fn delayed_worker(
    started: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
) -> Result<bundles::BundlePart, TestError> {
    let compiled = graph(pravah::Flow::root()).finish::<Input>()?;
    let executor = Arc::new(compiled.prepared().executor(pravah::Context::default()));
    Ok(bundles::work(
        move |input: Data<AgentJob>| {
            let (finished, executor) = (finished.clone(), executor.clone());
            let (started, release) = (started.clone(), release.clone());
            async move {
                started.store(true, Ordering::SeqCst);
                while !release.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let response = executor.execute(&input.request).await;
                finished.store(true, Ordering::SeqCst);
                response
            }
        },
        TaskDefinition::new("agent_work"),
    ))
}

/// Finds a claimed child through inspection; no private scheduler hooks are needed.
async fn running_child(site: &Site) -> Result<TaskId, TestError> {
    Ok(tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let children = site
                .tasks()
                .list(vyuh::tasks::TaskFilter::new().name("agent_work"))
                .await?;
            if let Some(task) = children
                .items
                .iter()
                .find(|task| task.status() == TaskStatus::Running)
            {
                return Ok::<_, TaskRuntimeError>(task.id());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await??)
}
