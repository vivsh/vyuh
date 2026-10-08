use super::*;
use pravah::HistoryStore;
use std::convert::Infallible;

struct HistoryDispatcher;
impl PravahDispatcher for HistoryDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }
    fn agent(&self, request: &pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Dispatcher.agent(request)
    }
    fn history_policy(&self) -> pravah::HistoryPolicy {
        pravah::HistoryPolicy {
            persist: true,
            load: true,
            compact: true,
        }
    }
}

struct Summarize;
impl pravah::Compactor for Summarize {
    type Error = Infallible;
    async fn compact(
        &self,
        request: pravah::CompactionRequest<'_>,
        _: pravah::Context,
    ) -> Result<pravah::CompactionResult, Infallible> {
        Ok(pravah::CompactionResult {
            evict_indices: (0..request.committed().len()).collect(),
            summary: (!request.committed().is_empty()).then(|| "prior exchange".into()),
        })
    }
}

/// Seeds immutable archived identities; the worker, not the Flow handler, loads them.
async fn archived() -> pravah::testing::CapturingHistoryStore {
    let store = pravah::testing::CapturingHistoryStore::new();
    let mut history = pravah::MessageHistory::new();
    history.push(
        "key:test-conversation",
        "assistant",
        pravah::clients::Message::user("old question"),
    );
    history.push(
        "key:test-conversation",
        "assistant",
        pravah::clients::Message::assistant("old answer"),
    );
    for entry in history.entries() {
        match store.record(entry).await {
            Ok(()) => {}
            Err(error) => match error {},
        }
    }
    store
}

/// Successful load, compaction and final persistence acknowledgements survive child delivery.
#[tokio::test]
async fn history_round_trip() -> Result<(), TestError> {
    let store = archived().await;
    let script = pravah::testing::ScriptedFactory::new().then_output(serde_json::json!(42));
    let compiled = graph(pravah::Flow::root()).finish::<Input>()?;
    let context =
        pravah::Context::default().with_providers(pravah::testing::providers(script.clone())?);
    let executor = compiled
        .prepared()
        .executor(context)
        .with_store(store.clone())
        .with_compactor(Summarize);
    let site = site::<HistoryDispatcher>(worker(executor)).await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input("question".into())).await?.id();
    let result = terminal(&site, id).await?;
    runtime.shutdown_and_wait().await;
    assert_eq!(result.last_result::<u32>()?, Some(Ok(42)));
    assert_eq!(store.all_entries().len(), 4);
    assert!(script.calls().first().is_some_and(|(_, messages)| {
        messages
            .iter()
            .any(|message| message.content.contains("prior exchange"))
    }));
    let children = site
        .tasks()
        .list(vyuh::tasks::TaskFilter::new().name("agent_work"))
        .await?;
    let responses = children
        .items
        .iter()
        .map(|task| task.last_result::<pravah::AgentResponse>())
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        responses
            .iter()
            .filter_map(|result| result.as_ref()?.as_ref().ok())
            .any(|response| serde_json::to_value(response)
                .is_ok_and(|value| value.get("compaction").is_some_and(|v| !v.is_null())))
    );
    assert_eq!(children.total, 3);
    Ok(())
}

/// Agent failures retain earlier successful stage acknowledgements inside a successful Work output.
#[tokio::test]
async fn partial_acknowledgements_survive() -> Result<(), TestError> {
    let store = archived().await;
    let script =
        pravah::testing::ScriptedFactory::new().then_err(pravah::clients::ClientError::new(
            pravah::clients::ErrorKind::Provider,
            "provider unavailable",
        ));
    let compiled = graph(pravah::Flow::root()).finish::<Input>()?;
    let context =
        pravah::Context::default().with_providers(pravah::testing::providers(script.clone())?);
    let executor = compiled
        .prepared()
        .executor(context)
        .with_store(store.clone())
        .with_compactor(Summarize);
    let site = site::<HistoryDispatcher>(worker(executor)).await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input("question".into())).await?.id();
    let result = terminal(&site, id).await?;
    runtime.shutdown_and_wait().await;
    assert_eq!(result.status(), TaskStatus::Failed);
    assert_eq!(script.calls().len(), 1);
    assert_eq!(store.all_entries().len(), 3);
    let children = site
        .tasks()
        .list(vyuh::tasks::TaskFilter::new().name("agent_work"))
        .await?;
    assert!(
        children
            .items
            .iter()
            .all(|child| child.status() == TaskStatus::Succeeded)
    );
    assert_partial_acknowledgements(&children.items)?;
    Ok(())
}

/// A failed agent response retains successful history-stage evidence in its serialized envelope.
fn assert_partial_acknowledgements(children: &[vyuh::tasks::TaskInfo]) -> Result<(), TestError> {
    let responses = children
        .iter()
        .map(|task| task.last_result::<pravah::AgentResponse>())
        .collect::<Result<Vec<_>, _>>()?;
    let response = responses
        .iter()
        .filter_map(|r| r.as_ref()?.as_ref().ok())
        .find(|r| r.outcome().is_err())
        .ok_or_else(|| pravah::GraphError::Invalid("missing failed response".into()))?;
    let encoded = serde_json::to_value(response)?;
    assert!(
        encoded
            .get("persisted_through")
            .is_some_and(|v| !v.is_null())
    );
    assert!(encoded.get("compaction").is_some_and(|v| !v.is_null()));
    Ok(())
}

/// Requested services are never silently replaced with default stores or compactors.
#[tokio::test]
async fn missing_history_dependencies() -> Result<(), TestError> {
    for with_store in [false, true] {
        let compiled = graph(pravah::Flow::root()).finish::<Input>()?;
        let executor = compiled.prepared().executor(pravah::Context::default());
        let executor = if with_store {
            executor.with_store(archived().await)
        } else {
            executor
        };
        let site = site::<HistoryDispatcher>(worker(executor)).await?;
        let runtime = vyuh::testing::TestSite::new(site.clone());
        runtime.start_runtime().await?;
        let id = site.tasks().submit(Input("question".into())).await?.id();
        let result = terminal(&site, id).await?;
        runtime.shutdown_and_wait().await;
        assert_eq!(result.status(), TaskStatus::Failed);
    }
    Ok(())
}

/// History acknowledgements obey the existing whole-envelope limit; no partial delivery is stored.
#[tokio::test]
async fn oversized_loaded_history_fails() -> Result<(), TestError> {
    let store = archived().await;
    let mut history = pravah::MessageHistory::new();
    history.push(
        "key:test-conversation",
        "assistant",
        pravah::clients::Message::user("x".repeat(32768)),
    );
    for entry in history.entries() {
        match store.record(entry).await {
            Ok(()) => {}
            Err(error) => match error {},
        }
    }
    let script = pravah::testing::ScriptedFactory::new().then_output(serde_json::json!(42));
    let compiled = graph(pravah::Flow::root()).finish::<Input>()?;
    let context = pravah::Context::default().with_providers(pravah::testing::providers(script)?);
    let executor = compiled
        .prepared()
        .executor(context)
        .with_store(store)
        .with_compactor(Summarize);
    let site = site::<HistoryDispatcher>(worker(executor)).await?;
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
    assert!(
        children
            .items
            .iter()
            .any(|task| task.status() == TaskStatus::Failed)
    );
    assert!(children.items.iter().all(|task| {
        task.last_result_json()
            .is_some_and(|json| json.len() <= 32768)
    }));
    Ok(())
}
