//! Agent responses cross the real Work/store/Flow boundary without losing acknowledgements.
#![cfg(all(
    feature = "pravah",
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]

use std::{sync::Arc, time::Duration};
use vyuh::tasks::{PravahDispatcher, TaskConf, TaskDefinition, TaskStatus, WorkRequest};
use vyuh::{pravah, prelude::*};

#[path = "pravah_agents/failures.rs"]
mod failures;
#[path = "pravah_agents/history.rs"]
mod history;
#[path = "pravah_agents/tools.rs"]
mod tools;

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Input(String);

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct AgentJob {
    #[schemars(with = "serde_json::Value")]
    request: pravah::AgentRequest,
}

struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }
    fn agent(&self, request: &pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Ok(WorkRequest::new(AgentJob {
            request: request.clone(),
        }))
    }
}

#[bundles::flow(dispatch = Dispatcher)]
fn graph(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
    root.agent(|agent| agent.configure(configure))
}

async fn configure(
    input: Input,
    _: pravah::Context,
) -> Result<pravah::AgentConfig, pravah::GraphError> {
    Ok(pravah::AgentConfig::new(
        "test:///model",
        "Return a number",
        pravah::clients::Message::user(input.0),
    )
    .key("test-conversation"))
}

/// Real configure and generate workers deliver ordered committed responses to the parent.
#[tokio::test]
async fn agent_work_round_trip() -> Result<(), TestError> {
    let script = pravah::testing::ScriptedFactory::new().then_output(serde_json::json!(42));
    let compiled = graph(pravah::Flow::root()).finish::<Input>()?;
    let context =
        pravah::Context::default().with_providers(pravah::testing::providers(script.clone())?);
    let executor = compiled.prepared().executor(context);
    let site = site::<Dispatcher>(worker(executor)).await?;
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
    assert_eq!(children.total, 2);
    assert!(
        children
            .items
            .iter()
            .all(|child| child.parent_id() == Some(id) && child.root_id() == Some(id))
    );
    Ok(())
}

/// Provides a compatible application-owned executor without creating a framework registry.
fn worker(executor: pravah::AgentExecutor) -> bundles::BundlePart {
    let executor = Arc::new(executor);
    bundles::work(
        move |input: Data<AgentJob>| {
            let executor = executor.clone();
            async move { executor.execute(&input.request).await }
        },
        TaskDefinition::new("agent_work"),
    )
}

/// Uses normal admission and one shared output queue for all graph and Work steps.
async fn site<D: PravahDispatcher>(worker: bundles::BundlePart) -> Result<Site, vyuh::SiteError> {
    Site::build(
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(5))),
        bundles::bundle([
            bundles::flow(graph, FlowConf::new("graph").dispatch::<D>()),
            worker,
        ]),
    )
    .await
}

/// Inspects results without performing any execution or durable lifecycle mutation.
async fn terminal(site: &Site, id: TaskId) -> Result<vyuh::tasks::TaskInfo, TestError> {
    Ok(tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(task) = site.tasks().get(id).await?
                && matches!(task.status(), TaskStatus::Succeeded | TaskStatus::Failed)
            {
                return Ok::<_, TaskRuntimeError>(task);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??)
}

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Site(#[from] vyuh::SiteError),
    #[error(transparent)]
    Runtime(#[from] TaskRuntimeError),
    #[error(transparent)]
    Graph(#[from] pravah::GraphError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
}
