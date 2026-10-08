//! Agent and ordinary external work use the same durable child/result boundary.
//! Run with `cargo run -p vyuh --example pravah_tasks --features pravah`.
//! The scripted model and external worker are local demonstrations, not network clients.

use std::{sync::Arc, time::Duration};
use vyuh::tasks::{PravahDispatcher, TaskConf, TaskDefinition, TaskStatus, WorkRequest};
use vyuh::{pravah, prelude::*};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Question(String);

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct AgentJob {
    #[schemars(with = "serde_json::Value")]
    request: pravah::AgentRequest,
}

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Lookup(String);

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct LookupJob(String);

struct AppDispatcher;
impl PravahDispatcher for AppDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }

    fn agent(&self, request: &pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Ok(WorkRequest::new(AgentJob {
            request: request.clone(),
        }))
    }

    fn suspend(&self, request: &pravah::Suspension) -> Result<Option<WorkRequest>, FlowError> {
        let lookup: Lookup = pravah::graph::from_value(request.payload().clone())
            .map_err(|_| FlowError::fail("Invalid lookup request"))?;
        Ok(Some(WorkRequest::new(LookupJob(lookup.0))))
    }
}

#[bundles::flow(dispatch = AppDispatcher)]
fn answer(root: pravah::Flow<Question>) -> pravah::Flow<u16> {
    root.agent(|agent| agent.configure(configure))
}

/// This asynchronous callback runs in Agent Work, never in synchronous Flow advancement.
async fn configure(
    input: Question,
    _: pravah::Context,
) -> Result<pravah::AgentConfig, pravah::GraphError> {
    Ok(pravah::AgentConfig::new(
        "test:///model",
        "Return a number",
        pravah::clients::Message::user(input.0),
    ))
}

#[bundles::flow(dispatch = AppDispatcher)]
fn lookup(root: pravah::Flow<Lookup>) -> pravah::Flow<u16> {
    root.suspend::<u16>()
}

/// Ordinary network/application work uses typed suspension, not the Agent protocol.
#[bundles::work]
async fn lookup_work(input: Data<LookupJob>) -> u16 {
    println!("Demo lookup: {}", input.as_ref().0);
    200
}

/// Constructs the worker's compatible registry once; production uses application services.
fn agent_worker() -> Result<bundles::BundlePart, Error> {
    let model = pravah::testing::ScriptedFactory::new().then_output(serde_json::json!(42));
    let context = pravah::Context::default()
        .with_providers(pravah::testing::providers(model).map_err(Error::other)?);
    // Match the factory's symbolic root naming; compile(answer) chooses a different root name.
    let definition = answer(pravah::Flow::root())
        .finish::<Question>()
        .map_err(Error::other)?;
    let executor = Arc::new(definition.prepared().executor(context));
    Ok(bundles::work(
        move |input: Data<AgentJob>| {
            let executor = executor.clone();
            async move {
                // Keep the full response, including successful stages preceding an agent failure.
                executor.execute(&input.request).await
            }
        },
        TaskDefinition::new("agent_work"),
    ))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(10))),
        bundles::bundle([
            __bundle_part_answer(),
            agent_worker()?,
            __bundle_part_lookup(),
            __bundle_part_lookup_work(),
        ]),
    )
    .await
    .map_err(Error::other)?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await.map_err(Error::other)?;
    let result = run(&site).await;
    runtime.shutdown_and_wait().await;
    result
}

/// Submits both kinds of boundary through ordinary tasks and observes persisted outputs.
async fn run(site: &Site) -> Result<(), Error> {
    let agent = site
        .tasks()
        .submit(Question("What is the answer?".into()))
        .await
        .map_err(Error::other)?
        .id();
    let external = site
        .tasks()
        .submit(Lookup("https://example.invalid".into()))
        .await
        .map_err(Error::other)?
        .id();
    for id in [agent, external] {
        observe(site, id).await?;
    }
    Ok(())
}

/// Observes completion while advancement and child delivery use independently paced claims.
async fn observe(site: &Site, id: TaskId) -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(task) = site.tasks().get(id).await.map_err(Error::other)?
                && matches!(task.status(), TaskStatus::Succeeded | TaskStatus::Failed)
            {
                println!(
                    "Flow result: {:?}",
                    task.last_result::<u16>().map_err(Error::other)?
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(Error::other)?
}
