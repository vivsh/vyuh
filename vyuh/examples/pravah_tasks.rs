//! A graph factory routes Fetch to explicit Work, then consumes its persisted response.
//! Run with `cargo run -p vyuh --example pravah_tasks --features pravah`.

use std::time::Duration;
use vyuh::tasks::{PravahDispatcher, TaskConf, TaskStatus, WorkRequest};
use vyuh::{pravah, prelude::*};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Lookup(String);

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct FetchJob {
    #[schemars(with = "String")]
    id: uuid::Uuid,
    request: pravah::FetchRequest,
}

struct AppDispatcher;

impl PravahDispatcher for AppDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }

    fn fetch(&self, fetch: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Ok(WorkRequest::new(FetchJob {
            id: fetch.id(),
            request: fetch.request().clone(),
        }))
    }
}

#[bundles::flow(dispatch = AppDispatcher)]
fn lookup(root: pravah::Flow<Lookup>) -> pravah::Flow<u16> {
    root.map(|input: Lookup| pravah::FetchRequest::new("GET", input.0))
        .fetch()
        .map(|response| response.map(|value| value.status()).unwrap_or(503))
}

/// Explicit demonstration worker: replace the stub with a service-selected executor.
/// The framework never installs an implicit network or agent/tool executor.
#[bundles::work]
async fn fetch(input: Data<FetchJob>) -> Result<pravah::FetchResponse, WorkError> {
    println!("Demo Fetch {}: {}", input.id, input.request.method());
    Ok(pravah::FetchResponse::new(200))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(10))),
        bundles::bundle([__bundle_part_lookup(), __bundle_part_fetch()]),
    )
    .await
    .map_err(Error::other)?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await.map_err(Error::other)?;
    let result = run(&site).await;
    runtime.shutdown_and_wait().await;
    result
}

/// Observes completion while advancement, child delivery, and resumption use normal polls.
async fn run(site: &Site) -> Result<(), Error> {
    let id = site
        .tasks()
        .submit(Lookup("https://example.invalid".into()))
        .await
        .map_err(Error::other)?
        .id();
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
