//! Fixed Work-result delivery through the public synchronous Pravah factory API.
#![cfg(all(
    feature = "pravah",
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]

use std::time::Duration;

use vyuh::tasks::{PravahDispatcher, TaskConf, TaskStatus, WorkRequest};
use vyuh::{PartialSite, pravah, prelude::*};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Input(u32);

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct FetchJob {
    #[schemars(with = "String")]
    id: uuid::Uuid,
    request: pravah::FetchRequest,
}

struct Dispatcher;

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct ApprovalJob(u32);

struct ApprovalDispatcher;

impl PravahDispatcher for ApprovalDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }
    fn fetch(&self, _: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }
    fn suspend(&self, request: &pravah::Suspension) -> Result<Option<WorkRequest>, FlowError> {
        let input: Input = pravah::graph::from_value(request.payload().clone())
            .map_err(|_| FlowError::fail("Invalid approval input"))?;
        Ok(Some(WorkRequest::new(ApprovalJob(input.0))))
    }
}

#[bundles::work]
async fn approve(input: Data<ApprovalJob>) -> Result<u32, WorkError> {
    if input.as_ref().0 == 0 {
        Err(WorkError::fail("Approval rejected"))
    } else {
        Ok(input.as_ref().0 * 2)
    }
}

impl PravahDispatcher for Dispatcher {
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

#[bundles::work]
async fn fetch(input: Data<FetchJob>) -> Result<pravah::FetchResponse, WorkError> {
    if input.request.method() == "FAIL" {
        return Err(WorkError::fail("Safe provider failure"));
    }
    Ok(pravah::FetchResponse::new(201))
}

#[bundles::flow(dispatch = Dispatcher)]
fn fetching(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
    root.map(|input: Input| {
        pravah::FetchRequest::new(
            if input.0 == 0 { "FAIL" } else { "GET" },
            "https://example.invalid",
        )
    })
    .fetch()
    .map(|response| match response {
        Ok(response) => u32::from(response.status()),
        Err(error) if error.code() == "vyuh_task_failure" => 499,
        Err(_) => 500,
    })
}

#[bundles::flow]
fn external(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
    root.suspend::<u32>().map(|value| value + 1)
}

fn pure(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
    root.map(|input: Input| input.0)
        .map(|value| value + 1)
        .map(|value| value * 2)
}

/// Fetch success and terminal child failures take the same later-poll result boundary.
#[tokio::test]
async fn fetch_result_delivery() -> Result<(), TestError> {
    let site = site(bundles::bundle([
        __bundle_part_fetching(),
        __bundle_part_fetch(),
    ]))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for (input, expected) in [(1, 201), (0, 499)] {
        let id = site.tasks().submit(Input(input)).await?.id();
        assert_eq!(
            terminal(&site, id).await?.last_result::<u32>()?,
            Some(Ok(expected))
        );
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Ordinary suspension has no dispatcher requirement and failed resume fails the Flow.
#[tokio::test]
async fn external_resume_without_dispatch() -> Result<(), TestError> {
    let site = site(bundles::bundle([__bundle_part_external()])).await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for failed in [false, true] {
        let id = site.tasks().submit(Input(1)).await?.id();
        wait_status(&site, id, TaskStatus::Suspended).await?;
        if failed {
            site.tasks()
                .resume_failed(id, TaskFailure::new(None, "Rejected"))
                .await?;
        } else {
            site.tasks().resume(id, 8u32).await?;
        }
        let result = terminal(&site, id).await?.last_result::<u32>()?;
        if failed {
            assert!(matches!(result, Some(Err(_))));
        } else {
            assert_eq!(result, Some(Ok(9)));
        }
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// A one-instruction budget checkpoints and resumes without manufacturing resume input.
#[tokio::test]
async fn budget_yields() -> Result<(), TestError> {
    let site = site(bundles::bundle([bundles::flow(
        pure,
        FlowConf::new("pure").step_limit(1),
    )]))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input(3)).await?.id();
    assert_eq!(
        terminal(&site, id).await?.last_result::<u32>()?,
        Some(Ok(8))
    );
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Static Fetch requirements fail site construction when no routing policy was selected.
#[tokio::test]
async fn missing_dispatch_rejected() {
    let result = site(bundles::bundle([bundles::flow(
        fetching,
        FlowConf::new("missing"),
    )]))
    .await;
    assert!(matches!(result, Err(error) if error.to_string().contains("dispatcher")));
}

static POLICY_BUILDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct CountedDispatcher;

struct PanickingDispatcher;
impl PravahDispatcher for PanickingDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        panic!("private policy configuration");
    }
    fn fetch(&self, _: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }
}

/// Policy panics abort construction with safe context and are not misreported as missing dispatch.
#[tokio::test]
async fn policy_panics_are_contained() {
    let result = site(bundles::bundle([bundles::flow(
        pure,
        FlowConf::new("panicking-policy").dispatch::<PanickingDispatcher>(),
    )]))
    .await;
    assert!(
        matches!(result, Err(error) if error.to_string().contains("factory panicked")
        && !error.to_string().contains("private policy"))
    );
}
impl PravahDispatcher for CountedDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        POLICY_BUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Self)
    }
    fn fetch(&self, _: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }
}

/// Two definitions share one policy construction, but independent sites do not share it.
#[tokio::test]
async fn dispatcher_constructs_once() -> Result<(), TestError> {
    let bundle = || {
        bundles::bundle([
            bundles::flow(pure, FlowConf::new("first").dispatch::<CountedDispatcher>()),
            bundles::flow(
                |_: PartialSite, root: pravah::Flow<String>| root.map(|value| value.len() as u32),
                FlowConf::new("second").dispatch::<CountedDispatcher>(),
            ),
        ])
    };
    site(bundle()).await?;
    assert_eq!(POLICY_BUILDS.load(std::sync::atomic::Ordering::SeqCst), 1);
    site(bundle()).await?;
    assert_eq!(POLICY_BUILDS.load(std::sync::atomic::Ordering::SeqCst), 2);
    Ok(())
}

/// Mapped suspension forwards successful Work values and terminally propagates failure.
#[tokio::test]
async fn mapped_suspension() -> Result<(), TestError> {
    let site = site(bundles::bundle([
        bundles::flow(
            external,
            FlowConf::new("approval").dispatch::<ApprovalDispatcher>(),
        ),
        __bundle_part_approve(),
    ]))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for input in [4, 0] {
        let id = site.tasks().submit(Input(input)).await?.id();
        let result = terminal(&site, id).await?.last_result::<u32>()?;
        if input == 0 {
            assert!(
                matches!(result, Some(Err(failure)) if failure.message() == "Approval rejected")
            );
        } else {
            assert_eq!(result, Some(Ok(9)));
        }
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Compiled definitions and opaque factory returns use the same preparation contract.
#[tokio::test]
async fn compiled_and_opaque_factories() -> Result<(), TestError> {
    fn opaque(root: pravah::Flow<Input>) -> impl vyuh::tasks::IntoFlow<Input> {
        pure(root)
    }
    fn opaque_dispatch(root: pravah::Flow<Input>) -> impl vyuh::tasks::IntoFlow<Input, Dispatcher> {
        pure(root)
    }
    let definitions = [
        bundles::flow(opaque, FlowConf::new("opaque")),
        bundles::flow(
            opaque_dispatch,
            FlowConf::new("opaque-dispatch").dispatch::<Dispatcher>(),
        ),
        bundles::flow(
            || pravah::compile(pure).map_err(|error| FlowError::fail(error.to_string())),
            FlowConf::new("compiled"),
        ),
    ];
    for definition in definitions {
        let site = site(bundles::bundle([definition])).await?;
        let runtime = vyuh::testing::TestSite::new(site.clone());
        runtime.start_runtime().await?;
        let id = site.tasks().submit(Input(2)).await?.id();
        assert_eq!(
            terminal(&site, id).await?.last_result::<u32>()?,
            Some(Ok(6))
        );
        runtime.shutdown_and_wait().await;
    }
    Ok(())
}

/// Sequential Fetch boundaries reuse the definition but create distinct durable Work children.
#[tokio::test]
async fn sequential_fetches() -> Result<(), TestError> {
    fn sequence(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
        fetching(root)
            .map(|value| pravah::FetchRequest::new("GET", value.to_string()))
            .fetch()
            .map(|result| {
                result
                    .map(|response| u32::from(response.status()))
                    .unwrap_or(500)
            })
    }
    let site = site(bundles::bundle([
        bundles::flow(sequence, FlowConf::new("sequence").dispatch::<Dispatcher>()),
        __bundle_part_fetch(),
    ]))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input(1)).await?.id();
    assert_eq!(
        terminal(&site, id).await?.last_result::<u32>()?,
        Some(Ok(201))
    );
    let children = site
        .tasks()
        .list(vyuh::tasks::TaskFilter::new().name("fetch"))
        .await?;
    assert_eq!(children.total, 2);
    assert!(
        children
            .items
            .iter()
            .all(|child| child.parent_id() == Some(id) && child.root_id() == Some(id))
    );
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Nested each-graph Fetches restore the correct frame and preserve item result ordering.
#[tokio::test]
async fn nested_fetches() -> Result<(), TestError> {
    fn nested(root: pravah::Flow<Input>) -> pravah::Flow<Vec<u32>> {
        root.map(|input: Input| (0..input.0).collect::<Vec<_>>())
            .each(|root| {
                root.map(|value| Input(value))
                    .map(|input: Input| {
                        pravah::FetchRequest::new(
                            if input.0 == 0 { "FAIL" } else { "GET" },
                            "https://example.invalid",
                        )
                    })
                    .fetch()
                    .map(|result| result.map(|r| u32::from(r.status())).unwrap_or(499))
            })
    }
    let site = site(bundles::bundle([
        bundles::flow(nested, FlowConf::new("nested").dispatch::<Dispatcher>()),
        __bundle_part_fetch(),
    ]))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for (count, expected) in [(0, vec![]), (1, vec![499]), (3, vec![499, 201, 201])] {
        let id = site.tasks().submit(Input(count)).await?.id();
        assert_eq!(
            terminal(&site, id).await?.last_result::<Vec<u32>>()?,
            Some(Ok(expected))
        );
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Batched effect handlers preserve independent ordered Work results for each waiting graph.
#[tokio::test]
async fn batched_effects() -> Result<(), TestError> {
    async fn batch(
        input: Data<Batch<FetchJob>>,
    ) -> Batch<Result<WorkState<pravah::FetchResponse>, WorkError>> {
        input
            .iter()
            .map(|job| {
                if job.request.method() == "FAIL" {
                    Err(WorkError::fail("Rejected"))
                } else {
                    Ok(WorkState::complete(pravah::FetchResponse::new(201)))
                }
            })
            .collect()
    }
    let site = site(bundles::bundle([
        __bundle_part_fetching(),
        bundles::work_batch(batch, vyuh::tasks::TaskDefinition::new("fetch")),
    ]))
    .await?;
    let ids = site
        .tasks()
        .submit_many((0..32).map(|n| Input(n % 2)))
        .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for (n, receipt) in ids.into_iter().enumerate() {
        let expected = if n % 2 == 0 { 499 } else { 201 };
        assert_eq!(
            terminal(&site, receipt.id()).await?.last_result::<u32>()?,
            Some(Ok(expected))
        );
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Cooperative yields can exceed the lifetime retry limit because each checkpoint commits a step.
#[tokio::test]
async fn repeated_budget_yields() -> Result<(), TestError> {
    fn many(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
        let mut graph = root.map(|input: Input| input.0);
        for _ in 0..12 {
            graph = graph.map(|value| value + 1);
        }
        graph
    }
    let site = site(bundles::bundle([bundles::flow(
        many,
        FlowConf::new("many").step_limit(1),
    )]))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input(0)).await?.id();
    let task = terminal(&site, id).await?;
    assert_eq!(task.last_result::<u32>()?, Some(Ok(12)));
    assert!(task.attempts() > 10);
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// VM snapshots cannot bypass the existing configurable checkpoint limit.
#[tokio::test]
async fn oversized_checkpoint_fails() -> Result<(), TestError> {
    fn large(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
        root.map(|_| "x".repeat(2048))
            .map(|value| value.len() as u32)
    }
    let site = Site::build(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .max_payload_bytes(1024)
                .poll_interval(Duration::from_millis(5)),
        ),
        bundles::bundle([bundles::flow(large, FlowConf::new("large").step_limit(1))]),
    )
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input(1)).await?.id();
    assert_eq!(terminal(&site, id).await?.status(), TaskStatus::Failed);
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Rejected oversize resumption leaves the wait intact; an exact-limit value resumes unchanged.
#[tokio::test]
async fn resume_envelope_limit() -> Result<(), TestError> {
    fn waiting(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
        root.suspend::<String>().map(|value| value.len() as u32)
    }
    let site = site(bundles::bundle([bundles::flow(
        waiting,
        FlowConf::new("waiting"),
    )]))
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input(1)).await?.id();
    let before = wait_status(&site, id, TaskStatus::Suspended).await?;
    assert!(matches!(
        site.tasks().resume(id, "x".repeat(32760)).await,
        Err(TaskRuntimeError::ResultTooLarge { .. })
    ));
    let after = site
        .tasks()
        .get(id)
        .await?
        .ok_or_else(|| TaskRuntimeError::TaskNotFound("wait".into()))?;
    assert_eq!(after.status(), before.status());
    assert!(site.tasks().resume(id, "x".repeat(32759)).await?);
    assert_eq!(
        terminal(&site, id).await?.last_result::<u32>()?,
        Some(Ok(32759))
    );
    runtime.shutdown_and_wait().await;
    Ok(())
}

async fn site(bundle: bundles::Bundle) -> Result<Site, vyuh::SiteError> {
    Site::build(
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(5))),
        bundle,
    )
    .await
}

/// Observes immutable inspection snapshots while the normal runner owns all progress.
async fn terminal(site: &Site, id: TaskId) -> Result<vyuh::tasks::TaskInfo, TestError> {
    wait_status(site, id, TaskStatus::Succeeded).await
}

/// Waits through ordinary runner polls without mutating snapshots or skipping lifecycle gates.
async fn wait_status(
    site: &Site,
    id: TaskId,
    status: TaskStatus,
) -> Result<vyuh::tasks::TaskInfo, TestError> {
    Ok(tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(task) = site.tasks().get(id).await?
                && (task.status() == status || task.status() == TaskStatus::Failed)
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
    Timeout(#[from] tokio::time::error::Elapsed),
}
