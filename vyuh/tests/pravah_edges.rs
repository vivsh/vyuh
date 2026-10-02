//! Adversarial routing and shared-definition tests through the real task runner.
#![cfg(all(
    feature = "pravah",
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]

use std::time::Duration;
use vyuh::tasks::{
    PravahDispatcher, TaskConf, TaskFilter, TaskOptions, TaskRetry, TaskStatus, WorkRequest,
};
use vyuh::{pravah, prelude::*};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Input {
    mode: String,
    value: u16,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Job {
    mode: String,
    value: u16,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Unknown;

struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }
    fn fetch(&self, fetch: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        let mode = fetch.request().method();
        match mode {
            "route-panic" => panic!("private routing panic"),
            "route-error" => return Err(FlowError::fail("Routing rejected")),
            "unknown" => return Ok(WorkRequest::new(Unknown)),
            "flow" => {
                return Ok(WorkRequest::new(Input {
                    mode: "ok".into(),
                    value: 0,
                }));
            }
            _ => {}
        }
        let value = fetch
            .request()
            .url()
            .parse()
            .map_err(|_| FlowError::fail("Invalid test input"))?;
        let request = WorkRequest::new(Job {
            mode: mode.into(),
            value,
        });
        Ok(match mode {
            "conflict" => request.options(TaskOptions::new().ignore_conflicts())?,
            "delayed" => request.options(TaskOptions::new().delay(Duration::from_millis(80)))?,
            _ => request,
        })
    }
}

fn graph(root: pravah::Flow<Input>) -> pravah::Flow<u16> {
    root.map(|input: Input| {
        assert!(input.mode != "graph-panic", "private graph panic");
        pravah::FetchRequest::new(input.mode, input.value.to_string())
    })
    .fetch()
    .map(|result| result.map(|r| r.status()).unwrap_or(499))
}

async fn work(id: TaskId, site: Site, input: Data<Job>) -> Result<serde_json::Value, WorkError> {
    match input.mode.as_str() {
        "work-panic" => panic!("private worker panic"),
        "work-fail" => return Err(WorkError::fail("Provider failed")),
        "malformed" => return Ok(serde_json::json!({"Ok":201})),
        "oversized" => return Ok(serde_json::json!("x".repeat(32768))),
        "retry"
            if site
                .tasks()
                .get(id)
                .await?
                .is_some_and(|task| task.attempts() == 1) =>
        {
            return Err(WorkError::retry("Retry once"));
        }
        _ => {}
    }
    serde_json::to_value(pravah::FetchResponse::new(input.value))
        .map_err(|_| WorkError::fail("Test serialization failed"))
}

/// Uses fast ordinary polls and retries, without altering production scheduling rules.
async fn site() -> Result<Site, TestError> {
    Ok(Site::build(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .concurrency(16)
                .poll_interval(Duration::from_millis(5))
                .fallback_poll_interval(Duration::from_millis(10))
                .lane(
                    vyuh::tasks::TaskLaneConf::new(vyuh::tasks::DEFAULT_TASK_LANE, 16)
                        .retry(TaskRetry::exponential(3, Duration::from_millis(5))),
                ),
        ),
        bundles::bundle([
            bundles::flow(graph, FlowConf::new("graph").dispatch::<Dispatcher>()),
            bundles::work(work, vyuh::tasks::TaskDefinition::new("worker")),
        ]),
    )
    .await?)
}

/// Unknown targets, Flow targets, unsafe options and routing failures create no child.
#[tokio::test]
async fn rejected_routes_do_not_spawn() -> Result<(), TestError> {
    let site = site().await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for mode in [
        "unknown",
        "flow",
        "conflict",
        "route-error",
        "route-panic",
        "graph-panic",
    ] {
        let id = site
            .tasks()
            .submit(Input {
                mode: mode.into(),
                value: 202,
            })
            .await?
            .id();
        let task = terminal(&site, id).await?;
        assert_eq!(task.status(), TaskStatus::Failed, "{mode}");
        assert!(
            !task
                .last_result_json()
                .unwrap_or_default()
                .contains("private")
        );
    }
    assert!(
        site.tasks()
            .list(TaskFilter::new().name("worker"))
            .await?
            .items
            .is_empty()
    );
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Child failure/panic/oversize is a Fetch error; malformed success fails protocol decoding.
#[tokio::test]
async fn effect_outcome_matrix() -> Result<(), TestError> {
    let site = site().await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for (mode, expected) in [
        ("ok", Some(202)),
        ("retry", Some(202)),
        ("work-fail", Some(499)),
        ("work-panic", Some(499)),
        ("oversized", Some(499)),
        ("malformed", None),
    ] {
        let id = site
            .tasks()
            .submit(Input {
                mode: mode.into(),
                value: 202,
            })
            .await?
            .id();
        let result = terminal(&site, id).await?.last_result::<u16>()?;
        match expected {
            Some(expected) => assert_eq!(result, Some(Ok(expected)), "{mode}"),
            None => assert!(matches!(result, Some(Err(_))), "{mode}"),
        }
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// One immutable definition keeps independent task identities, snapshots, and child results.
#[tokio::test]
async fn concurrent_graphs_do_not_share_progress() -> Result<(), TestError> {
    let site = site().await?;
    let ids = site
        .tasks()
        .submit_many((200..232).map(|value| Input {
            mode: "ok".into(),
            value,
        }))
        .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for (receipt, expected) in ids.into_iter().zip(200..232u16) {
        assert_eq!(
            terminal(&site, receipt.id()).await?.last_result::<u16>()?,
            Some(Ok(expected))
        );
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Scheduling options delay the Work child, not the parent checkpoint or other lanes.
#[tokio::test]
async fn delayed_effect_preserves_wait() -> Result<(), TestError> {
    let site = site().await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let started = std::time::Instant::now();
    let id = site
        .tasks()
        .submit(Input {
            mode: "delayed".into(),
            value: 204,
        })
        .await?
        .id();
    assert_eq!(
        terminal(&site, id).await?.last_result::<u16>()?,
        Some(Ok(204))
    );
    assert!(started.elapsed() >= Duration::from_millis(80));
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Locked-lane accumulation still dispatches a lone effect at deadline with global concurrency one.
#[tokio::test]
async fn locked_effect_deadline() -> Result<(), TestError> {
    use vyuh::tasks::{TaskDefinition, TaskLaneConf, TaskLaneLock};
    let lane = vyuh::tasks::DEFAULT_TASK_LANE;
    let conf = TaskConf::default()
        .concurrency(1)
        .poll_interval(Duration::from_millis(5))
        .fallback_poll_interval(Duration::from_millis(10))
        .lane(
            TaskLaneConf::new(lane, 1).lock(
                TaskLaneLock::new(4)
                    .deadline(Duration::from_millis(20))
                    .idle_after(Duration::from_millis(20)),
            ),
        );
    let site = Site::build(
        SiteConf::default().log_init(false).tasks(conf),
        bundles::bundle([
            bundles::flow(graph, FlowConf::new("graph").dispatch::<Dispatcher>()),
            bundles::work(work, TaskDefinition::new("worker").lane(lane)),
        ]),
    )
    .await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site
        .tasks()
        .submit(Input {
            mode: "ok".into(),
            value: 206,
        })
        .await?
        .id();
    assert_eq!(
        terminal(&site, id).await?.last_result::<u16>()?,
        Some(Ok(206))
    );
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Observes committed state only; no handler snapshot is mutated by the test driver.
async fn terminal(site: &Site, id: TaskId) -> Result<vyuh::tasks::TaskInfo, TestError> {
    Ok(tokio::time::timeout(Duration::from_secs(10), async {
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
    Timeout(#[from] tokio::time::error::Elapsed),
}
