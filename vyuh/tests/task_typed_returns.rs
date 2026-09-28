//! Public-runtime proofs for typed Work output and external continuation resumption.
#![cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]

use std::time::Duration;
use vyuh::prelude::*;
use vyuh::tasks::{TaskConf, TaskStatus};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Calculate(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct AwaitResponse;

#[bundles::work]
async fn calculate(input: Data<Calculate>) -> u32 {
    input.0.0 * 2
}

#[bundles::work]
async fn await_response(
    continuation: Continuation<u32, u32>,
    _: Data<AwaitResponse>,
) -> Result<WorkState<u32>, WorkError> {
    match continuation.into_parts() {
        (None, _) => Ok(WorkState::suspend(7u32)?),
        (Some(checkpoint), Some(response)) => Ok(WorkState::complete(checkpoint + response?)),
        _ => Err(WorkError::fail("Missing response")),
    }
}

/// Drives actual registration, polling, invocation, and persistence through public APIs.
#[tokio::test]
async fn direct_output_and_external_resume() -> Result<(), TestError> {
    let site = vyuh::Site::build(
        vyuh::SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(10))),
        bundles::bundle([__bundle_part_calculate(), __bundle_part_await_response()]),
    )
    .await?;
    let client = vyuh::testing::TestSite::new(site.clone());
    client.start_runtime().await?;
    let direct = site.tasks().submit(Calculate(21)).await?.id();
    let suspended = site.tasks().submit(AwaitResponse).await?.id();
    wait_status(&site, direct, TaskStatus::Succeeded).await?;
    wait_status(&site, suspended, TaskStatus::Suspended).await?;
    assert_eq!(
        site.tasks()
            .get(direct)
            .await?
            .ok_or(TestError::Missing)?
            .last_result::<u32>()?,
        Some(Ok(42))
    );
    assert!(site.tasks().resume(suspended, 8u32).await?);
    wait_status(&site, suspended, TaskStatus::Succeeded).await?;
    assert_eq!(
        site.tasks()
            .get(suspended)
            .await?
            .ok_or(TestError::Missing)?
            .last_result::<u32>()?,
        Some(Ok(15))
    );
    client.shutdown_and_wait().await;
    Ok(())
}

/// Waits for committed status, not for synchronous handler completion.
async fn wait_status(site: &vyuh::Site, id: TaskId, status: TaskStatus) -> Result<(), TestError> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if site
                .tasks()
                .get(id)
                .await?
                .is_some_and(|task| task.status() == status)
            {
                return Ok::<_, TaskRuntimeError>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Site(#[from] vyuh::SiteError),
    #[error(transparent)]
    Runtime(#[from] TaskRuntimeError),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("missing task")]
    Missing,
}
