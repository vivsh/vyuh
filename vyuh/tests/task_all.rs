//! Public-runtime all-settled flows use ordinary Work handlers and durable continuations.
#![cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]

use std::time::Duration;
use vyuh::prelude::*;
use vyuh::tasks::{TaskConf, TaskDefinition, TaskStatus};

const WORK: TaskLane = TaskLane::new("all-work");

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Item(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Group(u32);

#[bundles::task(lane = WORK)]
async fn item(input: Data<Item>) -> u32 {
    input.0.0 * 2
}

#[bundles::flow]
fn group(
    continuation: Continuation<(), Vec<Result<u32, vyuh::tasks::TaskFailure>>>,
    input: Data<Group>,
) -> Result<FlowState<u32>, FlowError> {
    match continuation.into_parts() {
        (None, _) => Ok(FlowState::all((0..input.0.0).map(Item).collect(), ())?),
        (Some(()), Some(results)) => {
            let sum = results?
                .into_iter()
                .try_fold(0, |sum, value| value.map(|value| sum + value))?;
            Ok(FlowState::complete(sum))
        }
        _ => Err(FlowError::fail("Missing all result")),
    }
}

/// Macro and direct registration both support zero/full fan-out even with one-row flushes.
#[tokio::test]
async fn all_registration_parity() -> Result<(), TestError> {
    for direct in [false, true] {
        let bundle = if direct {
            bundles::bundle([
                bundles::task(item, TaskDefinition::new("item").lane(WORK)),
                bundles::flow(group, TaskDefinition::new("group")),
            ])
        } else {
            bundles::bundle([__bundle_part_item(), __bundle_part_group()])
        };
        let site = vyuh::Site::build(
            vyuh::SiteConf::default().log_init(false).tasks(
                TaskConf::default()
                    .batch_size(1)
                    .lane(vyuh::tasks::TaskLaneConf::new(WORK, 8))
                    .max_all_children(32)
                    .poll_interval(Duration::from_millis(5)),
            ),
            bundle,
        )
        .await?;
        let client = vyuh::testing::TestSite::new(site.clone());
        client.start_runtime().await?;
        for size in [0, 1, 32, 33] {
            let id = site.tasks().submit(Group(size)).await?.id();
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(task) = site.tasks().get(id).await?
                        && matches!(task.status(), TaskStatus::Succeeded | TaskStatus::Failed)
                    {
                        return task.last_result::<u32>();
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await??;
            if size > 32 {
                assert!(matches!(result, Some(Err(_))));
            } else {
                assert_eq!(result, Some(Ok(size.saturating_sub(1) * size)));
            }
        }
        client.shutdown_and_wait().await;
    }
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
}
