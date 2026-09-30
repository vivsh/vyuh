//! Factory construction and ordinary durable manual advancement contracts.
#![cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use vyuh::prelude::*;
use vyuh::tasks::{TaskConf, TaskDefinition, TaskStatus};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Parent(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Child(u32);

struct Manual;

/// Duplicate input registrations fail before invoking either user factory.
#[tokio::test]
async fn duplicates_precede_factory_execution() {
    let calls = Arc::new(AtomicUsize::new(0));
    let first = calls.clone();
    let second = calls.clone();
    let result = Site::build(
        SiteConf::default().log_init(false),
        bundles::bundle([
            bundles::flow(
                move || {
                    first.fetch_add(1, Ordering::SeqCst);
                    Manual
                },
                FlowConf::new("first"),
            ),
            bundles::flow(
                move || {
                    second.fetch_add(1, Ordering::SeqCst);
                    Manual
                },
                FlowConf::new("second"),
            ),
        ]),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

impl Flow for Manual {
    type Input = Parent;
    type Output = u32;
    type Checkpoint = ();
    type Resume = u32;

    fn advance(
        &self,
        _: TaskId,
        input: Data<Parent>,
        continuation: Continuation<(), u32>,
    ) -> Result<FlowState<u32>, FlowError> {
        match continuation.into_parts() {
            (None, None) => Ok(FlowState::spawn(Child(input.0.0), ())?),
            (Some(()), Some(result)) => Ok(FlowState::complete(result? + 1)),
            _ => Err(FlowError::fail("Invalid manual continuation")),
        }
    }
}

#[bundles::flow]
fn parent() -> impl Flow<Input = Parent, Output = u32> {
    Manual
}

#[bundles::work]
async fn child(input: Data<Child>) -> u32 {
    input.0.0 * 2
}

/// An immutable definition is built once, but each submission advances independently.
#[tokio::test]
async fn manual_child_round_trip() -> Result<(), TestError> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let bundle = bundles::bundle([
        bundles::flow(
            move || {
                counted.fetch_add(1, Ordering::SeqCst);
                Manual
            },
            FlowConf::new("parent"),
        ),
        bundles::work(child, TaskDefinition::new("child")),
    ]);
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(5))),
        bundle,
    )
    .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        Arc::strong_count(&calls),
        1,
        "built site must drop the consumed factory capture"
    );
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for value in [2, 7] {
        let id = site.tasks().submit(Parent(value)).await?.id();
        assert_eq!(wait_output(&site, id).await?, Some(Ok(value * 2 + 1)));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Macro registration accepts opaque manual definitions without inspecting type spelling.
#[tokio::test]
async fn macro_factory_builds() -> Result<(), TestError> {
    Site::build(
        SiteConf::default().log_init(false),
        bundles::bundle([__bundle_part_parent(), __bundle_part_child()]),
    )
    .await?;
    Ok(())
}

/// Factory failures/panics and invalid configuration fail construction, not a runtime task.
#[tokio::test]
async fn build_failures_are_contained() {
    let conf = || SiteConf::default().log_init(false);
    let failed = bundles::flow(
        || -> Result<Manual, FlowError> { Err(FlowError::fail("Build failed")) },
        FlowConf::new("failed"),
    );
    assert!(
        Site::build(conf(), bundles::bundle([failed]))
            .await
            .is_err()
    );
    let panicked = bundles::flow(
        || -> Manual { panic!("private panic payload") },
        FlowConf::new("panicked"),
    );
    let error = Site::build(conf(), bundles::bundle([panicked]))
        .await
        .err()
        .map(|error| error.to_string());
    assert!(error.is_some_and(
        |error| error.contains("factory panicked") && !error.contains("private panic payload")
    ));
    for limit in [0, 10_001] {
        let invalid = bundles::flow(parent, FlowConf::new("invalid").step_limit(limit));
        assert!(
            Site::build(conf(), bundles::bundle([invalid]))
                .await
                .is_err()
        );
    }
}

/// Factories may acquire a build-time database handle but cannot receive runtime input.
#[tokio::test]
async fn partial_site_and_fallible_factory() -> Result<(), TestError> {
    fn factory(site: vyuh::PartialSite) -> Result<Manual, FlowError> {
        let _pool = site.db();
        Ok(Manual)
    }
    Site::build(
        SiteConf::default().log_init(false),
        bundles::bundle([bundles::flow(factory, FlowConf::new("manual"))]),
    )
    .await?;
    Ok(())
}

/// Waits through the public task facade without driving any store mutations itself.
async fn wait_output(
    site: &Site,
    id: TaskId,
) -> Result<Option<Result<u32, TaskFailure>>, TestError> {
    Ok(tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(task) = site.tasks().get(id).await?
                && matches!(task.status(), TaskStatus::Succeeded | TaskStatus::Failed)
            {
                return task.last_result::<u32>();
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
