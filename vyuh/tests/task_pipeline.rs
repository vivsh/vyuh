//! Public-runtime pipeline coverage; cancellation intentionally never interrupts running handlers.
#![cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
use vyuh::prelude::*;
use vyuh::tasks::{TaskConf, TaskDefinition, TaskLaneConf, TaskLaneLock, TaskStatus};
use vyuh::testing::TestSite;

const SLOW: TaskLane = TaskLane::new("pipeline-slow");

/// A retained lane owner can claim a later cohort instead of endlessly coordinating.
#[tokio::test]
async fn owned_lane_refills_after_flushing() {
    let site = Site::build(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .db_concurrency(4)
                .concurrency(4)
                .poll_interval(Duration::from_millis(5))
                .lane(
                    TaskLaneConf::new(SLOW, 1).lock(
                        TaskLaneLock::new(2)
                            .deadline(Duration::from_millis(5))
                            .idle_after(Duration::from_secs(1)),
                    ),
                ),
        ),
        bundles::bundle([bundles::work(
            |input: Data<Fast>| async move { input.0.0 },
            TaskDefinition::new("locked").lane(SLOW),
        )]),
    )
    .await
    .unwrap();
    let runtime = TestSite::new(site.clone());
    runtime.start_runtime().await.unwrap();
    for value in 0..3 {
        let id = site.tasks().submit(Fast(value)).await.unwrap().id();
        status(&site, id, TaskStatus::Succeeded).await;
    }
    runtime.shutdown_and_wait().await;
}

/// One batch invocation persists every constituent output at each database budget.
#[tokio::test]
async fn batches_persist_all_results() {
    for limit in [1, 2, 4] {
        let site = Site::build(
            SiteConf::default().log_init(false).tasks(
                TaskConf::default()
                    .db_concurrency(limit)
                    .concurrency(8)
                    .batch_size(8)
                    .poll_interval(Duration::from_millis(5)),
            ),
            bundles::bundle([bundles::work_batch(
                |input: Data<Batch<Fast>>| async move {
                    input
                        .0
                        .iter()
                        .map(|item| Ok(WorkState::complete(item.0 * 2)))
                        .collect::<Batch<Result<WorkState<u32>, WorkError>>>()
                },
                TaskDefinition::new("batch"),
            )]),
        )
        .await
        .unwrap();
        let runtime = TestSite::new(site.clone());
        let mut ids = Vec::new();
        for value in 0..24 {
            ids.push(site.tasks().submit(Fast(value)).await.unwrap().id());
        }
        runtime.start_runtime().await.unwrap();
        for (value, id) in ids.into_iter().enumerate() {
            status(&site, id, TaskStatus::Succeeded).await;
            assert_eq!(
                site.tasks()
                    .get(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .last_result::<u32>()
                    .unwrap(),
                Some(Ok(value as u32 * 2))
            );
        }
        runtime.shutdown_and_wait().await;
    }
}

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Slow;
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Fast(u32);

/// Waits for an authoritative status without introducing a store mutation.
async fn status(site: &Site, id: TaskId, expected: TaskStatus) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if site
                .tasks()
                .get(id)
                .await
                .unwrap()
                .is_some_and(|row| row.status() == expected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("task did not reach expected status");
}

/// Builds isolated runtime gates without sharing state between tests.
async fn gated(limit: usize) -> (TestSite, Arc<Notify>, Arc<AtomicUsize>) {
    let release = Arc::new(Notify::new());
    let started = Arc::new(AtomicUsize::new(0));
    let gate = release.clone();
    let count = started.clone();
    let slow = move |_: Data<Slow>| {
        let gate = gate.clone();
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            gate.notified().await;
            7u32
        }
    };
    let site = Site::build(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .db_concurrency(limit)
                .concurrency(4)
                .batch_size(4)
                .poll_interval(Duration::from_millis(5))
                .lane(TaskLaneConf::new(SLOW, 1)),
        ),
        bundles::bundle([
            bundles::work(slow, TaskDefinition::new("slow").lane(SLOW)),
            bundles::work(
                |input: Data<Fast>| async move { input.0.0 * 2 },
                TaskDefinition::new("fast"),
            ),
        ]),
    )
    .await
    .unwrap();
    (TestSite::new(site), release, started)
}

/// A blocked invocation does not stop another lane from claiming and committing work.
#[tokio::test]
async fn unrelated_lanes_progress_at_each_database_limit() {
    for limit in [1, 2, 4] {
        let (runtime, release, _) = gated(limit).await;
        let site = runtime.site().clone();
        let slow = site.tasks().submit(Slow).await.unwrap().id();
        runtime.start_runtime().await.unwrap();
        status(&site, slow, TaskStatus::Running).await;
        let mut fast = Vec::new();
        for value in 0..16 {
            fast.push(site.tasks().submit(Fast(value)).await.unwrap().id());
        }
        for id in fast {
            status(&site, id, TaskStatus::Succeeded).await;
        }
        assert_eq!(
            site.tasks().get(slow).await.unwrap().unwrap().status(),
            TaskStatus::Running
        );
        release.notify_one();
        status(&site, slow, TaskStatus::Succeeded).await;
        runtime.shutdown_and_wait().await;
    }
}

/// Cancellation terminally commits while running execution continues; its later output is obsolete.
#[tokio::test]
async fn cancellation_overrides_a_later_successful_output() {
    let (runtime, release, started) = gated(4).await;
    let site = runtime.site().clone();
    let id = site.tasks().submit(Slow).await.unwrap().id();
    runtime.start_runtime().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    site.tasks().cancel(id).await.unwrap();
    site.tasks().cancel(id).await.unwrap();
    assert_eq!(
        site.tasks().get(id).await.unwrap().unwrap().status(),
        TaskStatus::Failed
    );
    release.notify_one();
    runtime.shutdown_and_wait().await;
    status(&site, id, TaskStatus::Failed).await;
    let result = site
        .tasks()
        .get(id)
        .await
        .unwrap()
        .unwrap()
        .last_result::<u32>()
        .unwrap();
    assert!(matches!(result, Some(Err(failure)) if failure.message() == "Task cancelled"));
}

/// Graceful shutdown waits for admitted execution and persists its buffered completion.
#[tokio::test]
async fn shutdown_drains_admitted_work() {
    let (runtime, release, started) = gated(2).await;
    let site = runtime.site().clone();
    let id = site.tasks().submit(Slow).await.unwrap().id();
    runtime.start_runtime().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let shutdown = tokio::spawn(runtime.shutdown_and_wait());
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        site.tasks().get(id).await.unwrap().unwrap().status(),
        TaskStatus::Succeeded
    );
}

/// Invalid database budgets are rejected while finalizing runtime configuration.
#[tokio::test]
async fn invalid_database_budgets_are_rejected() {
    for limit in [0, tokio::sync::Semaphore::MAX_PERMITS + 1] {
        let result = Site::build(
            SiteConf::default()
                .log_init(false)
                .tasks(TaskConf::default().db_concurrency(limit)),
            bundles::bundle([bundles::work(
                |_: Data<Fast>| async {},
                TaskDefinition::new("fast"),
            )]),
        )
        .await;
        assert!(result.is_err());
    }
}
