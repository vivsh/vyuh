use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

/// Live renewal preserves sibling work beyond lease expiry and defers lane idle until completion.
#[tokio::test]
async fn batch_cancel_debounces_idle() -> Result<(), String> {
    let (started, mut receiver) = mpsc::channel(1);
    let idle = Arc::new(AtomicUsize::new(0));
    let dispatcher = dispatcher(started, idle.clone()).map_err(|e| e.to_string())?;
    let first = dispatcher
        .submit(BatchJob)
        .await
        .map_err(|e| e.to_string())?
        .id();
    let second = dispatcher
        .submit(BatchJob)
        .await
        .map_err(|e| e.to_string())?
        .id();
    let runner = AbstractTaskRunner::new(dispatcher.clone()).map_err(|e| e.to_string())?;
    let site = Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
    .map_err(|e| e.to_string())?;
    let running = tokio::spawn(runner.run(site.clone()));
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let (size, release) = receiver.recv().await.ok_or_else(missing)?;
        assert_eq!(size, 2);
        exercise(&dispatcher, first, second, release, &idle).await?;
        assert!(
            receiver.try_recv().is_err(),
            "the batch must execute only once"
        );
        Ok::<(), TaskRuntimeError>(())
    })
    .await;
    site.shutdown_and_wait().await;
    running.abort();
    let _ = running.await;
    result
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

/// Keeps the bulk operation in flight long enough to require multiple task and lane renewals.
async fn exercise(
    dispatcher: &crate::tasks::TaskDispatcher<MemoryTaskStore>,
    first: TaskId,
    second: TaskId,
    release: oneshot::Sender<()>,
    idle: &AtomicUsize,
) -> Result<(), TaskRuntimeError> {
    assert!(dispatcher.cancel(first).await?);
    wait_status(dispatcher, first, TaskStatus::Failed).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let sibling = dispatcher.get(second).await?.ok_or_else(missing)?;
    assert_eq!(sibling.status, TaskStatus::Running);
    assert_eq!(sibling.attempts, 1);
    assert!(
        sibling
            .leased_until
            .is_some_and(|at| at > chrono::Utc::now())
    );
    assert_eq!(idle.load(Ordering::SeqCst), 0);
    let completed = tokio::time::Instant::now();
    release.send(()).map_err(|_| missing())?;
    wait_status(dispatcher, second, TaskStatus::Succeeded).await?;
    while idle.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(completed.elapsed() >= Duration::from_millis(50));
    let cancelled = dispatcher.get(first).await?.ok_or_else(missing)?;
    assert_eq!(cancelled.status, TaskStatus::Failed);
    assert_eq!(cancelled.attempts, 1);
    assert_eq!(
        dispatcher.get(second).await?.ok_or_else(missing)?.attempts,
        1
    );
    Ok(())
}

async fn wait_status(
    dispatcher: &crate::tasks::TaskDispatcher<MemoryTaskStore>,
    id: TaskId,
    status: TaskStatus,
) -> Result<(), TaskRuntimeError> {
    while dispatcher.get(id).await?.ok_or_else(missing)?.status != status {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

/// Shares ordinary lane machinery with a gated bulk handler and observable idle hook.
fn dispatcher(
    started: mpsc::Sender<(usize, oneshot::Sender<()>)>,
    idle: Arc<AtomicUsize>,
) -> Result<crate::tasks::TaskDispatcher<MemoryTaskStore>, TaskRuntimeError> {
    let lock = crate::tasks::TaskLaneLock::new(2)
        .idle_after(Duration::from_millis(50))
        .on_busy(|| async { Ok::<(), crate::Error>(()) })
        .on_idle(move || {
            let idle = idle.clone();
            async move {
                idle.fetch_add(1, Ordering::SeqCst);
                Ok::<(), crate::Error>(())
            }
        });
    let lease = Duration::from_millis(150);
    let conf = TaskConf::default()
        .concurrency(2)
        .batch_size(4)
        .poll_interval(Duration::from_millis(10))
        .fallback_poll_interval(Duration::from_millis(20))
        .lease_duration(lease)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 2).lock(lock));
    let mut registry = TaskRegistry::new();
    registry.register(RegisteredTask::new_batch(
        crate::tasks::TaskDefinition::new("gated-batch"),
        move |Data(items): Data<crate::tasks::Batch<BatchJob>>| {
            let started = started.clone();
            async move {
                let (release, wait) = oneshot::channel();
                let _ = started.send((items.len(), release)).await;
                let _ = wait.await;
            }
        },
    ))?;
    Ok(Arc::new(registry.with_config(conf)?).dispatcher(
        Arc::new(MemoryTaskStore::with_lease_duration(4, lease)),
        Vec::new(),
    ))
}
