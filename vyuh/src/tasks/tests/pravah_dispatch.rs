//! Shared real-graph dispatch contract over the memory and SQL stores.

use crate::tasks::*;
use crate::{PartialSite, Site, bundles, prelude::Data};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

const EFFECTS: TaskLane = TaskLane::new("graph-effects");

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Input(u32);

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Job {
    #[schemars(with = "String")]
    id: uuid::Uuid,
    request: pravah::FetchRequest,
}

struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }
    fn fetch(&self, request: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Ok(WorkRequest::new(Job {
            id: request.id(),
            request: request.request().clone(),
        }))
    }
}

fn graph(root: pravah::Flow<Input>) -> pravah::Flow<u16> {
    root.map(|input: Input| {
        pravah::FetchRequest::new(input.0.to_string(), "https://example.invalid")
    })
    .fetch()
    .map(|response| response.map(|r| r.status()).unwrap_or(499))
}

async fn work(_: Data<Job>) -> pravah::FetchResponse {
    pravah::FetchResponse::new(207)
}

/// Shares one retry and lane policy between site preparation and each backend fixture.
pub(crate) fn conf() -> TaskConf {
    TaskConf::default().lane(
        TaskLaneConf::new(EFFECTS, 4).retry(TaskRetry::exponential(3, Duration::from_millis(1))),
    )
}

/// Declares matching graph and Work input identities for private child resolution.
pub(crate) fn bundle() -> bundles::Bundle {
    bundles::bundle([
        bundles::flow(graph, FlowConf::new("graph").dispatch::<Dispatcher>()),
        bundles::work(work, work_definition()),
    ])
}

fn work_definition() -> TaskDefinition<Job> {
    TaskDefinition::new("effect")
        .lane(EFFECTS)
        .idempotency(TaskIdempotency::new("request", |job: &Job| {
            job.request.method().to_owned()
        }))
}

/// Independently exposes the same immutable roster to deterministic store turns.
async fn dispatcher<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    store: Arc<S>,
) -> Result<TaskDispatcher<S>, String> {
    let mut registry = TaskRegistry::new();
    registry
        .register(RegisteredTask::new_flow(
            FlowConf::new("graph").dispatch::<Dispatcher>(),
            graph,
        ))
        .map_err(|e| e.to_string())?;
    registry
        .register(RegisteredTask::new(work_definition(), work))
        .map_err(|e| e.to_string())?;
    let registry = registry
        .prepare_flows(&PartialSite::new(site.db()))
        .and_then(|r| r.finalize(conf()))
        .map_err(|e| e.to_string())?;
    let dispatcher = Arc::new(registry).dispatcher(store, Vec::new());
    dispatcher
        .ensure_initialized()
        .await
        .map_err(|e| e.to_string())?;
    Ok(dispatcher)
}

/// Every graph/effect transition uses a normal store turn, including duplicate acknowledgements.
async fn tick<S: AbstractTaskStore>(store: &S, commits: &[TaskCommit]) -> Result<TaskTick, String> {
    let claims = [DEFAULT_TASK_LANE, EFFECTS].map(|lane| LaneClaim {
        lane,
        limit: 32,
        owner: None,
    });
    store
        .tick("owner", &claims, commits, &[])
        .await
        .map_err(|e| e.to_string())
}

async fn next<S: AbstractTaskStore>(store: &S) -> Result<TaskRecord, String> {
    next_runner(store, "owner").await
}

/// Polls using a concrete process identity, accommodating backend clock resolution.
async fn next_runner<S: AbstractTaskStore>(store: &S, owner: &str) -> Result<TaskRecord, String> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let claims = [DEFAULT_TASK_LANE, EFFECTS].map(|lane| LaneClaim {
                lane,
                limit: 32,
                owner: None,
            });
            let mut records: Vec<_> = store
                .tick(owner, &claims, &[], &[])
                .await
                .map_err(|e| e.to_string())?
                .poll
                .lanes
                .into_iter()
                .flat_map(|lane| lane.tasks)
                .collect();
            if records.len() > 1 {
                return Err(format!("expected one claim, got {}", records.len()));
            }
            if let Some(record) = records.pop() {
                return Ok(record);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "no claim before timeout".to_owned())?
}

fn commit(record: &TaskRecord, outcome: TaskOutcome) -> TaskCommit {
    TaskCommit {
        task_id: record.id,
        lane: if record.kind == TaskKind::Flow {
            DEFAULT_TASK_LANE
        } else {
            EFFECTS
        },
        owner_token: None,
        outcome,
    }
}

async fn execute<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
    record: &TaskRecord,
) -> Result<TaskCommit, String> {
    let handler = dispatcher
        .registry
        .tasks
        .get(&record.name)
        .ok_or("missing handler")?;
    Ok(commit(
        record,
        handler
            .execute(site.clone(), Arc::new(record.clone()))
            .await,
    ))
}

/// Commit-created children/resumptions never appear in that same turn's claim output.
fn assert_deferred(turn: &TaskTick) {
    assert!(turn.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
}

/// Real graph checkpoints preserve atomic creation, scalar results, and later-poll execution.
pub(crate) async fn contract<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    store: Arc<S>,
) -> Result<(), String> {
    let dispatcher = dispatcher(site, store).await?;
    for mode in 0..7 {
        round_trip(site, &dispatcher, mode)
            .await
            .map_err(|error| format!("mode {mode}: {error}"))?;
    }
    crash_replay(site, &dispatcher).await?;
    conflicting_effect(site, &dispatcher).await?;
    Ok(())
}

/// An independently submitted Work task cannot be adopted by a graph's atomic spawn.
async fn conflicting_effect<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
) -> Result<(), String> {
    let existing = dispatcher
        .submit(Job {
            id: uuid::Uuid::now_v7(),
            request: pravah::FetchRequest::new("8", "https://example.invalid"),
        })
        .await
        .map_err(|e| e.to_string())?
        .id();
    let child = next(dispatcher.store.as_ref()).await?;
    assert_eq!(child.id, existing);
    let id = dispatcher
        .submit(Input(8))
        .await
        .map_err(|e| e.to_string())?
        .id();
    let parent = next(dispatcher.store.as_ref()).await?;
    let spawn = execute(site, dispatcher, &parent).await?;
    assert_deferred(&tick(dispatcher.store.as_ref(), &[spawn]).await?);
    let failed = dispatcher
        .get(id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing parent")?;
    assert_eq!(failed.status, TaskStatus::Failed);
    let original = dispatcher
        .get(existing)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing original")?;
    assert_eq!(original.parent_id, None);
    let completion = execute(site, dispatcher, &child).await?;
    assert_deferred(&tick(dispatcher.store.as_ref(), &[completion]).await?);
    Ok(())
}

/// Lease takeover before spawn and after result delivery preserves checkpoint ownership.
async fn crash_replay<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
) -> Result<(), String> {
    dispatcher
        .submit(Input(7))
        .await
        .map_err(|e| e.to_string())?;
    let first = next(dispatcher.store.as_ref()).await?;
    let stale_spawn = execute(site, dispatcher, &first).await?;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let takeover = next_runner(dispatcher.store.as_ref(), "takeover").await?;
    assert_eq!(first.id, takeover.id);
    dispatcher
        .store
        .commit_outcomes("owner", &[stale_spawn])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        dispatcher
            .get(first.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing")?
            .status,
        TaskStatus::Running
    );
    let spawn = execute(site, dispatcher, &takeover).await?;
    dispatcher
        .store
        .commit_outcomes("takeover", &[spawn])
        .await
        .map_err(|e| e.to_string())?;
    let child = next(dispatcher.store.as_ref()).await?;
    let done = execute(site, dispatcher, &child).await?;
    assert_deferred(&tick(dispatcher.store.as_ref(), &[done]).await?);
    let resumed = next(dispatcher.store.as_ref()).await?;
    replay_delivered(site, dispatcher, resumed).await
}

/// A takeover after result delivery consumes the same bytes without another Work dispatch.
async fn replay_delivered<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
    resumed: TaskRecord,
) -> Result<(), String> {
    let stale_completion = execute(site, dispatcher, &resumed).await?;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let replay = next_runner(dispatcher.store.as_ref(), "takeover").await?;
    assert_eq!(replay.id, resumed.id);
    assert_eq!(replay.resume_input, resumed.resume_input);
    assert_eq!(replay.state, resumed.state);
    dispatcher
        .store
        .commit_outcomes("owner", &[stale_completion])
        .await
        .map_err(|e| e.to_string())?;
    let completion = execute(site, dispatcher, &replay).await?;
    assert!(matches!(
        completion.outcome,
        TaskOutcome::CompleteWith { .. }
    ));
    dispatcher
        .store
        .commit_outcomes("takeover", &[completion])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        dispatcher
            .get(resumed.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing")?
            .last_result
            .as_deref(),
        Some("{\"Ok\":207}")
    );
    Ok(())
}

/// Exercises accepted, retried, failed, cancelled-child and cancelled-parent Fetch lifecycles.
async fn round_trip<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
    mode: u32,
) -> Result<(), String> {
    let id = dispatcher
        .submit(Input(mode))
        .await
        .map_err(|e| e.to_string())?
        .id();
    let parent = next(dispatcher.store.as_ref())
        .await
        .map_err(|e| format!("initial parent: {e}"))?;
    assert_eq!(parent.id, id);
    let spawn = execute(site, dispatcher, &parent).await?;
    assert!(matches!(spawn.outcome, TaskOutcome::Spawn { .. }));
    assert_deferred(&tick(dispatcher.store.as_ref(), &[spawn.clone()]).await?);
    let child = next(dispatcher.store.as_ref())
        .await
        .map_err(|e| format!("spawned child: {e}"))?;
    assert_eq!(child.parent_id, Some(id));
    assert_eq!(child.root_id, Some(id));
    let checkpoint = dispatcher
        .get(id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("parent missing")?;
    assert!(
        checkpoint
            .state
            .as_ref()
            .is_some_and(|s| serde_json::from_str::<pravah::Snapshot>(s).is_ok())
    );
    assert_deferred(&tick(dispatcher.store.as_ref(), &[spawn]).await?);
    finish_child(site, dispatcher, &child, mode).await?;
    finish_parent(site, dispatcher, checkpoint, mode).await
}

/// Delivery restores the original snapshot unless the authoritative parent was cancelled.
async fn finish_parent<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
    checkpoint: TaskRecord,
    mode: u32,
) -> Result<(), String> {
    let id = checkpoint.id;
    if mode == 4 {
        let parent = dispatcher
            .get(id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing")?;
        assert_eq!(parent.status, TaskStatus::Failed);
        assert!(parent.cancelled);
        return Ok(());
    }
    let resumed = next(dispatcher.store.as_ref())
        .await
        .map_err(|e| format!("resumed parent: {e}"))?;
    assert_eq!(resumed.id, id);
    assert_eq!(resumed.state, checkpoint.state);
    let completion = execute(site, dispatcher, &resumed).await?;
    assert_deferred(&tick(dispatcher.store.as_ref(), &[completion]).await?);
    let parent = dispatcher
        .get(id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing")?;
    assert_eq!(parent.status, TaskStatus::Succeeded);
    let expected = if [2, 3, 6].contains(&mode) { 499 } else { 207 };
    assert_eq!(parent.last_result, Some(format!("{{\"Ok\":{expected}}}")));
    Ok(())
}

/// Tests effects through retry/suspend, terminal errors, cancellation, and duplicate flushes.
async fn finish_child<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
    child: &TaskRecord,
    mode: u32,
) -> Result<(), String> {
    let child = retry_child(dispatcher, child, mode).await?;
    if mode == 6 {
        return cancel_on_renewal(site, dispatcher, &child).await;
    }
    if mode == 3 || mode == 4 {
        let cancelled = if mode == 3 {
            child.id
        } else {
            child.parent_id.ok_or("missing parent")?
        };
        assert!(
            dispatcher
                .cancel(cancelled)
                .await
                .map_err(|e| e.to_string())?
        );
    }
    let completion = if mode == 2 {
        commit(&child, TaskOutcome::fail("provider failure"))
    } else {
        execute(site, dispatcher, &child).await?
    };
    assert_deferred(&tick(dispatcher.store.as_ref(), &[completion.clone()]).await?);
    // No claim selection: an uncertain acknowledgement must not consume the resumed parent.
    dispatcher
        .store
        .commit_outcomes("owner", &[completion])
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Central renewal finalizes cancellation, delivers the error, and fences a late Work success.
async fn cancel_on_renewal<S: AbstractTaskStore + Send + Sync + 'static>(
    site: &Site,
    dispatcher: &TaskDispatcher<S>,
    child: &TaskRecord,
) -> Result<(), String> {
    let late = execute(site, dispatcher, child).await?;
    assert!(
        dispatcher
            .cancel(child.id)
            .await
            .map_err(|e| e.to_string())?
    );
    let claims = [DEFAULT_TASK_LANE, EFFECTS].map(|lane| LaneClaim {
        lane,
        limit: 32,
        owner: None,
    });
    let renewal = TaskLease {
        task_id: child.id,
        lane: EFFECTS,
        owner_token: None,
    };
    let turn = dispatcher
        .store
        .tick("owner", &claims, &[], &[renewal])
        .await
        .map_err(|e| e.to_string())?;
    assert_deferred(&turn);
    assert!(turn.lost.contains(&child.id));
    assert!(turn.cancelled.contains(&child.id));
    dispatcher
        .store
        .commit_outcomes("owner", &[late])
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Retry and Work suspension retain the parent wait without delivering an intermediate result.
async fn retry_child<S: AbstractTaskStore + Send + Sync + 'static>(
    dispatcher: &TaskDispatcher<S>,
    child: &TaskRecord,
    mode: u32,
) -> Result<TaskRecord, String> {
    let mut child = child.clone();
    if mode == 1 || mode == 5 {
        let outcome = if mode == 1 {
            TaskOutcome::retry("transient")
        } else {
            TaskOutcome::Suspend {
                state: "null".into(),
            }
        };
        tick(dispatcher.store.as_ref(), &[commit(&child, outcome)]).await?;
        if mode == 5 {
            assert!(
                dispatcher
                    .resume(child.id, ())
                    .await
                    .map_err(|e| e.to_string())?
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        child = next(dispatcher.store.as_ref())
            .await
            .map_err(|e| format!("retried/resumed child: {e}"))?;
    }
    Ok(child)
}

/// Memory executes the identical graph-to-Work contract used by each SQL backend.
#[tokio::test]
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
async fn memory_graph_dispatch_contract() -> Result<(), String> {
    let site = Site::build(
        crate::SiteConf::default().log_init(false).tasks(conf()),
        bundle(),
    )
    .await
    .map_err(|e| e.to_string())?;
    contract(
        &site,
        Arc::new(crate::tasks::store::MemoryTaskStore::with_lease_duration(
            32,
            Duration::from_secs(2),
        )),
    )
    .await
}
