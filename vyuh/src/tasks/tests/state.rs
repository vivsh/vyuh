use crate::tasks::{FlowState, TaskOptions};
use std::{sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::*;
use crate::tasks::{
    AbstractTaskStore, Continuation, DEFAULT_TASK_LANE, LaneClaim, RegisteredTask, TaskCommit,
    TaskConf, TaskDefinition, TaskDispatcher, TaskIdempotency, TaskLane, TaskLaneConf, TaskRecord,
    TaskRegistry, TaskStatus, TaskTick, store::MemoryTaskStore,
};
use crate::tasks::{FlowError, WorkError};
use crate::{Data, Site, SiteConf, bundles};

const CHILD_LANE: TaskLane = TaskLane::new("child");

#[derive(Serialize, Deserialize, JsonSchema)]
struct Parent {
    value: u32,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct Child {
    value: u32,
}

/// Awaits one child using only typed continuation and input extractors.
fn parent(
    continuation: Continuation<u32, u32>,
    input: Data<Parent>,
) -> Result<FlowState<u32>, FlowError> {
    match continuation.resume() {
        None => Ok(FlowState::spawn(Child { value: input.value }, 7u32)?),
        Some(Ok(value)) => Ok(FlowState::complete(
            value + continuation.state().copied().unwrap_or(0),
        )),
        Some(Err(failure)) => Err(FlowError::fail(failure.message())),
    }
}

async fn child(input: Data<Child>) -> Result<WorkState<u32>, WorkError> {
    Ok(WorkState::complete(input.value * 2))
}

fn child_definition() -> TaskDefinition<Child> {
    TaskDefinition::new("child")
        .lane(CHILD_LANE)
        .idempotency(TaskIdempotency::new("v1", |input: &Child| {
            input.value.to_string()
        }))
}

/// Uses identical registered policies for invocation and an independently inspected store.
async fn fixture() -> Result<(Site, TaskDispatcher<MemoryTaskStore>), String> {
    fixture_with(TaskConf::default()).await
}

/// Configures matching runtime and memory-store policy for one invocation fixture.
async fn fixture_with(conf: TaskConf) -> Result<(Site, TaskDispatcher<MemoryTaskStore>), String> {
    let conf = conf.lane(TaskLaneConf::new(CHILD_LANE, 1));
    let site = Site::build(
        SiteConf::default().log_init(false).tasks(conf.clone()),
        bundles::bundle([
            bundles::flow(parent, TaskDefinition::new("parent")),
            bundles::work(child, child_definition()),
        ]),
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut registry = TaskRegistry::new();
    registry
        .register(RegisteredTask::new_flow(
            TaskDefinition::new("parent"),
            parent,
        ))
        .map_err(|error| error.to_string())?;
    registry
        .register(RegisteredTask::new(child_definition(), child))
        .map_err(|error| error.to_string())?;
    let registry = registry.finalize(conf).map_err(|error| error.to_string())?;
    let dispatcher = Arc::new(registry).dispatcher(Arc::new(MemoryTaskStore::new(32)), Vec::new());
    dispatcher
        .ensure_initialized()
        .await
        .map_err(|error| error.to_string())?;
    Ok((site, dispatcher))
}

/// Runs one store turn with both lanes eligible, without invoking returned work.
async fn tick(store: &MemoryTaskStore, commits: &[TaskCommit]) -> Result<TaskTick, String> {
    let claims = [DEFAULT_TASK_LANE, CHILD_LANE].map(|lane| LaneClaim {
        lane,
        limit: 32,
        owner: None,
    });
    store
        .tick("owner", &claims, commits, &[])
        .await
        .map_err(|error| error.to_string())
}

async fn next_task(store: &MemoryTaskStore) -> Result<TaskRecord, String> {
    tick(store, &[])
        .await?
        .poll
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .next()
        .ok_or_else(|| "no ready task".into())
}

/// Executes a real typed handler and commits its outcome through the ordinary turn.
async fn execute(
    site: &Site,
    dispatcher: &TaskDispatcher<MemoryTaskStore>,
    record: TaskRecord,
) -> Result<TaskTick, String> {
    let handler = dispatcher
        .registry
        .tasks
        .get(&record.name)
        .ok_or("missing handler")?;
    let outcome = handler
        .execute(site.clone(), Arc::new(record.clone()))
        .await;
    let lane = if record.name == "child" {
        CHILD_LANE
    } else {
        DEFAULT_TASK_LANE
    };
    tick(
        &dispatcher.store,
        &[TaskCommit {
            task_id: record.id,
            lane,
            owner_token: None,
            outcome,
        }],
    )
    .await
}

/// A handler without Site extraction spawns and resumes exclusively through later polls.
#[tokio::test]
async fn spawn_defers_child_execution() -> Result<(), String> {
    let (site, dispatcher) = fixture().await?;
    let parent_id = dispatcher
        .submit(Parent { value: 4 })
        .await
        .map_err(|e| e.to_string())?
        .id();
    let first = next_task(&dispatcher.store).await?;
    assert_eq!(first.kind, crate::tasks::TaskKind::Flow);
    let turn = execute(&site, &dispatcher, first).await?;
    assert!(turn.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
    assert_eq!(turn.wake_lanes, [CHILD_LANE]);
    let child = next_task(&dispatcher.store).await?;
    assert_eq!(child.kind, crate::tasks::TaskKind::Work);
    assert_eq!(child.parent_id, Some(parent_id));
    assert_eq!(child.root_id, Some(parent_id));
    let turn = execute(&site, &dispatcher, child).await?;
    assert!(turn.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
    let resumed = next_task(&dispatcher.store).await?;
    assert_eq!(resumed.id, parent_id);
    assert_eq!(resumed.state.as_deref(), Some("7"));
    assert_eq!(resumed.resume_input.as_deref(), Some("{\"Ok\":8}"));
    execute(&site, &dispatcher, resumed).await?;
    let parent = dispatcher
        .get(parent_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing parent")?;
    assert_eq!(parent.status, TaskStatus::Succeeded);
    assert_eq!(parent.last_result.as_deref(), Some("{\"Ok\":15}"));
    Ok(())
}

/// The live runner shares its ordinary concurrency budget and lane rates across Flow/Work steps.
#[tokio::test]
async fn flow_work_live_runner() -> Result<(), String> {
    let conf = TaskConf::default()
        .concurrency(2)
        .poll_interval(Duration::from_millis(10))
        .lane(
            TaskLaneConf::new(DEFAULT_TASK_LANE, 1)
                .global_rate_limit(crate::tasks::TaskRate::per_second(1)),
        );
    let (site, dispatcher) = fixture_with(conf).await?;
    let id = dispatcher
        .submit(Parent { value: 4 })
        .await
        .map_err(|e| e.to_string())?
        .id();
    let runner =
        crate::tasks::AbstractTaskRunner::new(dispatcher.clone()).map_err(|e| e.to_string())?;
    let started = std::time::Instant::now();
    let running = tokio::spawn(runner.run(site.clone()));
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let task = dispatcher
                .get(id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or("missing parent")?;
            if task.status == TaskStatus::Succeeded {
                assert_eq!(task.last_result.as_deref(), Some("{\"Ok\":15}"));
                assert_eq!(task.attempts, 2);
                assert!(started.elapsed() >= Duration::from_millis(900));
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    running.abort();
    let _ = running.await;
    result.map_err(|e| e.to_string())?
}

/// Constructing, resolving, or dropping an outcome never submits any work.
#[tokio::test]
async fn preparation_preserves_policy() -> Result<(), String> {
    let (site, dispatcher) = fixture().await?;
    let state = FlowState::<()>::spawn_with(
        Child { value: 8 },
        "checkpoint",
        TaskOptions::new().delay(Duration::from_secs(60)),
    )
    .map_err(|e| e.to_string())?;
    let TaskOutcome::Spawn {
        state: checkpoint,
        child,
    } = state.resolve(&site).map_err(|e| e.to_string())?
    else {
        return Err("expected a prepared spawn".into());
    };
    assert_eq!(checkpoint, "\"checkpoint\"");
    assert_eq!(child.record.lane, CHILD_LANE.as_str());
    assert_eq!(child.initial_delay, Some(Duration::from_secs(60)));
    assert_eq!(child.record.idempotency_key.as_deref(), Some("8"));
    assert!(child.record.parent_id.is_none());
    assert!(child.record.root_id.is_none());
    drop(state);
    assert_eq!(dispatcher.store.task_count().await, 0);
    let id = dispatcher
        .submit(Child { value: 8 })
        .await
        .map_err(|e| e.to_string())?
        .id();
    let submitted = dispatcher
        .get(id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing child")?;
    assert_eq!(child.record.input, submitted.input);
    assert_eq!(
        child.record.idempotency_fingerprint,
        submitted.idempotency_fingerprint
    );
    Ok(())
}

/// Construction rejects conflicting options and serialization errors without a site.
#[test]
fn spawn_validates_construction() {
    assert!(
        FlowState::<()>::spawn_with(
            Child { value: 1 },
            (),
            TaskOptions::new().ignore_conflicts()
        )
        .is_err()
    );
    assert!(
        FlowState::<()>::spawn_with(
            Child { value: 1 },
            (),
            TaskOptions::new().delay(Duration::MAX)
        )
        .is_err()
    );
    let invalid = std::collections::BTreeMap::from([((1, 2), 3)]);
    assert!(FlowState::<()>::spawn(Child { value: 1 }, invalid).is_err());
}

/// Registry-dependent limits are enforced on both child input and parent checkpoint.
#[tokio::test]
async fn preparation_checks_payload_limits() -> Result<(), String> {
    let (site, dispatcher) = fixture_with(TaskConf::default().max_payload_bytes(16)).await?;
    let checkpoint =
        FlowState::spawn(Child { value: 1 }, "x".repeat(17)).map_err(|e| e.to_string())?;
    let input = FlowState::spawn(Child { value: u32::MAX }, ()).map_err(|e| e.to_string())?;
    assert!(matches!(
        checkpoint.resolve(&site),
        Err(TaskRuntimeError::InvalidOptions(_))
    ));
    assert!(matches!(
        input.resolve(&site),
        Err(TaskRuntimeError::InvalidOptions(_))
    ));
    assert_eq!(dispatcher.store.task_count().await, 0);
    Ok(())
}

/// Deferred child serialization errors become ordinary contained handler failures.
#[tokio::test]
async fn malformed_child_fails_parent() -> Result<(), String> {
    #[derive(Serialize, Deserialize, JsonSchema)]
    struct Malformed {
        entries: std::collections::BTreeMap<(u32, u32), u32>,
    }
    async fn malformed(_: Data<Malformed>) {}
    fn spawn(_: Data<Parent>) -> Result<FlowState, FlowError> {
        Ok(FlowState::spawn(
            Malformed {
                entries: std::collections::BTreeMap::from([((1, 2), 3)]),
            },
            (),
        )?)
    }
    let site = Site::build(
        SiteConf::default().log_init(false),
        bundles::bundle([bundles::work(malformed, TaskDefinition::new("malformed"))]),
    )
    .await
    .map_err(|e| e.to_string())?;
    let (_, dispatcher) = fixture().await?;
    dispatcher
        .submit(Parent { value: 4 })
        .await
        .map_err(|e| e.to_string())?;
    let record = next_task(&dispatcher.store).await?;
    let handler = RegisteredTask::new_flow(TaskDefinition::new("spawn"), spawn);
    let outcome = handler.execute(site, Arc::new(record)).await;
    assert!(matches!(outcome, TaskOutcome::Fail { error } if error == "Task handler failed"));
    assert_eq!(dispatcher.store.task_count().await, 1);
    Ok(())
}

/// Unregistered requests fail the parent normally instead of escaping into store commits.
#[tokio::test]
async fn unregistered_child_fails_parent() -> Result<(), String> {
    fn missing(_input: Data<Parent>) -> Result<FlowState, FlowError> {
        Ok(FlowState::spawn("unregistered child".to_owned(), ())?)
    }
    let (site, dispatcher) = fixture().await?;
    dispatcher
        .submit(Parent { value: 4 })
        .await
        .map_err(|e| e.to_string())?;
    let record = next_task(&dispatcher.store).await?;
    let task = RegisteredTask::new_flow(TaskDefinition::new("missing"), missing);
    let outcome = task.execute(site, Arc::new(record.clone())).await;
    assert!(matches!(&outcome, TaskOutcome::Fail { error } if error == "Task handler failed"));
    tick(
        &dispatcher.store,
        &[TaskCommit {
            task_id: record.id,
            lane: DEFAULT_TASK_LANE,
            owner_token: None,
            outcome,
        }],
    )
    .await?;
    assert_eq!(dispatcher.store.task_count().await, 1);
    assert_eq!(
        dispatcher
            .get(record.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing parent")?
            .status,
        TaskStatus::Failed
    );
    Ok(())
}
