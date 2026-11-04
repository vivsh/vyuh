use super::*;
use crate::tasks::store::TestError;
use crate::tasks::{FlowAllOut, TaskFailure, TaskLane, TaskStore};

const OTHER: TaskLane = TaskLane::new("mixed-join-flow");

/// Preserves normal policy while declaring a separate lane for a differently typed child.
pub(crate) fn configuration() -> TaskStoreConf {
    let mut configuration = conf();
    configuration.lanes.push(TaskLaneConf::new(OTHER, 8));
    configuration
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("missing mixed join fixture".into())
}

/// Every store executes mixed Work/Flow joins without changing completion or recovery semantics.
pub(crate) async fn contract(store: &impl TaskStore, cancel: bool) -> Result<(), TestError> {
    store.initialize().await?;
    let node = uuid::Uuid::now_v7();
    store.register_node(&node).await?;
    store.store_tasks(vec![write(flow_record())]).await?;
    let parent = next(store, node)
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    let work = record();
    let flow = TaskRecord {
        lane: OTHER.to_string(),
        ..flow_record()
    };
    store
        .commit_tasks(&[output(
            &parent,
            TaskOutcome::All {
                state: "42".into(),
                children: vec![write(work.clone()), write(flow.clone())],
            },
        )?])
        .await?;
    let children = next(store, node).await?;
    assert_eq!(children.len(), 2);
    for child in &children {
        assert_eq!(child.parent_id, Some(parent.id));
        assert_eq!(child.root_id, Some(parent.id));
        assert_eq!(child.attempts, 1);
    }
    complete_children(store, &children, work.id, flow.id, cancel).await?;
    materialized_parent(store, node, parent.id, work.id, cancel).await
}

/// Claims both configured lanes through the unchanged store entry point.
async fn next(store: &impl TaskStore, node: uuid::Uuid) -> Result<Vec<TaskRecord>, TestError> {
    let requests = [DEFAULT_TASK_LANE, OTHER].map(|lane| LaneClaim {
        lane,
        limit: 8,
        owner: None,
    });
    Ok(store
        .claim_tasks(&node, &requests)
        .await?
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .collect())
}

/// Derives fenced outputs from the committed immutable snapshot, including its original lane.
fn output(task: &TaskRecord, outcome: TaskOutcome) -> Result<TaskCommit, TaskRuntimeError> {
    Ok(TaskCommit::Outcome {
        node_id: task.node_id.ok_or_else(missing)?,
        task_id: task.id,
        attempts: task.attempts,
        lane: if task.lane == OTHER.as_str() {
            OTHER
        } else {
            DEFAULT_TASK_LANE
        },
        owner_token: None,
        outcome,
    })
}

/// Completes the second child first; duplicate acknowledgements cannot satisfy the join twice.
async fn complete_children(
    store: &impl TaskStore,
    children: &[TaskRecord],
    work: TaskId,
    flow: TaskId,
    cancel: bool,
) -> Result<(), TestError> {
    let flow = children
        .iter()
        .find(|task| task.id == flow)
        .ok_or_else(missing)?;
    assert_eq!(
        (flow.name.as_str(), flow.kind, flow.lane.as_str()),
        ("flow", TaskKind::Flow, OTHER.as_str())
    );
    let commit = output(
        flow,
        TaskOutcome::CompleteWith {
            output: "\"document\"".into(),
        },
    )?;
    store.commit_tasks(std::slice::from_ref(&commit)).await?;
    assert_eq!(store.commit_tasks(&[commit]).await?.rejected, vec![flow.id]);
    let parent = store
        .get_task(flow.parent_id.ok_or_else(missing)?)
        .await?
        .ok_or_else(missing)?;
    assert_eq!(parent.status, TaskStatus::Suspended);
    let child = children
        .iter()
        .find(|task| task.id == work)
        .ok_or_else(missing)?;
    assert_eq!(
        (child.name.as_str(), child.kind),
        ("workflow", TaskKind::Work)
    );
    let commit = if cancel {
        TaskCommit::Cancel { task_id: work }
    } else {
        output(child, TaskOutcome::CompleteWith { output: "7".into() })?
    };
    store.commit_tasks(&[commit]).await?;
    let stale = output(child, TaskOutcome::Complete)?;
    assert_eq!(store.commit_tasks(&[stale]).await?.rejected, vec![work]);
    Ok(())
}

/// Ordered resume bytes materialize on claim and survive give-back/reclaim without reinterpretation.
async fn materialized_parent(
    store: &impl TaskStore,
    node: uuid::Uuid,
    id: TaskId,
    work: TaskId,
    cancel: bool,
) -> Result<(), TestError> {
    let pending = store.get_task(id).await?.ok_or_else(missing)?;
    assert_eq!(pending.status, TaskStatus::Pending);
    assert!(pending.resume_input.is_none());
    let parent = next(store, node)
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    assert_eq!(parent.id, id);
    assert_eq!(parent.state.as_deref(), Some("42"));
    type Results = (Result<u32, TaskFailure>, Result<String, TaskFailure>);
    let results = parent
        .resume_input::<Results>()?
        .ok_or_else(missing)?
        .map_err(|_| missing())?;
    let dynamic = parent
        .resume_input::<FlowAllOut>()?
        .ok_or_else(missing)?
        .map_err(|_| missing())?;
    assert_eq!(dynamic.decode::<u32>(0)?, results.0);
    assert_eq!(dynamic.decode::<String>(1)?, results.1);
    if cancel {
        assert!(
            matches!(results.0, Err(error) if error.task_id() == Some(&work) && error.message() == "Task cancelled")
        );
    } else {
        assert_eq!(results.0, Ok(7));
    }
    assert_eq!(results.1, Ok("document".into()));
    store.return_claims(Some(node)).await?;
    let recovered = next(store, node)
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    assert_eq!(recovered.resume_input, parent.resume_input);
    assert_eq!(recovered.attempts, parent.attempts + 1);
    store
        .commit_tasks(&[output(&recovered, TaskOutcome::Complete)?])
        .await?;
    Ok(())
}

/// Memory joins retain ordering, cancellation failure delivery, stale fencing and replayed inputs.
#[tokio::test]
async fn mixed_join_contract() -> Result<(), Box<TestError>> {
    for cancel in [false, true] {
        contract(&MemoryTaskStore::new(32, configuration()), cancel).await?;
    }
    Ok(())
}
