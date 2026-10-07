use super::*;
use crate::tasks::store::TestError;

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("missing pipeline fixture".into())
}

/// Creates a registered node and a pending Flow using the independent store API.
async fn fixture() -> Result<(MemoryTaskStore, uuid::Uuid, TaskRecord), TestError> {
    let store = MemoryTaskStore::new(32, conf());
    store.initialize().await?;
    let node = uuid::Uuid::now_v7();
    store.register_node(&node).await?;
    store.store_tasks(vec![write(flow_record())]).await?;
    let parent = next(&store, node).await?;
    Ok((store, node, parent))
}

/// Claims the next immutable fixture through the ordinary independent operation.
async fn next(store: &MemoryTaskStore, node: uuid::Uuid) -> Result<TaskRecord, TestError> {
    store
        .claim_tasks(&node, &[claim()])
        .await?
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .next()
        .ok_or_else(|| missing().into())
}

fn output(task: &TaskRecord, outcome: TaskOutcome) -> Result<TaskCommit, TaskRuntimeError> {
    Ok(TaskCommit::Outcome {
        node_id: task.node_id.ok_or_else(missing)?,
        attempts: task.attempts,
        task_id: task.id,
        lane: DEFAULT_TASK_LANE,
        outcome,
        owner_token: None,
    })
}

/// Scalar children remain atomic, fenced, independently claimed and continuation-preserving.
#[tokio::test]
async fn sequential_children() -> Result<(), TestError> {
    let (store, node, mut parent) = fixture().await?;
    for value in 0..8 {
        let spawn = output(
            &parent,
            TaskOutcome::Spawn {
                state: value.to_string(),
                child: write(record()),
            },
        )?;
        let result = store.commit_tasks(std::slice::from_ref(&spawn)).await?;
        assert_eq!(result.wake_lanes, vec![DEFAULT_TASK_LANE]);
        assert_eq!(
            store.commit_tasks(&[spawn]).await?.rejected,
            vec![parent.id]
        );
        let child = next(&store, node).await?;
        assert_eq!(child.parent_id, Some(parent.id));
        assert_eq!(child.root_id, Some(parent.id));
        store
            .commit_tasks(&[output(
                &child,
                TaskOutcome::CompleteWith {
                    output: value.to_string(),
                },
            )?])
            .await?;
        parent = next(&store, node).await?;
        assert_eq!(parent.state.as_deref(), Some(value.to_string().as_str()));
        assert_eq!(parent.resume_input::<i32>()?, Some(Ok(value)));
        assert_eq!(parent.step_attempts, 1);
        assert_eq!(parent.attempts, value + 2);
    }
    store
        .commit_tasks(&[output(&parent, TaskOutcome::Complete)?])
        .await?;
    Ok(())
}

/// Out-of-order completion and cancellation count once; claim materializes submission order.
#[tokio::test]
async fn ordered_all_and_reclaim() -> Result<(), TestError> {
    let (store, node, parent) = fixture().await?;
    let children = [record(), record(), record()];
    let ids = children.each_ref().map(|child| child.id);
    store
        .commit_tasks(&[output(
            &parent,
            TaskOutcome::All {
                state: "42".into(),
                children: children.into_iter().map(write).collect(),
            },
        )?])
        .await?;
    let poll = store.claim_tasks(&node, &[claim()]).await?;
    let rows: Vec<_> = poll.lanes.into_iter().flat_map(|lane| lane.tasks).collect();
    for position in [2, 0] {
        let id = ids.get(position).ok_or_else(missing)?;
        let child = rows.iter().find(|row| row.id == *id).ok_or_else(missing)?;
        let result = output(
            child,
            TaskOutcome::CompleteWith {
                output: position.to_string(),
            },
        )?;
        store.commit_tasks(std::slice::from_ref(&result)).await?;
        assert_eq!(store.commit_tasks(&[result]).await?.rejected, vec![*id]);
    }
    assert_eq!(
        store.get_task(parent.id).await?.ok_or_else(missing)?.status,
        TaskStatus::Suspended
    );
    store
        .commit_tasks(&[TaskCommit::Cancel {
            task_id: *ids.get(1).ok_or_else(missing)?,
        }])
        .await?;
    let ready = store.get_task(parent.id).await?.ok_or_else(missing)?;
    assert_eq!(ready.status, TaskStatus::Pending);
    assert!(ready.resume_input.is_none());
    let resumed = next(&store, node).await?;
    verify_ordered_resume_and_reclaim(&store, node, resumed).await
}

/// Reclamation reuses the materialized envelope rather than collecting child results again.
async fn verify_ordered_resume_and_reclaim(
    store: &MemoryTaskStore,
    node: uuid::Uuid,
    resumed: TaskRecord,
) -> Result<(), TestError> {
    let values = resumed
        .resume_input::<Vec<Result<u32, crate::tasks::TaskFailure>>>()?
        .ok_or_else(missing)?
        .map_err(|e| TaskRuntimeError::TaskExecutionError(e.message().into()))?;
    assert_eq!(values.first(), Some(&Ok(0)));
    assert!(values.get(1).is_some_and(Result::is_err));
    assert_eq!(values.get(2), Some(&Ok(2)));
    store.return_claims(Some(node)).await?;
    let reclaimed = next(store, node).await?;
    assert_eq!(reclaimed.resume_input, resumed.resume_input);
    assert_eq!(reclaimed.attempts, resumed.attempts + 1);
    Ok(())
}

/// A cancelled parent cannot create children from an already-buffered stale spawn.
#[tokio::test]
async fn cancellation_fences_spawn() -> Result<(), TestError> {
    let (store, _, parent) = fixture().await?;
    let child = record();
    let child_id = child.id;
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: parent.id }])
        .await?;
    let result = store
        .commit_tasks(&[output(
            &parent,
            TaskOutcome::Spawn {
                state: "null".into(),
                child: write(child),
            },
        )?])
        .await?;
    assert_eq!(result.rejected, vec![parent.id]);
    assert!(store.get_task(child_id).await?.is_none());
    assert_eq!(
        store.get_task(parent.id).await?.ok_or_else(missing)?.status,
        TaskStatus::Failed
    );
    Ok(())
}
