use super::*;

#[path = "all_edges.rs"]
mod edges;
#[path = "all_recovery.rs"]
mod recovery;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) use edges::{
    cancellation_contract, conflicts_contract, invalid_group_contract, limits_contract,
    mixed_contract, nested_contract,
};

/// Proves durable ordering, counter-only completion, and later-claim materialization.
#[tokio::test]
async fn ordered_memory_join() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    ordered_contract(&store).await
}

/// Shared backend proof uses only the immutable public store contract.
pub(crate) async fn ordered_contract(
    store: &impl AbstractTaskStore,
) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("parent", &[claim()]).await?;
    let children = vec![write(record()), write(record()), write(record())];
    let ids = children
        .iter()
        .map(|child| child.record.id)
        .collect::<Vec<_>>();
    let tick = store
        .tick(
            "parent",
            &[claim()],
            &[commit(
                id,
                TaskOutcome::All {
                    state: "7".into(),
                    children,
                },
            )],
            &[],
        )
        .await?;
    assert!(tick.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
    let tasks = store.claim_tasks("children", &[claim()]).await?;
    assert_eq!(tasks.lanes.first().map(|lane| lane.tasks.len()), Some(3));
    for (position, child) in ids.iter().enumerate().rev() {
        let task = store
            .get_task(*child)
            .await?
            .ok_or_else(|| TaskRuntimeError::TaskExecutionError("Missing fixture task".into()))?;
        assert_eq!(task.parent_id, Some(id));
        assert_eq!(task.root_id, Some(id));
        let outcome = TaskOutcome::CompleteWith {
            output: position.to_string(),
        };
        let tick = store
            .tick(
                "children",
                &[claim()],
                &[commit(*child, outcome.clone())],
                &[],
            )
            .await?;
        assert!(tick.poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
        store
            .commit_outcomes("children", &[commit(*child, outcome)])
            .await?;
        let parent = store
            .get_task(id)
            .await?
            .ok_or_else(|| TaskRuntimeError::TaskExecutionError("Missing fixture task".into()))?;
        assert_eq!(
            parent.status,
            if position == 0 {
                TaskStatus::Pending
            } else {
                TaskStatus::Suspended
            }
        );
        assert!(parent.resume_input.is_none());
    }
    let poll = store.claim_tasks("resumed", &[claim()]).await?;
    let parent = poll
        .lanes
        .iter()
        .flat_map(|lane| &lane.tasks)
        .find(|task| task.id == id)
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("Missing fixture task".into()))?;
    assert_eq!(
        parent.resume_input.as_deref(),
        Some("{\"Ok\":[{\"Ok\":0},{\"Ok\":1},{\"Ok\":2}]}")
    );
    assert_eq!(parent.state.as_deref(), Some("7"));
    assert_eq!(
        store.get_task(id).await?.and_then(|task| task.resume_input),
        parent.resume_input
    );
    Ok(())
}
