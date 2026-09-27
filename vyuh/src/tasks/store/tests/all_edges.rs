use super::*;

/// Covers empty/maximum fan-out and bounded outer failures on every store backend.
pub(crate) async fn limits_contract(
    store: &impl AbstractTaskStore,
) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    for (size, payload) in [
        (0, "null".to_owned()),
        (1, "null".into()),
        (256, "0".into()),
        (1, serde_json::to_string(&"x".repeat(32_750))?),
        (1, serde_json::to_string(&"x".repeat(32_751))?),
    ] {
        let (id, _) = start(store, size, false).await?;
        loop {
            let poll = store.claim_tasks("children", &[claim()]).await?;
            let children = poll
                .lanes
                .iter()
                .flat_map(|lane| &lane.tasks)
                .filter(|task| task.id != id)
                .map(|task| {
                    commit(
                        task.id,
                        TaskOutcome::CompleteWith {
                            output: payload.clone(),
                        },
                    )
                })
                .collect::<Vec<_>>();
            if children.is_empty() {
                break;
            }
            store.commit_outcomes("children", &children).await?;
        }
        let parent = store.get_task(id).await?.ok_or_else(missing)?;
        assert_eq!(parent.status, TaskStatus::Running);
        let input: Result<
            Vec<Result<serde_json::Value, crate::tasks::TaskFailure>>,
            crate::tasks::TaskFailure,
        > = serde_json::from_str(parent.resume_input.as_deref().ok_or_else(missing)?)?;
        if payload.len() > 32_752 {
            assert!(input.is_err());
        } else {
            assert_eq!(input.map(|items| items.len()).ok(), Some(size));
        }
        store
            .commit_outcomes("children", &[commit(id, TaskOutcome::Complete)])
            .await?;
    }
    Ok(())
}

/// Retry and suspension do not satisfy a wait; failure and cancellation do.
pub(crate) async fn mixed_contract(store: &impl AbstractTaskStore) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let (id, children) = start(store, 3, false).await?;
    store.claim_tasks("children", &[claim()]).await?;
    let [a, b, c] = children.as_slice() else {
        return Err(missing());
    };
    store
        .commit_outcomes(
            "children",
            &[
                commit(*a, TaskOutcome::Complete),
                commit(*b, TaskOutcome::Suspend { state: "1".into() }),
                commit(*c, TaskOutcome::retry("later")),
            ],
        )
        .await?;
    assert_eq!(
        store.get_task(id).await?.ok_or_else(missing)?.status,
        TaskStatus::Suspended
    );
    assert!(store.resume(*b, "{\"Ok\":2}".into()).await?);
    assert!(store.cancel(*c).await?);
    store.claim_tasks("children", &[claim()]).await?;
    store
        .commit_outcomes("children", &[commit(*b, TaskOutcome::fail("failed"))])
        .await?;
    let poll = store.claim_tasks("parent", &[claim()]).await?;
    let parent = poll
        .lanes
        .iter()
        .flat_map(|lane| &lane.tasks)
        .find(|task| task.id == id)
        .ok_or_else(missing)?;
    let input: Result<Vec<Result<(), crate::tasks::TaskFailure>>, crate::tasks::TaskFailure> =
        serde_json::from_str(parent.resume_input.as_deref().ok_or_else(missing)?)?;
    let items = input.map_err(|_| missing())?;
    assert!(matches!(items.as_slice(), [Ok(()), Err(_), Err(_)]));
    store
        .commit_outcomes("parent", &[commit(id, TaskOutcome::Complete)])
        .await?;
    Ok(())
}

/// Group rejection removes reservations for uninserted siblings and preserves independent keys.
pub(crate) async fn conflicts_contract(
    store: &impl AbstractTaskStore,
) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let mut independent = record();
    independent.idempotency_key = Some("occupied".into());
    independent.idempotency_fingerprint = Some("fingerprint".into());
    independent.status = TaskStatus::Suspended;
    store.store_tasks(vec![write(independent.clone())]).await?;
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("parent", &[claim()]).await?;
    let mut fresh = record();
    fresh.idempotency_key = Some("fresh".into());
    fresh.idempotency_fingerprint = Some("fingerprint".into());
    let mut conflict = independent.clone();
    conflict.id = record().id;
    conflict.status = TaskStatus::Pending;
    store
        .commit_outcomes(
            "parent",
            &[commit(
                id,
                TaskOutcome::All {
                    state: "0".into(),
                    children: vec![write(fresh.clone()), write(conflict)],
                },
            )],
        )
        .await?;
    assert_eq!(
        store.get_task(id).await?.ok_or_else(missing)?.status,
        TaskStatus::Failed
    );
    assert!(store.get_task(fresh.id).await?.is_none());
    assert!(matches!(
        store.store_tasks(vec![write(fresh)]).await?.as_slice(),
        [TaskReceipt::Queued(_)]
    ));
    Ok(())
}

/// Nested joins preserve root lineage and do not deliver an intermediate checkpoint.
pub(crate) async fn nested_contract(
    store: &impl AbstractTaskStore,
) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let (root, children) = start(store, 1, true).await?;
    let child = *children.first().ok_or_else(missing)?;
    store.claim_tasks("nested", &[claim()]).await?;
    let leaf = record();
    let leaf_id = leaf.id;
    store
        .commit_outcomes(
            "nested",
            &[commit(
                child,
                TaskOutcome::All {
                    state: "1".into(),
                    children: vec![write(leaf)],
                },
            )],
        )
        .await?;
    assert_eq!(
        store.get_task(leaf_id).await?.ok_or_else(missing)?.root_id,
        Some(root)
    );
    store.claim_tasks("leaf", &[claim()]).await?;
    store
        .commit_outcomes("leaf", &[commit(leaf_id, TaskOutcome::Complete)])
        .await?;
    let poll = store.claim_tasks("nested", &[claim()]).await?;
    assert!(
        poll.lanes
            .iter()
            .flat_map(|lane| &lane.tasks)
            .all(|task| task.id != root)
    );
    store
        .commit_outcomes(
            "nested",
            &[commit(
                child,
                TaskOutcome::CompleteWith {
                    output: "42".into(),
                },
            )],
        )
        .await?;
    store.claim_tasks("root", &[claim()]).await?;
    assert_eq!(
        store
            .get_task(root)
            .await?
            .and_then(|task| task.resume_input)
            .as_deref(),
        Some("{\"Ok\":[{\"Ok\":42}]}")
    );
    Ok(())
}

/// Creates and accepts a group, leaving its members eligible only for a later claim.
pub(super) async fn start(
    store: &impl AbstractTaskStore,
    size: usize,
    flow: bool,
) -> Result<(TaskId, Vec<TaskId>), TaskRuntimeError> {
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("parent", &[claim()]).await?;
    let children = (0..size)
        .map(|_| write(if flow { flow_record() } else { record() }))
        .collect::<Vec<_>>();
    let ids = children.iter().map(|child| child.record.id).collect();
    store
        .commit_outcomes(
            "parent",
            &[commit(
                id,
                TaskOutcome::All {
                    state: "0".into(),
                    children,
                },
            )],
        )
        .await?;
    Ok((id, ids))
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("Missing join fixture".into())
}

/// Runs independent edge fixtures through memory storage without shared state between scenarios.
#[tokio::test]
async fn memory_join_edges() -> Result<(), TaskRuntimeError> {
    limits_contract(&MemoryTaskStore::new(32)).await?;
    mixed_contract(&MemoryTaskStore::new(32)).await?;
    conflicts_contract(&MemoryTaskStore::new(32)).await?;
    nested_contract(&MemoryTaskStore::new(32)).await?;
    cancellation_contract(&MemoryTaskStore::new(32)).await?;
    invalid_group_contract(&MemoryTaskStore::new(32)).await
}

/// Duplicate keys and malformed prepared children fail only their parent before reserving any key.
pub(crate) async fn invalid_group_contract(
    store: &impl AbstractTaskStore,
) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    for invalid in ["duplicate", "fingerprint", "handler"] {
        let parent = flow_record();
        let other = record();
        let (parent_id, other_id) = (parent.id, other.id);
        store.store_tasks(vec![write(parent), write(other)]).await?;
        store.claim_tasks("owner", &[claim()]).await?;
        let mut a = record();
        a.idempotency_key = Some(invalid.into());
        a.idempotency_fingerprint = Some("fingerprint".into());
        let mut b = a.clone();
        b.id = record().id;
        if invalid == "fingerprint" {
            a.idempotency_fingerprint = None;
        }
        if invalid == "handler" {
            a.name = "unregistered".into();
        }
        let child = a.id;
        store
            .commit_outcomes(
                "owner",
                &[
                    commit(
                        parent_id,
                        TaskOutcome::All {
                            state: "0".into(),
                            children: vec![write(a), write(b)],
                        },
                    ),
                    commit(other_id, TaskOutcome::Complete),
                ],
            )
            .await?;
        assert_eq!(
            store.get_task(parent_id).await?.ok_or_else(missing)?.status,
            TaskStatus::Failed
        );
        assert_eq!(
            store.get_task(other_id).await?.ok_or_else(missing)?.status,
            TaskStatus::Succeeded
        );
        assert!(store.get_task(child).await?.is_none());
    }
    Ok(())
}

/// Cancelling either a waiting or satisfied parent prevents delivery and result collection.
pub(crate) async fn cancellation_contract(
    store: &impl AbstractTaskStore,
) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    for satisfied in [false, true] {
        let (parent, children) = start(store, 2, false).await?;
        store.claim_tasks("children", &[claim()]).await?;
        let outcomes = children
            .iter()
            .map(|id| commit(*id, TaskOutcome::Complete))
            .collect::<Vec<_>>();
        if satisfied {
            store.commit_outcomes("children", &outcomes).await?;
        }
        assert!(store.cancel(parent).await?);
        let turn = store.tick("children", &[claim()], &outcomes, &[]).await?;
        assert!(
            turn.poll
                .lanes
                .iter()
                .flat_map(|lane| &lane.tasks)
                .all(|task| task.id != parent)
        );
        let parent = store.get_task(parent).await?.ok_or_else(missing)?;
        assert_eq!(parent.status, TaskStatus::Failed);
        assert!(parent.cancelled);
        assert!(parent.resume_input.is_none());
        assert!(
            parent
                .last_result
                .as_deref()
                .is_some_and(|result| result.contains("Task cancelled"))
        );
    }
    Ok(())
}
