use super::*;
use crate::tasks::store::memory::tests::{claim, conf, flow_record, record, write};
use crate::tasks::{TaskCommit, TaskOutcome, TaskRecord, TaskStatus, TaskStore};

/// SQLite shares the partial-All local delivery contract with the memory store.
#[tokio::test]
async fn partial_all_delivery() -> Result<(), String> {
    let mut store = fresh_store().await?;
    store.runtime_conf = std::sync::Arc::new(conf());
    crate::tasks::store::memory::tests::delivery::local_all_contract(&store)
        .await
        .map_err(|error| error.to_string())
}
/// Work's exhausted global rate does not hide ready Flow rows or delay a future Flow hint.
#[tokio::test]
async fn flow_ignores_work_rate() -> Result<(), String> {
    let (store, node) = rate_limited_store().await?;
    let flow = flow_record();
    store
        .store_tasks(vec![write(record()), write(record()), write(flow.clone())])
        .await
        .map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[claim()])
        .await
        .map_err(|e| e.to_string())?;
    let tasks: Vec<_> = poll.lanes.into_iter().flat_map(|lane| lane.tasks).collect();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks.first().ok_or("missing Flow")?.id, flow.id);
    let mut delayed = write(flow_record());
    delayed.initial_delay = Some(std::time::Duration::from_secs(5));
    store
        .store_tasks(vec![delayed])
        .await
        .map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[claim()])
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        poll.lanes
            .first()
            .ok_or("missing hint")?
            .next_wake_in
            .is_some_and(|wait| wait <= std::time::Duration::from_secs(5))
    );
    Ok(())
}

/// Initializes the ordinary mixed-kind lane and spends its only Work claim permit.
async fn rate_limited_store() -> Result<(DbTaskStore, uuid::Uuid), String> {
    let mut store = fresh_store().await?;
    let mut configuration = conf();
    configuration.lanes = vec![
        crate::tasks::TaskLaneConf::new(crate::tasks::DEFAULT_TASK_LANE, 8).global_rate_limit(
            crate::tasks::TaskRate::new(1, std::time::Duration::from_secs(3600)),
        ),
    ];
    store.runtime_conf = std::sync::Arc::new(configuration);
    store.initialize().await.map_err(|e| e.to_string())?;
    let node = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    store
        .store_tasks(vec![write(record())])
        .await
        .map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[claim()])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        poll.lanes
            .iter()
            .map(|lane| lane.tasks.len())
            .sum::<usize>(),
        1
    );
    Ok((store, node))
}

/// SQLite exercises atomic direct child delivery, cached scalar resume and lost acknowledgements.
#[tokio::test]
async fn acknowledged_flow_pipeline() -> Result<(), String> {
    let mut store = fresh_store().await?;
    store.runtime_conf = std::sync::Arc::new(conf());
    store.initialize().await.map_err(|e| e.to_string())?;
    let node = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    let (_, rows) = store
        .store_admitted(vec![write(flow_record())], &node, 1)
        .await
        .map_err(|e| e.to_string())?;
    let parent = rows.first().ok_or("missing parent")?;
    let result = store
        .commit_admitted(
            &[output(
                parent,
                TaskOutcome::Spawn {
                    state: "42".into(),
                    child: write(record()),
                },
            )?],
            &node,
            1,
            &[],
        )
        .await
        .map_err(|e| e.to_string())?;
    let child = result
        .tasks
        .iter()
        .find(|task| task.kind == crate::tasks::TaskKind::Work)
        .ok_or("missing direct child")?;
    let checkpoint = result
        .tasks
        .iter()
        .find(|task| task.id == parent.id)
        .ok_or("missing checkpoint")?;
    assert_eq!(checkpoint.node_id, None);
    assert_eq!(checkpoint.status, TaskStatus::Suspended);
    assert_no_ready(&store, node).await?;
    cached_completion(&store, node, child, checkpoint.clone()).await
}

/// Ownerless suspended checkpoints are not runnable before resumption.
async fn assert_no_ready(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    assert!(
        store
            .claim_tasks(&node, &[claim()])
            .await
            .map_err(|e| e.to_string())?
            .lanes
            .iter()
            .all(|lane| lane.tasks.is_empty())
    );
    Ok(())
}

/// Reconciliation returns the exact committed attempt, never reapplies the child result.
async fn cached_completion(
    store: &DbTaskStore,
    node: uuid::Uuid,
    child: &TaskRecord,
    checkpoint: std::sync::Arc<TaskRecord>,
) -> Result<(), String> {
    let commit = output(child, TaskOutcome::CompleteWith { output: "7".into() })?;
    let result = store
        .commit_admitted(
            std::slice::from_ref(&commit),
            &node,
            1,
            &[checkpoint.clone()],
        )
        .await
        .map_err(|e| e.to_string())?;
    let resumed = result
        .tasks
        .iter()
        .find(|task| task.id == checkpoint.id)
        .ok_or("missing cached resume")?;
    assert_eq!(
        resumed.resume_input::<i32>().map_err(|e| e.to_string())?,
        Some(Ok(7))
    );
    assert_eq!(
        (resumed.status, resumed.attempts, resumed.step_attempts),
        (TaskStatus::Running, 2, 1)
    );
    assert_eq!(resumed.state.as_deref(), Some("42"));
    assert_recovered_delivery(store, node, child, commit, result).await
}

/// Lost acknowledgements reuse the exact attempt and identical persisted result bytes.
async fn assert_recovered_delivery(
    store: &DbTaskStore,
    node: uuid::Uuid,
    child: &TaskRecord,
    commit: TaskCommit,
    result: crate::tasks::CommitResult,
) -> Result<(), String> {
    let resumed = result.tasks.first().ok_or("missing resumed snapshot")?;
    let recovered = store
        .recover_outputs(&node, &[commit], result.clone())
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(recovered.tasks.len(), 1);
    assert_eq!(
        recovered
            .tasks
            .first()
            .ok_or("missing recovered snapshot")?
            .attempts,
        2
    );
    assert_eq!(
        store
            .get_task(child.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing completed child")?
            .last_result,
        resumed.resume_input
    );
    Ok(())
}

/// Both nodes see the same future hint for an ownerless sleeping Flow.
#[tokio::test]
async fn ownerless_sleep_and_hints() -> Result<(), String> {
    let mut store = fresh_store().await?;
    store.runtime_conf = std::sync::Arc::new(conf());
    store.initialize().await.map_err(|e| e.to_string())?;
    let node = uuid::Uuid::now_v7();
    let peer = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    store
        .register_node(&peer)
        .await
        .map_err(|e| e.to_string())?;
    let (_, rows) = store
        .store_admitted(vec![write(flow_record())], &node, 1)
        .await
        .map_err(|e| e.to_string())?;
    let parent = rows.first().ok_or("missing parent")?;
    store
        .commit_tasks(&[output(
            parent,
            TaskOutcome::Sleep {
                state: "null".into(),
                delay: std::time::Duration::from_secs(60),
            },
        )?])
        .await
        .map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[claim()])
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        poll.lanes
            .first()
            .ok_or("missing lane")?
            .next_wake_in
            .is_some()
    );
    assert!(
        store
            .claim_tasks(&peer, &[claim()])
            .await
            .map_err(|e| e.to_string())?
            .lanes
            .iter()
            .all(|lane| lane.tasks.is_empty())
    );
    store
        .return_claims(Some(node))
        .await
        .map_err(|e| e.to_string())?;
    let task = store
        .get_task(parent.id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing parent")?;
    assert_eq!(task.node_id, None);
    assert_eq!(task.status, TaskStatus::Pending);
    assert!(task.ready_at.is_some_and(|at| at > chrono::Utc::now()));
    Ok(())
}

/// Constructs an execution-fenced outcome from an immutable store snapshot.
fn output(task: &TaskRecord, outcome: TaskOutcome) -> Result<TaskCommit, String> {
    Ok(TaskCommit::Outcome {
        task_id: task.id,
        node_id: task.node_id.ok_or("missing node")?,
        attempts: task.attempts,
        lane: crate::tasks::DEFAULT_TASK_LANE,
        outcome,
        owner_token: None,
    })
}
