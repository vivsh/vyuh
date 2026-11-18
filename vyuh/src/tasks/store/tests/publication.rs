use crate::tasks::{
    DEFAULT_TASK_LANE, LaneClaim, PublishResult, TaskCommit, TaskConf, TaskId, TaskKind, TaskLane,
    TaskLaneConf, TaskOutcome, TaskRecord, TaskRegistry, TaskRuntimeError, TaskStatus, TaskStore,
    TaskStoreConf, TaskWrite, submission::build_record,
};
use uuid::Uuid;

pub(super) const OTHER: TaskLane = TaskLane::new("unrelated");
const INPUT: &str = r#"{"Ok":42}"#;

pub(super) fn configuration() -> Result<TaskStoreConf, TaskRuntimeError> {
    let config = TaskConf::default().lane(TaskLaneConf::new(OTHER, 8));
    let mut conf = TaskRegistry::new().with_config(config)?.store_conf()?;
    conf.handlers.extend([
        ("topic_work".into(), TaskKind::Work),
        ("topic_flow".into(), TaskKind::Flow),
    ]);
    Ok(conf)
}

pub(super) fn request(lane: TaskLane, limit: usize) -> LaneClaim {
    LaneClaim {
        lane,
        limit,
        owner: None,
    }
}

pub(super) fn write(lane: TaskLane, index: usize) -> Result<TaskWrite, TaskRuntimeError> {
    let kind = if index % 2 == 0 {
        TaskKind::Work
    } else {
        TaskKind::Flow
    };
    Ok(TaskWrite {
        record: build_record(
            if kind == TaskKind::Work {
                "topic_work"
            } else {
                "topic_flow"
            },
            "null".into(),
            None,
            None,
            lane,
            kind,
        )?,
        initial_delay: None,
    })
}

pub(super) fn output(
    row: &TaskRecord,
    lane: TaskLane,
    outcome: TaskOutcome,
) -> Result<TaskCommit, String> {
    Ok(TaskCommit::Outcome {
        node_id: row.node_id.ok_or("fixture was not claimed")?,
        task_id: row.id,
        attempts: row.attempts,
        lane,
        owner_token: None,
        outcome,
    })
}

pub(super) fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

/// Installs waits through the real store claim and fenced commit paths.
pub(super) async fn prepare(
    store: &impl TaskStore,
    node: Uuid,
    count: usize,
    outcome: TaskOutcome,
) -> Result<Vec<TaskId>, String> {
    let ids = store
        .store_tasks(
            (0..count)
                .map(|index| write(DEFAULT_TASK_LANE, index))
                .collect::<Result<Vec<_>, _>>()
                .map_err(error)?,
        )
        .await
        .map_err(error)?
        .into_iter()
        .map(|r| r.queued_id())
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    let mut remaining = count;
    while remaining > 0 {
        let rows = next_page(store, node, remaining).await?;
        let commits = rows
            .iter()
            .map(|row| output(row, DEFAULT_TASK_LANE, outcome.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        store.commit_tasks(&commits).await.map_err(error)?;
        remaining = remaining
            .checked_sub(rows.len())
            .ok_or("unexpected fixture task")?;
    }
    Ok(ids)
}

/// Waits for ordinary polling eligibility during fixture setup, outside timed publication.
async fn next_page(
    store: &impl TaskStore,
    node: Uuid,
    limit: usize,
) -> Result<Vec<TaskRecord>, String> {
    for _ in 0..100 {
        let claims = store
            .claim_tasks(&node, &[request(DEFAULT_TASK_LANE, limit)])
            .await
            .map_err(error)?;
        let rows = claims
            .lanes
            .into_iter()
            .flat_map(|lane| lane.tasks)
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            return Ok(rows);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Err("fixture tasks did not become ready".into())
}

pub(super) fn wait_outcome(topic: &str) -> TaskOutcome {
    TaskOutcome::Wait {
        topic: topic.into(),
        state: "7".into(),
    }
}

/// The same contract drives memory and each SQL backend.
pub(super) async fn contract(store: &impl TaskStore) -> Result<(), String> {
    store.initialize().await.map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?.resumed,
        0
    );
    assert!(store.publish(" \u{2003}", INPUT).await.is_err());
    let ids = prepare(store, node, 65, wait_outcome("Topic")).await?;
    let lower = prepare(store, node, 1, wait_outcome("topic")).await?;
    check_caps(store, node).await?;
    let direct = prepare(store, node, 1, TaskOutcome::Suspend { state: "8".into() }).await?;
    let id = *ids.first().ok_or("no wait id")?;
    assert!(!store.resume(id, INPUT.into()).await.map_err(error)?);
    let too_large = format!("{{\"Ok\":\"{}\"}}", "x".repeat(32768));
    assert!(matches!(
        store.publish("Topic", &too_large).await,
        Err(TaskRuntimeError::ResultTooLarge { .. })
    ));
    assert_eq!(
        store.publish("Topic ", INPUT).await.map_err(error)?.resumed,
        0
    );
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?,
        PublishResult {
            resumed: 64,
            more: true
        }
    );
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?,
        PublishResult {
            resumed: 1,
            more: false
        }
    );
    assert_eq!(
        store.publish("Topic", INPUT).await.map_err(error)?.resumed,
        0
    );
    check_delivery(
        store,
        &ids,
        lower.first().copied().ok_or("no lowercase id")?,
    )
    .await?;
    assert!(
        store
            .resume(*direct.first().ok_or("no direct id")?, INPUT.into())
            .await
            .map_err(error)?
    );
    Ok(())
}

/// Checks that an exactly full result does not advertise a missing sentinel.
async fn check_caps(store: &impl TaskStore, node: Uuid) -> Result<(), String> {
    for (topic, count) in [("cap:63", 63), ("cap:64", 64)] {
        let ids = prepare(store, node, count, wait_outcome(topic)).await?;
        let result = store.publish(topic, INPUT).await.map_err(error)?;
        assert_eq!(
            result,
            PublishResult {
                resumed: count as u64,
                more: false
            }
        );
        for batch in ids.chunks(64) {
            let cancel = batch
                .iter()
                .map(|&task_id| TaskCommit::Cancel { task_id })
                .collect::<Vec<_>>();
            store.commit_tasks(&cancel).await.map_err(error)?;
        }
    }
    Ok(())
}

/// Checks that delivery preserves checkpoints and does not affect a different topic.
async fn check_delivery(
    store: &impl TaskStore,
    ids: &[TaskId],
    unrelated: TaskId,
) -> Result<(), String> {
    for &id in ids {
        let row = store
            .get_task(id)
            .await
            .map_err(error)?
            .ok_or("missing resumed task")?;
        assert_eq!(row.status, TaskStatus::Pending);
        assert_eq!(row.resume_input.as_deref(), Some(INPUT));
        assert_eq!(row.state.as_deref(), Some("7"));
        assert_eq!(row.attempts, 1);
        assert_eq!(row.step_attempts, 0);
    }
    assert_eq!(
        store
            .get_task(unrelated)
            .await
            .map_err(error)?
            .ok_or("missing unrelated task")?
            .status,
        TaskStatus::Suspended
    );
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: unrelated }])
        .await
        .map_err(error)?;
    assert_eq!(
        store.publish("topic", INPUT).await.map_err(error)?.resumed,
        0
    );
    Ok(())
}

/// Memory stores use the same topic, cancellation, and bounded resume contract.
#[tokio::test]
async fn memory_publication() -> Result<(), String> {
    let store = super::MemoryTaskStore::new(1024, configuration().map_err(error)?);
    contract(&store).await
}
