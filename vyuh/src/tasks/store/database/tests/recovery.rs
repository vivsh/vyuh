use super::*;
use crate::tasks::TaskStatus;
use crate::tasks::{TaskCommit, TaskId, TaskOutcome, TaskPoll};

/// Creates committed claim evidence; the caller deliberately treats its acknowledgement as lost.
async fn claimed() -> Result<(DbTaskStore, uuid::Uuid, TaskPoll), String> {
    let store = fresh_store().await?;
    let mut conf = (*store.runtime_conf).clone();
    conf.handlers.extend([
        ("fixture".into(), crate::tasks::TaskKind::Work),
        ("child".into(), crate::tasks::TaskKind::Work),
        ("flow".into(), crate::tasks::TaskKind::Flow),
    ]);
    let store = configured(store, conf);
    let node = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    let id = insert_task(&store, 0, None).await?;
    db::sqlx::query("UPDATE vyuh_tasks SET cancelled = false, ready_at = NULL, waiting_children = NULL WHERE id = ?")
        .bind(id).execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[request()])
        .await
        .map_err(|e| e.to_string())?;
    Ok((store, node, poll))
}

/// Rebuilds a fixture with construction-time policy instead of runtime configuration mutation.
fn configured(store: DbTaskStore, conf: crate::tasks::TaskStoreConf) -> DbTaskStore {
    DbTaskStore::new(
        store.pool.as_sqlx().clone(),
        store.commit_batch_size,
        store.lease_duration,
        conf,
    )
}

fn request() -> crate::tasks::LaneClaim {
    crate::tasks::LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit: 32,
        owner: None,
    }
}

/// Acquires a configured lane token and one task through the ordinary store path.
async fn owned_claim()
-> Result<(DbTaskStore, uuid::Uuid, crate::tasks::LaneClaim, TaskPoll), String> {
    let (store, node, _) = claimed().await?;
    store
        .return_claims(Some(node))
        .await
        .map_err(|e| e.to_string())?;
    let mut conf = (*store.runtime_conf).clone();
    conf.lanes = vec![
        crate::tasks::TaskLaneConf::new(request().lane, 1).lock(crate::tasks::TaskLaneLock::new(1)),
    ];
    let store = configured(store, conf);
    store.initialize().await.map_err(|e| e.to_string())?;
    let mut claim = request();
    claim.owner = Some(crate::tasks::LaneOwnerRequest {
        token: None,
        quiescent: true,
        allow_claim: true,
        completed_work: false,
        hook: None,
    });
    let poll = store
        .claim_tasks(&node, std::slice::from_ref(&claim))
        .await
        .map_err(|e| e.to_string())?;
    Ok((store, node, claim, poll))
}

/// A recovered acquisition publishes only the retained token, without charging another attempt.
#[tokio::test]
async fn sqlite_recovery_owned_claim() -> Result<(), String> {
    let (store, node, claim, poll) = owned_claim().await?;
    let expected = commit(&poll)?;
    let token = poll
        .lanes
        .first()
        .and_then(|lane| lane.owner.as_ref())
        .and_then(|owner| owner.token.clone())
        .ok_or("missing token")?;
    let recovered = store
        .recover_claims(&node, &[claim], poll)
        .await
        .map_err(|e| e.to_string())?;
    let lane = recovered.lanes.first().ok_or("missing lane")?;
    assert_eq!(
        lane.owner.as_ref().and_then(|owner| owner.token.as_ref()),
        Some(&token)
    );
    assert_eq!(commit(&recovered)?.attempts(), expected.attempts());
    assert_eq!(commit(&recovered)?.task_id(), expected.task_id());
    Ok(())
}

/// Lost lane ownership relinquishes claims durably; heartbeats cannot leave them stranded.
#[tokio::test]
async fn sqlite_recovery_lost_lane() -> Result<(), String> {
    let (store, node, claim, poll) = owned_claim().await?;
    let expected = commit(&poll)?;
    db::sqlx::query("UPDATE vyuh_task_lane_locks SET leased_until = '2000-01-01 00:00:00'")
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    let recovered = store
        .recover_claims(&node, &[claim], poll)
        .await
        .map_err(|e| e.to_string())?;
    assert!(recovered.lanes.iter().all(|lane| lane.tasks.is_empty()));
    assert!(recovered.lanes.iter().all(|lane| {
        lane.owner
            .as_ref()
            .is_some_and(|owner| owner.token.is_none())
    }));
    let row = store
        .get_task(expected.task_id())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing")?;
    assert_eq!(row.node_id, None);
    assert_eq!(Some(row.attempts), expected.attempts());
    assert_eq!(row.status, TaskStatus::Running);
    Ok(())
}

/// A same-node UUID is insufficient when the durable lane token no longer matches the request.
#[tokio::test]
async fn sqlite_recovery_changed_token() -> Result<(), String> {
    let (store, node, claim, poll) = owned_claim().await?;
    let expected = commit(&poll)?;
    db::sqlx::query("UPDATE vyuh_task_lane_locks SET owner_token = 'replacement'")
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    let recovered = store
        .recover_claims(&node, &[claim], poll)
        .await
        .map_err(|e| e.to_string())?;
    assert!(recovered.lanes.iter().all(|lane| lane.tasks.is_empty()));
    let row = store
        .get_task(expected.task_id())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing")?;
    assert_eq!(row.node_id, None);
    assert_eq!(Some(row.attempts), expected.attempts());
    Ok(())
}

fn commit(poll: &TaskPoll) -> Result<TaskCommit, String> {
    let task = poll
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or("missing task")?;
    Ok(TaskCommit::Outcome {
        node_id: task.node_id.ok_or("missing node")?,
        task_id: task.id,
        attempts: task.attempts,
        lane: request().lane,
        outcome: TaskOutcome::Complete,
        owner_token: None,
    })
}

/// A mixed batch atomically satisfies an all wait; replay cannot decrement or deliver twice.
#[tokio::test]
async fn sqlite_mixed_cancellation_commit() -> Result<(), String> {
    let (store, node, poll) = claimed().await?;
    let parent = group(&store, &poll, 2).await?;
    store
        .commit_tasks(std::slice::from_ref(&parent))
        .await
        .map_err(|e| e.to_string())?;
    let children = store
        .claim_tasks(&node, &[request()])
        .await
        .map_err(|e| e.to_string())?;
    let first = commit(&children)?;
    let second = children
        .lanes
        .first()
        .and_then(|lane| lane.tasks.get(1))
        .ok_or("missing second child")?
        .id;
    let batch = [first, TaskCommit::Cancel { task_id: second }];
    assert!(
        store
            .commit_tasks(&batch)
            .await
            .map_err(|e| e.to_string())?
            .rejected
            .is_empty()
    );
    assert_cancelled_join(&store, second, parent.task_id()).await?;
    assert_eq!(
        store
            .commit_tasks(&batch)
            .await
            .map_err(|e| e.to_string())?
            .rejected
            .len(),
        2
    );
    let resumed = store
        .claim_tasks(&node, &[request()])
        .await
        .map_err(|e| e.to_string())?;
    assert_join_resume(&resumed, parent.task_id())
}

/// A later claim reconstructs the ordered success and cancellation results.
fn assert_join_resume(resumed: &TaskPoll, parent: TaskId) -> Result<(), String> {
    let row = resumed
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or("missing parent claim")?;
    assert_eq!(row.id, parent);
    let input = row.resume_input.as_deref().ok_or("missing results")?;
    assert!(input.contains("Task cancelled"));
    assert!(input.contains("\"Ok\":null"));
    Ok(())
}

/// Failure while delivering cancellation rolls back both child finalization and the join counter.
#[tokio::test]
async fn sqlite_cancellation_delivery_rollback() -> Result<(), String> {
    let (store, node, poll) = claimed().await?;
    let parent = group(&store, &poll, 1).await?;
    store
        .commit_tasks(&[parent])
        .await
        .map_err(|e| e.to_string())?;
    let children = store
        .claim_tasks(&node, &[request()])
        .await
        .map_err(|e| e.to_string())?;
    let child = children
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or("missing child")?;
    db::sqlx::query("CREATE TRIGGER reject_delivery BEFORE UPDATE ON vyuh_tasks WHEN OLD.remaining_completions > 0 BEGIN SELECT RAISE(ABORT, 'injected parent update failure'); END")
        .execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    let cancel = [TaskCommit::Cancel { task_id: child.id }];
    assert!(store.commit_tasks(&cancel).await.is_err());
    let unchanged = store
        .get_task(child.id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing child")?;
    assert_eq!(format!("{child:?}"), format!("{unchanged:?}"));
    db::sqlx::query("DROP TRIGGER reject_delivery")
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    store
        .commit_tasks(&cancel)
        .await
        .map_err(|e| e.to_string())?;
    assert_cancelled_join(&store, child.id, child.parent_id.ok_or("missing parent")?).await
}

/// Checks terminal cancellation and deferred join-result materialization before the parent claim.
async fn assert_cancelled_join(
    store: &DbTaskStore,
    child: TaskId,
    parent: TaskId,
) -> Result<(), String> {
    let cancelled = store
        .get_task(child)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing child")?;
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.status, TaskStatus::Failed);
    assert_eq!(cancelled.node_id, None);
    let ready = store
        .get_task(parent)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing parent")?;
    assert_eq!(ready.status, TaskStatus::Pending);
    assert!(ready.resume_input.is_none());
    Ok(())
}

/// Builds store-owned child requests without exposing lineage mutation to applications.
async fn group(store: &DbTaskStore, poll: &TaskPoll, count: usize) -> Result<TaskCommit, String> {
    let mut parent = commit(poll)?;
    db::sqlx::query("UPDATE vyuh_tasks SET kind = 1, name = 'flow' WHERE id = ?")
        .bind(parent.task_id().into_uuid())
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    let source = poll
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or("missing source")?;
    let children = (0..count)
        .map(|_| {
            let mut record = source.clone();
            record.id = TaskId::new(uuid::Uuid::now_v7());
            record.kind = crate::tasks::TaskKind::Work;
            record.name = "child".into();
            record.status = TaskStatus::Pending;
            record.node_id = None;
            record.attempts = 0;
            record.step_attempts = 0;
            record.state = None;
            record.resume_input = None;
            record.last_result = None;
            crate::tasks::TaskWrite {
                record,
                ignore_conflicts: false,
                initial_delay: None,
            }
        })
        .collect();
    if let TaskCommit::Outcome { outcome, .. } = &mut parent {
        *outcome = TaskOutcome::All {
            state: "{\"step\":2}".into(),
            children,
        };
    }
    Ok(parent)
}

/// Replaying both fan-out and terminal child flushes cannot duplicate children or decrement twice.
#[tokio::test]
async fn sqlite_recovery_all_once() -> Result<(), String> {
    let (store, node, poll) = claimed().await?;
    let parent = group(&store, &poll, 2).await?;
    let result = store
        .commit_tasks(std::slice::from_ref(&parent))
        .await
        .map_err(|e| e.to_string())?;
    store
        .recover_outputs(&node, std::slice::from_ref(&parent), result)
        .await
        .map_err(|e| e.to_string())?;
    let children = store
        .claim_tasks(&node, &[request()])
        .await
        .map_err(|e| e.to_string())?;
    let lane = children.lanes.first().ok_or("missing lane")?;
    assert_eq!(lane.tasks.len(), 2);
    let first = commit(&children)?;
    let result = store
        .commit_tasks(std::slice::from_ref(&first))
        .await
        .map_err(|e| e.to_string())?;
    store
        .recover_outputs(&node, std::slice::from_ref(&first), result)
        .await
        .map_err(|e| e.to_string())?;
    let count: i32 =
        db::sqlx::query_scalar("SELECT remaining_completions FROM vyuh_tasks WHERE id = ?")
            .bind(parent.task_id().into_uuid())
            .fetch_one(store.pool.as_sqlx())
            .await
            .map_err(|e| e.to_string())?;
    assert_eq!(count, 1);
    assert_eq!(
        store
            .get_task(parent.task_id())
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing parent")?
            .status,
        TaskStatus::Suspended
    );
    Ok(())
}

/// A child-insert failure rolls back the parent checkpoint; a later retry can create the whole group.
#[tokio::test]
async fn sqlite_recovery_group_rollback() -> Result<(), String> {
    let (store, _node, poll) = claimed().await?;
    let parent = group(&store, &poll, 2).await?;
    let before = persisted_bytes(&store, parent.task_id().into_uuid()).await?;
    db::sqlx::query("CREATE TRIGGER reject_child BEFORE INSERT ON vyuh_tasks WHEN NEW.parent_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'injected child insert failure'); END")
        .execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    let error = store
        .commit_tasks(std::slice::from_ref(&parent))
        .await
        .err()
        .ok_or("expected failure")?;
    assert!(matches!(
        error,
        crate::tasks::store::StoreFailure::BeforeCommit(_)
    ));
    assert_eq!(
        before,
        persisted_bytes(&store, parent.task_id().into_uuid()).await?
    );
    db::sqlx::query("DROP TRIGGER reject_child")
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    store
        .commit_tasks(std::slice::from_ref(&parent))
        .await
        .map_err(|e| e.to_string())?;
    let count: i64 = db::sqlx::query_scalar("SELECT COUNT(*) FROM vyuh_tasks WHERE parent_id = ?")
        .bind(parent.task_id().into_uuid())
        .fetch_one(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(count, 2);
    Ok(())
}

/// Claim reconciliation preserves durable attempt/checkpoint bytes and returns the same identity.
#[tokio::test]
async fn sqlite_recovery_claim() -> Result<(), String> {
    let (store, node, prepared) = claimed().await?;
    let before = commit(&prepared)?;
    let bytes = persisted_bytes(&store, before.task_id().into_uuid()).await?;
    let recovered = store
        .recover_claims(&node, &[request()], prepared)
        .await
        .map_err(|e| e.to_string())?;
    let after = commit(&recovered)?;
    assert_eq!(
        (before.task_id(), before.attempts()),
        (after.task_id(), after.attempts())
    );
    assert_eq!(
        bytes,
        persisted_bytes(&store, before.task_id().into_uuid()).await?
    );
    Ok(())
}

/// A replayed committed flush is obsolete; it cannot change terminal bytes or counters.
#[tokio::test]
async fn sqlite_recovery_committed_flush() -> Result<(), String> {
    let (store, node, prepared) = claimed().await?;
    let commits = [commit(&prepared)?];
    let result = store
        .commit_tasks(&commits)
        .await
        .map_err(|e| e.to_string())?;
    let bytes = persisted_bytes(&store, commits[0].task_id().into_uuid()).await?;
    let recovered = store
        .recover_outputs(&node, &commits, result)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(recovered.rejected, vec![commits[0].task_id()]);
    assert_eq!(
        bytes,
        persisted_bytes(&store, commits[0].task_id().into_uuid()).await?
    );
    Ok(())
}

/// A flush that never committed remains eligible and is applied through the normal fenced engine.
#[tokio::test]
async fn sqlite_recovery_uncommitted_flush() -> Result<(), String> {
    let (store, node, prepared) = claimed().await?;
    let commits = [commit(&prepared)?];
    let result = crate::tasks::CommitResult {
        wake_lanes: Vec::new(),
        rejected: Vec::new(),
        tasks: Vec::new(),
    };
    let recovered = store
        .recover_outputs(&node, &commits, result)
        .await
        .map_err(|e| e.to_string())?;
    assert!(recovered.rejected.is_empty());
    let row = store
        .get_task(commits[0].task_id())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing")?;
    assert_eq!(row.status, TaskStatus::Succeeded);
    assert_eq!(row.node_id, None);
    Ok(())
}

/// Cancellation discovered during acknowledgement recovery fails the child and wakes its parent atomically.
#[tokio::test]
async fn sqlite_recovery_cancellation() -> Result<(), String> {
    let (store, node, prepared) = claimed().await?;
    let child = commit(&prepared)?.task_id();
    let parent = insert_task(&store, 2, None).await?;
    db::sqlx::query("UPDATE vyuh_tasks SET cancelled = false, waiting_children = NULL, resume_input = NULL WHERE id = ?")
        .bind(parent).execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    db::sqlx::query("UPDATE vyuh_tasks SET parent_id = ? WHERE id = ?")
        .bind(parent)
        .bind(child.into_uuid())
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: child }])
        .await
        .map_err(|e| e.to_string())?;
    let poll = store
        .recover_claims(&node, &[request()], prepared)
        .await
        .map_err(|e| e.to_string())?;
    assert!(poll.lanes.iter().all(|lane| lane.tasks.is_empty()));
    let child = store
        .get_task(child)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing child")?;
    let parent = store
        .get_task(TaskId::new(parent))
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing parent")?;
    assert_eq!(child.status, TaskStatus::Failed);
    assert_eq!(parent.status, TaskStatus::Pending);
    assert_eq!(parent.resume_input, child.last_result);
    assert_eq!(parent.state.as_deref(), Some("{\"step\":1}"));
    Ok(())
}

/// Recovery cannot reapply an output belonging to an older lifetime attempt.
#[tokio::test]
async fn sqlite_recovery_advanced_attempt() -> Result<(), String> {
    let (store, node, prepared) = claimed().await?;
    let commits = [commit(&prepared)?];
    store
        .return_claims(Some(node))
        .await
        .map_err(|e| e.to_string())?;
    let newer = store
        .claim_tasks(&node, &[request()])
        .await
        .map_err(|e| e.to_string())?;
    assert!(commit(&newer)?.attempts() > commits[0].attempts());
    let old = store
        .recover_claims(&node, &[request()], prepared)
        .await
        .map_err(|e| e.to_string())?;
    assert!(old.lanes.iter().all(|lane| lane.tasks.is_empty()));
    let result = crate::tasks::CommitResult {
        wake_lanes: Vec::new(),
        rejected: Vec::new(),
        tasks: Vec::new(),
    };
    assert_eq!(
        store
            .recover_outputs(&node, &commits, result)
            .await
            .map_err(|e| e.to_string())?
            .rejected,
        vec![commits[0].task_id()]
    );
    let row = store
        .get_task(commits[0].task_id())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing")?;
    assert_eq!(row.status, TaskStatus::Running);
    Ok(())
}

/// One uncertain batch can contain both obsolete outcomes and still-current executions.
#[tokio::test]
async fn sqlite_recovery_mixed_flush() -> Result<(), String> {
    let (store, node, first) = claimed().await?;
    let first = commit(&first)?;
    let id = insert_task(&store, 0, None).await?;
    db::sqlx::query("UPDATE vyuh_tasks SET cancelled = false, ready_at = NULL, waiting_children = NULL WHERE id = ?")
        .bind(id).execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    let second = store
        .claim_tasks(&node, &[request()])
        .await
        .map_err(|e| e.to_string())?;
    let second = commit(&second)?;
    let result = store
        .commit_tasks(std::slice::from_ref(&first))
        .await
        .map_err(|e| e.to_string())?;
    let first_id = first.task_id();
    let result = store
        .recover_outputs(&node, &[first, second], result)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(result.rejected, vec![first_id]);
    let row = store
        .get_task(TaskId::new(id))
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing")?;
    assert_eq!(row.status, TaskStatus::Succeeded);
    assert_eq!(row.node_id, None);
    Ok(())
}

/// A cancelled parent cannot create children when an uncertain output is replayed.
#[tokio::test]
async fn sqlite_recovery_cancelled_group() -> Result<(), String> {
    let (store, node, poll) = claimed().await?;
    let parent = group(&store, &poll, 2).await?;
    store
        .commit_tasks(&[TaskCommit::Cancel {
            task_id: parent.task_id(),
        }])
        .await
        .map_err(|e| e.to_string())?;
    let result = crate::tasks::CommitResult {
        wake_lanes: Vec::new(),
        rejected: Vec::new(),
        tasks: Vec::new(),
    };
    store
        .recover_outputs(&node, std::slice::from_ref(&parent), result)
        .await
        .map_err(|e| e.to_string())?;
    let count: i64 = db::sqlx::query_scalar("SELECT count(*) FROM vyuh_tasks WHERE parent_id = ?")
        .bind(parent.task_id().into_uuid())
        .fetch_one(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(count, 0);
    let row = store
        .get_task(parent.task_id())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing")?;
    assert_eq!(row.status, TaskStatus::Failed);
    assert!(
        row.last_result
            .as_deref()
            .is_some_and(|result| result.contains("Task cancelled"))
    );
    Ok(())
}

/// Compares every persisted column, including join metadata omitted by public projections.
#[cfg(feature = "sqlite")]
async fn persisted_bytes(store: &DbTaskStore, id: uuid::Uuid) -> Result<String, String> {
    use db::sqlx::{Column, Row};
    let row = db::sqlx::query("SELECT * FROM vyuh_tasks WHERE id = ?")
        .bind(id)
        .fetch_one(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    let values = row
        .columns()
        .iter()
        .map(|column| format!("quote(\"{}\")", column.name()))
        .collect::<Vec<_>>()
        .join(",");
    db::sqlx::query_scalar(&format!(
        "SELECT json_array({values}) FROM vyuh_tasks WHERE id = ?"
    ))
    .bind(id)
    .fetch_one(store.pool.as_sqlx())
    .await
    .map_err(|e| e.to_string())
}
