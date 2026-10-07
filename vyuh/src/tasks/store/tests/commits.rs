use super::*;

/// The shared commit operation terminally cancels any active status without execution ownership.
#[tokio::test]
async fn terminal_cancellation_states() -> Result<(), TestError> {
    for status in [
        TaskStatus::Pending,
        TaskStatus::Suspended,
        TaskStatus::Running,
        TaskStatus::Succeeded,
        TaskStatus::Failed,
    ] {
        let (store, _, _, original) = abandoned().await?;
        store
            .state
            .lock()
            .await
            .tasks
            .first_mut()
            .ok_or_else(missing)?
            .status = status;
        let before = store.get_task(original.id).await?.ok_or_else(missing)?;
        let request = [TaskCommit::Cancel {
            task_id: original.id,
        }];
        let result = store.commit_tasks(&request).await?;
        let after = store.get_task(original.id).await?.ok_or_else(missing)?;
        if matches!(status, TaskStatus::Succeeded | TaskStatus::Failed) {
            assert_eq!(result.rejected, vec![original.id]);
            assert_eq!(format!("{before:?}"), format!("{after:?}"));
        } else {
            assert!(result.rejected.is_empty());
            assert_terminal_cancel(&before, &after);
            assert_eq!(
                store.commit_tasks(&request).await?.rejected,
                vec![original.id]
            );
            let repeated = store.get_task(original.id).await?.ok_or_else(missing)?;
            assert_eq!(format!("{after:?}"), format!("{repeated:?}"));
        }
    }
    Ok(())
}

/// Cancellation persists a bounded failure and releases ownership without counting an execution.
fn assert_terminal_cancel(before: &TaskRecord, after: &TaskRecord) {
    assert_eq!(after.status, TaskStatus::Failed);
    assert!(after.cancelled);
    assert_eq!(after.node_id, None);
    assert_eq!(after.attempts, before.attempts);
    assert!(
        after
            .last_result
            .as_deref()
            .is_some_and(|value| value.contains("Task cancelled"))
    );
}

/// Terminal child cancellation delivers failure immediately; a stale later completion cannot redeliver.
#[tokio::test]
async fn cancellation_delivers_parent() -> Result<(), TestError> {
    let (store, node, _, child) = abandoned().await?;
    let mut parent = record(node);
    parent.status = TaskStatus::Suspended;
    parent.node_id = None;
    parent.resume_input = None;
    {
        let mut state = store.state.lock().await;
        state.tasks.first_mut().ok_or_else(missing)?.parent_id = Some(parent.id);
        state.tasks.push(parent.clone());
    }
    store
        .commit_tasks(&[TaskCommit::Cancel { task_id: child.id }])
        .await?;
    let terminal = store.get_task(child.id).await?.ok_or_else(missing)?;
    let resumed = store.get_task(parent.id).await?.ok_or_else(missing)?;
    assert_eq!(resumed.status, TaskStatus::Pending);
    assert_eq!(resumed.state, parent.state);
    assert_eq!(resumed.resume_input, terminal.last_result);
    assert_eq!(
        store.commit_tasks(&[commit(&child)?]).await?.rejected,
        vec![child.id]
    );
    let unchanged = store.get_task(parent.id).await?.ok_or_else(missing)?;
    assert_eq!(format!("{resumed:?}"), format!("{unchanged:?}"));
    Ok(())
}

/// One batch handles cancellation and ordinary outcomes independently without weakening fencing.
#[tokio::test]
async fn mixed_commits_and_rollback() -> Result<(), TestError> {
    let (store, node, _, first) = abandoned().await?;
    let second = record(node);
    store.state.lock().await.tasks.push(second.clone());
    let cancel = TaskCommit::Cancel { task_id: first.id };
    assert!(
        store
            .commit_tasks(&[cancel.clone(), commit(&first)?])
            .await
            .is_err()
    );
    assert!(
        !store
            .get_task(first.id)
            .await?
            .ok_or_else(missing)?
            .cancelled
    );
    let mut output = commit(&second)?;
    if let TaskCommit::Outcome { node_id, .. } = &mut output {
        *node_id = uuid::Uuid::new_v4();
    }
    let result = store.commit_tasks(&[cancel, output]).await?;
    assert_eq!(result.rejected, vec![second.id]);
    assert_eq!(
        store.get_task(first.id).await?.ok_or_else(missing)?.status,
        TaskStatus::Failed
    );
    assert_eq!(
        store.get_task(second.id).await?.ok_or_else(missing)?.status,
        TaskStatus::Running
    );
    store.commit_tasks(&[commit(&second)?]).await?;
    assert_eq!(
        store.get_task(second.id).await?.ok_or_else(missing)?.status,
        TaskStatus::Succeeded
    );
    Ok(())
}
