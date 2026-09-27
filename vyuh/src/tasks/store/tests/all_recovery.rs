use super::*;

/// Claim consumes membership once; lease recovery reuses the persisted envelope even if children disappear.
#[tokio::test]
async fn materialization_survives_reclaim() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    store.initialize(conf()).await?;
    let (parent, children) = edges::start(&store, 1, false).await?;
    store.claim_tasks("children", &[claim()]).await?;
    store
        .commit_outcomes("children", &[commit(children[0], TaskOutcome::Complete)])
        .await?;
    store.claim_tasks("first", &[claim()]).await?;
    let before = store.get_task(parent).await?.ok_or_else(missing)?;
    {
        let mut state = store.state.lock().await;
        assert!(!state.waits.contains_key(&parent));
        state.tasks.retain(|task| task.id != children[0]);
        let task = state
            .tasks
            .iter_mut()
            .find(|task| task.id == parent)
            .ok_or_else(missing)?;
        task.leased_until = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    }
    store.claim_tasks("replacement", &[claim()]).await?;
    let after = store.get_task(parent).await?.ok_or_else(missing)?;
    assert_eq!(after.locked_by.as_deref(), Some("replacement"));
    assert_eq!(after.resume_input, before.resume_input);
    assert_eq!(after.attempts, before.attempts + 1);
    store
        .commit_outcomes("first", &[commit(parent, TaskOutcome::Complete)])
        .await?;
    assert_eq!(
        store.get_task(parent).await?.ok_or_else(missing)?.status,
        TaskStatus::Running
    );
    Ok(())
}

/// Missing, nonterminal, and malformed member results yield only an outer failure for that parent.
#[tokio::test]
async fn broken_results_are_isolated() -> Result<(), TaskRuntimeError> {
    for mode in ["missing", "nonterminal", "malformed"] {
        let store = MemoryTaskStore::new(32);
        store.initialize(conf()).await?;
        let (parent, children) = edges::start(&store, 1, false).await?;
        store.claim_tasks("children", &[claim()]).await?;
        store
            .commit_outcomes("children", &[commit(children[0], TaskOutcome::Complete)])
            .await?;
        {
            let mut state = store.state.lock().await;
            if mode == "missing" {
                state.tasks.retain(|task| task.id != children[0]);
            } else {
                let child = state
                    .tasks
                    .iter_mut()
                    .find(|task| task.id == children[0])
                    .ok_or_else(missing)?;
                if mode == "malformed" {
                    child.last_result = Some("bad json".into());
                } else {
                    child.status = TaskStatus::Suspended;
                }
            }
        }
        store.claim_tasks("parent", &[claim()]).await?;
        let task = store.get_task(parent).await?.ok_or_else(missing)?;
        let result: Result<Vec<Result<(), crate::tasks::TaskFailure>>, crate::tasks::TaskFailure> =
            serde_json::from_str(task.resume_input.as_deref().ok_or_else(missing)?)?;
        assert!(result.is_err(), "{mode}");
        assert_eq!(task.status, TaskStatus::Running);
    }
    Ok(())
}

/// Counter underflow preserves both accepted child results and wakes the parent with a diagnostic.
#[tokio::test]
async fn counter_underflow_fails_outer_result() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    store.initialize(conf()).await?;
    let (parent, children) = edges::start(&store, 2, false).await?;
    store.claim_tasks("children", &[claim()]).await?;
    store
        .state
        .lock()
        .await
        .waits
        .get_mut(&parent)
        .ok_or_else(missing)?
        .remaining_completions = 1;
    let commits = children
        .iter()
        .map(|id| commit(*id, TaskOutcome::Complete))
        .collect::<Vec<_>>();
    store.commit_outcomes("children", &commits).await?;
    assert!(!store.state.lock().await.waits.contains_key(&parent));
    for id in children {
        assert_eq!(
            store.get_task(id).await?.ok_or_else(missing)?.status,
            TaskStatus::Succeeded
        );
    }
    let task = store.get_task(parent).await?.ok_or_else(missing)?;
    assert_eq!(task.status, TaskStatus::Pending);
    assert!(
        task.resume_input
            .as_deref()
            .is_some_and(|input| input.contains("underflow"))
    );
    Ok(())
}

/// Cancellation clears the sole authoritative wait map after ordinary candidate finalization.
#[tokio::test]
async fn cancelled_wait_is_removed() -> Result<(), TaskRuntimeError> {
    let store = MemoryTaskStore::new(32);
    store.initialize(conf()).await?;
    let (parent, _) = edges::start(&store, 2, false).await?;
    store.cancel(parent).await?;
    store.claim_tasks("cancel", &[claim()]).await?;
    assert!(!store.state.lock().await.waits.contains_key(&parent));
    Ok(())
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("Missing all fixture".into())
}

/// Join-created work prevents an idle hook in the same fenced lane turn.
#[tokio::test]
async fn join_respects_locked_lane() -> Result<(), TaskRuntimeError> {
    locked_all_contract(&MemoryTaskStore::new(32)).await
}
