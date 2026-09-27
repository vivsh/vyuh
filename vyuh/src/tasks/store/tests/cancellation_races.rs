use super::*;

/// Real competing transactions preserve either terminal success or accepted cancellation.
pub(super) async fn contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    for _ in 0..16 {
        completion_race(store).await?;
        resume_race(store).await?;
    }
    Ok(())
}

/// Racing completion delivers exactly the winning result, and acknowledgement replay is inert.
async fn completion_race<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut parent = record();
    parent.status = TaskStatus::Suspended;
    let parent_id = parent.id;
    let mut child = record();
    child.parent_id = Some(parent_id);
    let id = child.id;
    store.store_tasks(vec![write(parent), write(child)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let commits = [commit(
        id,
        TaskOutcome::CompleteWith {
            output: "42".into(),
        },
    )];
    let (cancelled, completed) =
        tokio::join!(store.cancel(id), store.commit_outcomes("owner", &commits));
    let cancelled = cancelled?;
    completed?;
    let child = get(store, id).await?;
    assert_eq!(child.cancelled, cancelled);
    if cancelled {
        assert_cancelled(&child)?;
    } else {
        assert_eq!(child.status, TaskStatus::Succeeded);
    }
    assert_eq!(get(store, parent_id).await?.resume_input, child.last_result);
    let replay = store.tick("owner", &[], &commits, &[]).await?;
    assert!(replay.wake_lanes.is_empty());
    assert_eq!(get(store, id).await?.last_result, child.last_result);
    store.claim_tasks("owner", &[claim()]).await?;
    store
        .commit_outcomes("owner", &[commit(parent_id, TaskOutcome::Complete)])
        .await?;
    Ok(())
}

/// Resume and cancellation serialize without reviving a cancelled continuation.
async fn resume_race<S: AbstractTaskStore>(store: &S) -> Result<(), TaskRuntimeError> {
    let mut task = record();
    task.status = TaskStatus::Suspended;
    task.state = Some("9".into());
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    let (cancelled, resumed) =
        tokio::join!(store.cancel(id), store.resume(id, "{\"Ok\":1}".into()));
    assert!(cancelled?);
    resumed?;
    assert!(get(store, id).await?.cancelled);
    let claimed = store.claim_tasks("owner", &[claim()]).await?;
    assert!(claimed.lanes.iter().all(|lane| lane.tasks.is_empty()));
    assert_cancelled(&get(store, id).await?)?;
    assert!(!store.resume(id, "{\"Ok\":2}".into()).await?);
    Ok(())
}
