use super::*;
use crate::tasks::{TaskFailure, TaskInfo};

/// Shared persistence contract covers full-sized delivery, stale writes, and isolated invalid output.
pub(crate) async fn result_contract<S: AbstractTaskStore>(store: &S) -> Result<(), TaskError> {
    store.initialize(conf()).await?;
    for output in [
        "null".into(),
        "42".into(),
        format!("\"{}\"", "x".repeat(32_759)),
        serde_json::to_string(&format!("{}x", "é".repeat(16_379)))?,
        serde_json::to_string(&format!("{}x", "\n".repeat(16_379)))?,
    ] {
        deliver_result(store, output).await?;
    }
    invalid_outputs(store).await?;
    resume_limits(store).await?;
    checkpoints_clear(store).await
}

/// A child retains exactly the same bytes delivered to its parent on the next poll.
async fn deliver_result<S: AbstractTaskStore>(store: &S, output: String) -> Result<(), TaskError> {
    let mut parent = record();
    parent.status = TaskStatus::Suspended;
    let parent_id = parent.id;
    let mut child = record();
    child.parent_id = Some(parent_id);
    let id = child.id;
    store.store_tasks(vec![write(parent), write(child)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let outcome = TaskOutcome::CompleteWith {
        output: output.clone(),
    };
    let tick = store
        .tick("owner", &[claim()], &[commit(id, outcome)], &[])
        .await?;
    assert!(
        tick.poll
            .lanes
            .first()
            .ok_or_else(missing)?
            .tasks
            .is_empty()
    );
    let child = store.get_task(id).await?.ok_or_else(missing)?;
    let parent = store.get_task(parent_id).await?.ok_or_else(missing)?;
    assert!(
        child.last_result == parent.resume_input,
        "delivery must preserve exact bytes"
    );
    assert!(child.last_result.as_deref() == Some(format!("{{\"Ok\":{output}}}").as_str()));
    store
        .commit_outcomes("owner", &[commit(id, TaskOutcome::fail("stale"))])
        .await?;
    assert_eq!(
        store.get_task(id).await?.ok_or_else(missing)?.last_result,
        child.last_result
    );
    let info = TaskInfo::from(child);
    assert!(
        info.last_result::<serde_json::Value>()?
            .is_some_and(|value| value.is_ok())
    );
    store.claim_tasks("owner", &[claim()]).await?;
    store
        .commit_outcomes("owner", &[commit(parent_id, TaskOutcome::Complete)])
        .await?;
    Ok(())
}

fn missing() -> TaskError {
    TaskError::TaskExecutionError("missing test task".into())
}

/// Invalid low-level output fails only its own row while a valid sibling still completes.
async fn invalid_outputs<S: AbstractTaskStore>(store: &S) -> Result<(), TaskError> {
    let tasks = [record(), record(), record()];
    store
        .store_tasks(tasks.iter().cloned().map(write).collect())
        .await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let outcomes = [
        TaskOutcome::CompleteWith { output: "{".into() },
        TaskOutcome::CompleteWith {
            output: format!("\"{}\"", "x".repeat(32_760)),
        },
        TaskOutcome::Complete,
    ];
    let commits = tasks
        .iter()
        .zip(outcomes)
        .map(|(task, outcome)| commit(task.id, outcome))
        .collect::<Vec<_>>();
    store.commit_outcomes("owner", &commits).await?;
    for task in tasks.iter().take(2) {
        let stored = store.get_task(task.id).await?.ok_or_else(missing)?;
        assert_eq!(stored.status, TaskStatus::Failed);
        let result = TaskInfo::from(stored)
            .last_result::<()>()?
            .ok_or_else(missing)?;
        assert_eq!(
            result.err().and_then(|failure| failure.task_id().copied()),
            Some(task.id)
        );
    }
    let stored = store
        .get_task(tasks.last().ok_or_else(missing)?.id)
        .await?
        .ok_or_else(missing)?;
    assert_eq!(
        stored.last_result.as_deref(),
        Some(crate::tasks::result::UNIT_RESULT)
    );
    Ok(())
}

/// Resume limits apply before mutation and do not confuse JSON null with absent input.
async fn resume_limits<S: AbstractTaskStore>(store: &S) -> Result<(), TaskError> {
    let mut task = record();
    task.status = TaskStatus::Suspended;
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    for value in [
        "42".into(),
        format!("{{\"Ok\":\"{}\"}}", "x".repeat(32_760)),
    ] {
        assert!(store.resume(id, value).await.is_err());
        assert_eq!(
            store.get_task(id).await?.ok_or_else(missing)?.status,
            TaskStatus::Suspended
        );
    }
    let value = format!("{{\"Ok\":\"{}\"}}", "x".repeat(32_759));
    assert!(store.resume(id, value.clone()).await?);
    store.claim_tasks("owner", &[claim()]).await?;
    store
        .commit_outcomes("owner", &[commit(id, TaskOutcome::retry("retry"))])
        .await?;
    let task = store.get_task(id).await?.ok_or_else(missing)?;
    assert_eq!(task.resume_input.as_deref(), Some(value.as_str()));
    let result = serde_json::from_str::<Result<(), TaskFailure>>(
        task.last_result.as_deref().ok_or_else(missing)?,
    )?;
    assert!(result.is_err());
    Ok(())
}

/// Every successful checkpoint clears a prior failure without rewriting it during claim.
async fn checkpoints_clear<S: AbstractTaskStore>(store: &S) -> Result<(), TaskError> {
    for outcome in [
        TaskOutcome::Suspend { state: "0".into() },
        TaskOutcome::Sleep {
            state: "0".into(),
            delay: Duration::from_secs(60),
        },
        TaskOutcome::Spawn {
            state: "0".into(),
            child: write(record()),
        },
    ] {
        let mut task = record();
        task.last_result = Some(crate::tasks::result::failure(task.id, "old".into()));
        let id = task.id;
        store.store_tasks(vec![write(task)]).await?;
        store.claim_tasks("owner", &[claim()]).await?;
        assert!(
            store
                .get_task(id)
                .await?
                .ok_or_else(missing)?
                .last_result
                .is_some()
        );
        store
            .commit_outcomes("owner", &[commit(id, outcome)])
            .await?;
        assert!(
            store
                .get_task(id)
                .await?
                .ok_or_else(missing)?
                .last_result
                .is_none()
        );
    }
    Ok(())
}

/// Memory implements the same persisted JSON and checkpoint contract as SQL stores.
#[tokio::test]
async fn memory_results() -> Result<(), TaskError> {
    result_contract(&MemoryTaskStore::new(32)).await
}

/// Typed inspection distinguishes malformed data, wrong success types, and persisted failures.
#[test]
fn result_inspection_errors() -> Result<(), TaskError> {
    let mut task = record();
    assert!(TaskInfo::from(task.clone()).last_result::<()>()?.is_none());
    task.last_result = Some("{".into());
    assert!(TaskInfo::from(task.clone()).last_result::<()>().is_err());
    task.last_result = Some("{\"Ok\":42}".into());
    let info = TaskInfo::from(task.clone());
    assert!(info.last_result::<String>().is_err());
    assert_eq!(info.last_result_json(), Some("{\"Ok\":42}"));
    task.last_result = Some(crate::tasks::result::failure(task.id, "failed".into()));
    assert!(
        TaskInfo::from(task)
            .last_result::<String>()?
            .is_some_and(|value| value.is_err())
    );
    Ok(())
}
