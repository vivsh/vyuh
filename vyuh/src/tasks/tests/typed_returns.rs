use super::*;
use crate::tasks::{Batch, FlowError, FlowState};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counted {
    calls: Arc<AtomicUsize>,
    value: std::cell::Cell<u32>,
}

impl Serialize for Counted {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        serializer.serialize_u32(self.value.get())
    }
}

/// Work Result decisions are decoded as outcomes rather than serialized as successful data.
#[tokio::test]
async fn work_result_decisions() -> Result<(), String> {
    async fn handler(input: Data<DirectJob>) -> Result<u32, TaskError> {
        match input.id {
            1 => Err(TaskError::retry("later")),
            2 => Err(TaskError::fail("stop")),
            _ => Ok(42),
        }
    }
    let task = RegisteredTask::new(TaskDefinition::new("decisions"), handler);
    let site = test_site().await.map_err(|e| e.to_string())?;
    let records = (1..4)
        .map(|id| record("decisions", &DirectJob { id }))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let results = task.execute_many(site, records).await;
    assert!(matches!(
        results.first().map(|r| &r.outcome),
        Some(TaskOutcome::Retry { .. })
    ));
    assert!(matches!(
        results.get(1).map(|r| &r.outcome),
        Some(TaskOutcome::Fail { .. })
    ));
    assert!(
        matches!(results.get(2).map(|r| &r.outcome), Some(TaskOutcome::CompleteWith { output }) if output == "42")
    );
    Ok(())
}

/// Serializable application Result values are data, not implicit retry/failure decisions.
#[tokio::test]
async fn application_result_is_data() -> Result<(), String> {
    async fn handler(_: Data<DirectJob>) -> Result<u32, String> {
        Err("application value".into())
    }
    let task = RegisteredTask::new(TaskDefinition::new("data"), handler);
    let outcome = task
        .execute(
            test_site().await.map_err(|e| e.to_string())?,
            record("data", &DirectJob { id: 1 }).map_err(|e| e.to_string())?,
        )
        .await;
    assert!(
        matches!(outcome, TaskOutcome::CompleteWith { output } if output == "{\"Err\":\"application value\"}")
    );
    Ok(())
}

/// Unit Work and Flow preserve the original tiny erased return allocation.
#[test]
fn unit_erasure_fast_path() {
    assert_eq!(
        crate::tasks::returns::erase_outcome(TaskOutcome::Complete).payload_type_id(),
        std::any::TypeId::of::<()>()
    );
    assert_eq!(
        FlowState::complete(()).prepare().erase().payload_type_id(),
        std::any::TypeId::of::<()>()
    );
}

/// Direct/state conversion serializes once and accepts outputs without Sync or Clone.
#[tokio::test]
async fn deferred_non_sync_output() -> Result<(), String> {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let handler = move |_: Data<DirectJob>| {
        let calls = captured.clone();
        async move {
            let state = TaskState::complete(Counted {
                calls: calls.clone(),
                value: 42.into(),
            });
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            state
        }
    };
    let task = RegisteredTask::new(TaskDefinition::new("counted"), handler);
    let outcome = task
        .execute(
            test_site().await.map_err(|e| e.to_string())?,
            record("counted", &DirectJob { id: 1 }).map_err(|e| e.to_string())?,
        )
        .await;
    assert!(matches!(outcome, TaskOutcome::CompleteWith { output } if output == "42"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Explicit decisions retain retry/failure identity, whereas automatic conversions are safe terminal errors.
#[test]
fn handler_error_contract() {
    assert!(
        matches!(TaskError::retry("later").into_outcome(), TaskOutcome::Retry { error } if error == "later")
    );
    assert!(
        matches!(TaskError::fail("stop").into_outcome(), TaskOutcome::Fail { error } if error == "stop")
    );
    let error = TaskError::from(TaskRuntimeError::TaskExecutionError("secret".into()));
    assert!(
        matches!(error.into_outcome(), TaskOutcome::Fail { error } if !error.contains("secret"))
    );
    let error = FlowError::from(crate::Error::invalid("secret"));
    assert!(
        matches!(error.into_outcome(), TaskOutcome::Fail { error } if !error.contains("secret"))
    );
}

/// Ordered batch serialization and suspension rejection affect only the corresponding item.
#[tokio::test]
async fn batch_serialization_isolation() -> Result<(), String> {
    async fn handler(_: Data<Batch<DirectJob>>) -> Batch<Result<TaskState<String>, TaskError>> {
        vec![
            Ok(TaskState::complete("x".repeat(32_759))),
            Ok(TaskState::complete("x".repeat(32_760))),
            TaskState::suspend(7u32).map_err(TaskError::from),
            Err(TaskError::retry("later")),
        ]
        .into()
    }
    let task = RegisteredTask::new_batch(TaskDefinition::new("batch"), handler);
    let records = (0..4)
        .map(|id| record("batch", &DirectJob { id }))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let outcomes = task
        .execute_many(test_site().await.map_err(|e| e.to_string())?, records)
        .await;
    assert!(matches!(
        outcomes.first().map(|r| &r.outcome),
        Some(TaskOutcome::CompleteWith { .. })
    ));
    assert!(matches!(
        outcomes.get(1).map(|r| &r.outcome),
        Some(TaskOutcome::Fail { .. })
    ));
    assert!(matches!(
        outcomes.get(2).map(|r| &r.outcome),
        Some(TaskOutcome::Fail { .. })
    ));
    assert!(matches!(
        outcomes.get(3).map(|r| &r.outcome),
        Some(TaskOutcome::Retry { .. })
    ));
    Ok(())
}

/// Invalid successful values are contained for both Work and Flow after infallible construction.
#[tokio::test]
async fn serialization_errors_are_terminal() -> Result<(), String> {
    let invalid = std::collections::BTreeMap::from([((1, 2), 3)]);
    assert!(matches!(
        TaskState::complete(invalid.clone()).into_outcome(),
        TaskOutcome::Fail { .. }
    ));
    let state = FlowState::complete(invalid).prepare();
    assert!(matches!(
        state
            .resolve(&test_site().await.map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?,
        TaskOutcome::Fail { .. }
    ));
    for value in ["\u{0}".repeat(6000), "é".repeat(20_000)] {
        assert!(matches!(
            TaskState::complete(value).into_outcome(),
            TaskOutcome::Fail { .. }
        ));
    }
    Ok(())
}

/// Uniform batch output is serialized once, with encoded outcomes copied per durable task.
#[tokio::test]
async fn uniform_output_serializes_once() -> Result<(), String> {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let handler = move |_: Data<Batch<DirectJob>>| {
        let calls = captured.clone();
        async move {
            TaskState::complete(Counted {
                calls,
                value: 42.into(),
            })
        }
    };
    let task = RegisteredTask::new_batch(TaskDefinition::new("counted"), handler);
    let records = (0..4)
        .map(|id| record("counted", &DirectJob { id }))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let results = task
        .execute_many(test_site().await.map_err(|e| e.to_string())?, records)
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(results.len(), 4);
    assert!(
        results
            .iter()
            .all(|r| matches!(&r.outcome, TaskOutcome::CompleteWith { output } if output == "42"))
    );
    Ok(())
}
