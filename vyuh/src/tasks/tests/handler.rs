use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::*;
use crate::{
    Data, SiteError,
    tasks::{
        DEFAULT_TASK_LANE, LaneClaim, TaskCommit, TaskId, TaskIdempotency, TaskOptions,
        TaskReceipt, store::AbstractTaskStore, store::MemoryTaskStore,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct DirectJob {
    id: i64,
}

async fn direct_job(_input: Data<DirectJob>) -> Result<TaskState, crate::Error> {
    Ok(TaskState::complete(())?)
}

async fn unit_job(_input: Data<DirectJob>) {}

async fn result_unit_job(_input: Data<DirectJob>) -> Result<(), crate::Error> {
    Ok(())
}

async fn failed_job(_input: Data<DirectJob>) -> Result<(), crate::Error> {
    Err(crate::Error::invalid("secret task detail"))
}

async fn batch_job(
    input: Data<super::super::Batch<DirectJob>>,
) -> Result<super::super::Batch<TaskState>, crate::Error> {
    input
        .iter()
        .map(|job| {
            if job.id % 2 == 0 {
                TaskState::complete(()).map_err(Into::into)
            } else {
                Ok(TaskState::retry("odd job"))
            }
        })
        .collect()
}

async fn short_batch(
    _input: Data<super::super::Batch<DirectJob>>,
) -> super::super::Batch<TaskState> {
    super::super::Batch::new(Vec::new())
}

async fn sleeping_batch(
    _input: Data<super::super::Batch<DirectJob>>,
) -> Result<TaskState, crate::Error> {
    Ok(TaskState::sleep("state", Duration::from_secs(1))?)
}

async fn suspended_batch(
    _input: Data<super::super::Batch<DirectJob>>,
) -> Result<TaskState, crate::Error> {
    Ok(TaskState::suspend("state")?)
}

async fn unit_batch(_input: Data<super::super::Batch<DirectJob>>) {}

async fn retrying_batch(_input: Data<super::super::Batch<DirectJob>>) -> TaskState {
    TaskState::retry("try again")
}

async fn failing_batch(_input: Data<super::super::Batch<DirectJob>>) -> TaskState {
    TaskState::fail("permanent failure")
}

async fn error_batch(_input: Data<super::super::Batch<DirectJob>>) -> Result<(), crate::Error> {
    Err(crate::Error::invalid("batch handler failed"))
}

fn record<T: Serialize>(name: &str, input: &T) -> Result<Arc<TaskRecord>, TaskError> {
    let now = chrono::Utc::now();
    Ok(Arc::new(TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind: super::super::TaskKind::Work,
        name: name.to_string(),
        input: serde_json::to_string(input)?,
        state: None,
        resume_input: None,
        status: TaskStatus::Running,
        attempts: 0,
        step_attempts: 0,
        lane: DEFAULT_TASK_LANE.to_string(),
        lease_duration_ms: None,
        last_result: None,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        locked_by: Some("runner-a".to_string()),
        leased_until: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }))
}

async fn test_site() -> Result<Site, SiteError> {
    Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::bundle([]),
    )
    .await
}

/// Verifies direct batch registration preserves ordered per-task outcomes.
#[tokio::test]
async fn batch_registration_maps_ordered_outcomes() -> Result<(), String> {
    let task = RegisteredTask::new_batch(TaskDefinition::new("batch_job"), batch_job);
    let records = vec![
        record("batch_job", &DirectJob { id: 1 }).map_err(|error| error.to_string())?,
        record("batch_job", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
    ];
    let results = task
        .execute_many(
            test_site().await.map_err(|error| error.to_string())?,
            records,
        )
        .await;
    assert!(matches!(
        results.first().map(|result| &result.outcome),
        Some(TaskOutcome::Retry { error }) if error == "odd job"
    ));
    assert!(matches!(
        results.get(1).map(|result| &result.outcome),
        Some(TaskOutcome::Complete)
    ));
    Ok(())
}

/// Verifies malformed rows fail alone while valid rows still reach the batch handler.
#[tokio::test]
async fn batch_invalid_input_is_isolated() -> Result<(), String> {
    let task = RegisteredTask::new_batch(TaskDefinition::new("batch_job"), batch_job);
    let mut invalid = record("batch_job", &DirectJob { id: 1 })
        .map_err(|error| error.to_string())?
        .as_ref()
        .clone();
    invalid.input = "{".into();
    let records = vec![
        Arc::new(invalid),
        record("batch_job", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
    ];
    let results = task
        .execute_many(
            test_site().await.map_err(|error| error.to_string())?,
            records,
        )
        .await;
    assert!(matches!(
        results.first().map(|result| &result.outcome),
        Some(TaskOutcome::Fail { error }) if error == "Task input is invalid"
    ));
    assert!(matches!(
        results.get(1).map(|result| &result.outcome),
        Some(TaskOutcome::Complete)
    ));
    Ok(())
}

/// Verifies invalid cardinality fails every valid member of an invocation.
#[tokio::test]
async fn batch_cardinality_mismatch_fails_all() -> Result<(), String> {
    let task = RegisteredTask::new_batch(TaskDefinition::new("short_batch"), short_batch);
    let records = vec![
        record("short_batch", &DirectJob { id: 1 }).map_err(|error| error.to_string())?,
        record("short_batch", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
    ];
    let results = task
        .execute_many(
            test_site().await.map_err(|error| error.to_string())?,
            records,
        )
        .await;
    assert!(
        results
            .iter()
            .all(|result| matches!(result.outcome, TaskOutcome::Fail { .. }))
    );
    Ok(())
}

/// Verifies value-only batches reject continuation lifecycle outcomes.
#[tokio::test]
async fn batch_sleep_is_rejected() -> Result<(), String> {
    let task = RegisteredTask::new_batch(TaskDefinition::new("sleeping_batch"), sleeping_batch);
    let results = task
        .execute_many(
            test_site().await.map_err(|error| error.to_string())?,
            vec![
                record("sleeping_batch", &DirectJob { id: 1 })
                    .map_err(|error| error.to_string())?,
            ],
        )
        .await;
    assert!(matches!(
        results.first().map(|result| &result.outcome),
        Some(TaskOutcome::Fail { error }) if error.contains("cannot suspend or sleep")
    ));
    Ok(())
}

/// Verifies unit and uniform task-state returns map independently to every batch member.
#[tokio::test]
async fn batch_uniform_returns_apply_to_every_member() -> Result<(), String> {
    let site = test_site().await.map_err(|error| error.to_string())?;
    let records = || -> Result<Vec<Arc<TaskRecord>>, String> {
        Ok(vec![
            record("uniform", &DirectJob { id: 1 }).map_err(|error| error.to_string())?,
            record("uniform", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
        ])
    };
    let unit = RegisteredTask::new_batch(TaskDefinition::new("unit"), unit_batch)
        .execute_many(site.clone(), records()?)
        .await;
    assert!(
        unit.iter()
            .all(|result| matches!(result.outcome, TaskOutcome::Complete))
    );
    let retried = RegisteredTask::new_batch(TaskDefinition::new("retry"), retrying_batch)
        .execute_many(site.clone(), records()?)
        .await;
    assert!(retried.iter().all(|result| matches!(
        &result.outcome,
        TaskOutcome::Retry { error } if error == "try again"
    )));
    let failed = RegisteredTask::new_batch(TaskDefinition::new("fail"), failing_batch)
        .execute_many(site, records()?)
        .await;
    assert!(failed.iter().all(|result| matches!(
        &result.outcome,
        TaskOutcome::Fail { error } if error == "permanent failure"
    )));
    Ok(())
}

/// Verifies a handler error becomes one contained terminal failure per valid input.
#[tokio::test]
async fn batch_handler_error_fails_every_valid_member() -> Result<(), String> {
    let task = RegisteredTask::new_batch(TaskDefinition::new("error_batch"), error_batch);
    let results = task
        .execute_many(
            test_site().await.map_err(|error| error.to_string())?,
            vec![
                record("error_batch", &DirectJob { id: 1 }).map_err(|error| error.to_string())?,
                record("error_batch", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
            ],
        )
        .await;
    assert!(results.iter().all(|result| matches!(
        &result.outcome,
        TaskOutcome::Fail { error } if error == "Task handler failed"
    )));
    Ok(())
}

/// Verifies an all-malformed invocation is rejected without applying handler failure outcomes.
#[tokio::test]
async fn batch_all_invalid_inputs_skip_the_handler() -> Result<(), String> {
    let task = RegisteredTask::new_batch(TaskDefinition::new("error_batch"), error_batch);
    let mut first = record("error_batch", &DirectJob { id: 1 })
        .map_err(|error| error.to_string())?
        .as_ref()
        .clone();
    first.input = "{".into();
    let mut second = record("error_batch", &DirectJob { id: 2 })
        .map_err(|error| error.to_string())?
        .as_ref()
        .clone();
    second.input = "[".into();
    let results = task
        .execute_many(
            test_site().await.map_err(|error| error.to_string())?,
            vec![Arc::new(first), Arc::new(second)],
        )
        .await;
    assert!(results.iter().all(|result| matches!(
        &result.outcome,
        TaskOutcome::Fail { error } if error == "Task input is invalid"
    )));
    Ok(())
}

/// Verifies value-only batches reject both continuation lifecycle variants.
#[tokio::test]
async fn batch_suspend_is_rejected() -> Result<(), String> {
    let task = RegisteredTask::new_batch(TaskDefinition::new("suspended_batch"), suspended_batch);
    let results = task
        .execute_many(
            test_site().await.map_err(|error| error.to_string())?,
            vec![
                record("suspended_batch", &DirectJob { id: 1 })
                    .map_err(|error| error.to_string())?,
            ],
        )
        .await;
    assert!(matches!(
        results.first().map(|result| &result.outcome),
        Some(TaskOutcome::Fail { error }) if error.contains("cannot suspend or sleep")
    ));
    Ok(())
}

/// Verifies direct task registration retains typed task submission without result storage.
#[tokio::test]
async fn direct_registration_supports_typed_submit() -> Result<(), TaskError> {
    let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
    registry.register(RegisteredTask::new(
        TaskDefinition::new("direct_job"),
        direct_job,
    ))?;

    let store = Arc::new(MemoryTaskStore::new(10));
    let dispatcher = Arc::new(registry).dispatcher(store.clone(), Vec::new());
    let task_id = dispatcher.submit(DirectJob { id: 42 }).await?.id();
    let claimed = store
        .claim_tasks(
            "runner-a",
            &[LaneClaim {
                lane: DEFAULT_TASK_LANE,
                limit: 10,
                owner: None,
            }],
        )
        .await?;
    let task = claimed
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or_else(|| TaskError::TaskExecutionError("task was not claimed".into()))?;

    assert_eq!(task.id, task_id);
    assert_eq!(task.name, "direct_job");
    assert_eq!(task.input::<DirectJob>()?.id, 42);
    assert_eq!(task.parent_id, None);
    assert_eq!(task.root_id, None);
    assert_eq!(task.kind, super::super::TaskKind::Work);

    store
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id,
                lane: DEFAULT_TASK_LANE,
                outcome: TaskOutcome::complete(),
                owner_token: None,
            }],
        )
        .await?;
    Ok(())
}

/// Verifies invalid submission delay values surface only from submission terminals.
#[tokio::test]
async fn task_options_defer_errors_to_submission() -> Result<(), TaskError> {
    let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
    registry.register(RegisteredTask::new(
        TaskDefinition::new("direct_job"),
        direct_job,
    ))?;
    let dispatcher = Arc::new(registry).dispatcher(Arc::new(MemoryTaskStore::new(10)), Vec::new());
    let oversized = TaskOptions::new().delay(Duration::from_secs(u64::MAX));
    assert!(matches!(
        dispatcher.submit_with(DirectJob { id: 1 }, oversized).await,
        Err(TaskError::InvalidOptions(_))
    ));
    Ok(())
}

/// Verifies typed bulk key derivation preserves ordered queued and existing receipts.
#[tokio::test]
async fn typed_bulk_idempotency_preserves_receipt_order() -> Result<(), TaskError> {
    let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
    registry.register(RegisteredTask::new(
        TaskDefinition::new("direct_job")
            .idempotency(TaskIdempotency::new("direct-job-v1", |job: &DirectJob| {
                format!("job:{}", job.id)
            })),
        direct_job,
    ))?;
    let dispatcher = Arc::new(registry).dispatcher(Arc::new(MemoryTaskStore::new(10)), Vec::new());
    let receipts = dispatcher
        .submit_many_with(
            [DirectJob { id: 1 }, DirectJob { id: 1 }],
            TaskOptions::new(),
        )
        .await?;
    assert!(matches!(receipts.first(), Some(TaskReceipt::Queued(_))));
    assert!(matches!(receipts.get(1), Some(TaskReceipt::Existing(_))));
    assert_eq!(
        receipts.first().map(|receipt| receipt.id()),
        receipts.get(1).map(|receipt| receipt.id())
    );
    Ok(())
}

/// Verifies a task's static key rule inherits retention from its finalized lane.
#[test]
fn static_idempotency_inherits_lane_retention() -> Result<(), TaskError> {
    let mut registry = TaskRegistry::new();
    registry.register(RegisteredTask::new(
        TaskDefinition::new("direct_job")
            .idempotency(TaskIdempotency::new("direct-job-v1", |job: &DirectJob| {
                format!("job:{}", job.id)
            })),
        direct_job,
    ))?;
    let registry = registry.finalize(
        TaskConf::default().lane(
            super::super::TaskLaneConf::new(DEFAULT_TASK_LANE, 10)
                .idempotency_retention(Duration::from_secs(60)),
        ),
    )?;
    let policy = registry
        .idempotency_conf()?
        .into_iter()
        .next()
        .ok_or_else(|| TaskError::TaskNotFound("direct_job".into()))?;

    assert_eq!(policy.handler, "direct_job");
    assert_eq!(policy.lane, DEFAULT_TASK_LANE.as_str());
    assert!(matches!(
        policy.retention,
        super::super::IdempotencyRetention::RetainFor(duration)
            if duration == Duration::from_secs(60)
    ));
    Ok(())
}

/// Verifies an empty typed batch succeeds without touching durable storage.
#[tokio::test]
async fn empty_bulk_submission_returns_no_receipts() -> Result<(), TaskError> {
    let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
    registry.register(RegisteredTask::new(
        TaskDefinition::new("direct_job"),
        direct_job,
    ))?;
    let store = Arc::new(MemoryTaskStore::new(10));
    let dispatcher = Arc::new(registry).dispatcher(store.clone(), Vec::new());

    let receipts = dispatcher.submit_many(Vec::<DirectJob>::new()).await?;

    assert!(receipts.is_empty());
    assert_eq!(store.task_count().await, 0);
    Ok(())
}

/// Verifies a task handler receives the canonical ID stored in its operation metadata.
#[tokio::test]
async fn task_extracts_operation_id() -> Result<(), String> {
    let seen = Arc::new(parking_lot::Mutex::new(None));
    let captured = Arc::clone(&seen);
    let handler = move |id: crate::OperationId, _input: Data<DirectJob>| {
        let captured = Arc::clone(&captured);
        async move {
            *captured.lock() = Some(id);
        }
    };
    let service = RegisteredTask::new(TaskDefinition::new("operation_job"), handler);
    let expected = service.operation().id;
    let task = record("operation_job", &DirectJob { id: 1 }).map_err(|error| error.to_string())?;
    service
        .execute(test_site().await.map_err(|error| error.to_string())?, task)
        .await;
    assert_eq!(*seen.lock(), Some(expected));
    Ok(())
}

/// Verifies a unit task uses the allocation-free outcome before the store records unit success.
#[tokio::test]
async fn task_unit_uses_fast_path() -> Result<(), String> {
    let service = RegisteredTask::new(TaskDefinition::new("unit_job"), unit_job);
    let outcome = service
        .execute(
            test_site().await.map_err(|error| error.to_string())?,
            record("unit_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?,
        )
        .await;

    assert!(matches!(outcome, TaskOutcome::Complete));
    Ok(())
}

/// Verifies fallible unit tasks use the same unit-completion outcome.
#[tokio::test]
async fn task_result_unit_uses_fast_path() -> Result<(), String> {
    let service = RegisteredTask::new(TaskDefinition::new("result_unit_job"), result_unit_job);
    let outcome = service
        .execute(
            test_site().await.map_err(|error| error.to_string())?,
            record("result_unit_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?,
        )
        .await;

    assert!(matches!(outcome, TaskOutcome::Complete));
    Ok(())
}

/// Verifies handler errors retain no native detail in durable task outcomes.
#[tokio::test]
async fn task_handler_failure_uses_safe_summary() -> Result<(), String> {
    let service = RegisteredTask::new(TaskDefinition::new("failed_job"), failed_job);
    let outcome = service
        .execute(
            test_site().await.map_err(|error| error.to_string())?,
            record("failed_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?,
        )
        .await;

    assert!(matches!(
        outcome,
        TaskOutcome::Fail { ref error } if error == "Task handler failed"
    ));
    Ok(())
}

/// Verifies explicit unit completion uses the unit fast path.
#[tokio::test]
async fn task_state_unit_uses_fast_path() -> Result<(), String> {
    let service = RegisteredTask::new(TaskDefinition::new("direct_job"), direct_job);
    let outcome = service
        .execute(
            test_site().await.map_err(|error| error.to_string())?,
            record("direct_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?,
        )
        .await;

    assert!(matches!(outcome, TaskOutcome::Complete));
    Ok(())
}

/// Verifies continuations expose borrowed state and resume input without cloning.
#[tokio::test]
async fn continuation_decodes_state_and_resume_input() -> Result<(), String> {
    let mut record =
        record("direct_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?;
    let mutable = Arc::get_mut(&mut record).ok_or("task record unexpectedly shared")?;
    mutable.state = Some(serde_json::to_string(&"waiting").map_err(|e| e.to_string())?);
    mutable.resume_input = Some(
        serde_json::to_string(&Ok::<_, super::super::TaskFailure>(42_u32))
            .map_err(|e| e.to_string())?,
    );
    let context = TaskContext {
        site: test_site().await.map_err(|error| error.to_string())?,
        payload: callables::DataBox::new(DirectJob { id: 7 }),
        record,
        operation_id: crate::OperationId::new(),
    };
    let continuation =
        <Continuation<String, u32> as callables::FromContextParts<_>>::from_context_parts(&context)
            .map_err(|error| error.to_string())?;
    assert_eq!(continuation.state().map(String::as_str), Some("waiting"));
    assert_eq!(continuation.resume(), Some(&Ok(42)));
    Ok(())
}

/// Verifies every lifecycle control maps to a payload-free store outcome.
#[tokio::test]
async fn task_state_encodes_only_lifecycle() -> Result<(), TaskError> {
    assert!(matches!(
        TaskState::complete(())?.into_outcome(),
        TaskOutcome::Complete
    ));
    assert!(matches!(
        TaskState::suspend("approval")?.into_outcome(),
        TaskOutcome::Suspend { .. }
    ));
    assert!(matches!(
        TaskState::sleep("retry", Duration::ZERO)?.into_outcome(),
        TaskOutcome::Sleep { .. }
    ));
    assert!(matches!(
        TaskState::retry("temporary").into_outcome(),
        TaskOutcome::Retry { .. }
    ));
    assert!(matches!(
        TaskState::fail("permanent").into_outcome(),
        TaskOutcome::Fail { .. }
    ));
    Ok(())
}
/// Spawn preparation performs no submission and external resumes share the result envelope.
#[tokio::test]
async fn spawn_preparation_and_external_results() -> Result<(), TaskError> {
    let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
    registry.register(RegisteredTask::new(
        TaskDefinition::new("direct_job"),
        direct_job,
    ))?;
    let store = Arc::new(MemoryTaskStore::new(10));
    let dispatcher = Arc::new(registry).dispatcher(store.clone(), Vec::new());
    let prepared = dispatcher.spawn_with(DirectJob { id: 7 }, "checkpoint", TaskOptions::new())?;
    assert!(matches!(prepared.into_outcome(), TaskOutcome::Spawn { .. }));
    assert_eq!(store.task_count().await, 0);
    assert!(
        dispatcher
            .spawn_with(
                DirectJob { id: 7 },
                (),
                TaskOptions::new().ignore_conflicts()
            )
            .is_err()
    );
    let id = dispatcher.submit(DirectJob { id: 8 }).await?.id();
    let claim = LaneClaim {
        lane: DEFAULT_TASK_LANE,
        limit: 10,
        owner: None,
    };
    for failed in [false, true] {
        store
            .claim_tasks("owner", std::slice::from_ref(&claim))
            .await?;
        store
            .commit_outcomes(
                "owner",
                &[TaskCommit {
                    task_id: id,
                    lane: DEFAULT_TASK_LANE,
                    owner_token: None,
                    outcome: TaskOutcome::suspend(&"checkpoint")?,
                }],
            )
            .await?;
        if failed {
            assert!(
                dispatcher
                    .resume_failed(id, crate::tasks::TaskFailure::new(None, "external failure"))
                    .await?
            );
        } else {
            assert!(dispatcher.resume(id, 42u32).await?);
        }
        let result = store
            .get_task(id)
            .await?
            .ok_or_else(|| TaskError::TaskExecutionError("missing task".into()))?
            .resume_input::<u32>()?;
        assert_eq!(result.as_ref().map(Result::is_err), Some(failed));
        assert!(!dispatcher.resume(id, 99u32).await?);
    }
    Ok(())
}

/// External result limits are independent of checkpoint limits and reject before mutation.
#[tokio::test]
async fn external_result_limit_is_independent() -> Result<(), TaskError> {
    let config = TaskConf::default()
        .max_payload_bytes(32)
        .max_error_bytes(64 * 1024);
    let mut registry = TaskRegistry::new().with_config(config)?;
    registry.register(RegisteredTask::new(
        TaskDefinition::new("direct_job"),
        direct_job,
    ))?;
    let store = Arc::new(MemoryTaskStore::new(10));
    let dispatcher = Arc::new(registry).dispatcher(store.clone(), Vec::new());
    let id = dispatcher.submit(DirectJob { id: 1 }).await?.id();
    suspend_fixture(&store, id).await?;
    assert!(matches!(
        dispatcher.resume(id, "x".repeat(32_760)).await,
        Err(TaskError::ResultTooLarge { .. })
    ));
    assert!(matches!(
        dispatcher
            .resume_failed(
                id,
                crate::tasks::TaskFailure::new(None, "\u{0}".repeat(10_000))
            )
            .await,
        Err(TaskError::ResultTooLarge { .. })
    ));
    let record = store
        .get_task(id)
        .await?
        .ok_or_else(|| TaskError::TaskNotFound(id.to_string()))?;
    assert_eq!(record.status, crate::tasks::TaskStatus::Suspended);
    assert!(record.resume_input.is_none());
    assert!(dispatcher.resume(id, "x".repeat(32_759)).await?);
    Ok(())
}

/// Suspends a claimed fixture before testing the public resume validation boundary.
async fn suspend_fixture(store: &MemoryTaskStore, id: TaskId) -> Result<(), TaskError> {
    store
        .claim_tasks(
            "owner",
            &[LaneClaim {
                lane: DEFAULT_TASK_LANE,
                limit: 1,
                owner: None,
            }],
        )
        .await?;
    store
        .commit_outcomes(
            "owner",
            &[TaskCommit {
                task_id: id,
                lane: DEFAULT_TASK_LANE,
                owner_token: None,
                outcome: TaskOutcome::suspend(&())?,
            }],
        )
        .await?;
    Ok(())
}

/// Batch values can supply ordered outputs but cannot initiate continuation checkpoints.
#[tokio::test]
async fn batch_outputs_and_spawn_rejection() -> Result<(), String> {
    async fn outcomes(
        Data(_): Data<crate::tasks::Batch<DirectJob>>,
    ) -> crate::tasks::Batch<TaskState> {
        vec![
            TaskState::complete(42).expect("serialize integer"),
            TaskState {
                inner: TaskOutcome::Spawn {
                    state: "null".into(),
                    child: crate::tasks::TaskWrite {
                        record: record("child", &DirectJob { id: 3 })
                            .expect("serialize input")
                            .as_ref()
                            .clone(),
                        ignore_conflicts: false,
                        initial_delay: None,
                    },
                },
            },
        ]
        .into()
    }
    let task = RegisteredTask::new_batch(TaskDefinition::new("outputs"), outcomes);
    let records = [1, 2]
        .into_iter()
        .map(|id| record("outputs", &DirectJob { id }))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let results = task
        .execute_many(test_site().await.map_err(|e| e.to_string())?, records)
        .await;
    assert!(matches!(&results[0].outcome, TaskOutcome::CompleteWith { output } if output == "42"));
    assert!(matches!(&results[1].outcome, TaskOutcome::Fail { error } if error.contains("spawn")));
    Ok(())
}

/// Malformed/oversized output is a bounded task failure rather than a failed flush.
#[test]
fn output_validation_is_contained() {
    assert!(matches!(
        crate::tasks::store::normalize_outcome(
            TaskOutcome::CompleteWith {
                output: "\"larger than checkpoint limit\"".into()
            },
            1,
            100
        ),
        TaskOutcome::CompleteWith { .. }
    ));
    for output in ["{".to_owned(), format!("\"{}\"", "x".repeat(32_768))] {
        assert!(matches!(
            crate::tasks::store::normalize_outcome(TaskOutcome::CompleteWith { output }, 10, 100),
            TaskOutcome::Fail { .. }
        ));
    }
    let mut task = record("output", &DirectJob { id: 1 })
        .expect("serialize input")
        .as_ref()
        .clone();
    task.resume_input = Some("42".into());
    assert!(task.resume_input::<u32>().is_err());
    task.resume_input = Some("{\"Ok\":\"wrong type\"}".into());
    assert!(task.resume_input::<u32>().is_err());
}
