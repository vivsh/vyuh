use std::time::Duration;

use vyuh::tasks::{
    DEFAULT_TASK_GROUP, TaskGroup, TaskGroupConf, TaskId, TaskIdempotency, TaskRate, TaskReceipt,
    TaskRetry, TaskStatus,
    store::{
        AbstractTaskStore, GroupClaim, MemoryTaskStore, TaskCommit, TaskOutcome, TaskRecord,
        TaskStoreConf, TaskWrite,
    },
};

const EMAIL: TaskGroup = TaskGroup::new("email");
const MISSING: TaskGroup = TaskGroup::new("missing");

/// Builds one pending record for direct store-contract tests.
fn task_record(name: &str, group: TaskGroup) -> TaskRecord {
    let now = chrono::Utc::now();
    TaskRecord {
        id: uuid::Uuid::now_v7()
            .to_string()
            .parse()
            .expect("generated UUID is a valid task ID"),
        name: name.into(),
        input: r#"{"id":1}"#.into(),
        state: None,
        resume_input: None,
        status: TaskStatus::Pending,
        attempts: 0,
        group: group.to_string(),
        lease_duration_ms: None,
        last_error: None,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        locked_by: None,
        leased_until: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

/// Wraps one record in the store-facing batch submission value.
fn write(record: TaskRecord) -> TaskWrite {
    TaskWrite {
        record,
        ignore_conflicts: false,
        initial_delay: None,
    }
}

/// Creates the two-group runtime policy used throughout store tests.
fn store_conf(idempotency: TaskIdempotency) -> TaskStoreConf {
    TaskStoreConf {
        handlers: [
            "email",
            "archive",
            "job-0",
            "job-1",
            "job-2",
            "one",
            "two",
            "default-job",
            "email-job",
            "unknown",
            "later",
            "exhausted",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        groups: vec![
            TaskGroupConf::new(DEFAULT_TASK_GROUP, 2),
            TaskGroupConf::new(EMAIL, 2),
        ],
        batch_size: 8,
        lease_duration: Duration::from_secs(30),
        idempotency,
        max_error_bytes: 8 * 1024,
    }
}

/// Claims one group and flattens its single result for concise assertions.
async fn claim(
    store: &MemoryTaskStore,
    runner: &str,
    group: TaskGroup,
    limit: usize,
) -> Result<vyuh::tasks::store::TaskPoll, vyuh::tasks::TaskError> {
    store
        .claim_tasks(runner, &[GroupClaim { group, limit }])
        .await
}

/// Verifies batch submission, saturation evidence, batch claiming, and batch completion.
#[tokio::test]
async fn memory_store_batches_claims_and_outcomes() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let records = (0..3)
        .map(|index| write(task_record(&format!("job-{index}"), EMAIL)))
        .collect::<Vec<_>>();
    let receipts = store.store_tasks(records).await?;
    assert_eq!(receipts.len(), 3);
    assert!(
        receipts
            .iter()
            .all(|receipt| matches!(receipt, TaskReceipt::Queued(_)))
    );

    let poll = claim(&store, "runner-a", EMAIL, 2).await?;
    let group = poll
        .groups
        .first()
        .ok_or_else(|| vyuh::tasks::TaskError::TaskExecutionError("missing group poll".into()))?;
    assert_eq!(group.tasks.len(), 2);
    assert!(group.saturated);
    let commits = group
        .tasks
        .iter()
        .map(|task| TaskCommit {
            task_id: task.id,
            group: EMAIL,
            outcome: TaskOutcome::complete(),
        })
        .collect::<Vec<_>>();
    store.commit_outcomes("runner-a", &commits).await?;

    let remaining = claim(&store, "runner-b", EMAIL, 2).await?;
    assert_eq!(
        remaining
            .groups
            .first()
            .map_or(0, |group| group.tasks.len()),
        1
    );
    Ok(())
}

/// Verifies a batch size of one remains valid and reports remaining candidate pressure.
#[tokio::test]
async fn memory_store_supports_single_row_batches() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(1);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    store
        .store_tasks(vec![
            write(task_record("one", EMAIL)),
            write(task_record("two", EMAIL)),
        ])
        .await?;

    let poll = claim(&store, "runner-a", EMAIL, 8).await?;
    let group = poll
        .groups
        .first()
        .ok_or_else(|| vyuh::tasks::TaskError::TaskExecutionError("missing group poll".into()))?;
    assert_eq!(group.tasks.len(), 1);
    assert!(group.saturated);
    Ok(())
}

/// Verifies one group never claims work belonging to another configured lane.
#[tokio::test]
async fn memory_store_isolates_named_groups() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    store
        .store_tasks(vec![
            write(task_record("default-job", DEFAULT_TASK_GROUP)),
            write(task_record("email-job", EMAIL)),
        ])
        .await?;

    let email = claim(&store, "runner-a", EMAIL, 8).await?;
    let names = email
        .groups
        .first()
        .into_iter()
        .flat_map(|group| &group.tasks)
        .map(|task| task.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["email-job"]);
    Ok(())
}

/// Verifies low-level store mutations cannot bypass configured group membership.
#[tokio::test]
async fn memory_store_rejects_unknown_group_mutations() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    assert!(matches!(
        store
            .store_tasks(vec![write(task_record("unknown", MISSING))])
            .await,
        Err(vyuh::tasks::TaskError::UnknownGroup(group)) if group == MISSING.as_str()
    ));
    assert!(matches!(
        store
            .reassign_group(EMAIL.as_str(), MISSING.as_str())
            .await,
        Err(vyuh::tasks::TaskError::UnknownGroup(group)) if group == MISSING.as_str()
    ));
    Ok(())
}

/// Verifies idempotent replay, conflict rejection, and deliberate conflict ignoring.
#[tokio::test]
async fn memory_store_resolves_idempotency_receipts() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let mut first = task_record("email", EMAIL);
    first.idempotency_key = Some("message-1".into());
    first.idempotency_fingerprint = Some("intent-a".into());
    let first_id = first.id;
    assert_eq!(
        store.store_tasks(vec![write(first.clone())]).await?,
        vec![TaskReceipt::Queued(first_id)]
    );

    let replay_id = task_record("email", EMAIL).id;
    let mut replay = first.clone();
    replay.id = replay_id;
    assert_eq!(
        store.store_tasks(vec![write(replay)]).await?,
        vec![TaskReceipt::Existing(first_id)]
    );

    let mut conflict = first;
    conflict.id = uuid::Uuid::now_v7()
        .to_string()
        .parse::<TaskId>()
        .expect("generated UUID is a valid task ID");
    conflict.idempotency_fingerprint = Some("intent-b".into());
    assert!(
        matches!(store.store_tasks(vec![write(conflict.clone())]).await,
        Err(vyuh::tasks::TaskError::IdempotencyConflict(id)) if id == first_id)
    );
    assert_eq!(
        store
            .store_tasks(vec![TaskWrite {
                record: conflict,
                ignore_conflicts: true,
                initial_delay: None,
            }])
            .await?,
        vec![TaskReceipt::Ignored(first_id)]
    );
    Ok(())
}

/// Verifies duplicate keys inside one atomic batch resolve against its first ordered intent.
#[tokio::test]
async fn memory_store_resolves_in_batch_idempotency_in_input_order()
-> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let mut first = task_record("email", EMAIL);
    first.idempotency_key = Some("same-batch".into());
    first.idempotency_fingerprint = Some("same-intent".into());
    let first_id = first.id;
    let mut replay = task_record("email", EMAIL);
    replay.idempotency_key = first.idempotency_key.clone();
    replay.idempotency_fingerprint = first.idempotency_fingerprint.clone();

    let receipts = store.store_tasks(vec![write(first), write(replay)]).await?;
    assert_eq!(
        receipts,
        vec![
            TaskReceipt::Queued(first_id),
            TaskReceipt::Existing(first_id)
        ]
    );
    Ok(())
}

/// Verifies active-only keys release on completion while retained keys remain archived.
#[tokio::test]
async fn memory_store_applies_idempotency_archive_policy() -> Result<(), vyuh::tasks::TaskError> {
    active_key_is_released().await?;
    retained_key_is_archived().await
}

/// Exercises key release after an active-only task completes.
async fn active_key_is_released() -> Result<(), vyuh::tasks::TaskError> {
    let active = MemoryTaskStore::new(8);
    active
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let mut first = task_record("archive", EMAIL);
    first.idempotency_key = Some("key".into());
    first.idempotency_fingerprint = Some("one".into());
    let first_id = first.id;
    active.store_tasks(vec![write(first)]).await?;
    claim(&active, "runner-a", EMAIL, 1).await?;
    active
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id: first_id,
                group: EMAIL,
                outcome: TaskOutcome::complete(),
            }],
        )
        .await?;
    let mut replacement = task_record("archive", EMAIL);
    replacement.idempotency_key = Some("key".into());
    replacement.idempotency_fingerprint = Some("two".into());
    assert!(matches!(
        active.store_tasks(vec![write(replacement)]).await?.first(),
        Some(TaskReceipt::Queued(_))
    ));
    Ok(())
}

/// Exercises conflict retention after a task reaches a terminal state.
async fn retained_key_is_archived() -> Result<(), vyuh::tasks::TaskError> {
    let retained = MemoryTaskStore::new(8);
    retained
        .initialize(store_conf(TaskIdempotency::retain_for(
            Duration::from_secs(60),
        )))
        .await?;
    let mut archived = task_record("archive", EMAIL);
    archived.idempotency_key = Some("key".into());
    archived.idempotency_fingerprint = Some("one".into());
    let archived_id = archived.id;
    retained.store_tasks(vec![write(archived)]).await?;
    claim(&retained, "runner-a", EMAIL, 1).await?;
    retained
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id: archived_id,
                group: EMAIL,
                outcome: TaskOutcome::complete(),
            }],
        )
        .await?;
    let mut conflict = task_record("archive", EMAIL);
    conflict.idempotency_key = Some("key".into());
    conflict.idempotency_fingerprint = Some("two".into());
    assert!(matches!(retained.store_tasks(vec![write(conflict)]).await,
        Err(vyuh::tasks::TaskError::IdempotencyConflict(id)) if id == archived_id));
    Ok(())
}

/// Verifies a store-wide bucket reserves starts in a batch and reports its next permit deadline.
#[tokio::test]
async fn memory_store_global_rate_limits_group_starts() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    let rate = TaskRate::per_minute(1).burst(1);
    let conf = TaskStoreConf {
        groups: vec![TaskGroupConf::new(EMAIL, 2).global_rate_limit(rate)],
        ..store_conf(TaskIdempotency::ActiveOnly)
    };
    store.initialize(conf).await?;
    store
        .store_tasks(vec![
            write(task_record("one", EMAIL)),
            write(task_record("two", EMAIL)),
        ])
        .await?;

    let first = claim(&store, "runner-a", EMAIL, 8).await?;
    assert_eq!(first.groups.first().map_or(0, |group| group.tasks.len()), 1);
    assert!(first.groups.first().is_some_and(|group| !group.saturated));
    assert!(
        first
            .groups
            .first()
            .is_some_and(|group| group.next_wake_in.is_some())
    );
    let second = claim(&store, "runner-b", EMAIL, 8).await?;
    assert!(
        second
            .groups
            .first()
            .is_some_and(|group| group.tasks.is_empty())
    );
    assert!(
        second
            .groups
            .first()
            .is_some_and(|group| group.next_wake_in.is_some())
    );
    Ok(())
}

/// Verifies concurrent runners share one rate bucket rather than enforcing local limits.
#[tokio::test]
async fn memory_store_global_rate_limit_is_store_wide() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    let rate = TaskRate::per_minute(1).burst(1);
    let conf = TaskStoreConf {
        groups: vec![TaskGroupConf::new(EMAIL, 4).global_rate_limit(rate)],
        ..store_conf(TaskIdempotency::ActiveOnly)
    };
    store.initialize(conf).await?;
    store
        .store_tasks(vec![
            write(task_record("one", EMAIL)),
            write(task_record("two", EMAIL)),
        ])
        .await?;

    let first_store = store.clone();
    let second_store = store.clone();
    let (first, second) = tokio::join!(
        claim(&first_store, "runner-a", EMAIL, 2),
        claim(&second_store, "runner-b", EMAIL, 2),
    );
    let claimed = [first?, second?]
        .into_iter()
        .flat_map(|poll| poll.groups)
        .map(|group| group.tasks.len())
        .sum::<usize>();
    assert_eq!(claimed, 1);
    Ok(())
}

/// Verifies future readiness is returned as a bounded store-relative wake hint.
#[tokio::test]
async fn memory_store_reports_future_readiness() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let record = task_record("later", EMAIL);
    let mut delayed = write(record);
    delayed.initial_delay = Some(Duration::from_secs(30));
    store.store_tasks(vec![delayed]).await?;

    let poll = claim(&store, "runner-a", EMAIL, 8).await?;
    assert!(
        poll.groups
            .first()
            .is_some_and(|group| group.tasks.is_empty())
    );
    assert!(
        poll.groups
            .first()
            .and_then(|group| group.next_wake_in)
            .is_some_and(|delay| delay <= Duration::from_secs(30))
    );
    Ok(())
}

/// Verifies removed groups remain explicit and can be reassigned only after running work drains.
#[tokio::test]
async fn memory_store_reassigns_only_drained_groups() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    store
        .store_tasks(vec![write(task_record("email", EMAIL))])
        .await?;
    let claimed = claim(&store, "runner-a", EMAIL, 1).await?;
    assert!(
        matches!(store.reassign_group(EMAIL.as_str(), DEFAULT_TASK_GROUP.as_str()).await,
        Err(vyuh::tasks::TaskError::GroupBusy(group)) if group == EMAIL.as_str())
    );
    let task_id = claimed
        .groups
        .first()
        .and_then(|group| group.tasks.first())
        .map(|task| task.id)
        .ok_or_else(|| vyuh::tasks::TaskError::TaskExecutionError("task was not claimed".into()))?;
    store
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id,
                group: EMAIL,
                outcome: TaskOutcome::sleep(&"later", Duration::from_secs(1))?,
            }],
        )
        .await?;
    assert_eq!(
        store
            .reassign_group(EMAIL.as_str(), DEFAULT_TASK_GROUP.as_str())
            .await?,
        1
    );
    Ok(())
}

/// Verifies incompatible worker group or rate policies fail runtime initialization.
#[tokio::test]
async fn memory_store_rejects_policy_mismatch() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let incompatible = TaskStoreConf {
        groups: vec![TaskGroupConf::new(DEFAULT_TASK_GROUP, 2)],
        ..store_conf(TaskIdempotency::ActiveOnly)
    };
    assert!(matches!(
        store.initialize(incompatible).await,
        Err(vyuh::tasks::TaskError::InvalidConfig(_))
    ));
    Ok(())
}

/// Verifies local rate tuning is not persisted while global rate policy is store-compatible.
#[tokio::test]
async fn memory_store_fingerprints_only_global_rate_policy() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;

    let mut local_only = store_conf(TaskIdempotency::ActiveOnly);
    local_only.groups = vec![
        TaskGroupConf::new(DEFAULT_TASK_GROUP, 2),
        TaskGroupConf::new(EMAIL, 2).rate_limit(TaskRate::per_second(1)),
    ];
    store.initialize(local_only).await?;

    let mut global = store_conf(TaskIdempotency::ActiveOnly);
    global.groups = vec![
        TaskGroupConf::new(DEFAULT_TASK_GROUP, 2),
        TaskGroupConf::new(EMAIL, 2).global_rate_limit(TaskRate::per_second(1)),
    ];
    assert!(matches!(
        store.initialize(global).await,
        Err(vyuh::tasks::TaskError::InvalidConfig(_))
    ));
    Ok(())
}

/// Verifies group retry configuration participates in the durable worker policy identity.
#[tokio::test]
async fn memory_store_rejects_retry_policy_mismatch() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let mut incompatible = store_conf(TaskIdempotency::ActiveOnly);
    incompatible.groups = vec![
        TaskGroupConf::new(DEFAULT_TASK_GROUP, 2),
        TaskGroupConf::new(EMAIL, 2).retry(TaskRetry::exponential(3, Duration::from_secs(2))),
    ];
    assert!(matches!(
        store.initialize(incompatible).await,
        Err(vyuh::tasks::TaskError::InvalidConfig(_))
    ));
    Ok(())
}

/// Verifies a retry outcome uses the selected group's exponential delay.
#[tokio::test]
async fn memory_store_applies_group_retry_delay() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    let mut conf = store_conf(TaskIdempotency::ActiveOnly);
    conf.groups = vec![
        TaskGroupConf::new(DEFAULT_TASK_GROUP, 2),
        TaskGroupConf::new(EMAIL, 2).retry(TaskRetry::exponential(4, Duration::from_secs(2))),
    ];
    store.initialize(conf).await?;
    let receipt = store
        .store_tasks(vec![write(task_record("email", EMAIL))])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| vyuh::tasks::TaskError::TaskExecutionError("missing task receipt".into()))?;
    let claimed = claim(&store, "runner-a", EMAIL, 1).await?;
    let task_id = claimed
        .groups
        .first()
        .and_then(|group| group.tasks.first())
        .map(|task| task.id)
        .ok_or_else(|| vyuh::tasks::TaskError::TaskExecutionError("task was not claimed".into()))?;
    assert_eq!(task_id, receipt.id());
    store
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id,
                group: EMAIL,
                outcome: TaskOutcome::retry("temporary failure"),
            }],
        )
        .await?;
    let retried = store.get_task(task_id).await?.ok_or_else(|| {
        vyuh::tasks::TaskError::TaskExecutionError("retried task disappeared".into())
    })?;
    assert_eq!(retried.status, TaskStatus::Pending);
    assert_eq!(retried.attempts, 1);
    assert_eq!(
        retried.ready_at.map(|ready| ready - retried.updated_at),
        Some(chrono::Duration::seconds(2))
    );
    Ok(())
}

/// Verifies an expired lease at its attempt limit becomes terminal without another invocation.
#[tokio::test]
async fn memory_store_enforces_group_attempt_limit() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    let mut conf = store_conf(TaskIdempotency::ActiveOnly);
    conf.groups = vec![
        TaskGroupConf::new(DEFAULT_TASK_GROUP, 2),
        TaskGroupConf::new(EMAIL, 2).retry(TaskRetry::exponential(1, Duration::from_secs(1))),
    ];
    store.initialize(conf).await?;
    let mut record = task_record("exhausted", EMAIL);
    record.status = TaskStatus::Running;
    record.attempts = 1;
    record.leased_until = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    let id = record.id;
    store.store_tasks(vec![write(record)]).await?;
    let poll = claim(&store, "runner-b", EMAIL, 1).await?;
    assert!(
        poll.groups
            .first()
            .is_some_and(|group| group.tasks.is_empty())
    );
    let failed = store.get_task(id).await?.ok_or_else(|| {
        vyuh::tasks::TaskError::TaskExecutionError("exhausted task disappeared".into())
    })?;
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.attempts, 1);
    Ok(())
}

/// Verifies reclaiming an expired lease is reported and consumes another invocation attempt.
#[tokio::test]
async fn memory_store_reports_reclaimed_leases() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let mut record = task_record("email", EMAIL);
    record.status = TaskStatus::Running;
    record.attempts = 1;
    record.leased_until = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    store.store_tasks(vec![write(record)]).await?;

    let poll = claim(&store, "runner-b", EMAIL, 1).await?;
    let group = poll
        .groups
        .first()
        .ok_or_else(|| vyuh::tasks::TaskError::TaskExecutionError("missing group poll".into()))?;
    assert_eq!(group.reclaimed, 1);
    assert_eq!(group.tasks.first().map(|task| task.attempts), Some(2));
    Ok(())
}

/// Verifies lease renewal extends owned work and reports ownership loss after completion.
#[tokio::test]
async fn memory_store_renews_only_owned_leases() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let receipt = store
        .store_tasks(vec![write(task_record("email", EMAIL))])
        .await?;
    let id = receipt[0].id();
    claim(&store, "runner-a", EMAIL, 1).await?;
    assert!(store.renew_leases("runner-a", &[id]).await?.is_empty());
    store
        .commit_outcomes(
            "runner-a",
            &[TaskCommit {
                task_id: id,
                group: EMAIL,
                outcome: TaskOutcome::complete(),
            }],
        )
        .await?;
    assert_eq!(store.renew_leases("runner-a", &[id]).await?, vec![id]);
    Ok(())
}

/// Verifies a conflicting bulk submission rolls back earlier writes in the same batch.
#[tokio::test]
async fn memory_store_rolls_back_conflicting_batch() -> Result<(), vyuh::tasks::TaskError> {
    let store = MemoryTaskStore::new(8);
    store
        .initialize(store_conf(TaskIdempotency::ActiveOnly))
        .await?;
    let mut owner = task_record("email", EMAIL);
    owner.idempotency_key = Some("key".into());
    owner.idempotency_fingerprint = Some("first".into());
    store.store_tasks(vec![write(owner)]).await?;
    let before = store.task_count().await;
    let plain = write(task_record("one", EMAIL));
    let mut conflict = task_record("email", EMAIL);
    conflict.idempotency_key = Some("key".into());
    conflict.idempotency_fingerprint = Some("different".into());
    assert!(matches!(
        store.store_tasks(vec![plain, write(conflict)]).await,
        Err(vyuh::tasks::TaskError::IdempotencyConflict(_))
    ));
    assert_eq!(store.task_count().await, before);
    Ok(())
}
