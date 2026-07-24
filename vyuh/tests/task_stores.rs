use std::{collections::HashSet, time::Duration};

use vyuh::tasks::{AbstractTaskStore, MemoryTaskStore, TaskOutcome, TaskRecord, TaskStatus};

fn task_record(name: &str) -> TaskRecord {
    let now = chrono::Utc::now();
    TaskRecord {
        id: uuid::Uuid::now_v7(),
        name: name.to_string(),
        input: r#"{"id":1}"#.to_string(),
        state: None,
        resume_input: None,
        output: None,
        result: None,
        status: TaskStatus::Pending,
        attempts: 0,
        priority: 0,
        max_attempts: Some(3),
        retry_delay_ms: None,
        lease_duration_ms: None,
        last_error: None,
        identity: None,
        locked_by: None,
        leased_until: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

fn task_record_with_identity(name: &str, identity: &str) -> TaskRecord {
    let mut record = task_record(name);
    record.identity = Some(identity.to_string());
    record
}

async fn claims_higher_priority_first<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut low = task_record("low_priority");
    let low_id = low.id;
    low.priority = -10;
    let mut normal = task_record("normal_priority");
    let normal_id = normal.id;
    normal.priority = 0;
    let mut high = task_record("high_priority");
    let high_id = high.id;
    high.priority = 20;

    store.store_task(low).await?;
    store.store_task(normal).await?;
    store.store_task(high).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    let claimed_ids = claimed.iter().map(|task| task.id).collect::<Vec<_>>();
    assert_eq!(claimed_ids, vec![high_id, normal_id, low_id]);
    Ok(())
}

async fn stores_and_claims_pending_tasks<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let record = task_record("claim_complete");
    let id = record.id;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].status, TaskStatus::Running);
    assert_eq!(claimed[0].locked_by.as_deref(), Some("runner-a"));

    store
        .commit_outcome(id, "runner-a", TaskOutcome::complete(&"done")?)
        .await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.is_empty());
    Ok(())
}

async fn suspends_and_resumes_by_topic<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let record = task_record("suspend_resume");
    let id = record.id;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);

    store
        .commit_outcome(
            id,
            "runner-a",
            TaskOutcome::suspend(&"waiting", Some(&"needs approval"))?,
        )
        .await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.is_empty());

    // Resume by task ID (no topic needed)
    let resumed = store
        .resume(id, serde_json::to_string(&"approved")?)
        .await?;
    assert_eq!(resumed, 1);

    let claimed = store.claim_tasks("runner-b").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].resume_input.as_deref(), Some(r#""approved""#));

    store
        .commit_outcome(id, "runner-b", TaskOutcome::complete(&"done")?)
        .await?;
    Ok(())
}

async fn stale_runner_cannot_commit<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let record = task_record("stale_runner");
    let id = record.id;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);

    store
        .commit_outcome(
            id,
            "runner-b",
            TaskOutcome::sleep(&"wrong-runner", Duration::ZERO)?,
        )
        .await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.is_empty());

    store
        .commit_outcome(
            id,
            "runner-a",
            TaskOutcome::sleep(&"correct-runner", Duration::ZERO)?,
        )
        .await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].state.as_deref(), Some(r#""correct-runner""#));

    store
        .commit_outcome(id, "runner-b", TaskOutcome::complete(&"done")?)
        .await?;
    Ok(())
}

async fn stale_runner_cannot_retry<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let record = task_record("stale_retry_runner");
    let id = record.id;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);

    store
        .commit_outcome(id, "runner-b", TaskOutcome::retry(None, "wrong-runner"))
        .await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.is_empty());

    store
        .commit_outcome(id, "runner-a", TaskOutcome::complete(&"done")?)
        .await?;
    Ok(())
}

async fn active_identity_is_unique_until_terminal<S>(
    store: &S,
) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let first = task_record_with_identity("identity_first", "identity:1");
    let first_id = first.id;
    store.store_task(first).await?;

    let duplicate = task_record_with_identity("identity_duplicate", "identity:1");
    assert!(matches!(
        store.store_task(duplicate.clone()).await,
        Err(vyuh::tasks::TaskError::IdentityError)
    ));

    let claimed = store.claim_tasks("runner-a").await?;
    assert!(claimed.iter().any(|task| task.id == first_id));
    store
        .commit_outcome(first_id, "runner-a", TaskOutcome::complete(&"done")?)
        .await?;

    store.store_task(duplicate.clone()).await?;
    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.iter().any(|task| task.id == duplicate.id));
    store
        .commit_outcome(duplicate.id, "runner-b", TaskOutcome::complete(&"done")?)
        .await?;
    Ok(())
}

async fn null_ready_at_is_claimable<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut record = task_record("null_ready_at");
    let id = record.id;
    record.ready_at = None;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);

    store
        .commit_outcome(id, "runner-a", TaskOutcome::complete(&"done")?)
        .await?;
    Ok(())
}

async fn future_ready_at_is_not_claimed<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut record = task_record("future_ready_at");
    record.ready_at = Some(chrono::Utc::now() + chrono::Duration::minutes(5));
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert!(claimed.is_empty());
    Ok(())
}

async fn retry_respects_max_attempts<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut record = task_record_with_identity("retry_limit", "identity:retry");
    record.max_attempts = Some(1);
    let id = record.id;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    store
        .commit_outcome(id, "runner-a", TaskOutcome::retry(None, "boom"))
        .await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.is_empty());

    let replacement = task_record_with_identity("retry_replacement", "identity:retry");
    let replacement_id = replacement.id;
    store.store_task(replacement).await?;
    let claimed = store.claim_tasks("runner-c").await?;
    assert!(claimed.iter().any(|task| task.id == replacement_id));
    store
        .commit_outcome(
            replacement_id,
            "runner-c",
            TaskOutcome::complete(&"replacement")?,
        )
        .await?;
    Ok(())
}

async fn retry_uses_stored_retry_delay<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut record = task_record("retry_delay");
    let id = record.id;
    record.retry_delay_ms = Some(60_000);
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    store
        .commit_outcome(id, "runner-a", TaskOutcome::retry(None, "try later"))
        .await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.is_empty());
    Ok(())
}

async fn claims_expired_running_tasks<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut record = task_record("expired_lease_claim");
    let id = record.id;
    record.status = TaskStatus::Running;
    record.locked_by = Some("dead-runner".to_string());
    record.leased_until = Some(chrono::Utc::now() - chrono::Duration::minutes(1));
    record.ready_at = None;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].status, TaskStatus::Running);
    assert_eq!(claimed[0].locked_by.as_deref(), Some("runner-a"));

    store
        .commit_outcome(id, "runner-a", TaskOutcome::complete(&"done")?)
        .await?;
    Ok(())
}

async fn per_task_lease_controls_reclaim<S>(store: &S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut long_lease = task_record("long_lease");
    long_lease.status = TaskStatus::Running;
    long_lease.locked_by = Some("slow-runner".to_string());
    long_lease.leased_until = Some(chrono::Utc::now() + chrono::Duration::minutes(50));
    long_lease.ready_at = None;
    long_lease.lease_duration_ms = Some(chrono::Duration::hours(1).num_milliseconds());
    store.store_task(long_lease).await?;

    let claimed = store.claim_tasks("runner-a").await?;
    assert!(
        claimed.is_empty(),
        "unexpired task lease should prevent premature reclaim"
    );

    let mut short_lease = task_record("short_lease");
    let short_id = short_lease.id;
    short_lease.status = TaskStatus::Running;
    short_lease.locked_by = Some("dead-runner".to_string());
    short_lease.leased_until = Some(chrono::Utc::now() - chrono::Duration::minutes(1));
    short_lease.ready_at = None;
    short_lease.lease_duration_ms = Some(1_000);
    store.store_task(short_lease).await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert!(claimed.iter().any(|task| task.id == short_id));

    store
        .commit_outcome(short_id, "runner-b", TaskOutcome::complete(&"done")?)
        .await?;
    Ok(())
}

async fn stale_runner_cannot_overwrite_reclaimed_task<S>(
    store: &S,
) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Send + Sync,
{
    let mut record = task_record("reclaimed_stale_commit");
    let id = record.id;
    record.status = TaskStatus::Running;
    record.locked_by = Some("runner-a".to_string());
    record.leased_until = Some(chrono::Utc::now() - chrono::Duration::minutes(1));
    record.ready_at = None;
    store.store_task(record).await?;

    let claimed = store.claim_tasks("runner-b").await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].locked_by.as_deref(), Some("runner-b"));

    store
        .commit_outcome(id, "runner-a", TaskOutcome::complete(&"stale")?)
        .await?;

    store
        .commit_outcome(id, "runner-b", TaskOutcome::complete(&"fresh")?)
        .await?;

    let claimed = store.claim_tasks("runner-c").await?;
    assert!(claimed.is_empty());
    Ok(())
}

async fn concurrent_claimers_claim_each_task_once<S>(store: S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Clone + Send + Sync + 'static,
{
    let mut expected = HashSet::new();
    for i in 0..25 {
        let record = task_record(&format!("concurrent_claim_{i}"));
        expected.insert(record.id);
        store.store_task(record).await?;
    }

    let mut handles = Vec::new();
    for i in 0..8 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store.claim_tasks(&format!("runner-{i}")).await
        }));
    }

    let mut claimed_ids = HashSet::new();
    for handle in handles {
        let claimed = handle.await.map_err(|err| {
            vyuh::tasks::TaskError::TaskExecutionError(format!("claim task join error: {err}"))
        })??;
        for task in claimed {
            assert!(
                claimed_ids.insert(task.id),
                "task {} was claimed more than once",
                task.id
            );
        }
    }

    assert_eq!(claimed_ids, expected);
    Ok(())
}

async fn run_store_contract<S>(store: S) -> Result<(), vyuh::tasks::TaskError>
where
    S: AbstractTaskStore + Clone + Send + Sync + 'static,
{
    stores_and_claims_pending_tasks(&store).await?;
    claims_higher_priority_first(&store).await?;
    suspends_and_resumes_by_topic(&store).await?;
    stale_runner_cannot_commit(&store).await?;
    stale_runner_cannot_retry(&store).await?;
    active_identity_is_unique_until_terminal(&store).await?;
    null_ready_at_is_claimable(&store).await?;
    future_ready_at_is_not_claimed(&store).await?;
    retry_respects_max_attempts(&store).await?;
    retry_uses_stored_retry_delay(&store).await?;
    claims_expired_running_tasks(&store).await?;
    per_task_lease_controls_reclaim(&store).await?;
    stale_runner_cannot_overwrite_reclaimed_task(&store).await?;
    concurrent_claimers_claim_each_task_once(store.clone()).await?;
    Ok(())
}

#[tokio::test]
/// Verifies the full durable-task contract against the in-memory store.
async fn memory_task_store_contract() -> Result<(), vyuh::tasks::TaskError> {
    run_store_contract(MemoryTaskStore::new(10)).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
/// Verifies persistent stores reject retired startup schema provisioning.
#[allow(deprecated)]
async fn database_task_store_requires_external_migrations() -> Result<(), vyuh::tasks::TaskError> {
    let pool = sqlx::SqlitePool::connect(":memory:")
        .await
        .map_err(vyuh::tasks::TaskError::DatabaseError)?;
    let store = vyuh::tasks::TaskStore::new(pool, 10, Duration::from_secs(30));
    assert!(matches!(
        store.run_migrations().await,
        Err(vyuh::tasks::TaskError::MigrationRequired)
    ));
    Ok(())
}
