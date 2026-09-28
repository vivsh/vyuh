use super::super::common::DbTaskStore;
use super::*;
use crate::tasks::store::fixtures::{claim, commit, conf, record};
use crate::tasks::{AbstractTaskStore, TaskOutcome, TaskStatus};

/// Shared observations retain ownership fencing, narrow renewals and bounded chunking.
#[tokio::test]
#[ignore = "requires disposable dbharness"]
async fn observation_contract() -> Result<(), String> {
    contract(&super::super::common::tests::store().await?)
        .await
        .map_err(|e| e.to_string())
}

/// Reads owned, foreign, terminal and missing identities without fabricating lifecycle rows.
async fn contract(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    let config = conf();
    let fingerprint = crate::tasks::store::policy_fingerprint(&config);
    store.initialize(config).await?;
    let rows = observation_rows();
    db::from(&DbTaskStore::table())
        .insert_many(&rows)
        .exec(&mut store.pool.clone())
        .await?;
    let commits = rows
        .iter()
        .skip(1)
        .map(|r| commit(crate::tasks::TaskId::new(r.id), TaskOutcome::Complete))
        .collect::<Vec<_>>();
    let mut leases = rows
        .iter()
        .map(|r| lease(crate::tasks::TaskId::new(r.id)))
        .collect::<Vec<_>>();
    leases.push(lease(record().id));
    let mut tx = store.pool.begin().await?;
    for batch in [1, 32] {
        let (_, full, narrow) =
            read(&mut tx, "owner", &commits, &leases, &fingerprint, batch).await?;
        assert_eq!(full.len(), 1);
        assert_eq!(
            full.first().map(|r| &r.input),
            rows.get(1).map(|r| &r.input)
        );
        assert_eq!(narrow.len(), 3);
        assert_eq!(
            narrow
                .iter()
                .filter(|r| r.locked_by.as_deref() == Some("peer"))
                .count(),
            1
        );
    }
    assert!(
        read(&mut tx, "owner", &commits, &leases, "wrong-policy", 32)
            .await
            .is_err()
    );
    tx.rollback().await?;
    Ok(())
}

/// Duplicate outcomes are rejected before SQL; duplicate renewals share one observation.
#[test]
fn duplicate_requests() -> Result<(), TaskRuntimeError> {
    let task = record();
    let outcome = commit(task.id, TaskOutcome::Complete);
    assert!(requests(&[outcome.clone(), outcome.clone()], &[]).is_err());
    let lease = TaskLease {
        task_id: task.id,
        lane: claim().lane,
        owner_token: None,
    };
    assert_eq!(
        requests(&[outcome], &[lease.clone(), lease])?,
        vec![(task.id.into_uuid(), true, true)]
    );
    Ok(())
}

/// A renewal failure rolls back outcomes already applied from the combined observation.
#[tokio::test]
#[ignore = "requires disposable dbharness"]
async fn mixed_rollback() -> Result<(), String> {
    rollback(&super::super::common::tests::store().await?)
        .await
        .map_err(|e| e.to_string())
}

/// Invalid lease arithmetic must not acknowledge or persist an otherwise valid completion.
async fn rollback(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let mut first = record();
    first.status = TaskStatus::Running;
    first.locked_by = Some("owner".into());
    first.leased_until = Some(Utc::now() + chrono::Duration::hours(1));
    let mut invalid = first.clone();
    invalid.id = record().id;
    invalid.lease_duration_ms = Some(i64::MAX);
    let (first_id, invalid_id) = (first.id, invalid.id);
    db::from(&DbTaskStore::table())
        .insert_many(&[TaskRow::from(first), TaskRow::from(invalid)])
        .exec(&mut store.pool.clone())
        .await?;
    let commits = [commit(first_id, TaskOutcome::Complete)];
    let leases = [TaskLease {
        task_id: invalid_id,
        lane: claim().lane,
        owner_token: None,
    }];
    assert!(store.tick("owner", &[], &commits, &leases).await.is_err());
    let retained = store
        .get_task(first_id)
        .await?
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("missing fixture".into()))?;
    assert_eq!(retained.status, TaskStatus::Running);
    assert!(retained.last_result.is_none());
    Ok(())
}

/// Competing mixed observations lock the same identities consistently and reject foreign work.
#[tokio::test]
#[cfg(feature = "postgres")]
#[ignore = "requires disposable dbharness"]
async fn mixed_contention() -> Result<(), String> {
    contention(&super::super::common::tests::store().await?)
        .await
        .map_err(|e| e.to_string())
}

/// Opposite ownership and request order cannot turn peer outcomes into accepted local writes.
#[cfg(feature = "postgres")]
async fn contention(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let [first, second] = [record(), record()].map(|mut row| {
        row.status = TaskStatus::Running;
        row.leased_until = Some(Utc::now() + chrono::Duration::hours(1));
        row
    });
    let (one, two) = (first.id, second.id);
    let mut rows = [TaskRow::from(first), TaskRow::from(second)];
    for (row, owner) in rows.iter_mut().zip(["one", "two"]) {
        row.locked_by = Some(owner.into());
    }
    db::from(&DbTaskStore::table())
        .insert_many(&rows)
        .exec(&mut store.pool.clone())
        .await?;
    let outcomes_one = [commit(one, TaskOutcome::Complete)];
    let outcomes_two = [commit(two, TaskOutcome::Complete)];
    let leases = [one, two].map(|task_id| TaskLease {
        task_id,
        lane: claim().lane,
        owner_token: None,
    });
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(
            store.tick("one", &[], &outcomes_one, &leases),
            store.tick("two", &[], &outcomes_two, &leases)
        )
    })
    .await
    .map_err(|e| TaskRuntimeError::TaskExecutionError(e.to_string()))?;
    a?;
    b?;
    for id in [one, two] {
        assert_eq!(
            store.get_task(id).await?.map(|r| r.status),
            Some(TaskStatus::Succeeded)
        );
    }
    Ok(())
}

/// Large payload fixtures distinguish full outcome snapshots from narrow lease observations.
fn observation_rows() -> Vec<TaskRow> {
    (0..4)
        .map(|index| {
            let mut task = record();
            task.status = if index == 3 {
                TaskStatus::Failed
            } else {
                TaskStatus::Running
            };
            task.locked_by = Some(if index == 2 { "peer" } else { "owner" }.into());
            task.input = "x".repeat(64_000);
            task.state = Some("y".repeat(64_000));
            task.leased_until = Some(Utc::now() + chrono::Duration::minutes(5));
            TaskRow::from(task)
        })
        .collect::<Vec<_>>()
}

/// Constructs a plain-lane observation without introducing test-owned lease state.
fn lease(task_id: crate::tasks::TaskId) -> TaskLease {
    TaskLease {
        task_id,
        lane: claim().lane,
        owner_token: None,
    }
}
