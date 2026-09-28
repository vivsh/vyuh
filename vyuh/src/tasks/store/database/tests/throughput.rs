use super::*;
use crate::tasks::store::fixtures::{claim, commit, conf, record, write};
use crate::tasks::{AbstractTaskStore, TaskOutcome};
use std::sync::atomic::Ordering;

/// Housekeeping is clone-shared, bounded, and remains periodic after integer wraparound.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn maintenance_cadence() -> Result<(), String> {
    let store = store().await?;
    let clone = store.clone();
    assert!(store.maintenance_due());
    for _ in 1..32 {
        assert!(!clone.maintenance_due());
    }
    assert!(store.maintenance_due());
    clone.maintenance_turn.store(u64::MAX, Ordering::Relaxed);
    assert!(!store.maintenance_due());
    assert!(clone.maintenance_due());
    Ok(())
}

/// The combined read always decodes policy separately, including stale and empty outcomes.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn combined_read_contract() -> Result<(), String> {
    combined_read(&store().await?)
        .await
        .map_err(|error| format!("{error:?}"))
}

/// Checks payload decoding, cancellation authority, stale ownership and policy mismatch.
async fn combined_read(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    let config = conf();
    let fingerprint = crate::tasks::store::policy_fingerprint(&config);
    store.initialize(config).await?;
    let task = record();
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store.cancel(id).await?;
    let commits = [commit(id, TaskOutcome::Complete)];
    let mut tx = store.pool.begin().await?;
    let (_, rows) =
        super::super::super::turn_read::outcomes(&mut tx, "owner", &commits, &fingerprint, 32)
            .await?;
    assert_eq!(rows.len(), 1);
    let row = rows.first().expect("one task");
    assert!(row.cancelled);
    assert_eq!(row.id, id.into_uuid());
    assert_eq!(row.input, "null");
    assert!(
        super::super::super::turn_read::outcomes(
            &mut tx,
            "wrong-owner",
            &commits,
            &fingerprint,
            32
        )
        .await?
        .1
        .is_empty()
    );
    assert!(
        super::super::super::turn_read::outcomes(&mut tx, "owner", &commits, "wrong-policy", 32)
            .await
            .is_err()
    );
    tx.rollback().await?;
    Ok(())
}

/// Empty polls cannot bypass a missing or changed deployment policy.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn empty_turn_policy_fencing() -> Result<(), String> {
    empty_policy(&store().await?)
        .await
        .map_err(|error| error.to_string())
}

/// Exercises both changed and absent policy records without any candidate rows.
async fn empty_policy(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use db::DbSession as _;
    store.initialize(conf()).await?;
    store
        .pool
        .clone()
        .execute(db::Statement::raw(
            "UPDATE vyuh_task_runtime SET policy_fingerprint = 'other'",
        ))
        .await?;
    assert!(store.tick("owner", &[], &[], &[]).await.is_err());
    assert!(store.claim_tasks("owner", &[claim()]).await.is_err());
    store
        .pool
        .clone()
        .execute(db::Statement::raw("DELETE FROM vyuh_task_runtime"))
        .await?;
    assert!(store.tick("owner", &[], &[], &[]).await.is_err());
    Ok(())
}

/// Lease metadata reads never select payloads, results, checkpoints or wait membership.
#[test]
fn renewal_projection_is_narrow() {
    use db::Record as _;
    let columns = super::super::super::model::TaskLeaseRow::record_column_names();
    assert_eq!(columns.len(), 7);
    for forbidden in [
        "input",
        "state",
        "resume_input",
        "last_result",
        "waiting_children",
        "updated_at",
    ] {
        assert!(!columns.iter().any(|column| column == forbidden));
    }
}

/// A cleanup selection cannot delete a reservation whose expiry changed before its delete.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn cleanup_rechecks_expiry() -> Result<(), String> {
    cleanup_race(&store().await?)
        .await
        .map_err(|error| error.to_string())
}

/// Models a stale selected identity after another operation renewed its retention.
async fn cleanup_race(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use super::super::super::model::TaskIdempotencyRow;
    let now = Utc::now();
    let mut owner = TaskIdempotencyRow {
        id: uuid::Uuid::now_v7(),
        task_name: "workflow".into(),
        key_value: "cleanup".into(),
        task_id: uuid::Uuid::now_v7(),
        fingerprint: "test".into(),
        expires_at: Some(now - ChronoDuration::seconds(1)),
        created_at: now,
        updated_at: now,
    };
    let table = DbTaskStore::idempotency_table();
    db::from(&table)
        .insert(&owner)
        .exec(&mut store.pool.clone())
        .await?;
    let selected = vec![owner.id];
    owner.expires_at = Some(now + ChronoDuration::hours(1));
    db::from(&table)
        .update_many(std::slice::from_ref(&owner), &table.expires_at)
        .exec(&mut store.pool.clone())
        .await?;
    let mut tx = store.pool.begin().await?;
    super::super::super::writes::delete_expired_owner_ids(&mut tx, selected, now).await?;
    assert!(
        db::from(&table)
            .filter(table.id.eq(db::val(owner.id)))
            .exists()
            .exec(&mut tx)
            .await?
    );
    tx.commit().await?;
    Ok(())
}

/// Accepted outcomes and renewal observations agree, including lost cancellation ACKs.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn settled_renewals_preserve_observations() -> Result<(), String> {
    settled_renewals(&store().await?)
        .await
        .map_err(|error| error.to_string())
}

/// Reuses current-transaction state, then repeats the turn as if its ACK had been lost.
async fn settled_renewals(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use crate::tasks::{DEFAULT_TASK_LANE, TaskLease};
    store.initialize(conf()).await?;
    let first = record();
    let second = record();
    let ids = [first.id, second.id];
    store.store_tasks(vec![write(first), write(second)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    store.cancel(ids[1]).await?;
    let commits = ids.map(|id| commit(id, TaskOutcome::Complete));
    let leases = ids.map(|task_id| TaskLease {
        task_id,
        lane: DEFAULT_TASK_LANE,
        owner_token: None,
    });
    for _ in 0..2 {
        let tick = store.tick("owner", &[], &commits, &leases).await?;
        assert_eq!(tick.lost, ids);
        assert_eq!(tick.cancelled, vec![ids[1]]);
    }
    assert_eq!(
        store.get_task(ids[0]).await?.expect("task").status,
        TaskStatus::Succeeded
    );
    assert_eq!(
        store.get_task(ids[1]).await?.expect("task").status,
        TaskStatus::Failed
    );
    Ok(())
}

/// An empty SKIP LOCKED selection must retain ready-backlog evidence for prompt polling.
#[tokio::test]
#[cfg(feature = "postgres")]
#[ignore = "requires disposable dbharness"]
async fn contended_claim_retains_saturation() -> Result<(), String> {
    contended_claim(&store().await?)
        .await
        .map_err(|error| format!("{error:?}"))
}

/// Holds a ready task in another transaction while the optimized claim observes its lane.
#[cfg(feature = "postgres")]
async fn contended_claim(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use db::backend::RowLockExt as _;
    store.initialize(conf()).await?;
    store.store_tasks(vec![write(record())]).await?;
    let mut competing = store.pool.begin().await?;
    db::from(&DbTaskStore::table())
        .for_update()
        .all::<super::super::super::model::TaskRow>()
        .exec(&mut competing)
        .await?;
    let request = crate::tasks::LaneClaim {
        limit: 1,
        ..claim()
    };
    let tick = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.tick("owner", &[request], &[], &[]),
    )
    .await
    .expect("SKIP LOCKED must not wait")?;
    let lane = tick.poll.lanes.first().expect("lane");
    assert!(lane.tasks.is_empty());
    assert!(lane.saturated);
    competing.rollback().await?;
    Ok(())
}

/// Records the database's plan for the real statement, including its bounded deadline probes.
#[tokio::test]
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[ignore = "query-plan evidence; run alone with --nocapture"]
async fn ordinary_claim_plan() -> Result<(), String> {
    let store = store().await?;
    let config = conf();
    let fingerprint = crate::tasks::store::policy_fingerprint(&config);
    store
        .initialize(config)
        .await
        .map_err(|error| error.to_string())?;
    store
        .store_tasks((0..128).map(|_| write(record())).collect())
        .await
        .map_err(|error| error.to_string())?;
    let mut tx = store
        .pool
        .begin()
        .await
        .map_err(|error| error.to_string())?;
    let plan = super::super::super::claim_read::explain(&mut tx, &claim(), &fingerprint)
        .await
        .map_err(|error| error.to_string())?;
    eprintln!("{}", plan.join("\n"));
    tx.rollback().await.map_err(|error| error.to_string())?;
    Ok(())
}
