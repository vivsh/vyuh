use super::super::{common::DbTaskStore, model::TaskClaimRow};
use super::*;
use crate::tasks::store::fixtures::{claim, commit, conf, record, write};
use crate::tasks::{AbstractTaskStore, TaskLease, TaskOutcome, TaskStatus};

/// Both recovery branches preserve reference ordering, payloads, limits and wake evidence.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn recovery_gate_matches_reference() -> Result<(), String> {
    for expired in [false, true] {
        let store = super::super::common::tests::store().await?;
        differential(&store, expired)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Compares same-clock snapshots with the original predicate on heterogeneous rows.
async fn differential(store: &DbTaskStore, expired: bool) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let mut tx = store.pool.begin().await?;
    let now = super::super::runtime::verify_runtime_policy(
        &mut tx,
        &crate::tasks::store::policy_fingerprint(&conf()),
    )
    .await?;
    let table = DbTaskStore::table();
    db::from(&table)
        .insert_many(&candidates(now, expired))
        .exec(&mut tx)
        .await?;
    #[cfg(feature = "postgres")]
    tx.execute(db::Statement::raw(
        "SET LOCAL plan_cache_mode = force_generic_plan",
    ))
    .await?;
    for limit in [0, 1, 7, 32, 64] {
        let request = LaneClaim { limit, ..claim() };
        let reference = reference_rows(&mut tx, now, limit.min(32)).await?;
        let (probe, selected, deadline) = read_after(&mut tx, &request, now, 32).await?;
        assert_eq!(format!("{selected:?}"), format!("{reference:?}"));
        assert_eq!(
            probe.iter().map(|t| t.id).collect::<Vec<_>>(),
            reference.iter().map(|t| t.id).collect::<Vec<_>>()
        );
        let expected =
            super::super::claim::next_task_deadline(&mut tx, request.lane.as_str(), now).await?;
        assert_eq!(deadline.and_then(|d| (d - now).to_std().ok()), expected);
    }
    tx.rollback().await?;
    Ok(())
}

/// Seeds ties, null/future readiness, malformed payloads, cancellation and exhausted leases.
fn candidates(now: DateTime<Utc>, expired: bool) -> Vec<TaskRow> {
    (0..96)
        .map(|index| {
            let mut task = record();
            task.created_at = now - chrono::Duration::seconds(index % 3);
            task.ready_at = Some(now - chrono::Duration::seconds(5));
            task.input = format!("payload-{index}");
            task.state = Some(format!("checkpoint-{index}"));
            match index % 8 {
                0 => task.ready_at = None,
                1 => task.ready_at = Some(now + chrono::Duration::seconds(60)),
                2 | 3 => {
                    task.status = TaskStatus::Running;
                    task.locked_by = Some("owner".into());
                    task.leased_until =
                        Some(now + chrono::Duration::seconds(if expired { -5 } else { 90 }));
                    task.step_attempts = if index % 8 == 3 { 100 } else { 1 };
                }
                4 => task.cancelled = true,
                5 => task.status = TaskStatus::Suspended,
                6 => task.status = TaskStatus::Succeeded,
                _ => {}
            }
            TaskRow::from(task)
        })
        .collect()
}

/// Mixed turns retain outcome-before-renew-before-claim semantics and durable cancellation.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn mixed_turn_ordering() -> Result<(), String> {
    mixed(&super::super::common::tests::store().await?)
        .await
        .map_err(|e| e.to_string())
}

/// A renewed expired lease cannot be reclaimed, and a committed outcome cannot be replayed.
async fn mixed(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    store.initialize(conf()).await?;
    let tasks = [record(), record(), record(), record()];
    let [completed, renewing, cancelled, pending] = tasks.each_ref().map(|task| task.id);
    store
        .store_tasks(tasks.into_iter().map(write).collect())
        .await?;
    store
        .claim_tasks(
            "owner",
            &[LaneClaim {
                limit: 2,
                ..claim()
            }],
        )
        .await?;
    expire_lease(store, renewing).await?;
    store.cancel(cancelled).await?;
    let leases = [TaskLease {
        task_id: renewing,
        lane: claim().lane,
        owner_token: None,
    }];
    let tick = store
        .tick(
            "owner",
            &[claim()],
            &[commit(completed, TaskOutcome::Complete)],
            &leases,
        )
        .await?;
    let selected = tick
        .poll
        .lanes
        .iter()
        .flat_map(|l| &l.tasks)
        .map(|t| t.id)
        .collect::<Vec<_>>();
    assert_eq!(selected, vec![pending]);
    assert!(tick.lost.is_empty());
    assert_settled(store, completed, cancelled).await
}

/// Verifies the mixed turn persisted completion and cancellation before returning candidates.
async fn assert_settled(
    store: &DbTaskStore,
    completed: crate::tasks::TaskId,
    cancelled: crate::tasks::TaskId,
) -> Result<(), TaskRuntimeError> {
    assert_eq!(
        store.get_task(completed).await?.map(|t| t.status),
        Some(TaskStatus::Succeeded)
    );
    assert_eq!(
        store.get_task(cancelled).await?.map(|t| t.status),
        Some(TaskStatus::Failed)
    );
    Ok(())
}

/// Contention skips a locked prefix and still fills the requested batch on either branch.
#[tokio::test]
#[cfg(feature = "postgres")]
#[ignore = "requires disposable dbharness"]
async fn contention_backfills() -> Result<(), String> {
    for expired in [false, true] {
        backfill(&super::super::common::tests::store().await?, expired)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Locks earlier rows in a peer transaction without hiding available rows past the limit.
#[cfg(feature = "postgres")]
async fn backfill(store: &DbTaskStore, expired: bool) -> Result<(), TaskRuntimeError> {
    use db::backend::RowLockExt as _;
    store.initialize(conf()).await?;
    let table = DbTaskStore::table();
    let rows = locked_prefix(expired);
    db::from(&table)
        .insert_many(&rows)
        .exec(&mut store.pool.clone())
        .await?;
    let mut peer = store.pool.begin().await?;
    db::from(&table)
        .sort(table.created_at.asc())
        .for_update()
        .slice::<TaskRow>(0, 3)
        .exec(&mut peer)
        .await?;
    let tick = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.tick(
            "owner",
            &[LaneClaim {
                limit: 3,
                ..claim()
            }],
            &[],
            &[],
        ),
    )
    .await
    .map_err(|e| TaskRuntimeError::TaskExecutionError(e.to_string()))??;
    let actual = tick
        .poll
        .lanes
        .iter()
        .flat_map(|l| &l.tasks)
        .map(|t| t.id.into_uuid())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        rows.iter()
            .skip(3)
            .take(3)
            .map(|r| r.id)
            .collect::<Vec<_>>()
    );
    assert!(tick.poll.lanes.iter().all(|l| l.saturated));
    peer.rollback().await?;
    Ok(())
}

/// Uses the unchanged typed readiness query as a same-clock differential oracle.
async fn reference_rows(
    tx: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    let table = DbTaskStore::table();
    let request = claim();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(request.lane.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now))
        .sort(table.ready_at.asc())
        .sort(table.created_at.asc())
        .sort(table.id.asc())
        .slice::<TaskClaimRow>(0, limit)
        .exec(tx)
        .await?
        .into_iter()
        .map(TaskRow::from)
        .collect::<Vec<_>>())
}

/// Builds an ordered backlog containing either pending tasks or reclaimable leases.
#[cfg(feature = "postgres")]
fn locked_prefix(expired: bool) -> Vec<TaskRow> {
    let now = Utc::now() - chrono::Duration::minutes(1);
    (0..8)
        .map(|i| {
            let mut task = record();
            task.ready_at = Some(now);
            task.created_at = now + chrono::Duration::seconds(i);
            if expired {
                task.status = TaskStatus::Running;
                task.locked_by = Some("dead".into());
                task.leased_until = Some(now);
            }
            TaskRow::from(task)
        })
        .collect::<Vec<_>>()
}

/// Forces a same-owner lease into recovery eligibility before a mixed renewal turn.
async fn expire_lease(
    store: &DbTaskStore,
    id: crate::tasks::TaskId,
) -> Result<(), TaskRuntimeError> {
    let sql = format!(
        "UPDATE vyuh_tasks SET leased_until = {} WHERE id = {}",
        claim_sql::parameter(1),
        claim_sql::parameter(2)
    );
    store
        .pool
        .clone()
        .execute(
            db::Statement::raw(&sql)
                .bind(Utc::now() - chrono::Duration::seconds(60))
                .bind(id.into_uuid()),
        )
        .await?;
    Ok(())
}
