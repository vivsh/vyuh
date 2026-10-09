use super::*;

/// Database claims obey lane policy even when the output batch is smaller.
#[tokio::test]
#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn independent_claim_bound() -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let original = fresh_store().await?;
    let mut conf = (*original.runtime_conf).clone();
    conf.lanes = vec![
        crate::tasks::TaskLaneConf::new(crate::tasks::DEFAULT_TASK_LANE, 8).claim_batch_size(3),
    ];
    let store = DbTaskStore::new(
        original.pool.as_sqlx().clone(),
        1,
        Duration::from_secs(300),
        conf,
    );
    let node = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    insert(
        &store,
        (1..=5).map(|id| row(id, Utc::now(), None)).collect(),
    )
    .await?;
    for expected in [3, 2] {
        let poll = store
            .claim_tasks(&node, &[request(32)])
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(
            poll.lanes.first().map(|lane| lane.tasks.len()),
            Some(expected)
        );
    }
    Ok(())
}

/// The optimized read loads payloads once and keeps lock policy and bounded ordering intact.
#[test]
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn claim_projection_is_payload_once() {
    let sql = statement(&request(32), 16).sql().to_owned();
    for column in [
        "input",
        "state",
        "resume_input",
        "last_result",
        "waiting_children",
    ] {
        assert_eq!(sql.matches(&format!("t.{column},")).count(), 1, "{sql}");
    }
    assert!(!sql.contains("COUNT("));
    assert!(sql.contains("ORDER BY t.ready_at, t.created_at, t.id LIMIT"));
    assert!(sql.contains("FROM clock UNION ALL SELECT 3"));
    #[cfg(feature = "postgres")]
    {
        assert_eq!(sql.matches("FOR UPDATE OF t SKIP LOCKED").count(), 1);
        assert!(
            sql.contains("SELECT t.kind, t.cancelled, t.step_attempts, t.ready_at, t.created_at")
        );
    }
    #[cfg(feature = "sqlite")]
    {
        assert!(!sql.contains("FOR UPDATE"));
        assert_eq!(sql.matches("FROM vyuh_tasks t").count(), 1);
        assert!(sql.contains("FROM selected"));
    }
}

/// Claims ignore owned rows of either kind and retain ownerless ordering and future hints.
#[tokio::test]
#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn ownerless_order_and_hints() -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let store = fresh_store().await?;
    let node = uuid::Uuid::now_v7();
    let other = uuid::Uuid::now_v7();
    for owner in [node, other] {
        store
            .register_node(&owner)
            .await
            .map_err(|e| e.to_string())?;
    }
    let now = Utc::now();
    insert(&store, ownership_rows(now, node, other)).await?;
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let (probes, selected, deadline) = read_after(&mut tx, &request(2), now, 32)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        selected.iter().map(|t| t.id.as_u128()).collect::<Vec<_>>(),
        vec![1, 6]
    );
    assert_eq!(probes.len(), 2);
    assert_eq!(deadline, Some(now + chrono::Duration::hours(2)));
    let (_, selected, _) = read_after(&mut tx, &request(32), now, 32)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        selected.iter().map(|t| t.id.as_u128()).collect::<Vec<_>>(),
        vec![1, 6]
    );
    let (_, selected, deadline) = read_after(&mut tx, &request(0), now, 32)
        .await
        .map_err(|e| e.to_string())?;
    assert!(selected.is_empty());
    assert_eq!(deadline, Some(now + chrono::Duration::hours(2)));
    tx.rollback().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Supplies ownerless/reclaimable work, ineligible ownership and future hints.
#[cfg(all(any(feature = "sqlite", feature = "postgres"), feature = "migrations"))]
pub(in crate::tasks::store::database) fn ownership_rows(
    now: DateTime<Utc>,
    node: uuid::Uuid,
    other: uuid::Uuid,
) -> Vec<TaskRow> {
    (1..=9)
        .map(|id| {
            let mut task = row(
                id,
                now,
                Some(now - chrono::Duration::seconds(10 - id as i64)),
            );
            match id {
                2 | 5 | 7 => {
                    task.kind = 1;
                    task.node_id = Some(node);
                }
                3 | 9 => {
                    task.kind = 1;
                    task.node_id = Some(other);
                }
                4 => task.node_id = Some(node),
                _ => {}
            }
            if id == 5 || id == 6 {
                task.status = 1;
            }
            if id == 1 || id == 2 {
                task.ready_at = None;
            }
            if id >= 7 {
                task.ready_at = Some(now + chrono::Duration::hours((id - 6) as i64));
            }
            if id == 9 {
                task.ready_at = Some(now + chrono::Duration::minutes(30));
            }
            task
        })
        .collect()
}

/// Typed probes contain only bounded eligibility evidence, not durable task payloads.
#[test]
fn mool_probe_projection_is_narrow() -> Result<(), String> {
    let table = DbTaskStore::table();
    let sql = db::from(&table)
        .slice::<TaskProbeRow>(0, 32)
        .plan()
        .map_err(|error| error.to_string())?
        .sql;
    for column in ["cancelled", "step_attempts", "ready_at", "created_at"] {
        assert!(sql.contains(column), "{sql}");
    }
    for column in ["input", "state", "last_result", "waiting_children"] {
        assert!(!sql.contains(column), "{sql}");
    }
    Ok(())
}

/// Both optimized read entrypoints return a clock/hint even without selected candidates.
#[tokio::test]
#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn empty_and_future_claim_reads() -> Result<(), String> {
    let store = fresh_store().await?;
    let mut tx = store
        .pool
        .begin()
        .await
        .map_err(|error| error.to_string())?;
    let (now, probe, selected, deadline) = read(&mut tx, &request(32), 32)
        .await
        .map_err(|error| error.to_string())?;
    assert!(probe.is_empty() && selected.is_empty() && deadline.is_none());
    tx.commit().await.map_err(|error| error.to_string())?;
    let future = now + chrono::Duration::hours(1);
    insert(&store, vec![row(1, now, Some(future))]).await?;
    let mut tx = store
        .pool
        .begin()
        .await
        .map_err(|error| error.to_string())?;
    let (probe, selected, deadline) = read_after(&mut tx, &request(32), now, 32)
        .await
        .map_err(|error| error.to_string())?;
    assert!(probe.is_empty() && selected.is_empty());
    assert_eq!(deadline, Some(future));
    assert_eq!(
        next_task_deadline(&mut tx, "default", now)
            .await
            .map_err(|error| error.to_string())?,
        Some(Duration::from_secs(3600))
    );
    let (_, _, deadline) = read_after(&mut tx, &request(0), now, 32)
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(deadline, Some(future));
    Ok(())
}

/// Full claims preserve ordering, cancellation/exhaustion handling, saturation, and reclamation.
#[tokio::test]
#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn mixed_claims_preserve_transition_contract() -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let store = fresh_store().await?;
    let node = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|error| error.to_string())?;
    let now = Utc::now() - chrono::Duration::hours(1);
    let mut rows = (1..=5).map(|id| row(id, now, None)).collect::<Vec<_>>();
    rows[0].cancelled = true;
    rows[1].step_attempts = i32::MAX;
    rows[2].status = TaskStatus::Running.as_i16();
    rows[2].input = "x".repeat(32_768);
    rows[4].ready_at = Some(now + chrono::Duration::days(1));
    insert(&store, rows).await?;
    let poll = store
        .claim_tasks(&node, &[request(4)])
        .await
        .map_err(|e| e.to_string())?;
    let lane = poll.lanes.first().ok_or("missing lane")?;
    assert!(lane.saturated);
    assert_eq!(lane.reclaimed, 1);
    assert_eq!(
        lane.tasks
            .iter()
            .map(|task| task.id.into_uuid())
            .collect::<Vec<_>>(),
        vec![uuid::Uuid::from_u128(3), uuid::Uuid::from_u128(4)]
    );
    assert_eq!(lane.tasks[0].input.len(), 32_768);
    assert!(lane.next_wake_in.is_some());
    assert_failed_and_drained(&store, node).await
}

/// Cancelled/exhausted rows terminate while claimed and scheduled rows are not reclaimed early.
#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn assert_failed_and_drained(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    for id in [1, 2] {
        let task = store
            .get_task(crate::tasks::TaskId::new(uuid::Uuid::from_u128(id)))
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing failed task")?;
        assert_eq!(task.status, TaskStatus::Failed);
    }
    let poll = store
        .claim_tasks(&node, &[request(4)])
        .await
        .map_err(|e| e.to_string())?;
    assert!(poll.lanes[0].tasks.is_empty());
    assert!(!poll.lanes[0].saturated);
    Ok(())
}

/// Narrow probes still reserve rates before locking tasks, including a rate-blocked poll.
#[tokio::test]
#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn rate_probe_preserves_permit_accounting() -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let store = fresh_store().await?;
    let node = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    let now = Utc::now();
    insert(&store, vec![row(1, now, None), row(2, now, None)]).await?;
    seed_rate(&store, now).await?;
    let conf = &store.runtime_conf;
    let claim = request(32);
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    for expected in [1, 0] {
        let turn = ClaimTurn {
            runner_id: &node,
            claim: &claim,
            rate: Some(TaskRate::new(1, Duration::from_secs(60))),
            retry: TaskRetry::default(),
            conf: &conf,
            now,
        };
        let lane = store
            .claim_sequential(&mut tx, turn, &mut Vec::new(), &mut Vec::new())
            .await
            .map_err(|e| e.to_string())?;
        assert_eq!(lane.tasks.len(), expected);
        if expected == 0 {
            assert_eq!(lane.next_wake_in, Some(Duration::from_secs(60)));
        }
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Seeds the durable rate balance normally established by runner initialization.
#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn seed_rate(store: &DbTaskStore, now: DateTime<Utc>) -> Result<(), String> {
    db::from(&DbTaskStore::rate_table())
        .insert_many(&[TaskRateRow {
            id: uuid::Uuid::now_v7(),
            lane_name: "default".into(),
            tokens_micros: TOKEN_SCALE,
            updated_at: now,
        }])
        .exec(&mut store.pool.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Builds a lane request without lifecycle coordination.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn request(limit: usize) -> LaneClaim {
    LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit,
        owner: None,
    }
}

/// PostgreSQL keeps backlog evidence when another transaction holds the candidate locks.
#[tokio::test]
#[cfg(all(feature = "postgres", feature = "migrations"))]
#[ignore = "requires an empty disposable dbharness VYUH_TASK_TEST_URL"]
async fn postgres_contention_preserves_probe() -> Result<(), String> {
    use crate::db::backend::RowLockExt as _;
    let store = fresh_store().await?;
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let (now, probe, selected, deadline) = read(&mut tx, &request(2), 32)
        .await
        .map_err(|e| e.to_string())?;
    assert!(probe.is_empty() && selected.is_empty() && deadline.is_none());
    tx.rollback().await.map_err(|e| e.to_string())?;
    let future = now + chrono::Duration::hours(1);
    insert(
        &store,
        vec![
            row(1, now, None),
            row(2, now, None),
            row(3, now, None),
            row(4, now, Some(future)),
        ],
    )
    .await?;
    let table = DbTaskStore::table();
    let mut peer = store.pool.begin().await.map_err(|e| e.to_string())?;
    db::from(&table)
        .filter(table.id.eq(db::val(uuid::Uuid::from_u128(1))))
        .for_update()
        .all::<TaskRow>()
        .exec(&mut peer)
        .await
        .map_err(|e| e.to_string())?;
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let (_, probe, selected, deadline) = read(&mut tx, &request(2), 32)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(probe.len(), 2);
    assert_eq!(
        selected.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![uuid::Uuid::from_u128(2), uuid::Uuid::from_u128(3)]
    );
    assert_eq!(deadline, Some(future));
    tx.rollback().await.map_err(|e| e.to_string())?;
    all_locked_still_has_backlog(&store, peer, now, future).await
}

/// A fully locked page is contention, not evidence that the lane is empty.
#[cfg(all(feature = "postgres", feature = "migrations"))]
async fn all_locked_still_has_backlog(
    store: &DbTaskStore,
    mut peer: db::DbTransaction<'_>,
    now: DateTime<Utc>,
    future: DateTime<Utc>,
) -> Result<(), String> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::table();
    db::from(&table)
        .for_update()
        .all::<TaskRow>()
        .exec(&mut peer)
        .await
        .map_err(|e| e.to_string())?;
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let (probe, selected, deadline) = read_after(&mut tx, &request(2), now, 32)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(probe.len(), 2);
    assert!(selected.is_empty());
    assert_eq!(deadline, Some(future));
    tx.rollback().await.map_err(|e| e.to_string())?;
    peer.rollback().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Builds an isolated desired-schema database, never an application database.
#[cfg(all(any(feature = "sqlite", feature = "postgres"), feature = "migrations"))]
pub(in crate::tasks::store::database) async fn fresh_store() -> Result<DbTaskStore, String> {
    #[cfg(feature = "sqlite")]
    let (url, dialect, connections) = ("sqlite::memory:".to_owned(), db::Dialect::Sqlite, 1);
    #[cfg(feature = "postgres")]
    let (url, dialect, connections) = (
        std::env::var("VYUH_TASK_TEST_URL").map_err(|e| e.to_string())?,
        db::Dialect::Postgres,
        2,
    );
    let pool = db::sqlx::pool::PoolOptions::<db::Database>::new()
        .max_connections(connections)
        .connect(&url)
        .await
        .map_err(|e| e.to_string())?;
    let planner = db::gaman::OfflinePlanner::new(dialect);
    let schema = super::super::schema::task_schema().map_err(|e| e.to_string())?;
    let migration = planner
        .make_migration(schema, &[])
        .map_err(|e| e.to_string())?
        .ok_or("missing migration")?;
    for sql in planner
        .sql_migrate(&[migration])
        .map_err(|e| e.to_string())?
    {
        db::sqlx::query(&sql)
            .execute(&pool)
            .await
            .map_err(|e| e.to_string())?;
    }
    let conf = crate::tasks::TaskRegistry::new()
        .with_config(crate::tasks::TaskConf::default())
        .and_then(|registry| registry.store_conf())
        .map_err(|e| e.to_string())?;
    Ok(DbTaskStore::new(pool, 32, Duration::from_secs(300), conf))
}

/// Inserts the same durable rows used by normal claim projections.
#[cfg(all(any(feature = "sqlite", feature = "postgres"), feature = "migrations"))]
pub(in crate::tasks::store::database) async fn insert(
    store: &DbTaskStore,
    rows: Vec<TaskRow>,
) -> Result<(), String> {
    db::from(&DbTaskStore::table())
        .insert_many(&rows)
        .exec(&mut store.pool.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Provides stable IDs and ordering with representative checkpoint/result bytes.
#[cfg(all(any(feature = "sqlite", feature = "postgres"), feature = "migrations"))]
pub(in crate::tasks::store::database) fn row(
    id: u128,
    now: DateTime<Utc>,
    ready_at: Option<DateTime<Utc>>,
) -> TaskRow {
    TaskRow {
        id: uuid::Uuid::from_u128(id),
        parent_id: None,
        root_id: None,
        kind: 0,
        name: "fixture".into(),
        input: "{}".into(),
        state: Some("{}".into()),
        resume_input: Some("{\"Ok\":null}".into()),
        waiting_children: None,
        remaining_completions: 0,
        status: 0,
        cancelled: false,
        attempts: 0,
        step_attempts: 0,
        lane_name: "default".into(),
        last_result: None,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        node_id: None,
        ready_at,
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

/// Quantized refill retains elapsed time below one micro-token instead of losing it.
#[test]
fn slow_rate_refill_preserves_fractional_elapsed_time() -> Result<(), TaskRuntimeError> {
    let rate = TaskRate::new(1, Duration::from_secs(365 * 24 * 60 * 60));
    let started = Utc::now();
    let now = started + chrono::Duration::seconds(1);
    let mut row = TaskRateRow {
        id: uuid::Uuid::now_v7(),
        lane_name: "slow".into(),
        tokens_micros: 0,
        updated_at: started,
    };
    refill(&mut row.tokens_micros, &mut row.updated_at, rate, now)?;
    assert_eq!(row.tokens_micros, 0);
    assert_eq!(row.updated_at, started);
    let wake = next_permit(row.tokens_micros, rate, row.updated_at, now)
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("missing permit deadline".into()))?;
    assert!(wake < rate.period());
    Ok(())
}
