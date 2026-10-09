#[cfg(feature = "migrations")]
use super::super::claim::tests::{fresh_store, insert, row};
use super::*;

/// Real PostgreSQL verifies atomic ownership, exceptional fallback, rollback and contention.
#[tokio::test]
#[cfg(feature = "migrations")]
#[ignore = "requires an empty disposable dbharness VYUH_TASK_TEST_URL; run alone"]
async fn postgres_atomic_claims() -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    use tracing_subscriber::prelude::*;
    let counts = Counts::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(counts.clone()))
        .map_err(|e| e.to_string())?;
    let store = fresh_store().await?;
    let node = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    store
        .claim_tasks(&node, &[request(32)])
        .await
        .map_err(|e| e.to_string())?;
    let poll = measured(&store, node, 32, &counts, 1).await?;
    assert!(poll.lanes.first().ok_or("lane")?.tasks.is_empty());
    ordinary(&store, node, &counts).await?;
    exceptions(&store, node).await?;
    rollback_and_join(&store, node).await?;
    contention(&store, node).await?;
    invalid_counters(&store, node).await?;
    incomplete_update_rolls_back(&store, node).await?;
    ownerless_order_and_hints(&store, node).await?;
    ownerless_probe_projection(&store, node).await?;
    ownerless_contention(&store, node).await?;
    Ok(())
}

/// Full and partial batches update once and retain prepared evidence for lost acknowledgements.
#[cfg(feature = "migrations")]
async fn ordinary(store: &DbTaskStore, node: uuid::Uuid, counts: &Counts) -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let now = Utc::now() - chrono::Duration::hours(1);
    let mut reclaimed = row(3, now, None);
    reclaimed.status = 1;
    insert(
        store,
        vec![
            row(1, now, None),
            row(2, now, None),
            reclaimed,
            row(4, now, Some(now + chrono::Duration::days(1))),
        ],
    )
    .await?;
    let poll = measured(store, node, 2, counts, 1).await?;
    let lane = poll.lanes.first().ok_or("lane")?;
    assert!(lane.saturated);
    assert_eq!(lane.tasks.len(), 2);
    for task in &lane.tasks {
        assert_eq!(task.node_id, Some(node));
        assert_eq!((task.attempts, task.step_attempts), (1, 1));
    }
    let recovered = store
        .recover_claims(&node, &[request(2)], poll)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(recovered.lanes.first().ok_or("lane")?.tasks.len(), 2);
    remaining_claims(store, node, counts).await
}

/// Partial and future-only responses retain wake hints without adding a statement.
#[cfg(feature = "migrations")]
async fn remaining_claims(
    store: &DbTaskStore,
    node: uuid::Uuid,
    counts: &Counts,
) -> Result<(), String> {
    let poll = measured(store, node, 32, counts, 1).await?;
    let lane = poll.lanes.first().ok_or("lane")?;
    assert_eq!(lane.reclaimed, 1);
    assert_eq!(lane.tasks.len(), 1);
    assert!(!lane.saturated);
    assert!(lane.next_wake_in.is_some());
    assert!(
        measured(store, node, 0, counts, 1)
            .await?
            .lanes
            .first()
            .ok_or("lane")?
            .tasks
            .is_empty()
    );
    assert!(
        measured(store, node, 32, counts, 1)
            .await?
            .lanes
            .first()
            .ok_or("lane")?
            .tasks
            .is_empty()
    );
    Ok(())
}

/// Cancellation and exhaustion leave the selected batch to the existing terminal machinery.
#[cfg(feature = "migrations")]
async fn exceptions(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    use crate::tasks::{TaskStatus, TaskStore as _};
    let now = Utc::now() - chrono::Duration::hours(1);
    let mut cancelled = row(10, now, None);
    cancelled.cancelled = true;
    let mut exhausted = row(11, now, None);
    exhausted.step_attempts = i32::MAX;
    insert(store, vec![cancelled, exhausted, row(12, now, None)]).await?;
    let poll = store
        .claim_tasks(&node, &[request(32)])
        .await
        .map_err(|e| e.to_string())?;
    let task = poll
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or("task")?;
    assert_eq!(task.id.into_uuid(), uuid::Uuid::from_u128(12));
    assert_eq!(task.attempts, 1);
    for id in [10, 11] {
        let task = store
            .get_task(TaskId::new(uuid::Uuid::from_u128(id)))
            .await
            .map_err(|e| e.to_string())?
            .ok_or("task")?;
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.attempts, 0);
    }
    Ok(())
}

/// Rollback undoes the SQL claim; satisfied joins retain the existing materialization path.
#[cfg(feature = "migrations")]
async fn rollback_and_join(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let now = Utc::now();
    insert(store, vec![row(20, now, None)]).await?;
    let conf = &store.runtime_conf;
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let (_, poll) = store
        .claim_postgres(&mut tx, &node, &request(32), &conf, &mut Vec::new())
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(poll.lanes.first().ok_or("lane")?.tasks.len(), 1);
    tx.rollback().await.map_err(|e| e.to_string())?;
    let task = store
        .get_task(TaskId::new(uuid::Uuid::from_u128(20)))
        .await
        .map_err(|e| e.to_string())?
        .ok_or("task")?;
    assert_eq!((task.node_id, task.attempts), (None, 0));
    db::sqlx::query(
        "UPDATE vyuh_tasks SET waiting_children = '[]', remaining_completions = 0 WHERE id = $1",
    )
    .bind(task.id.into_uuid())
    .execute(store.pool.as_sqlx())
    .await
    .map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[request(32)])
        .await
        .map_err(|e| e.to_string())?;
    let task = poll
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or("task")?;
    assert_eq!(task.attempts, 1);
    assert_eq!(task.resume_input.as_deref(), Some("{\"Ok\":[]}"));
    Ok(())
}

/// Skipped locks retain normal polling and never duplicate a claim across transactions.
#[cfg(feature = "migrations")]
async fn contention(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    use crate::{db::backend::RowLockExt as _, tasks::TaskStore as _};
    insert(store, vec![row(30, Utc::now(), None)]).await?;
    let table = DbTaskStore::table();
    let mut peer = store.pool.begin().await.map_err(|e| e.to_string())?;
    db::from(&table)
        .filter(table.id.eq(db::val(uuid::Uuid::from_u128(30))))
        .for_update()
        .all::<TaskRow>()
        .exec(&mut peer)
        .await
        .map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[request(32)])
        .await
        .map_err(|e| e.to_string())?;
    let lane = poll.lanes.first().ok_or("lane")?;
    assert!(lane.tasks.is_empty());
    assert!(lane.saturated);
    peer.rollback().await.map_err(|e| e.to_string())?;
    let poll = store
        .claim_tasks(&node, &[request(32)])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(poll.lanes.first().ok_or("lane")?.tasks.len(), 1);
    missing_node(store).await
}

/// FK enforcement rolls back a claim whose node has disappeared.
#[cfg(feature = "migrations")]
async fn missing_node(store: &DbTaskStore) -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let missing = uuid::Uuid::now_v7();
    insert(store, vec![row(31, Utc::now(), None)]).await?;
    assert!(store.claim_tasks(&missing, &[request(32)]).await.is_err());
    let task = store
        .get_task(TaskId::new(uuid::Uuid::from_u128(31)))
        .await
        .map_err(|e| e.to_string())?
        .ok_or("task")?;
    assert_eq!((task.node_id, task.attempts), (None, 0));
    db::sqlx::query("DELETE FROM vyuh_tasks WHERE id = $1")
        .bind(task.id.into_uuid())
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Overflow and negative counters fall back to the existing errors without partial claims.
#[cfg(feature = "migrations")]
async fn invalid_counters(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    for (attempts, step) in [(i32::MAX, 0), (0, -1)] {
        let mut broken = row(40, Utc::now(), None);
        broken.attempts = attempts;
        broken.step_attempts = step;
        insert(store, vec![broken, row(41, Utc::now(), None)]).await?;
        assert!(store.claim_tasks(&node, &[request(32)]).await.is_err());
        let task = store
            .get_task(TaskId::new(uuid::Uuid::from_u128(41)))
            .await
            .map_err(|e| e.to_string())?
            .ok_or("task")?;
        assert_eq!((task.node_id, task.attempts), (None, 0));
        db::sqlx::query("DELETE FROM vyuh_tasks WHERE id = ANY($1)")
            .bind(vec![uuid::Uuid::from_u128(40), uuid::Uuid::from_u128(41)])
            .execute(store.pool.as_sqlx())
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Missing RETURNING identities fail the whole transaction rather than acknowledging siblings.
#[cfg(feature = "migrations")]
async fn incomplete_update_rolls_back(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let mut skipped = row(50, Utc::now(), None);
    skipped.name = "skip_claim".into();
    insert(store, vec![skipped, row(51, Utc::now(), None)]).await?;
    db::sqlx::raw_sql("CREATE FUNCTION skip_claim() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.name = 'skip_claim' THEN RETURN NULL; END IF; RETURN NEW; END $$;
        CREATE TRIGGER skip_claim BEFORE UPDATE ON vyuh_tasks FOR EACH ROW EXECUTE FUNCTION skip_claim();")
        .execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    assert!(store.claim_tasks(&node, &[request(32)]).await.is_err());
    for id in [50, 51] {
        let task = store
            .get_task(TaskId::new(uuid::Uuid::from_u128(id)))
            .await
            .map_err(|e| e.to_string())?
            .ok_or("task")?;
        assert_eq!((task.node_id, task.attempts), (None, 0));
    }
    db::sqlx::raw_sql("DROP TRIGGER skip_claim ON vyuh_tasks; DROP FUNCTION skip_claim();")
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Counts only the measured claim, after first-turn maintenance has been consumed.
#[cfg(feature = "migrations")]
async fn measured(
    store: &DbTaskStore,
    node: uuid::Uuid,
    limit: usize,
    counts: &Counts,
    expected: usize,
) -> Result<TaskPoll, String> {
    use crate::tasks::TaskStore as _;
    let before = counts.0.load(std::sync::atomic::Ordering::Relaxed);
    let poll = store
        .claim_tasks(&node, &[request(limit)])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        counts.0.load(std::sync::atomic::Ordering::Relaxed) - before,
        expected
    );
    Ok(poll)
}

/// Returns an ordinary, uncoordinated lane request.
fn request(limit: usize) -> LaneClaim {
    LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit,
        owner: None,
    }
}

#[cfg(feature = "migrations")]
#[derive(Clone, Default)]
pub(crate) struct Counts(pub(crate) std::sync::Arc<std::sync::atomic::AtomicUsize>);

#[cfg(feature = "migrations")]
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Counts {
    /// Observes actual SQLx statements rather than counting store calls.
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "sqlx::query" {
            let mut control = Control(false);
            event.record(&mut control);
            if !control.0 {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

#[cfg(feature = "migrations")]
struct Control(bool);

#[cfg(feature = "migrations")]
impl tracing::field::Visit for Control {
    /// PostgreSQL logs COMMIT separately; transaction controls are not claim statements.
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "summary" {
            let sql = format!("{value:?}");
            self.0 = matches!(sql.trim_matches('"'), "COMMIT" | "ROLLBACK" | "BEGIN");
        }
    }
}

/// The ordinary path combines ownership mutation with one locked payload selection.
#[test]
fn single_statement_shape() {
    let claim = LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit: 32,
        owner: None,
    };
    let sql = statement(&uuid::Uuid::now_v7(), &claim, 32, 5)
        .sql()
        .to_owned();
    assert_eq!(sql.matches("UPDATE vyuh_tasks").count(), 1);
    assert_eq!(sql.matches("FOR UPDATE OF t SKIP LOCKED").count(), 1);
    assert_eq!(sql.matches("t.input,").count(), 1);
    assert!(!sql.contains("COUNT("));
    assert!(!sql.contains("probes"));
    assert!(sql.contains("RETURNING s.status = 1 AS reclaimed"));
    assert!(!sql.contains("LEFT JOIN claimed"));
    assert!(sql.contains("s.waiting_children IS NOT NULL"));
}

/// Ownerless-only claims never merge affinity streams or look up each selected identity.
#[test]
fn ownerless_statement_shape() {
    let claim = LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit: 32,
        owner: None,
    };
    let node = uuid::Uuid::now_v7();
    let sql = statement(&node, &claim, 32, 5).sql().to_owned();
    assert_eq!(sql.matches("UPDATE vyuh_tasks").count(), 1);
    assert_eq!(sql.matches("FOR UPDATE OF t SKIP LOCKED").count(), 1);
    assert_eq!(sql.matches("t.input,").count(), 1);
    assert!(!sql.contains("t.status = 0 AND t.node_id = $3"));
    assert!(!sql.contains("CROSS JOIN LATERAL"));
    assert!(sql.contains("ORDER BY t.ready_at, t.created_at, t.id LIMIT $2"));
    assert_eq!(sql.matches("LIMIT $2").count(), 1);
    assert!(!sql.contains("LEAST("));
    assert!(!sql.contains("COUNT("));
}

/// Owned rows are excluded while ownerless rows retain ordering and future hints.
#[cfg(feature = "migrations")]
async fn ownerless_order_and_hints(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    use crate::tasks::TaskStore as _;
    let other = uuid::Uuid::now_v7();
    store
        .register_node(&other)
        .await
        .map_err(|e| e.to_string())?;
    let now = Utc::now();
    let mut rows = super::super::claim::tests::ownership_rows(now, node, other);
    for task in &mut rows {
        task.id = uuid::Uuid::from_u128(100 + task.id.as_u128());
        task.lane_name = "sticky".into();
    }
    insert(store, rows).await?;
    let claim = LaneClaim {
        lane: crate::tasks::TaskLane::new("sticky"),
        limit: 32,
        owner: None,
    };
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let rows = tx
        .fetch_all::<PgClaimRow>(statement(&node, &claim, 32, 5))
        .await
        .map_err(|e| e.to_string())?;
    let (_, deadline, remaining, fast) = hints(&rows).map_err(|e| e.to_string())?;
    assert!(fast && !remaining);
    assert_eq!(deadline, Some(now + chrono::Duration::hours(2)));
    let (_, tasks, reclaimed) = decode(rows, fast).map_err(|e| e.to_string())?;
    assert_eq!(claimed_ids(&tasks), vec![106, 101]);
    assert_eq!(reclaimed, 1);
    tx.rollback().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Narrow ownerless probes avoid returning payloads or locking twice.
#[cfg(feature = "migrations")]
async fn ownerless_probe_projection(store: &DbTaskStore, _node: uuid::Uuid) -> Result<(), String> {
    use super::super::queries::StoreRow;
    let sql = super::super::claim::query(
        "WITH clock AS MATERIALIZED (SELECT NOW() AS now)",
        "$1",
        "$2",
        true,
    );
    assert_eq!(sql.matches("FOR UPDATE OF t SKIP LOCKED").count(), 1);
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let rows = tx
        .fetch_all::<StoreRow>(
            db::Statement::raw(&sql)
                .bind("sticky".to_owned())
                .bind(32_i64),
        )
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        rows.iter()
            .filter(|r| matches!(r, StoreRow::Probe(_)))
            .count(),
        2
    );
    let selected = rows
        .into_iter()
        .filter_map(|row| match row {
            StoreRow::Candidate(task) => Some(TaskRow::from(task).id.as_u128()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(selected, vec![106, 101]);
    tx.rollback().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Locked stream heads beyond twice the limit neither underfill nor lock unselected tails.
#[cfg(feature = "migrations")]
async fn ownerless_contention(store: &DbTaskStore, node: uuid::Uuid) -> Result<(), String> {
    insert(store, contention_rows(node)).await?;
    let mut peer = store.pool.begin().await.map_err(|e| e.to_string())?;
    peer.fetch_all::<(uuid::Uuid,)>(
        db::Statement::raw(
            "SELECT id FROM vyuh_tasks WHERE lane_name = 'contended-sticky' AND id < $1 FOR UPDATE",
        )
        .bind(uuid::Uuid::from_u128(207)),
    )
    .await
    .map_err(|e| e.to_string())?;
    let claim = LaneClaim {
        lane: crate::tasks::TaskLane::new("contended-sticky"),
        limit: 2,
        owner: None,
    };
    let mut tx = store.pool.begin().await.map_err(|e| e.to_string())?;
    let rows = tx
        .fetch_all::<PgClaimRow>(statement(&node, &claim, 2, 5))
        .await
        .map_err(|e| e.to_string())?;
    let (_, tasks, _) = decode(rows, true).map_err(|e| e.to_string())?;
    assert_eq!(claimed_ids(&tasks), vec![207, 208]);
    let tail = peer.fetch_all::<(uuid::Uuid,)>(db::Statement::raw(
        "SELECT id FROM vyuh_tasks WHERE lane_name = 'contended-sticky' AND id >= $1 FOR UPDATE NOWAIT")
        .bind(uuid::Uuid::from_u128(209))).await.map_err(|e| e.to_string())?;
    assert_eq!(tail.len(), 2);
    let rows = tx
        .fetch_all::<PgClaimRow>(statement(&node, &claim, 32, 5))
        .await
        .map_err(|e| e.to_string())?;
    assert!(hints(&rows).map_err(|e| e.to_string())?.2);
    assert!(decode(rows, true).map_err(|e| e.to_string())?.1.is_empty());
    tx.rollback().await.map_err(|e| e.to_string())?;
    peer.rollback().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Alternates Work and Flow with equal readiness and stable tie-breaking IDs.
#[cfg(feature = "migrations")]
fn contention_rows(_node: uuid::Uuid) -> Vec<TaskRow> {
    let now = Utc::now() - chrono::Duration::hours(1);
    (1..=10)
        .map(|id| {
            let mut task = row(200 + id, now, Some(now));
            task.lane_name = "contended-sticky".into();
            if id % 2 == 0 {
                task.kind = 1;
            }
            task
        })
        .collect()
}

/// Compares selected identities without relying on row positions or allocation-heavy payloads.
#[cfg(feature = "migrations")]
fn claimed_ids(tasks: &[crate::tasks::TaskRecord]) -> Vec<u128> {
    tasks
        .iter()
        .map(|task| task.id.into_uuid().as_u128())
        .collect()
}

/// Timing evidence is mandatory and unique even when the task batch is empty.
#[test]
fn metadata_is_required() {
    assert!(hints(&[]).is_err());
    let hint = || PgClaimRow::Hints {
        now: Utc::now(),
        next_ready_at: None,
        ready_remaining: false,
        fast_path: true,
    };
    assert!(hints(&[hint()]).is_ok());
    assert!(hints(&[hint(), hint()]).is_err());
}
