use super::super::model::TaskNodeRow;
use super::*;
use crate::tasks::AbstractTaskStore;

#[cfg(feature = "sqlite")]
#[path = "recovery.rs"]
mod recovery;

/// FK retirement preserves task data, normal claims reclaim it, and old outputs remain fenced.
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn retirement_claim_contract() -> Result<(), String> {
    let store = fresh_store().await?;
    let old = uuid::Uuid::now_v7();
    let new = uuid::Uuid::now_v7();
    store.register_node(&old).await.map_err(|e| e.to_string())?;
    store.register_node(&new).await.map_err(|e| e.to_string())?;
    let id = insert_task(&store, 1, Some(old)).await?;
    db::sqlx::query("UPDATE vyuh_tasks SET cancelled = false, ready_at = NULL, waiting_children = NULL WHERE id = ?")
        .bind(id).execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    let original = store
        .get_task(crate::tasks::TaskId::new(id))
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing task")?;
    db::sqlx::query("UPDATE vyuh_task_nodes SET heartbeat_at = '2000-01-01 00:00:00' WHERE id = ?")
        .bind(old)
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        store
            .heartbeat(&new, std::time::Duration::from_secs(72))
            .await
            .map_err(|e| e.to_string())?
    );
    let retired = store
        .get_task(original.id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("missing task")?;
    assert_eq!(retired.node_id, None);
    assert_eq!(retired.status, crate::tasks::TaskStatus::Running);
    assert_eq!(retired.updated_at, original.updated_at);
    assert_eq!(retired.state, original.state);
    reclaim_and_fence(&store, old, new, original).await
}

/// Claims the abandoned attempt and verifies counters before comparing competing outputs.
#[cfg(feature = "sqlite")]
async fn reclaim_and_fence(
    store: &DbTaskStore,
    old: uuid::Uuid,
    new: uuid::Uuid,
    original: crate::tasks::TaskRecord,
) -> Result<(), String> {
    let claim = crate::tasks::LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit: 1,
        owner: None,
    };
    assert!(
        store
            .claim_tasks(&old, std::slice::from_ref(&claim))
            .await
            .is_err()
    );
    let poll = store
        .claim_tasks(&new, &[claim])
        .await
        .map_err(|e| e.to_string())?;
    let lane = poll.lanes.first().ok_or("missing lane")?;
    assert_eq!(lane.reclaimed, 1);
    let task = lane.tasks.first().ok_or("missing claim")?;
    assert_eq!((task.attempts, task.step_attempts), (18, 4));
    assert_eq!(task.resume_input, original.resume_input);
    fenced_outputs(store, old, new, &original, task).await
}

/// Only the new node and current lifetime attempt may persist an output after reclamation.
#[cfg(feature = "sqlite")]
async fn fenced_outputs(
    store: &DbTaskStore,
    old: uuid::Uuid,
    new: uuid::Uuid,
    original: &crate::tasks::TaskRecord,
    task: &crate::tasks::TaskRecord,
) -> Result<(), String> {
    let mut commit = crate::tasks::TaskCommit::Outcome {
        node_id: old,
        task_id: original.id,
        lane: crate::tasks::DEFAULT_TASK_LANE,
        attempts: original.attempts,
        outcome: crate::tasks::TaskOutcome::Complete,
        owner_token: None,
    };
    assert_eq!(
        store
            .commit_tasks(&[commit.clone()])
            .await
            .map_err(|e| e.to_string())?
            .rejected,
        vec![original.id]
    );
    if let crate::tasks::TaskCommit::Outcome {
        node_id, attempts, ..
    } = &mut commit
    {
        *node_id = new;
        *attempts = task.attempts;
    }
    store
        .commit_tasks(&[commit])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        store
            .get_task(original.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing task")?
            .status,
        crate::tasks::TaskStatus::Succeeded
    );
    Ok(())
}

/// A deleted caller cannot purge stale peers, and the FK rejects unknown owners.
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn missing_node_no_cleanup() -> Result<(), String> {
    let store = fresh_store().await?;
    let node = uuid::Uuid::now_v7();
    let missing = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    db::sqlx::query("UPDATE vyuh_task_nodes SET heartbeat_at = '2000-01-01 00:00:00'")
        .execute(store.pool.as_sqlx())
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        !store
            .heartbeat(&missing, std::time::Duration::from_secs(72))
            .await
            .map_err(|e| e.to_string())?
    );
    assert_eq!(rows(&store).await.map_err(|e| e.to_string())?.len(), 1);
    assert!(insert_task(&store, 1, Some(missing)).await.is_err());
    assert!(
        store
            .heartbeat(&node, std::time::Duration::from_secs(72))
            .await
            .map_err(|e| e.to_string())?
    );
    assert_eq!(rows(&store).await.map_err(|e| e.to_string())?.len(), 1);
    Ok(())
}

#[cfg(feature = "sqlite")]
#[derive(Clone, Default)]
struct SqlCounts {
    statements: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    nodes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(feature = "sqlite")]
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SqlCounts {
    /// Observes driver statements without counting other application tracing events.
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        let mut visit = NodeQuery(false);
        event.record(&mut visit);
        self.statements
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if visit.0 {
            self.nodes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[cfg(feature = "sqlite")]
struct NodeQuery(bool);

#[cfg(feature = "sqlite")]
impl tracing::field::Visit for NodeQuery {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if matches!(field.name(), "db.statement" | "summary") {
            self.0 |= format!("{value:?}").contains("vyuh_task_nodes");
        }
    }
}

/// Counts SQL emitted by one operation, including SQLite's dedicated worker thread.
#[cfg(feature = "sqlite")]
async fn counted<T, E: std::fmt::Display>(
    counts: &SqlCounts,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<(T, usize, usize), String> {
    let before = counts.statements.load(std::sync::atomic::Ordering::Relaxed);
    let nodes = counts.nodes.load(std::sync::atomic::Ordering::Relaxed);
    let result = future.await.map_err(|e| e.to_string())?;
    Ok((
        result,
        counts.statements.load(std::sync::atomic::Ordering::Relaxed) - before,
        counts.nodes.load(std::sync::atomic::Ordering::Relaxed) - nodes,
    ))
}

/// Heartbeat adds exactly two statements (one if missing); claims and flushes never query nodes.
#[tokio::test]
#[cfg(feature = "sqlite")]
#[ignore = "run alone: SQLite worker SQL counting uses a process-global subscriber"]
async fn node_query_budget() -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let store = fresh_store().await?;
    let counts = SqlCounts::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(counts.clone()))
        .map_err(|e| e.to_string())?;
    let node = uuid::Uuid::now_v7();
    let missing = uuid::Uuid::now_v7();
    assert_eq!(counted(&counts, store.register_node(&node)).await?.1, 1);
    let (_, statements, nodes) = counted(
        &counts,
        store.heartbeat(&node, std::time::Duration::from_secs(72)),
    )
    .await?;
    assert_eq!((statements, nodes), (2, 2));
    let (_, statements, nodes) = counted(
        &counts,
        store.heartbeat(&missing, std::time::Duration::from_secs(72)),
    )
    .await?;
    assert_eq!((statements, nodes), (1, 1));
    // Account for the existing first-claim idempotency maintenance before comparing steady state.
    store
        .claim_tasks(
            &node,
            &[crate::tasks::LaneClaim {
                lane: crate::tasks::DEFAULT_TASK_LANE,
                limit: 1,
                owner: None,
            }],
        )
        .await
        .map_err(|e| e.to_string())?;
    let mut expected = None;
    for status in [0, 1] {
        let measured = measured_cycle(&store, node, status, &counts).await?;
        assert_eq!(measured, (2, 2));
        if let Some(prior) = expected {
            assert_eq!(measured, prior);
        }
        expected = Some(measured);
    }
    cancellation_query_budget(&store, &counts).await?;
    Ok(())
}

/// One or many ordinary cancellations use a bounded read/write pair; repeats only read.
#[cfg(feature = "sqlite")]
async fn cancellation_query_budget(store: &DbTaskStore, counts: &SqlCounts) -> Result<(), String> {
    use crate::tasks::{TaskCommit, TaskId};
    for size in [1, 32] {
        let mut commits = Vec::new();
        for _ in 0..size {
            let id = insert_task(store, 0, None).await?;
            db::sqlx::query(
                "UPDATE vyuh_tasks SET cancelled = false, waiting_children = NULL WHERE id = ?",
            )
            .bind(id)
            .execute(store.pool.as_sqlx())
            .await
            .map_err(|e| e.to_string())?;
            commits.push(TaskCommit::Cancel {
                task_id: TaskId::new(id),
            });
        }
        let (result, queries, nodes) = counted(counts, store.commit_tasks(&commits)).await?;
        assert!(result.rejected.is_empty());
        assert_eq!((queries, nodes), (2, 0));
        let (result, queries, nodes) = counted(counts, store.commit_tasks(&commits)).await?;
        assert_eq!(result.rejected.len(), size);
        assert_eq!((queries, nodes), (1, 0));
    }
    Ok(())
}

/// Pending and abandoned Running tasks use the same normal claim and flush statements.
#[cfg(feature = "sqlite")]
async fn measured_cycle(
    store: &DbTaskStore,
    node: uuid::Uuid,
    status: i16,
    counts: &SqlCounts,
) -> Result<(usize, usize), String> {
    let id = insert_task(&store, status, None).await?;
    db::sqlx::query("UPDATE vyuh_tasks SET cancelled = false, ready_at = NULL, waiting_children = NULL WHERE id = ?")
            .bind(id).execute(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    let claim = crate::tasks::LaneClaim {
        lane: crate::tasks::DEFAULT_TASK_LANE,
        limit: 1,
        owner: None,
    };
    let (poll, reads, nodes) = counted(&counts, store.claim_tasks(&node, &[claim])).await?;
    assert_eq!(nodes, 0);
    let task = poll
        .lanes
        .first()
        .and_then(|lane| lane.tasks.first())
        .ok_or("missing claim")?;
    let commit = crate::tasks::TaskCommit::Outcome {
        node_id: node,
        task_id: task.id,
        attempts: task.attempts,
        lane: crate::tasks::DEFAULT_TASK_LANE,
        owner_token: None,
        outcome: crate::tasks::TaskOutcome::Complete,
    };
    let (_, writes, nodes) = counted(&counts, store.commit_tasks(&[commit])).await?;
    assert_eq!(nodes, 0);
    Ok((reads, writes))
}

/// Desired-schema migration creates precise timestamps and node writes preserve unrelated state.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness URL")]
async fn node_contract() -> Result<(), String> {
    let store = fresh_store().await?;
    exercise(&store).await.map_err(|e| e.to_string())
}

/// Checks missing-node semantics, unchanged-value updates, and duplicate initialization.
async fn exercise(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    let id = uuid::Uuid::now_v7();
    let other = uuid::Uuid::now_v7();
    assert!(
        !store
            .heartbeat(&id, std::time::Duration::from_secs(72))
            .await?
    );
    assert!(rows(store).await?.is_empty());
    store.register_node(&id).await?;
    store.register_node(&other).await?;
    let before = rows(store).await?;
    let original = before.iter().find(|row| row.id == id).unwrap();
    store.register_node(&id).await?;
    let duplicate = rows(store).await?;
    assert_eq!(timestamps(&before), timestamps(&duplicate));
    for _ in 0..10 {
        assert!(
            store
                .heartbeat(&id, std::time::Duration::from_secs(72))
                .await?
        );
    }
    let after = rows(store).await?;
    let updated = after.iter().find(|row| row.id == id).unwrap();
    assert_eq!(updated.started_at, original.started_at);
    assert!(updated.heartbeat_at >= original.heartbeat_at);
    assert_eq!(
        before
            .iter()
            .find(|row| row.id == other)
            .map(|r| r.heartbeat_at),
        after
            .iter()
            .find(|row| row.id == other)
            .map(|r| r.heartbeat_at)
    );
    unchanged_update(store, id).await?;
    Ok(())
}

/// The backend reports an existing row even when assigned its current value.
async fn unchanged_update(store: &DbTaskStore, id: uuid::Uuid) -> Result<(), TaskRuntimeError> {
    use db::DbSession as _;
    #[cfg(feature = "postgres")]
    let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = heartbeat_at WHERE id = $1";
    #[cfg(not(feature = "postgres"))]
    let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = heartbeat_at WHERE id = ?";
    assert_eq!(
        store
            .pool
            .clone()
            .execute(db::Statement::raw(sql).bind(id))
            .await?,
        1
    );
    Ok(())
}

fn timestamps(
    rows: &[TaskNodeRow],
) -> Vec<(uuid::Uuid, chrono::DateTime<Utc>, chrono::DateTime<Utc>)> {
    rows.iter()
        .map(|row| (row.id, row.started_at, row.heartbeat_at))
        .collect()
}

async fn rows(store: &DbTaskStore) -> Result<Vec<TaskNodeRow>, TaskRuntimeError> {
    let table = <TaskNodeRow as db::Model>::table();
    Ok(db::from(&table)
        .sort(table.id.asc())
        .all::<TaskNodeRow>()
        .exec(&mut store.pool.clone())
        .await?)
}

/// Uses only empty in-memory SQLite or an explicitly supplied disposable dbharness database.
async fn fresh_store() -> Result<DbTaskStore, String> {
    #[cfg(feature = "sqlite")]
    let (url, dialect) = ("sqlite::memory:".to_owned(), db::Dialect::Sqlite);
    #[cfg(feature = "postgres")]
    let (url, dialect) = (
        std::env::var("VYUH_TASK_TEST_URL").map_err(|e| e.to_string())?,
        db::Dialect::Postgres,
    );
    #[cfg(feature = "mysql")]
    let (url, dialect) = (
        std::env::var("VYUH_TASK_TEST_URL").map_err(|e| e.to_string())?,
        db::Dialect::Mysql,
    );
    let pool = db::sqlx::pool::PoolOptions::<db::Database>::new()
        .max_connections(1)
        .connect(&url)
        .await
        .map_err(|e| e.to_string())?;
    for sql in node_migration(dialect)? {
        db::sqlx::query(&sql)
            .execute(&pool)
            .await
            .map_err(|e| format!("{e}: {sql}"))?;
    }
    Ok(DbTaskStore::new(
        pool,
        32,
        std::time::Duration::from_secs(300),
        crate::tasks::TaskRegistry::new()
            .with_config(crate::tasks::TaskConf::default())
            .and_then(|registry| registry.store_conf())
            .map_err(|error| error.to_string())?,
    ))
}

/// Plans the node table with a primary key and no auxiliary index.
fn node_migration(dialect: db::Dialect) -> Result<Vec<String>, String> {
    let schema = super::super::schema::task_schema().map_err(|e| e.to_string())?;
    assert!(
        schema
            .tables
            .get("vyuh_task_nodes")
            .unwrap()
            .indexes
            .is_empty()
    );
    let planner = db::gaman::OfflinePlanner::new(dialect);
    let migration = planner
        .make_migration(schema, &[])
        .map_err(|e| e.to_string())?
        .ok_or("missing migration")?;
    planner.sql_migrate(&[migration]).map_err(|e| e.to_string())
}

/// SQLite lifecycle resets affect only selected running rows and preserve durable payloads.
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn claim_return_contract() -> Result<(), String> {
    let store = fresh_store().await?;
    let node = uuid::Uuid::now_v7();
    let other = uuid::Uuid::now_v7();
    store
        .register_node(&node)
        .await
        .map_err(|e| e.to_string())?;
    store
        .register_node(&other)
        .await
        .map_err(|e| e.to_string())?;
    let ids = seed_returns(&store, node, other).await?;
    assert_eq!(
        store
            .return_claims(Some(node))
            .await
            .map_err(|e| e.to_string())?,
        1
    );
    assert_eq!(
        store
            .return_claims(Some(node))
            .await
            .map_err(|e| e.to_string())?,
        0
    );
    let id = ids.first().ok_or("missing fixture")?;
    assert_returned(&store, *id).await?;
    assert_eq!(
        store.return_claims(None).await.map_err(|e| e.to_string())?,
        2
    );
    assert_eq!(
        store.return_claims(None).await.map_err(|e| e.to_string())?,
        0
    );
    assert_untouched(&store, *id).await?;
    Ok(())
}

/// Seeds matching, foreign-owned, ownerless, pending and terminal rows for lifecycle reset tests.
#[cfg(feature = "sqlite")]
async fn seed_returns(
    store: &DbTaskStore,
    node: uuid::Uuid,
    other: uuid::Uuid,
) -> Result<Vec<uuid::Uuid>, String> {
    let mut ids = Vec::new();
    for (status, owner) in [
        (1, Some(node)),
        (1, Some(other)),
        (1, None),
        (0, Some(node)),
        (2, Some(node)),
        (3, Some(node)),
        (4, Some(node)),
    ] {
        ids.push(insert_task(&store, status, owner).await?);
    }
    Ok(ids)
}

/// Seeds every relevant status with payload and scheduling evidence for the reset predicate.
#[cfg(feature = "sqlite")]
async fn insert_task(
    store: &DbTaskStore,
    status: i16,
    owner: Option<uuid::Uuid>,
) -> Result<uuid::Uuid, String> {
    let id = uuid::Uuid::now_v7();
    db::sqlx::query(
        "INSERT INTO vyuh_tasks (id,name,input,status,attempts,step_attempts,node_id,
         ready_at,created_at,updated_at,state,resume_input,last_result,cancelled,
         waiting_children,remaining_completions)
         VALUES (?,'fixture','null',?,17,3,?,'2099-01-01 00:00:00',
         '2000-01-01 00:00:00','2000-01-01 00:00:00',?,?,?,true,'[]',0)",
    )
    .bind(id)
    .bind(status)
    .bind(owner)
    .bind("{\"step\":1}")
    .bind("{\"Ok\":42}")
    .bind("{\"Ok\":null}")
    .execute(store.pool.as_sqlx())
    .await
    .map_err(|e| e.to_string())?;
    Ok(id)
}

/// Checks the reset projection without interpreting or rewriting continuation bytes.
#[cfg(feature = "sqlite")]
async fn assert_returned(store: &DbTaskStore, id: uuid::Uuid) -> Result<(), String> {
    let row = db::from(&DbTaskStore::table())
        .filter(DbTaskStore::table().id.eq(db::val(id)))
        .one::<TaskRow>()
        .exec(&mut store.pool.clone())
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(row.status, TaskStatus::Pending.as_i16());
    assert_eq!(row.node_id, None);
    assert_eq!((row.attempts, row.step_attempts), (17, 3));
    assert_eq!(row.state.as_deref(), Some("{\"step\":1}"));
    assert_eq!(row.resume_input.as_deref(), Some("{\"Ok\":42}"));
    assert_eq!(row.last_result.as_deref(), Some("{\"Ok\":null}"));
    assert!(row.cancelled);
    assert!(row.ready_at.is_some_and(|at| at < Utc::now()));
    assert!(row.updated_at > row.created_at);
    Ok(())
}

/// Unselected rows retain timestamps and claim return does not consume join membership.
#[cfg(feature = "sqlite")]
async fn assert_untouched(store: &DbTaskStore, id: uuid::Uuid) -> Result<(), String> {
    let untouched: i64 = db::sqlx::query_scalar(
        "SELECT COUNT(*) FROM vyuh_tasks WHERE status != 1 AND node_id IS NOT NULL AND updated_at = created_at"
    ).fetch_one(store.pool.as_sqlx()).await.map_err(|e| e.to_string())?;
    assert_eq!(untouched, 4);
    let wait: String =
        db::sqlx::query_scalar("SELECT waiting_children FROM vyuh_tasks WHERE id = ?")
            .bind(id)
            .fetch_one(store.pool.as_sqlx())
            .await
            .map_err(|e| e.to_string())?;
    assert_eq!(wait, "[]");
    Ok(())
}
