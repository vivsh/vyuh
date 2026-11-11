use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::*;
use crate::{
    Site, SiteConf, bundles,
    callables::Data,
    db::DbSession as _,
    tasks::{TaskConf, TaskDefinition, TaskIdempotency, Tasks},
    testing::TestSite,
};

#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct Job {
    key: u32,
    value: u32,
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct Plain(u32);

fn job(key: u32) -> Job {
    Job { key, value: key }
}

/// The live-backend tests provision a fresh database from the task schema.
async fn fixture() -> Result<(db::testing::TestDatabase, TestSite, Arc<AtomicUsize>), String> {
    #[cfg(feature = "sqlite")]
    let (conf, dialect) = (db::DbConf::default(), db::Dialect::Sqlite);
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    let conf = db::DbConf::from_url(&std::env::var("VYUH_TX_TEST_ADMIN_URL").map_err(error)?)
        .map_err(error)?;
    #[cfg(feature = "postgres")]
    let dialect = db::Dialect::Postgres;
    #[cfg(feature = "mysql")]
    let dialect = db::Dialect::Mysql;
    let database = db::testing::setup(conf).create().await.map_err(error)?;
    install_schema(database.pool(), dialect).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let handler = move |Data(input): Data<Job>| {
        invoked.fetch_add(1, Ordering::Relaxed);
        async move { input.value }
    };
    let runtime = TestSite::from_pool(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .poll_interval(Duration::from_millis(5))
                .fallback_poll_interval(Duration::from_millis(20)),
        ),
        bundles::bundle([
            bundles::work(
                handler,
                TaskDefinition::new("tx_job")
                    .idempotency(TaskIdempotency::new("1", |input: &Job| {
                        input.key.to_string()
                    })),
            ),
            bundles::work(|_: Data<Plain>| async {}, TaskDefinition::new("tx_plain")),
        ]),
        database.pool(),
    )
    .await
    .map_err(error)?;
    Ok((database, runtime, calls))
}

/// Installs the production task schema and one independent application table.
async fn install_schema(pool: &db::DbPool, dialect: db::Dialect) -> Result<(), String> {
    let planner = db::gaman::OfflinePlanner::new(dialect);
    let schema = super::super::schema::task_schema().map_err(error)?;
    let migration = planner
        .make_migration(schema, &[])
        .map_err(error)?
        .ok_or("missing schema")?;
    let mut conn = pool.as_sqlx().acquire().await.map_err(error)?;
    for sql in planner.sql_migrate(&[migration]).map_err(error)? {
        db::sqlx::query(&sql)
            .execute(&mut *conn)
            .await
            .map_err(error)?;
    }
    db::sqlx::query("CREATE TABLE submit_tx_domain (value INTEGER NOT NULL)")
        .execute(&mut *conn)
        .await
        .map_err(error)?;
    Ok(())
}

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

/// The full distinct-key limit works in both submit paths, including reverse-order replay.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn max_keyed_batch() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let pool = runtime.site().db();
    let limit = crate::tasks::config::MAX_BATCH_SIZE as u32;
    let ordinary = tasks
        .submit_many((0..limit).map(job))
        .await
        .map_err(error)?;
    assert_eq!(ordinary.len(), limit as usize);
    assert!(
        ordinary
            .iter()
            .all(|item| matches!(item, TaskReceipt::Queued(_)))
    );
    let mut tx = pool.begin().await.map_err(error)?;
    let staged = tasks
        .submit_many_tx(&mut tx, (limit..2 * limit).map(job))
        .await
        .map_err(error)?;
    assert_eq!(staged.len(), limit as usize);
    assert!(
        staged
            .iter()
            .all(|item| matches!(item, TaskReceipt::Queued(_)))
    );
    let replay = tasks
        .submit_many_tx(&mut tx, (limit..2 * limit).rev().map(job))
        .await
        .map_err(error)?;
    assert_eq!(replay.len(), limit as usize);
    for (original, duplicate) in staged.iter().rev().zip(replay) {
        assert_eq!(duplicate, TaskReceipt::Existing(original.id()));
    }
    tx.commit().await.map_err(error)?;
    let counts: (i64, i64) = db::sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM vyuh_tasks), (SELECT COUNT(*) FROM vyuh_task_idempotency)",
    )
    .fetch_one(pool.as_sqlx())
    .await
    .map_err(error)?;
    let count = i64::from(limit) * 2;
    assert_eq!(counts, (count, count));
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Mixed groups match exact pairs even when keys overlap, and return owners by ID.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn mixed_key_owner_pairs() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let pool = runtime.site().db();
    let mut tx = pool.begin().await.map_err(error)?;
    let now = statement_now(&mut tx).await.map_err(error)?;
    let mut stored = Vec::new();
    let mut requested = Vec::new();
    for index in 0..250 {
        let handler = format!("handler_{index}");
        let own_key = index.to_string();
        let excluded = ((index + 1) % 250).to_string();
        requested.push(key_owner(&handler, "shared", now));
        requested.push(key_owner(&handler, &own_key, now));
        stored.push(key_owner(&handler, &excluded, now));
    }
    stored.extend(requested.iter().cloned());
    upsert_key_owners(&mut tx, &stored, 250)
        .await
        .map_err(error)?;
    tx.commit().await.map_err(error)?;
    let mut tx = pool.begin().await.map_err(error)?;
    requested.reverse();
    let owners = load_key_owners(&mut tx, &requested).await.map_err(error)?;
    assert_eq!(owners.len(), requested.len());
    let mut expected = requested.iter().map(|row| row.id).collect::<Vec<_>>();
    expected.sort_unstable();
    assert!(owners.iter().map(|row| row.id).eq(expected));
    tx.rollback().await.map_err(error)?;
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

fn key_owner(task_name: &str, key: &str, now: DateTime<Utc>) -> TaskIdempotencyRow {
    TaskIdempotencyRow {
        id: uuid::Uuid::now_v7(),
        task_id: uuid::Uuid::now_v7(),
        task_name: task_name.to_owned(),
        key_value: key.to_owned(),
        fingerprint: "fixture".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    }
}

/// Inserts a business row through exactly the connection used to stage the task.
async fn domain_write(tx: &mut db::DbTransaction<'_>) -> Result<(), String> {
    tx.execute(db::Statement::raw(
        "INSERT INTO submit_tx_domain VALUES (42)",
    ))
    .await
    .map_err(error)?;
    Ok(())
}

async fn domain_count(site: &Site) -> Result<i64, String> {
    db::sqlx::query_scalar("SELECT COUNT(*) FROM submit_tx_domain")
        .fetch_one(site.db().as_sqlx())
        .await
        .map_err(error)
}

/// Reads committed state through the worker's normal inspection path.
async fn wait_result(site: &Site, id: TaskId) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(info) = site.tasks().get(id).await.map_err(error)?
                && info.status() == TaskStatus::Succeeded
            {
                assert_eq!(info.last_result::<u32>().map_err(error)?, Some(Ok(1)));
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(error)?
}

/// An active worker cannot see staged tasks; rollback undoes both writes and releases the key.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn atomic_visibility() -> Result<(), String> {
    let (database, runtime, calls) = fixture().await?;
    runtime.start_runtime().await.map_err(error)?;
    let site = runtime.site();
    let pool = site.db();
    let tasks = site.tasks();
    let mut tx = pool.begin().await.map_err(error)?;
    domain_write(&mut tx).await?;
    let rolled_back = tasks.submit_tx(&mut tx, job(1)).await.map_err(error)?;
    assert!(matches!(rolled_back, TaskReceipt::Queued(_)));
    let staged: i64 = tx
        .fetch_scalar(db::Statement::raw(
            "SELECT COUNT(*) FROM vyuh_tasks WHERE status = 0 AND node_id IS NULL",
        ))
        .await
        .map_err(error)?;
    assert_eq!(staged, 1);
    assert!(tasks.get(rolled_back.id()).await.map_err(error)?.is_none());
    assert_eq!(domain_count(site).await?, 0);
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    tx.rollback().await.map_err(error)?;

    let mut tx = pool.begin().await.map_err(error)?;
    domain_write(&mut tx).await?;
    let committed = tasks.submit_tx(&mut tx, job(1)).await.map_err(error)?;
    assert!(matches!(committed, TaskReceipt::Queued(_)));
    assert_ne!(rolled_back.id(), committed.id());
    assert!(tasks.get(rolled_back.id()).await.map_err(error)?.is_none());
    tx.commit().await.map_err(error)?;
    wait_result(site, committed.id()).await?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(domain_count(site).await?, 1);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Bulk receipts preserve order and caller savepoint rollback undoes a failed key reservation.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn bulk_and_conflict() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let pool = runtime.site().db();
    check_batch_limits(&pool, &tasks).await?;
    let mut tx = pool.begin().await.map_err(error)?;
    let receipts = tasks
        .submit_many_tx(&mut tx, [job(2), job(1), job(2)])
        .await
        .map_err(error)?;
    let [
        TaskReceipt::Queued(two),
        TaskReceipt::Queued(one),
        TaskReceipt::Existing(dup),
    ] = receipts.as_slice()
    else {
        return Err("unexpected bulk receipts".into());
    };
    assert_eq!(two, dup);
    tx.commit().await.map_err(error)?;
    let info = tasks
        .get(*one)
        .await
        .map_err(error)?
        .ok_or("missing ordered task")?;
    assert_eq!(info.input::<Job>().map_err(error)?.key, 1);

    let mut tx = pool.begin().await.map_err(error)?;
    domain_write(&mut tx).await?;
    let mut savepoint = tx.begin_nested().await.map_err(error)?;
    assert!(matches!(
        tasks.submit_many_tx(&mut savepoint, [job(3), Job { key: 1, value: 99 }]).await,
        Err(TaskRuntimeError::IdempotencyConflict(id)) if id == *one
    ));
    savepoint.rollback().await.map_err(error)?;
    assert!(matches!(
        tasks.submit_tx(&mut tx, job(3)).await.map_err(error)?,
        TaskReceipt::Queued(_)
    ));
    tx.commit().await.map_err(error)?;
    assert_eq!(domain_count(runtime.site()).await?, 1);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Empty and oversized batches do not insert tasks or reserve keys.
async fn check_batch_limits(pool: &db::DbPool, tasks: &Tasks) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(error)?;
    assert!(
        tasks
            .submit_many_tx(&mut tx, std::iter::empty::<Job>())
            .await
            .map_err(error)?
            .is_empty()
    );
    assert!(matches!(
        tasks.submit_many_tx(&mut tx, (0..=10_000).map(job)).await,
        Err(TaskRuntimeError::InvalidOptions(_))
    ));
    tx.rollback().await.map_err(error)
}

/// Competing transactions cannot create two tasks for the same idempotent input.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn concurrent_duplicate() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let pool = runtime.site().db();
    let mut first = pool.begin().await.map_err(error)?;
    let receipt = tasks.submit_tx(&mut first, job(1)).await.map_err(error)?;
    assert!(matches!(receipt, TaskReceipt::Queued(_)));
    let (ready, started) = tokio::sync::oneshot::channel();
    let peer_pool = pool.clone();
    let peer = tokio::spawn(async move {
        let mut tx = peer_pool.begin().await.map_err(error)?;
        let _ = ready.send(());
        let result = tasks.submit_tx(&mut tx, job(1)).await.map_err(error)?;
        tx.commit().await.map_err(error)?;
        Ok::<_, String>(result)
    });
    started.await.map_err(error)?;
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!peer.is_finished());
    first.commit().await.map_err(error)?;
    let duplicate = tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .map_err(error)?
        .map_err(error)??;
    assert_eq!(duplicate, TaskReceipt::Existing(receipt.id()));
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// Rejects the final row, after at least one full insert chunk can succeed.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn inject_insert_failure(pool: &db::DbPool) -> Result<(), String> {
    use db::Record as _;
    let columns = TaskRow::record_insert_column_names().len();
    let chunk = db::backend::PARAMETER_LIMIT / columns;
    assert!(chunk < 4_000);
    #[cfg(feature = "postgres")]
    let fault = "ALTER TABLE vyuh_tasks ADD CONSTRAINT submit_tx_fault CHECK (name <> 'tx_plain' OR input <> '4000')";
    #[cfg(feature = "sqlite")]
    let fault = "CREATE TRIGGER submit_tx_fault BEFORE INSERT ON vyuh_tasks
                 WHEN NEW.name = 'tx_plain' AND NEW.input = '4000'
                 BEGIN SELECT RAISE(ABORT, 'injected insert failure'); END";
    db::sqlx::query(fault)
        .execute(pool.as_sqlx())
        .await
        .map_err(error)?;
    Ok(())
}

/// A database failure after the first insert chunk leaves nothing after caller rollback.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires an isolated dbharness admin URL"
)]
async fn rollback_partial_batch() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let pool = runtime.site().db();
    let tasks = runtime.site().tasks();
    inject_insert_failure(&pool).await?;
    let mut tx = pool.begin().await.map_err(error)?;
    domain_write(&mut tx).await?;
    tasks.submit_tx(&mut tx, job(0)).await.map_err(error)?;
    let failure = tasks
        .submit_many_tx(&mut tx, (1..=4_000).map(Plain))
        .await
        .err()
        .ok_or("insert failure was not raised")?;
    #[cfg(feature = "postgres")]
    assert!(
        matches!(
            &failure,
            TaskRuntimeError::StoreError(db::DbError::Integrity { constraint: Some(name), .. })
                if name == "submit_tx_fault"
        ),
        "{failure}"
    );
    #[cfg(feature = "sqlite")]
    assert!(
        failure.to_string().contains("injected insert failure"),
        "{failure}"
    );
    #[cfg(feature = "sqlite")]
    {
        let staged: i64 = tx
            .fetch_scalar(db::Statement::raw("SELECT COUNT(*) FROM vyuh_tasks"))
            .await
            .map_err(error)?;
        assert!(
            staged > 1,
            "the first task insert chunk must have succeeded"
        );
    }
    tx.rollback().await.map_err(error)?;
    let writes: (i64, i64) = db::sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM vyuh_tasks), (SELECT COUNT(*) FROM vyuh_task_idempotency)",
    )
    .fetch_one(pool.as_sqlx())
    .await
    .map_err(error)?;
    assert_eq!(writes, (0, 0));
    assert_eq!(domain_count(runtime.site()).await?, 0);
    assert!(matches!(
        tasks.submit(job(0)).await.map_err(error)?,
        TaskReceipt::Queued(_)
    ));
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}

/// PostgreSQL's existing wake hint is suppressed on rollback and delivered only after commit.
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires an isolated dbharness admin URL"]
async fn wake_is_commit_bound() -> Result<(), String> {
    let (database, runtime, _) = fixture().await?;
    let tasks = runtime.site().tasks();
    let pool = runtime.site().db();
    let mut listener = db::sqlx::postgres::PgListener::connect_with(pool.as_sqlx())
        .await
        .map_err(error)?;
    listener
        .listen(super::super::notify::CHANNEL)
        .await
        .map_err(error)?;
    let delay = Duration::from_millis(40);
    let mut tx = pool.begin().await.map_err(error)?;
    tasks.submit_tx(&mut tx, job(1)).await.map_err(error)?;
    assert!(tokio::time::timeout(delay, listener.recv()).await.is_err());
    tx.rollback().await.map_err(error)?;
    assert!(tokio::time::timeout(delay, listener.recv()).await.is_err());
    let mut tx = pool.begin().await.map_err(error)?;
    tasks.submit_tx(&mut tx, job(1)).await.map_err(error)?;
    assert!(tokio::time::timeout(delay, listener.recv()).await.is_err());
    tx.commit().await.map_err(error)?;
    let event = tokio::time::timeout(Duration::from_secs(2), listener.recv())
        .await
        .map_err(error)?
        .map_err(error)?;
    assert_eq!(event.channel(), super::super::notify::CHANNEL);
    drop(listener);
    runtime.shutdown_and_wait().await;
    database.teardown().await.map_err(error)
}
