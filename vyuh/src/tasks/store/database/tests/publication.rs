use super::*;
use crate::tasks::store::publication_tests as fixture;
use crate::tasks::{TaskOutcome, TaskStore};
use fixture::error;
use std::time::{Duration, Instant};

/// Applies the real desired schema to a disposable database or SQLite file.
async fn store() -> Result<(DbTaskStore, tempfile::TempDir), String> {
    let dir = tempfile::tempdir().map_err(error)?;
    let pool = test_pool(&dir).await?;
    #[cfg(feature = "sqlite")]
    let dialect = db::Dialect::Sqlite;
    #[cfg(feature = "postgres")]
    let dialect = db::Dialect::Postgres;
    #[cfg(feature = "mysql")]
    let dialect = db::Dialect::Mysql;
    let planner = db::gaman::OfflinePlanner::new(dialect);
    let schema = super::super::schema::task_schema().map_err(error)?;
    let migration = planner
        .make_migration(schema, &[])
        .map_err(error)?
        .ok_or("missing schema")?;
    let mut setup = pool.acquire().await.map_err(error)?;
    for sql in planner.sql_migrate(&[migration]).map_err(error)? {
        db::sqlx::query(&sql)
            .execute(&mut *setup)
            .await
            .map_err(|cause| format!("{cause}: {sql}"))?;
    }
    drop(setup);
    let store = DbTaskStore::new(
        pool,
        1024,
        Duration::from_secs(300),
        fixture::configuration().map_err(error)?,
    );
    Ok((store, dir))
}

#[cfg(feature = "sqlite")]
async fn test_pool(dir: &tempfile::TempDir) -> Result<db::sqlx::Pool<db::Database>, String> {
    use db::sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
    let options = SqliteConnectOptions::new()
        .filename(dir.path().join("publication.sqlite"))
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    db::sqlx::pool::PoolOptions::<db::Database>::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .map_err(error)
}

/// Keeps independent PostgreSQL test runs in fresh schemas in the disposable harness.
#[cfg(feature = "postgres")]
async fn test_pool(_: &tempfile::TempDir) -> Result<db::sqlx::Pool<db::Database>, String> {
    let url = std::env::var("VYUH_TOPIC_TEST_URL").map_err(error)?;
    let schema = format!("topic_{}", Uuid::now_v7().simple());
    let primary = db::sqlx::Pool::<db::Database>::connect(&url)
        .await
        .map_err(error)?;
    db::sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&primary)
        .await
        .map_err(error)?;
    primary.close().await;
    let options = url
        .parse::<db::sqlx::postgres::PgConnectOptions>()
        .map_err(error)?
        .options([("search_path", schema.as_str())]);
    db::sqlx::pool::PoolOptions::<db::Database>::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .map_err(error)
}

#[cfg(feature = "mysql")]
async fn test_pool(_: &tempfile::TempDir) -> Result<db::sqlx::Pool<db::Database>, String> {
    let url = std::env::var("VYUH_TOPIC_TEST_URL").map_err(error)?;
    db::sqlx::pool::PoolOptions::<db::Database>::new()
        .max_connections(4)
        .connect(&url)
        .await
        .map_err(error)
}

/// Covers the actual schema, fenced lifecycle writes, and two competing publishers.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness URL"
)]
async fn database_publication() -> Result<(), String> {
    let (store, _dir) = store().await?;
    let node = Uuid::now_v7();
    store.initialize().await.map_err(error)?;
    store.register_node(&node).await.map_err(error)?;
    let ids = fixture::prepare(&store, node, 1, fixture::wait_outcome("race")).await?;
    let (a, b) = tokio::join!(
        store.publish("race", r#"{"Ok":"a"}"#),
        store.publish("race", r#"{"Ok":"b"}"#)
    );
    assert_eq!(a.map_err(error)?.resumed + b.map_err(error)?.resumed, 1);
    let task = store
        .get_task(*ids.first().ok_or("missing fixture id")?)
        .await
        .map_err(error)?
        .ok_or("missing task")?;
    let output = fixture::output(
        &task_for_claim(&store, node).await?,
        crate::tasks::DEFAULT_TASK_LANE,
        TaskOutcome::Complete,
    )?;
    assert_eq!(output.task_id(), task.id);
    store.commit_tasks(&[output]).await.map_err(error)?;
    fixture::contract(&store).await
}

/// A failed publication cannot consume even the waits updated before the error.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn publication_rollback() -> Result<(), String> {
    let (store, _dir) = store().await?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    let ids = fixture::prepare(&store, node, 2, fixture::wait_outcome("rollback")).await?;
    let last = ids
        .iter()
        .map(|id| id.into_uuid())
        .max()
        .ok_or("missing wait id")?;
    let sql = format!(
        "CREATE TRIGGER reject_one BEFORE UPDATE OF wait_topic ON vyuh_tasks
        WHEN OLD.id = X'{}' AND OLD.wait_topic IS NOT NULL AND NEW.wait_topic IS NULL
        BEGIN SELECT RAISE(ABORT, 'fixture refuses publication'); END",
        last.simple()
    );
    let pool = store.pool.as_sqlx();
    db::sqlx::query(&sql).execute(pool).await.map_err(error)?;
    let input = r#"{"Ok":42}"#;
    assert!(store.publish("rollback", input).await.is_err());
    for id in ids {
        let row = store
            .get_task(id)
            .await
            .map_err(error)?
            .ok_or("missing waiter")?;
        assert_eq!(row.status, crate::tasks::TaskStatus::Suspended);
        assert!(row.resume_input.is_none());
    }
    db::sqlx::query("DROP TRIGGER reject_one")
        .execute(pool)
        .await
        .map_err(error)?;
    assert_eq!(
        store
            .publish("rollback", input)
            .await
            .map_err(error)?
            .resumed,
        2
    );
    Ok(())
}

async fn task_for_claim(
    store: &DbTaskStore,
    node: Uuid,
) -> Result<crate::tasks::TaskRecord, String> {
    let poll = store
        .claim_tasks(
            &node,
            &[fixture::request(crate::tasks::DEFAULT_TASK_LANE, 1)],
        )
        .await
        .map_err(error)?;
    poll.lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .next()
        .ok_or("claim empty".into())
}

/// Uses a full-size, low-compressibility envelope so PostgreSQL TOAST is not benchmarked on runs of one byte.
fn payload() -> Result<String, String> {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut seed = 0x7c3b_3bcc_56e4_5ea9_u64;
    let mut result = String::from(r#"{"Ok":""#);
    for _ in 0..(32768 - result.len() - 2) {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        result.push(char::from(
            *alphabet.get((seed % 64) as usize).ok_or("alphabet")?,
        ));
    }
    result.push_str(r#""}"#);
    assert_eq!(result.len(), 32768);
    crate::tasks::result::validate_resume(&result).map_err(error)?;
    Ok(result)
}

/// Measures one ordinary persisted task on a lane unrelated to the publication.
async fn roundtrip(store: &DbTaskStore, node: Uuid) -> Result<Duration, String> {
    let start = Instant::now();
    let write = fixture::write(fixture::OTHER, 0).map_err(error)?;
    retry_busy(|| store.store_tasks(vec![write.clone()])).await?;
    let requests = [fixture::request(fixture::OTHER, 1)];
    let row = claim_ready(store, node, &requests).await?;
    let commits = [fixture::output(
        &row,
        fixture::OTHER,
        TaskOutcome::Complete,
    )?];
    retry_busy(|| store.commit_tasks(&commits)).await?;
    Ok(start.elapsed())
}

/// Times readiness delays on the unrelated lane but bounds anomalous fixture polling.
async fn claim_ready(
    store: &DbTaskStore,
    node: Uuid,
    requests: &[crate::tasks::LaneClaim],
) -> Result<crate::tasks::TaskRecord, String> {
    for _ in 0..100 {
        let poll = retry_busy(|| store.claim_tasks(&node, requests)).await?;
        if let Some(row) = poll.lanes.into_iter().flat_map(|lane| lane.tasks).next() {
            return Ok(row);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("unrelated task did not become ready".into())
}

/// Includes SQLite busy retries in end-to-end time without replacing production pacing.
async fn retry_busy<T, E: std::fmt::Display, Fut: std::future::Future<Output = Result<T, E>>>(
    mut operation: impl FnMut() -> Fut,
) -> Result<T, String> {
    let mut retries = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(cause) => {
                let message = error(cause);
                if !cfg!(feature = "sqlite")
                    || !message.contains("SQLite is busy")
                    || retries == 100
                {
                    return Err(message);
                }
                retries += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

/// Compares equal starting audiences, warming both caps and alternating sample order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit store benchmark; use an isolated disposable database"]
async fn publication_benchmark() -> Result<(), String> {
    let (store, _dir) = store().await?;
    store.initialize().await.map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    let payload = payload()?;
    let mut low = (Vec::new(), Vec::new());
    let mut high = (Vec::new(), Vec::new());
    for (i, limit) in [64, 512, 512, 64].into_iter().cycle().take(44).enumerate() {
        let (publish, unrelated) = sample(&store, node, &payload, limit, i).await?;
        if i >= 4 {
            let bucket = if limit == 64 { &mut low } else { &mut high };
            bucket.0.push(publish);
            bucket.1.extend(unrelated);
        }
    }
    for (limit, (publish, unrelated)) in [(64, low), (512, high)] {
        eprintln!(
            "limit={limit} publish={:?} unrelated={:?}",
            statistics(publish)?,
            statistics(unrelated)?
        );
    }
    Ok(())
}

/// Pairs one publication with unrelated store traffic and clears the fixture between samples.
async fn sample(
    store: &DbTaskStore,
    node: Uuid,
    payload: &str,
    limit: usize,
    i: usize,
) -> Result<(Duration, Vec<Duration>), String> {
    let topic = format!("benchmark:{i}");
    let ids = fixture::prepare(store, node, 512, fixture::wait_outcome(&topic)).await?;
    let work = async {
        let mut latencies = Vec::new();
        for _ in 0..4 {
            latencies.push(roundtrip(store, node).await?);
        }
        Ok::<_, String>(latencies)
    };
    let publish = async {
        let started = Instant::now();
        let result = store
            .publish_bounded(topic.as_bytes(), payload, limit)
            .await
            .map_err(error)?;
        assert_eq!(result.resumed, limit as u64);
        assert_eq!(result.more, limit < 512);
        Ok::<_, String>(started.elapsed())
    };
    let (unrelated, elapsed) = tokio::join!(work, publish);
    let commits = ids
        .into_iter()
        .map(|task_id| crate::tasks::TaskCommit::Cancel { task_id })
        .collect::<Vec<_>>();
    store.commit_tasks(&commits).await.map_err(error)?;
    Ok((elapsed?, unrelated?))
}

fn statistics(mut times: Vec<Duration>) -> Result<(usize, f64, f64), String> {
    times.sort_unstable();
    let pick = |percent: usize| {
        times
            .get((times.len() * percent).div_ceil(100).saturating_sub(1))
            .map(|time| time.as_secs_f64() * 1000.0)
            .ok_or_else(|| "missing benchmark samples".to_owned())
    };
    Ok((times.len(), pick(50)?, pick(95)?))
}

#[derive(Clone, Default)]
struct TopicSql(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for TopicSql {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "sqlx::query" {
            event.record(&mut self.clone());
        }
    }
}

impl tracing::field::Visit for TopicSql {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "db.statement"
            && let Ok(mut statements) = self.0.lock()
        {
            statements.push(format!("{value:?}"));
        }
    }
}

/// Checks actual SQL from ordinary work while a live topic registration exists.
#[tokio::test]
#[ignore = "run alone; SQLite driver's query tracing needs a global subscriber"]
async fn publication_ordinary_trace() -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let (store, _dir) = store().await?;
    store.initialize().await.map_err(error)?;
    let node = Uuid::now_v7();
    store.register_node(&node).await.map_err(error)?;
    fixture::prepare(&store, node, 1, fixture::wait_outcome("live")).await?;
    let trace = TopicSql::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(trace.clone()))
        .map_err(error)?;
    roundtrip(&store, node).await?;
    let statements = trace.0.lock().map_err(error)?;
    assert!(statements.iter().any(|sql| sql.contains("INSERT")));
    assert!(statements.iter().any(|sql| sql.contains("UPDATE")));
    assert!(
        statements
            .iter()
            .all(|sql| !sql.contains("wait_topic") && !sql.contains("vyuh_task_waits"))
    );
    eprintln!("ordinary SQL events: {}", statements.len());
    Ok(())
}
