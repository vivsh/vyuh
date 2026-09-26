use super::*;
use crate::tasks::store::memory::tests::{claim, commit, conf, record, write};
use crate::tasks::{AbstractTaskStore, TaskCommit, TaskOutcome};

/// Reports ordinary and workflow flush costs at fixed bounded sizes, outside setup time.
#[tokio::test]
#[ignore = "disposable database benchmark; run alone with --nocapture"]
async fn benchmark_flushes() -> Result<(), String> {
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    let store = DbTaskStore {
        batch_size: 250,
        ..store
    };
    for size in [1, 32, 250] {
        for workflow in ["ordinary", "error", "delivery-small", "delivery-large"] {
            let mut times = Vec::with_capacity(50);
            for _ in 0..50 {
                times.push(
                    flush_sample(&store, size, workflow)
                        .await
                        .map_err(|e| e.to_string())?,
                );
            }
            times.sort();
            let total: std::time::Duration = times.iter().copied().sum();
            eprintln!(
                "flush size={size} workflow={workflow} median_us={} p95_us={} tasks_per_second={:.0}",
                times[25].as_micros(),
                times[47].as_micros(),
                size as f64 * 50.0 / total.as_secs_f64()
            );
        }
    }
    Ok(())
}

/// Measures only the atomic outcome flush after all durable fixtures are claimed.
async fn flush_sample(
    store: &DbTaskStore,
    size: usize,
    workflow: &str,
) -> Result<std::time::Duration, TaskError> {
    let mut commits = prepare_flush(store, size, workflow.starts_with("delivery")).await?;
    for commit in &mut commits {
        commit.outcome = match workflow {
            "error" => TaskOutcome::fail("representative safe error"),
            "delivery-small" => TaskOutcome::CompleteWith {
                output: "42".into(),
            },
            "delivery-large" => TaskOutcome::CompleteWith {
                output: serde_json::to_string(&"x".repeat(32_759))?,
            },
            _ => TaskOutcome::Complete,
        };
    }
    let started = std::time::Instant::now();
    store.commit_outcomes("benchmark", &commits).await?;
    let elapsed = started.elapsed();
    let table = DbTaskStore::table();
    db::from(&table)
        .filter(table.name.eq(db::val("workflow".to_owned())))
        .delete()
        .exec(&mut store.pool.clone())
        .await?;
    Ok(elapsed)
}

/// Prepares a bounded flush while leaving fixture creation outside its measurements.
async fn prepare_flush(
    store: &DbTaskStore,
    size: usize,
    workflow: bool,
) -> Result<Vec<TaskCommit>, TaskError> {
    let mut writes = Vec::with_capacity(size * 2);
    let mut commits = Vec::<TaskCommit>::with_capacity(size);
    for _ in 0..size {
        let mut task = record();
        if workflow {
            let mut parent = record();
            parent.status = TaskStatus::Suspended;
            task.parent_id = Some(parent.id);
            task.root_id = Some(parent.id);
            writes.push(write(parent));
        }
        commits.push(commit(task.id, TaskOutcome::Complete));
        writes.push(write(task));
    }
    store.store_tasks(writes).await?;
    let mut claim = claim();
    claim.limit = size;
    for _ in 0..size.div_ceil(store.batch_size) {
        store
            .claim_tasks("benchmark", std::slice::from_ref(&claim))
            .await?;
    }
    Ok(commits)
}

#[derive(Clone, Default)]
struct Queries(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Queries {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        let mut sql = Sql::default();
        event.record(&mut sql);
        if let Ok(mut queries) = self.0.lock() {
            queries.push(sql.0);
        }
    }
}

#[derive(Default)]
struct Sql(String);

impl tracing::field::Visit for Sql {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if matches!(field.name(), "summary" | "db.statement") {
            self.0.push_str(&format!("{value:?}"));
        }
    }
}

/// Counts actual SQL statements: ordinary flushes never issue parent projection queries.
#[tokio::test]
#[ignore = "disposable database query instrumentation; run alone"]
async fn flush_query_counts() -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let queries = Queries::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(queries.clone()))
        .map_err(|e| e.to_string())?;
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    for size in [1usize, 32, 250] {
        for workflow in [false, true] {
            let commits = prepare_flush(&store, size, workflow)
                .await
                .map_err(|e| e.to_string())?;
            queries.0.lock().map_err(|e| e.to_string())?.clear();
            store
                .commit_outcomes("benchmark", &commits)
                .await
                .map_err(|e| e.to_string())?;
            let queries = queries.0.lock().map_err(|e| e.to_string())?;
            let updates = queries
                .iter()
                .filter(|sql| sql.contains("UPDATE vyuh_tasks"))
                .count();
            let expected = size.div_ceil(store.batch_size) * if workflow { 2 } else { 1 };
            assert_eq!(updates, expected, "{queries:?}");
            eprintln!(
                "SQL size={size} workflow={workflow}: {} statements, {updates} task updates",
                queries.len()
            );
            drop(queries);
            let table = DbTaskStore::table();
            db::from(&table)
                .filter(table.name.eq(db::val("workflow".to_owned())))
                .delete()
                .exec(&mut store.pool.clone())
                .await
                .map_err(|e| e.to_string())?;
        }
        spawn_queries(&store, &queries, size).await?;
    }
    queries.0.lock().map_err(|e| e.to_string())?.clear();
    store
        .store_tasks(vec![write(record())])
        .await
        .map_err(|e| e.to_string())?;
    let sql = queries.0.lock().map_err(|e| e.to_string())?;
    assert!(!sql.iter().any(|sql| sql.contains("vyuh_task_lane_locks")));
    assert!(
        !sql.iter()
            .any(|sql| sql.contains("SELECT") && sql.contains("vyuh_tasks"))
    );
    Ok(())
}

/// Spawn insertion scales by bounded chunks without a per-parent lookup or write.
async fn spawn_queries(store: &DbTaskStore, queries: &Queries, size: usize) -> Result<(), String> {
    let mut commits = prepare_flush(store, size, false)
        .await
        .map_err(|e| e.to_string())?;
    for commit in &mut commits {
        commit.outcome = TaskOutcome::Spawn {
            state: "null".into(),
            child: write(record()),
        };
    }
    queries.0.lock().map_err(|e| e.to_string())?.clear();
    store
        .commit_outcomes("benchmark", &commits)
        .await
        .map_err(|e| e.to_string())?;
    let sql = queries.0.lock().map_err(|e| e.to_string())?;
    let count = |operation: &str| {
        sql.iter()
            .filter(|sql| sql.contains(&format!("{operation} vyuh_tasks")))
            .count()
    };
    let chunks = size.div_ceil(store.batch_size);
    assert_eq!(count("UPDATE"), chunks, "{sql:?}");
    assert_eq!(count("INSERT INTO"), chunks, "{sql:?}");
    assert!(!sql.iter().any(|sql| sql.contains("vyuh_task_lane_locks")));
    eprintln!(
        "SQL size={size} spawn: {} statements, {} updates, {} inserts",
        sql.len(),
        count("UPDATE"),
        count("INSERT INTO")
    );
    drop(sql);
    let table = DbTaskStore::table();
    db::from(&table)
        .filter(table.name.eq(db::val("workflow".to_owned())))
        .delete()
        .exec(&mut store.pool.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
