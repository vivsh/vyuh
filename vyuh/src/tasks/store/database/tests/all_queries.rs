use super::*;
use crate::tasks::TaskId;

/// One result read per bounded shared chunk, never per parent, with no sibling read at completion.
#[tokio::test]
#[ignore = "query instrumentation must run alone"]
async fn all_query_counts() -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let queries = Queries::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(queries.clone()))
        .map_err(|e| e.to_string())?;
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    for size in [1usize, 32, 256] {
        let (parent, children) = prepare_group(&store, size)
            .await
            .map_err(|e| e.to_string())?;
        queries.0.lock().map_err(|e| e.to_string())?.clear();
        store
            .commit_outcomes("children", &children)
            .await
            .map_err(|e| e.to_string())?;
        let completion = queries.0.lock().map_err(|e| e.to_string())?.clone();
        assert_eq!(result_reads(&completion), 0, "{completion:?}");
        queries.0.lock().map_err(|e| e.to_string())?.clear();
        store
            .claim_tasks("parent", &[claim()])
            .await
            .map_err(|e| e.to_string())?;
        let claims = queries.0.lock().map_err(|e| e.to_string())?.clone();
        assert_eq!(result_reads(&claims), size.div_ceil(32), "{claims:?}");
        eprintln!(
            "all size={size} completion_statements={} claim_statements={} result_reads={}",
            completion.len(),
            claims.len(),
            result_reads(&claims)
        );
        store
            .commit_outcomes("parent", &[commit(parent, TaskOutcome::Complete)])
            .await
            .map_err(|e| e.to_string())?;
    }
    let mut children = Vec::new();
    for _ in 0..8 {
        children.extend(prepare_group(&store, 3).await.map_err(|e| e.to_string())?.1);
    }
    store
        .commit_outcomes("children", &children)
        .await
        .map_err(|e| e.to_string())?;
    queries.0.lock().map_err(|e| e.to_string())?.clear();
    store
        .claim_tasks("parents", &[claim()])
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        result_reads(&queries.0.lock().map_err(|e| e.to_string())?),
        1
    );
    Ok(())
}

fn result_reads(queries: &[String]) -> usize {
    queries
        .iter()
        .filter(|sql| {
            sql.contains("SELECT")
                && sql.contains("last_result")
                && !sql.contains("resume_input")
                && !sql.contains("locked_by")
        })
        .count()
}

/// Measures completed group counter transactions and subsequent claim materialization separately.
#[tokio::test]
#[ignore = "disposable database benchmark; run alone without concurrent compilation"]
async fn benchmark_all() -> Result<(), String> {
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    for size in [1usize, 32, 256] {
        let (mut completion, mut claiming) = (Vec::new(), Vec::new());
        for sample in 0..35 {
            let (parent, children) = prepare_group(&store, size)
                .await
                .map_err(|e| e.to_string())?;
            let start = std::time::Instant::now();
            store
                .commit_outcomes("children", &children)
                .await
                .map_err(|e| e.to_string())?;
            let done = start.elapsed();
            let start = std::time::Instant::now();
            store
                .claim_tasks("parent", &[claim()])
                .await
                .map_err(|e| e.to_string())?;
            if sample >= 5 {
                completion.push(done);
                claiming.push(start.elapsed());
            }
            store
                .commit_outcomes("parent", &[commit(parent, TaskOutcome::Complete)])
                .await
                .map_err(|e| e.to_string())?;
        }
        for (phase, mut times) in [("completion", completion), ("claim", claiming)] {
            times.sort();
            let total: std::time::Duration = times.iter().sum();
            eprintln!(
                "all size={size} phase={phase} median_us={} p95_us={} children_per_second={:.0}",
                times[15].as_micros(),
                times[28].as_micros(),
                (size * times.len()) as f64 / total.as_secs_f64()
            );
        }
    }
    Ok(())
}

/// Creates one complete fan-out outside measurement and claims children in ordinary bounded pages.
async fn prepare_group(
    store: &DbTaskStore,
    size: usize,
) -> Result<(TaskId, Vec<TaskCommit>), TaskRuntimeError> {
    let parent = flow_record().id;
    let mut row = flow_record();
    row.id = parent;
    store.store_tasks(vec![write(row)]).await?;
    store.claim_tasks("parent", &[claim()]).await?;
    let children = (0..size).map(|_| write(record())).collect::<Vec<_>>();
    let commits = children
        .iter()
        .map(|child| commit(child.record.id, TaskOutcome::Complete))
        .collect();
    store
        .commit_outcomes(
            "parent",
            &[commit(
                parent,
                TaskOutcome::All {
                    state: "0".into(),
                    children,
                },
            )],
        )
        .await?;
    for _ in 0..size.div_ceil(store.batch_size) {
        store.claim_tasks("children", &[claim()]).await?;
    }
    Ok((parent, commits))
}
