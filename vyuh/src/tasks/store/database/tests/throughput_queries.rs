use super::*;

/// Counts real statements, including amortized cleanup, rather than facade calls.
#[tokio::test]
#[ignore = "disposable database query instrumentation; run alone"]
async fn throughput_query_counts() -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let queries = Queries::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(queries.clone()))
        .map_err(|error| error.to_string())?;
    let store = DbTaskStore {
        batch_size: 256,
        ..store().await?
    };
    store
        .initialize(conf())
        .await
        .map_err(|error| error.to_string())?;
    queries.0.lock().map_err(|error| error.to_string())?.clear();
    for _ in 0..32 {
        store
            .tick("owner", &[], &[], &[])
            .await
            .map_err(|error| error.to_string())?;
    }
    let sql = take_queries(&queries)?;
    assert_eq!(
        sql.iter()
            .filter(|query| query.contains("vyuh_task_idempotency"))
            .count(),
        1,
        "{sql:?}"
    );
    assert_eq!(sql.len(), 33, "{sql:?}");
    for size in [1, 32, 256] {
        settled_count(&store, &queries, size).await?;
        mixed_count(&store, &queries, size).await?;
    }
    Ok(())
}

/// Mixed turns share observations and combine claim evidence without moving it before writes.
async fn mixed_count(store: &DbTaskStore, queries: &Queries, size: usize) -> Result<(), String> {
    store
        .store_tasks((0..size * 2).map(|_| write(record())).collect())
        .await
        .map_err(|e| e.to_string())?;
    let claims = [crate::tasks::LaneClaim {
        limit: size,
        ..claim()
    }];
    let tasks = store
        .tick("mixed", &claims, &[], &[])
        .await
        .map_err(|e| e.to_string())?
        .poll
        .lanes
        .into_iter()
        .flat_map(|l| l.tasks)
        .collect::<Vec<_>>();
    let commits = tasks
        .iter()
        .take(size.div_ceil(2))
        .map(|t| commit(t.id, TaskOutcome::Complete))
        .collect::<Vec<_>>();
    let leases = tasks
        .iter()
        .map(|t| crate::tasks::TaskLease {
            task_id: t.id,
            lane: claim().lane,
            owner_token: None,
        })
        .collect::<Vec<_>>();
    take_queries(queries)?;
    store
        .tick("mixed", &claims, &commits, &leases)
        .await
        .map_err(|e| e.to_string())?;
    let sql = take_queries(queries)?;
    assert_mixed_statements(&sql, size);
    // Leave the next fixture independent of this deliberately half-completed cohort.
    db::from(&DbTaskStore::table())
        .filter(DbTaskStore::table().name.eq(db::val("workflow".to_owned())))
        .delete()
        .exec(&mut store.pool.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Checks common mixed turns independently from separately accounted maintenance statements.
fn assert_mixed_statements(sql: &[String], size: usize) {
    let maintenance = sql
        .iter()
        .filter(|q| q.contains("vyuh_task_idempotency"))
        .count();
    let expected = if cfg!(feature = "mysql") {
        if size == 1 { 6 } else { 8 }
    } else if size == 1 {
        4
    } else if cfg!(feature = "sqlite") {
        6
    } else {
        5
    };
    assert_eq!(sql.len() - maintenance, expected, "{sql:?}");
    eprintln!(
        "throughput mixed size={size} application_statements={} maintenance={maintenance}",
        sql.len()
    );
}

/// Terminal flushes reuse their accepted rows for renewal without a second task read.
async fn settled_count(store: &DbTaskStore, queries: &Queries, size: usize) -> Result<(), String> {
    store
        .store_tasks((0..size).map(|_| write(record())).collect())
        .await
        .map_err(|error| error.to_string())?;
    take_queries(queries)?;
    let claims = [crate::tasks::LaneClaim {
        limit: size,
        ..claim()
    }];
    let claimed = store
        .tick("benchmark", &claims, &[], &[])
        .await
        .map_err(|error| error.to_string())?;
    let sql = take_queries(queries)?;
    let maintenance = sql
        .iter()
        .filter(|query| query.contains("vyuh_task_idempotency"))
        .count();
    assert_eq!(
        sql.len() - maintenance,
        if cfg!(feature = "mysql") { 5 } else { 2 },
        "{sql:?}"
    );
    eprintln!(
        "throughput claim size={size} application_statements={} maintenance={maintenance}",
        sql.len()
    );
    let commits = claimed
        .poll
        .lanes
        .into_iter()
        .flat_map(|lane| lane.tasks)
        .map(|task| commit(task.id, TaskOutcome::Complete))
        .collect::<Vec<_>>();
    let leases = commits
        .iter()
        .map(|commit| crate::tasks::TaskLease {
            task_id: commit.task_id,
            lane: commit.lane,
            owner_token: None,
        })
        .collect::<Vec<_>>();
    take_queries(queries)?;
    store
        .tick("benchmark", &[], &[], &leases)
        .await
        .map_err(|error| error.to_string())?;
    let renewal_sql = take_queries(queries)?;
    assert_eq!(renewal_sql.len(), 2, "{renewal_sql:?}");
    let tick = store
        .tick("benchmark", &[], &commits, &leases)
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(tick.lost.len(), size);
    assert!(tick.cancelled.is_empty());
    let sql = take_queries(queries)?;
    assert_eq!(sql.len(), 2, "{sql:?}");
    assert!(
        !sql.iter()
            .any(|query| query.contains("vyuh_task_lane_locks"))
    );
    eprintln!(
        "throughput commit+renew size={size} application_statements={} SQL={sql:?}",
        sql.len()
    );
    Ok(())
}

/// Separates transaction-control logs from application SQL; nothing else is filtered.
fn take_queries(queries: &Queries) -> Result<Vec<String>, String> {
    let mut queries = queries.0.lock().map_err(|error| error.to_string())?;
    let sql = std::mem::take(&mut *queries);
    Ok(sql
        .into_iter()
        .filter(|query| {
            !["COMMIT", "BEGIN", "ROLLBACK"]
                .iter()
                .any(|control| query.starts_with(&format!("\"{control}")))
        })
        .collect())
}
