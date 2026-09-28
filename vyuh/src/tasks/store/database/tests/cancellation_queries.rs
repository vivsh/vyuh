use super::*;

/// SQL instrumentation proves cancellation adds no ordinary claim, renewal, or submission query.
#[tokio::test]
#[ignore = "disposable database query instrumentation; run alone"]
async fn cancellation_query_counts() -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let queries = Queries::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(queries.clone()))
        .map_err(|e| e.to_string())?;
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    queries.0.lock().map_err(|e| e.to_string())?.clear();
    let task = record();
    let id = task.id;
    store
        .store_tasks(vec![write(task)])
        .await
        .map_err(|e| e.to_string())?;
    assert_queries(&queries, 0, 0, false)?;
    store
        .claim_tasks("owner", &[claim()])
        .await
        .map_err(|e| e.to_string())?;
    assert_queries(
        &queries,
        if cfg!(feature = "mysql") { 3 } else { 1 },
        1,
        false,
    )?;
    let leases = [crate::tasks::TaskLease {
        task_id: id,
        lane: crate::tasks::DEFAULT_TASK_LANE,
        owner_token: None,
    }];
    store
        .renew_leases("owner", &leases)
        .await
        .map_err(|e| e.to_string())?;
    assert_queries(&queries, 1, 1, true)?;
    store.cancel(id).await.map_err(|e| e.to_string())?;
    assert_queries(&queries, 0, 1, false)?;
    let lost = store
        .renew_leases("owner", &leases)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(lost, vec![id]);
    assert_queries(&queries, 2, 1, false)?;
    Ok(())
}

/// Counts only task statements; existing policy/time queries are separately included in totals.
fn assert_queries(
    queries: &Queries,
    selects: usize,
    updates: usize,
    renewal: bool,
) -> Result<(), String> {
    let mut sql = queries.0.lock().map_err(|e| e.to_string())?;
    let task_queries = sql
        .iter()
        .filter(|sql| sql.contains("vyuh_tasks"))
        .collect::<Vec<_>>();
    assert_eq!(
        task_queries
            .iter()
            .filter(|sql| sql.starts_with("\"SELECT ")
                || (sql.starts_with("\"WITH ") && !sql.contains("UPDATE vyuh_tasks")))
            .count(),
        selects,
        "{sql:?}"
    );
    assert_eq!(
        task_queries
            .iter()
            .filter(|sql| sql.contains("UPDATE vyuh_tasks"))
            .count(),
        updates,
        "{sql:?}"
    );
    assert!(!sql.iter().any(|sql| sql.contains("vyuh_task_lane_locks")));
    if renewal {
        assert!(
            task_queries
                .iter()
                .filter(|sql| sql.contains("UPDATE vyuh_tasks"))
                .all(|sql| !sql.contains("last_result") && !sql.contains("cancelled"))
        );
    }
    eprintln!(
        "cancellation contract: total={} task_selects={selects} task_updates={updates}",
        sql.len()
    );
    sql.clear();
    Ok(())
}
