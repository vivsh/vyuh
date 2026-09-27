use super::*;
use crate::tasks::store::memory::tests::workflow_contract;

#[path = "cancellation.rs"]
mod cancellation;

/// Database joins match the memory ordering and claim-time persistence contract.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn database_all_contract() -> Result<(), String> {
    crate::tasks::store::memory::tests::all::ordered_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())
}

/// Empty/large joins, overflow, mixed outcomes, conflicts, and nested lineage share one contract.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn database_all_edges() -> Result<(), String> {
    use crate::tasks::store::memory::tests::all;
    all::limits_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())?;
    all::mixed_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())?;
    all::conflicts_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())?;
    all::nested_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())?;
    all::cancellation_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())?;
    all::invalid_group_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())?;
    crate::tasks::store::memory::tests::locked_all_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())
}

/// SQL backends retain and deliver the same bounded JSON results as the memory store.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target"
)]
async fn database_result_contract() -> Result<(), String> {
    crate::tasks::store::memory::tests::result_contract(&store().await?)
        .await
        .map_err(|error| error.to_string())
}

#[path = "flush.rs"]
mod flush;

#[path = "upgrade.rs"]
mod upgrade;

#[path = "result_upgrade.rs"]
mod result_upgrade;

#[path = "all_database.rs"]
mod all_database;
#[path = "all_upgrade.rs"]
mod all_upgrade;
#[path = "flow_upgrade.rs"]
mod flow_upgrade;

/// SQL stores reject invalid capabilities without disturbing accepted sibling outcomes.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target"
)]
async fn database_capability_contract() -> Result<(), String> {
    crate::tasks::store::memory::tests::capabilities::contract(&store().await?)
        .await
        .map_err(|error| error.to_string())
}

/// Creates schema from the production models in an explicitly disposable target.
async fn store() -> Result<DbTaskStore, String> {
    #[cfg(feature = "sqlite")]
    let (url, dialect) = ("sqlite::memory:".to_owned(), db::Dialect::Sqlite);
    #[cfg(feature = "postgres")]
    let (url, dialect) = (
        std::env::var("VYUH_TASK_TEST_URL")
            .map_err(|_| "VYUH_TASK_TEST_URL must point at a disposable dbharness target")?,
        db::Dialect::Postgres,
    );
    #[cfg(feature = "mysql")]
    let (url, dialect) = (
        std::env::var("VYUH_TASK_TEST_URL")
            .map_err(|_| "VYUH_TASK_TEST_URL must point at a disposable dbharness target")?,
        db::Dialect::Mysql,
    );
    let pool = db::sqlx::pool::PoolOptions::<db::Database>::new()
        .max_connections(if cfg!(feature = "sqlite") { 1 } else { 4 })
        .connect(&url)
        .await
        .map_err(|e| e.to_string())?;
    for table in [
        "vyuh_tasks",
        "vyuh_task_idempotency",
        "vyuh_task_lane_rates",
        "vyuh_task_lane_locks",
        "vyuh_task_runtime",
        "vyuh_schedules",
    ] {
        db::sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&pool)
            .await
            .map_err(|e| e.to_string())?;
    }
    let schema = super::super::schema::task_schema().map_err(|e| e.to_string())?;
    let planner = gaman_core::OfflinePlanner::new(dialect);
    let migration = planner
        .make_migration(schema, &[])
        .map_err(|e| format!("{e:?}"))?
        .ok_or("missing schema migration")?;
    for statement in planner
        .sql_migrate(&[migration])
        .map_err(|e| e.to_string())?
    {
        db::sqlx::query(&statement)
            .execute(&pool)
            .await
            .map_err(|e| format!("schema: {e}: {statement}"))?;
    }
    Ok(DbTaskStore::new(
        pool,
        32,
        std::time::Duration::from_secs(300),
    ))
}

/// The shared memory/SQL contract preserves atomic checkpoints and next-poll delivery.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target"
)]
async fn database_workflow_contract() -> Result<(), String> {
    let store = store().await?;
    workflow_contract(&store).await.map_err(|e| e.to_string())?;
    crate::tasks::store::memory::tests::edge_contract(&store)
        .await
        .map_err(|e| e.to_string())?;
    rollback_contract(&store).await.map_err(|e| e.to_string())?;
    drop(store);
    crate::tasks::store::memory::tests::locked_workflow_contract(&self::store().await?)
        .await
        .map_err(|e| e.to_string())?;
    crate::tasks::store::memory::tests::cross_lane_contract(&self::store().await?)
        .await
        .map_err(|e| e.to_string())?;
    upgrade_adoption(&self::store().await?)
        .await
        .map_err(|e| e.to_string())
}

/// Adopting a migrated policy preserves leased work and a partially consumed rate bucket.
async fn upgrade_adoption(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use crate::tasks::store::memory::tests::{claim, conf, record, write};
    use crate::tasks::{AbstractTaskStore, TaskRate};
    let mut conf = conf();
    conf.lanes[0] = conf.lanes[0]
        .clone()
        .global_rate_limit(TaskRate::per_second(10));
    store.initialize(conf.clone()).await?;
    let task = record();
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let runtime = DbTaskStore::runtime_table();
    let rates = DbTaskStore::rate_table();
    let mut pool = store.pool.clone();
    let mut rows = db::from(&runtime)
        .all::<super::super::model::TaskRuntimeRow>()
        .exec(&mut pool)
        .await?;
    let mut buckets = db::from(&rates)
        .all::<super::super::model::TaskRateRow>()
        .exec(&mut pool)
        .await?;
    let tokens = buckets[0].tokens_micros;
    let migrated = crate::tasks::store::migration_fingerprint(&conf);
    assert_eq!(migrated.len(), 64);
    rows[0].policy_fingerprint = "unmigrated".into();
    db::from(&runtime)
        .update_many(&rows, &runtime.policy_fingerprint)
        .exec(&mut pool)
        .await?;
    assert!(store.initialize(conf.clone()).await.is_err());
    rows[0].policy_fingerprint = migrated.clone();
    buckets[0].policy_fingerprint = migrated;
    db::from(&runtime)
        .update_many(&rows, &runtime.policy_fingerprint)
        .exec(&mut pool)
        .await?;
    db::from(&rates)
        .filter(rates.lane_name.eq(db::val(buckets[0].lane_name.clone())))
        .update(&buckets[0])
        .exec(&mut pool)
        .await?;
    store.initialize(conf).await?;
    let task = store.get_task(id).await?.unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    assert_eq!(task.locked_by.as_deref(), Some("owner"));
    let buckets = db::from(&rates)
        .all::<super::super::model::TaskRateRow>()
        .exec(&mut pool)
        .await?;
    assert_eq!(buckets[0].tokens_micros, tokens);
    assert!(buckets[0].policy_fingerprint.starts_with("tr-v6:"));
    Ok(())
}

/// A rollback after child insertion restores the parent and removes the child together.
async fn rollback_contract(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use crate::tasks::store::memory::tests::{claim, commit, conf, flow_record, record, write};
    use crate::tasks::{AbstractTaskStore, TaskOutcome};
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("owner", &[claim()]).await?;
    let child = record();
    let child_id = child.id;
    let spawn = commit(
        id,
        TaskOutcome::Spawn {
            state: "null".into(),
            child: write(child),
        },
    );
    let mut tx = store.pool.begin().await?;
    let now = chrono::Utc::now();
    let (children, deliveries, waits) = store
        .commit_outcomes_tx(&mut tx, "owner", std::slice::from_ref(&spawn), &conf(), now)
        .await?;
    super::super::writes::finalize_workflow(
        &mut tx,
        &children,
        deliveries,
        &waits,
        &conf(),
        now,
        32,
    )
    .await?;
    tx.rollback().await?;
    assert!(store.get_task(child_id).await?.is_none());
    assert_eq!(
        store.get_task(id).await?.unwrap().status,
        TaskStatus::Running
    );
    store.commit_outcomes("owner", &[spawn]).await?;
    assert!(store.get_task(child_id).await?.is_some());
    rollback_delivery(store, child_id, id).await?;
    Ok(())
}

/// Rolling back after parent delivery preserves both the running child and suspended parent.
async fn rollback_delivery(
    store: &DbTaskStore,
    child: crate::tasks::TaskId,
    parent: crate::tasks::TaskId,
) -> Result<(), TaskRuntimeError> {
    use crate::tasks::store::memory::tests::{claim, commit, conf};
    use crate::tasks::{AbstractTaskStore, TaskOutcome};
    store.claim_tasks("owner", &[claim()]).await?;
    let terminal = commit(child, TaskOutcome::Complete);
    let mut tx = store.pool.begin().await?;
    let now = chrono::Utc::now();
    let (children, deliveries, waits) = store
        .commit_outcomes_tx(
            &mut tx,
            "owner",
            std::slice::from_ref(&terminal),
            &conf(),
            now,
        )
        .await?;
    super::super::writes::finalize_workflow(
        &mut tx,
        &children,
        deliveries,
        &waits,
        &conf(),
        now,
        32,
    )
    .await?;
    tx.rollback().await?;
    assert_eq!(
        store.get_task(child).await?.unwrap().status,
        TaskStatus::Running
    );
    assert_eq!(
        store.get_task(parent).await?.unwrap().status,
        TaskStatus::Suspended
    );
    assert_eq!(store.get_task(parent).await?.unwrap().resume_input, None);
    store.commit_outcomes("owner", &[terminal]).await?;
    assert_eq!(
        store
            .get_task(parent)
            .await?
            .unwrap()
            .resume_input::<()>()?,
        Some(Ok(()))
    );
    Ok(())
}
