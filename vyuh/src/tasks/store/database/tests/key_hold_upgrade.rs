//! Rehearsals of the offline key-hold migration through Gaman's real ledger.
use super::*;
use crate::db::engine::{
    ApplyCommand, CommandResult, Config, MigrationCommand, MigrationStore, NativeRunnerFactory,
    StoreError,
};
use crate::db::gaman::{Migration, OfflinePlanner};
use crate::tasks::{DEFAULT_TASK_LANE, TaskConf, TaskReceipt, TaskStore as _};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use std::{path::PathBuf, sync::Arc, time::Instant};
use uuid::Uuid;

const ID: &str = "0018_task_key_holds";
#[cfg(feature = "postgres")]
const TEMPLATE: &str =
    include_str!("../../../../../migrations/task_protocol/postgres/0018_task_key_holds.yaml");
#[cfg(feature = "sqlite")]
const TEMPLATE: &str =
    include_str!("../../../../../migrations/task_protocol/sqlite/0018_task_key_holds.yaml");

/// Fixed in-memory definitions let the test use the real execution and tracking adapters.
struct RehearsalHistory(Vec<Migration>);

impl MigrationStore for RehearsalHistory {
    fn load_all(&self) -> BoxFuture<'_, Result<Vec<Migration>, StoreError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }

    fn save<'a>(&'a self, migration: &'a Migration) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async {
            Err(StoreError::Save {
                id: migration.id.clone(),
                message: "rehearsal has fixed history".into(),
            })
        })
    }
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn upgrade() -> Result<Migration, String> {
    let mut migration = Migration::from_yaml_str(TEMPLATE).map_err(err)?;
    migration.id = ID.into();
    Ok(migration)
}

/// Creates an approved disposable target and installs the synthetic old schema through Gaman.
async fn fixture() -> Result<(db::testing::TestDatabase, Migration), String> {
    fixture_from(upgrade::owner_schema()?).await
}

/// Accepts a deliberately damaged old schema for corruption tests without touching live data.
async fn fixture_from(
    schema: db::Schema,
) -> Result<(db::testing::TestDatabase, Migration), String> {
    #[cfg(feature = "postgres")]
    let config =
        db::DbConf::from_url(&std::env::var("VYUH_HOLD_ADMIN_URL").map_err(err)?).map_err(err)?;
    #[cfg(feature = "sqlite")]
    let config = db::DbConf::default();
    let database = db::testing::setup(config).create().await.map_err(err)?;
    let mut old = OfflinePlanner::new(dialect())
        .make_migration(schema, &[])
        .map_err(err)?
        .ok_or("missing predecessor schema")?;
    // One synthetic predecessor records the final 0017 schema, not historical migration contents.
    old.id = "0017_local_sticky".into();
    assert_eq!(apply(&database, vec![old.clone()]).await?, 1);
    Ok((database, old))
}

/// Proves preflight rejects duplicate legacy owners even when the old unique index is missing.
#[tokio::test]
#[cfg_attr(
    feature = "postgres",
    ignore = "requires a disposable dbharness admin URL"
)]
async fn key_holds_reject_duplicate_owners() -> Result<(), String> {
    let mut schema = upgrade::owner_schema()?;
    schema
        .tables
        .get_mut("vyuh_task_idempotency")
        .ok_or("missing legacy table")?
        .indexes
        .retain(|index| index.name != "idx_vyuh_task_idempotency_owner");
    let (database, old) = fixture_from(schema).await?;
    let pool = database.pool().as_sqlx();
    seed(pool).await?;
    sql(pool, "INSERT INTO vyuh_task_idempotency (id,task_id,task_name,key_value,fingerprint,expires_at,created_at,updated_at)
        SELECT (SELECT id FROM vyuh_tasks WHERE idempotency_key='15'),task_id,task_name,key_value,fingerprint,
        expires_at,created_at,updated_at FROM vyuh_task_idempotency WHERE key_value='12'").await?;
    let failure = apply(&database, vec![old, upgrade()?])
        .await
        .err()
        .ok_or("duplicate owners admitted")?;
    assert!(failure.contains("duplicate_ownership"), "{failure}");
    assert_not_applied(pool).await?;
    assert_eq!(
        number(
            pool,
            "SELECT count(*) FROM vyuh_task_idempotency WHERE key_value='12'"
        )
        .await?,
        2
    );
    database.teardown().await.map_err(err)
}

/// Uses the same Gaman execution and database tracking implementation as the application.
async fn apply(
    database: &db::testing::TestDatabase,
    migrations: Vec<Migration>,
) -> Result<usize, String> {
    let location = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations/task_protocol");
    let config = Config::new(
        database.conf().url.clone(),
        location.clone(),
        location.join("schema.yaml"),
        dialect(),
    );
    let mut runner =
        NativeRunnerFactory::from_store(config, Arc::new(RehearsalHistory(migrations))).build();
    let response = runner
        .run_command(&MigrationCommand::Apply(ApplyCommand::Execute {
            target: None,
            fake: false,
            fake_verified: false,
            schemas: Vec::new(),
        }))
        .await
        .map_err(|error| format!("{error:?}"))?;
    match response {
        CommandResult::Movement(movement) => Ok(movement.applied),
        other => Err(format!("unexpected migration result {other:?}")),
    }
}

/// Proves retained bytes, ledger replay and candidate behavior on the same upgraded store.
#[tokio::test]
#[cfg_attr(
    feature = "postgres",
    ignore = "requires a disposable dbharness admin URL"
)]
async fn key_holds_preserve_and_replay() -> Result<(), String> {
    let (database, old) = fixture().await?;
    let pool = database.pool().as_sqlx();
    seed(pool).await?;
    let before = task_snapshot(pool).await?;
    let owners = owner_snapshot(pool).await?;
    let migration = upgrade()?;
    assert_replay_shape(&old, &migration)?;
    let history = vec![old, migration];
    assert_eq!(apply(&database, history.clone()).await?, 1);
    assert_eq!(task_snapshot(pool).await?, before);
    assert_eq!(owner_snapshot(pool).await?, owners);
    assert_eq!(scope_mismatches(pool).await?, 0);
    assert_eq!(index_count(pool).await?, 2);
    #[cfg(feature = "sqlite")]
    assert_eq!(
        number(pool, "SELECT count(*) FROM pragma_foreign_key_check").await?,
        0
    );
    assert_eq!(ledger_count(pool).await?, 1);
    println!("HOLD_SWEEP_PLAN,{}", expiry_plan(pool).await?);
    prove_runtime(&database).await?;
    let after_sweep = task_snapshot(pool).await?;
    assert_eq!(apply(&database, history).await?, 0);
    assert_eq!(task_snapshot(pool).await?, after_sweep);
    assert_eq!(owner_snapshot(pool).await?, owners);
    assert_eq!(ledger_count(pool).await?, 1);
    database.teardown().await.map_err(err)
}

/// Verifies preflight and late DDL rollback leave no schema, data or ledger side effects.
#[tokio::test]
#[cfg_attr(
    feature = "postgres",
    ignore = "requires a disposable dbharness admin URL"
)]
async fn key_holds_reject_bad_owners() -> Result<(), String> {
    let (database, old) = fixture().await?;
    let pool = database.pool().as_sqlx();
    seed(pool).await?;
    for (damage, repair, guard) in malformed() {
        sql(pool, damage).await?;
        let failure = apply(&database, vec![old.clone(), upgrade()?])
            .await
            .err()
            .ok_or("bad legacy rows were accepted")?;
        assert!(failure.contains(guard), "{failure}");
        assert_not_applied(pool).await?;
        sql(pool, repair).await?;
    }
    let old_tasks = task_snapshot(pool).await?;
    let old_owners = owner_snapshot(pool).await?;
    let mut broken = upgrade()?;
    broken.operations.extend(Migration::from_yaml_str(
        "operations:\n  - type: statement\n    up: INSERT INTO vyuh_hold_injected_failure VALUES (1)\n",
    ).map_err(err)?.operations);
    let failed = apply(&database, vec![old.clone(), broken])
        .await
        .err()
        .ok_or("injected post-DDL failure applied")?;
    assert!(failed.contains("vyuh_hold_injected_failure"), "{failed}");
    assert_not_applied(pool).await?;
    assert_eq!(task_snapshot(pool).await?, old_tasks);
    assert_eq!(owner_snapshot(pool).await?, old_owners);
    assert_eq!(apply(&database, vec![old, upgrade()?]).await?, 1);
    assert_eq!(scope_mismatches(pool).await?, 0);
    database.teardown().await.map_err(err)
}

/// Samples size-dependent offline time; only synthetic history, no production data is used.
#[tokio::test]
#[ignore = "explicit migration timing on a disposable fixture"]
async fn key_holds_rehearsal_timing() -> Result<(), String> {
    let rows = match std::env::var("VYUH_HOLD_HISTORY_ROWS") {
        Ok(raw) => raw.parse::<u32>().map_err(err)?,
        Err(std::env::VarError::NotPresent) => 100_000,
        Err(error) => return Err(err(error)),
    };
    if !(1..=500_000).contains(&rows) {
        return Err("supported rehearsal size is 1–500,000 rows".into());
    }
    let (database, old) = fixture().await?;
    let pool = database.pool().as_sqlx();
    seed(pool).await?;
    seed_history(pool, rows).await?;
    let held_rows = rows / 10;
    seed_holds(pool, held_rows).await?;
    let (applied, elapsed, lock_ms) = timed_apply(&database, vec![old, upgrade()?]).await?;
    assert_eq!(applied, 1);
    assert_eq!(
        number(pool, "SELECT count(*) FROM vyuh_tasks").await?,
        i64::from(rows + held_rows) + 8
    );
    assert_eq!(scope_mismatches(pool).await?, 0);
    assert_eq!(ledger_count(pool).await?, 1);
    sql(pool, "ANALYZE vyuh_tasks").await?;
    let access = expiry_plan(pool).await?;
    assert!(access.contains("idx_vyuh_tasks_held_expiry"), "{access}");
    #[cfg(feature = "sqlite")]
    let mode: String = db::sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(pool)
        .await
        .map_err(err)?;
    #[cfg(feature = "postgres")]
    let mode = "not applicable";
    println!("HOLD_MIGRATION_TIMING,{rows},{held_rows},{elapsed:.3},{lock_ms:.3},{mode}");
    println!("HOLD_SWEEP_PLAN,{access}");
    database.teardown().await.map_err(err)
}

/// Samples the PostgreSQL relation lock on an independent connection during execution.
#[cfg(feature = "postgres")]
async fn timed_apply(
    database: &db::testing::TestDatabase,
    history: Vec<Migration>,
) -> Result<(usize, f64, f64), String> {
    let mut first = None;
    let mut last = None;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(4));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let task_oid = number(
        database.pool().as_sqlx(),
        "SELECT 'vyuh_tasks'::regclass::oid::bigint",
    )
    .await?;
    let start = Instant::now();
    let migration = apply(database, history);
    tokio::pin!(migration);
    loop {
        tokio::select! {
            biased;
            result = &mut migration => {
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                let window = last.zip(first).map_or(0.0, |(end, start): (Instant,Instant)| {
                    end.duration_since(start).as_secs_f64() * 1000.0
                });
                return Ok((result?, elapsed, window));
            }
            _ = tick.tick() => {
                let locked: bool = db::sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks
                     WHERE relation::bigint = $1 AND mode = 'AccessExclusiveLock'
                       AND granted AND database = (SELECT oid FROM pg_database WHERE datname=current_database()))",
                ).bind(task_oid).fetch_one(database.pool().as_sqlx()).await.map_err(err)?;
                if locked {
                    let observed = Instant::now();
                    first.get_or_insert(observed);
                    last = Some(observed);
                }
            }
        }
    }
}

/// SQLite exposes no equivalent lock view; report total apply as a writer-window upper bound.
#[cfg(feature = "sqlite")]
async fn timed_apply(
    database: &db::testing::TestDatabase,
    history: Vec<Migration>,
) -> Result<(usize, f64, f64), String> {
    let start = Instant::now();
    let count = apply(database, history).await?;
    Ok((count, start.elapsed().as_secs_f64() * 1000.0, -1.0))
}

/// Seeds keyed but unowned terminal rows to exercise the full backfill cost.
async fn seed_history(pool: &db::Pool, rows: u32) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(err)?;
    for start in (0..rows).step_by(256) {
        let mut query = db::sqlx::QueryBuilder::<db::Database>::new(
            "INSERT INTO vyuh_tasks (id,idempotency_key,name,input,status,attempts,created_at,updated_at) ",
        );
        query.push_values(start..rows.min(start + 256), |mut row, value| {
            row.push_bind(Uuid::from_u128(u128::from(value) + 1_000))
                .push_bind(format!("history-{}", value % 1_000))
                .push("'tx_job'")
                .push("'null'")
                .push("3")
                .push("1")
                .push("CURRENT_TIMESTAMP")
                .push("CURRENT_TIMESTAMP");
        });
        query.build().execute(&mut *tx).await.map_err(err)?;
    }
    tx.commit().await.map_err(err)
}

/// Adds both active and retained owner rows so the benchmark builds nonempty indexes.
async fn seed_holds(pool: &db::Pool, rows: u32) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(err)?;
    let future = DateTime::from_timestamp(4_000_000_000, 0).ok_or("invalid future")?;
    for start in (0..rows).step_by(256) {
        let mut query = db::sqlx::QueryBuilder::<db::Database>::new(
            "INSERT INTO vyuh_tasks (id,idempotency_key,name,input,status,attempts,
             ready_at,idempotency_expires_at,created_at,updated_at) ",
        );
        query.push_values(start..rows.min(start + 256), |mut row, value| {
            let terminal = value % 2 != 0;
            row.push_bind(Uuid::from_u128(u128::from(value) + 1_000_000))
                .push_bind(format!("load-{value}"))
                .push("'tx_job'")
                .push("'null'")
                .push_bind(if terminal { 3_i16 } else { 0_i16 })
                .push("1")
                .push_bind(future)
                .push_bind(terminal.then_some(future))
                .push("CURRENT_TIMESTAMP")
                .push("CURRENT_TIMESTAMP");
        });
        query.build().execute(&mut *tx).await.map_err(err)?;
    }
    db::sqlx::query(
        "INSERT INTO vyuh_task_idempotency (id,task_id,task_name,key_value,fingerprint,expires_at,created_at,updated_at)
         SELECT id,id,name,idempotency_key,'old',idempotency_expires_at,created_at,updated_at
         FROM vyuh_tasks WHERE idempotency_key LIKE 'load-%'",
    ).execute(&mut *tx).await.map_err(err)?;
    tx.commit().await.map_err(err)
}

/// Generates immutable seeded state with active, superseded, due, future and zero-retention keys.
async fn seed(pool: &db::Pool) -> Result<(), String> {
    let node = Uuid::from_u128(10_000_000);
    let mut query = db::sqlx::QueryBuilder::<db::Database>::new(
        "INSERT INTO vyuh_task_nodes (id, started_at, heartbeat_at) ",
    );
    query.push_values([node], |mut row, node| {
        row.push_bind(node)
            .push("CURRENT_TIMESTAMP")
            .push("CURRENT_TIMESTAMP");
    });
    query.build().execute(pool).await.map_err(err)?;
    let past = DateTime::from_timestamp(1_000_000_000, 0).ok_or("invalid past")?;
    let future = DateTime::from_timestamp(4_000_000_000, 0).ok_or("invalid future")?;
    seed_tasks(pool, node, past, future).await?;
    sql(pool, "INSERT INTO vyuh_task_idempotency (id, task_id, task_name, key_value, fingerprint, expires_at, created_at, updated_at)
        SELECT id, id, name, idempotency_key, 'old', idempotency_expires_at, created_at, updated_at
        FROM vyuh_tasks WHERE status IN (0,1,2) OR idempotency_key IN ('13','14')").await?;
    seed_lineage(pool).await?;
    Ok(())
}

/// Creates deliberately distinct task rows without using the upgraded writer.
async fn seed_tasks(
    pool: &db::Pool,
    node: Uuid,
    past: DateTime<Utc>,
    future: DateTime<Utc>,
) -> Result<(), String> {
    let fixtures = [
        (1, "tx_plain", None, 4, None),
        (2, "tx_job", Some("11"), 3, Some(past)),
        (3, "tx_job", Some("11"), 1, None),
        (4, "tx_job", Some("12"), 2, None),
        (5, "tx_job", Some("13"), 4, Some(past)),
        (6, "tx_job", Some("14"), 3, Some(future)),
        (7, "tx_job", Some("15"), 3, None),
        (8, "tx_plain", Some("11"), 0, None),
    ];
    let mut tasks = db::sqlx::QueryBuilder::<db::Database>::new(
        "INSERT INTO vyuh_tasks (id,name,idempotency_key,status,idempotency_expires_at,node_id,
         input,attempts,step_attempts,created_at,updated_at,ready_at,state,resume_input,
         last_result,waiting_children,remaining_completions) ",
    );
    tasks.push_values(fixtures, |mut row, (id, name, key, status, expiration)| {
        row.push_bind(Uuid::from_u128(id))
            .push_bind(name)
            .push_bind(key)
            .push_bind(status as i16)
            .push_bind(expiration)
            .push_bind((status == 1).then_some(node))
            .push("'null'")
            .push("7")
            .push("3")
            .push("CURRENT_TIMESTAMP")
            .push("CURRENT_TIMESTAMP")
            .push_bind(future)
            .push("'{\"step\":1}'")
            .push("'{\"Ok\":42}'")
            .push("'{\"Ok\":null}'")
            .push("'[]'")
            .push("0");
    });
    tasks.build().execute(pool).await.map_err(err)?;
    Ok(())
}

/// Distinguishes saved join membership, lineage, and terminal history in comparison snapshots.
async fn seed_lineage(pool: &db::Pool) -> Result<(), String> {
    sql(pool, "UPDATE vyuh_tasks
        SET waiting_children = '[\"00000000-0000-0000-0000-000000000005\"]', remaining_completions = 1, kind = 1
        WHERE idempotency_key = '12'").await?;
    sql(pool, "UPDATE vyuh_tasks SET parent_id = (SELECT id FROM vyuh_tasks WHERE idempotency_key = '12'),
        root_id = (SELECT id FROM vyuh_tasks WHERE idempotency_key = '12'), completed_at = created_at
        WHERE idempotency_key = '13'").await?;
    Ok(())
}

/// Exercises conflict arbitration and actual store-clock expiry on the upgraded schema.
async fn prove_runtime(database: &db::testing::TestDatabase) -> Result<(), String> {
    use super::super::submit::tx_tests::{store, writes};
    let store = store(database.pool(), TaskConf::default()).map_err(err)?;
    for key in [11, 12, 13, 14] {
        let result = store
            .store_tasks(writes(true, DEFAULT_TASK_LANE, key, 1)?)
            .await
            .map_err(err)?;
        assert_eq!(result, [TaskReceipt::KeyHeld]);
    }
    let fresh = store
        .store_tasks(writes(true, DEFAULT_TASK_LANE, 15, 1)?)
        .await
        .map_err(err)?;
    assert!(matches!(fresh.as_slice(), [TaskReceipt::Queued(_)]));
    let mut tx = database.pool().begin().await.map_err(err)?;
    let now = super::super::queries::statement_now(&mut tx)
        .await
        .map_err(err)?;
    super::super::held::sweep(&mut tx, now, 250)
        .await
        .map_err(err)?;
    tx.commit().await.map_err(err)?;
    let due = store
        .store_tasks(writes(true, DEFAULT_TASK_LANE, 13, 1)?)
        .await
        .map_err(err)?;
    let future = store
        .store_tasks(writes(true, DEFAULT_TASK_LANE, 14, 1)?)
        .await
        .map_err(err)?;
    assert!(matches!(due.as_slice(), [TaskReceipt::Queued(_)]));
    assert_eq!(future, [TaskReceipt::KeyHeld]);
    Ok(())
}

async fn sql(pool: &db::Pool, query: &str) -> Result<u64, String> {
    Ok(db::sqlx::query(db::sqlx::AssertSqlSafe(query))
        .execute(pool)
        .await
        .map_err(err)?
        .rows_affected())
}

async fn number(pool: &db::Pool, query: &str) -> Result<i64, String> {
    db::sqlx::query_scalar(db::sqlx::AssertSqlSafe(query))
        .fetch_one(pool)
        .await
        .map_err(err)
}

async fn ledger_count(pool: &db::Pool) -> Result<i64, String> {
    number(
        pool,
        "SELECT count(*) FROM gaman_migrations WHERE id = '0018_task_key_holds'",
    )
    .await
}

async fn scope_mismatches(pool: &db::Pool) -> Result<i64, String> {
    number(pool, "SELECT count(*) FROM vyuh_tasks t WHERE t.idempotency_key IS NOT NULL
        AND ((t.idempotency_held = TRUE AND NOT EXISTS (SELECT 1 FROM vyuh_task_idempotency o WHERE o.task_id = t.id))
          OR (t.idempotency_held = FALSE AND EXISTS (SELECT 1 FROM vyuh_task_idempotency o WHERE o.task_id = t.id)))").await
}

async fn index_count(pool: &db::Pool) -> Result<i64, String> {
    #[cfg(feature = "postgres")]
    let names = "SELECT count(*) FROM pg_indexes WHERE tablename = 'vyuh_tasks' AND indexname IN ('idx_vyuh_tasks_held_key','idx_vyuh_tasks_held_expiry')";
    #[cfg(feature = "sqlite")]
    let names = "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'vyuh_tasks' AND name IN ('idx_vyuh_tasks_held_key','idx_vyuh_tasks_held_expiry')";
    number(pool, names).await
}

async fn assert_not_applied(pool: &db::Pool) -> Result<(), String> {
    #[cfg(feature = "postgres")]
    let columns = "SELECT count(*) FROM information_schema.columns WHERE table_name = 'vyuh_tasks' AND column_name = 'idempotency_held'";
    #[cfg(feature = "sqlite")]
    let columns =
        "SELECT count(*) FROM pragma_table_info('vyuh_tasks') WHERE name = 'idempotency_held'";
    assert_eq!(number(pool, columns).await?, 0);
    assert_eq!(index_count(pool).await?, 0);
    assert_eq!(ledger_count(pool).await?, 0);
    Ok(())
}

/// Compares every pre-existing task column without normalizing SQLite's persisted text.
async fn task_snapshot(pool: &db::Pool) -> Result<Vec<String>, String> {
    #[cfg(feature = "postgres")]
    let query = "SELECT (to_jsonb(t) - 'idempotency_held')::text FROM vyuh_tasks t ORDER BY id";
    #[cfg(feature = "sqlite")]
    let query = "SELECT json_array(hex(id),hex(parent_id),hex(root_id),kind,name,input,state,resume_input,
        waiting_children,remaining_completions,status,cancelled,attempts,step_attempts,lane_name,last_result,
        idempotency_key,idempotency_fingerprint,idempotency_expires_at,hex(node_id),ready_at,created_at,updated_at,
        completed_at) FROM vyuh_tasks ORDER BY id";
    db::sqlx::query_scalar(query)
        .fetch_all(pool)
        .await
        .map_err(err)
}

async fn owner_snapshot(pool: &db::Pool) -> Result<Vec<String>, String> {
    #[cfg(feature = "postgres")]
    let query = "SELECT to_jsonb(o)::text FROM vyuh_task_idempotency o ORDER BY id";
    #[cfg(feature = "sqlite")]
    let query = "SELECT json_array(hex(id),hex(task_id),task_name,key_value,fingerprint,expires_at,
         created_at,updated_at) FROM vyuh_task_idempotency ORDER BY id";
    db::sqlx::query_scalar(query)
        .fetch_all(pool)
        .await
        .map_err(err)
}

/// Replays operation metadata as the ledger would for future plan generation.
fn assert_replay_shape(old: &Migration, migration: &Migration) -> Result<(), String> {
    let planner =
        OfflinePlanner::new(dialect()).from_migrations(vec![old.clone(), migration.clone()]);
    let replay = planner.replay().map_err(err)?;
    let mut expected = upgrade::before_topics()?;
    let replay_tasks = replay
        .tables
        .get("vyuh_tasks")
        .ok_or("missing replayed task table")?;
    let desired_tasks = expected
        .tables
        .get("vyuh_tasks")
        .ok_or("missing desired task table")?;
    let flag = |table: &db::Table| {
        table
            .columns
            .iter()
            .find(|c| c.name == "idempotency_held")
            .cloned()
    };
    assert_eq!(flag(replay_tasks), flag(desired_tasks));
    for name in ["idx_vyuh_tasks_held_key", "idx_vyuh_tasks_held_expiry"] {
        assert_eq!(
            replay_tasks.indexes.iter().find(|index| index.name == name),
            desired_tasks
                .indexes
                .iter()
                .find(|index| index.name == name)
        );
    }
    assert!(replay.tables.contains_key("vyuh_task_idempotency"));
    let legacy = upgrade::owner_schema()?
        .tables
        .remove("vyuh_task_idempotency")
        .ok_or("missing archived owner schema")?;
    expected.tables.insert(legacy.name.clone(), legacy);
    if let Some(diff) = planner.make_migration(expected, &[]).map_err(err)? {
        return Err(diff.to_yaml_string().map_err(err)?);
    }
    Ok(())
}

/// Exercises the exact expiry predicate. Small PostgreSQL fixtures may prefer a sequential scan.
async fn expiry_plan(pool: &db::Pool) -> Result<String, String> {
    let select = "SELECT id FROM vyuh_tasks WHERE idempotency_held = TRUE AND idempotency_key IS NOT NULL
        AND idempotency_expires_at IS NOT NULL AND status IN (3,4) AND idempotency_expires_at <= CURRENT_TIMESTAMP
        ORDER BY idempotency_expires_at,id LIMIT 250";
    #[cfg(feature = "postgres")]
    return Ok(
        db::sqlx::query_scalar::<_, String>(db::sqlx::AssertSqlSafe(format!(
            "EXPLAIN (COSTS OFF) {select}"
        )))
        .fetch_all(pool)
        .await
        .map_err(err)?
        .join(" | "),
    );
    #[cfg(feature = "sqlite")]
    {
        let rows: Vec<(i32, i32, i32, String)> = db::sqlx::query_as(db::sqlx::AssertSqlSafe(
            format!("EXPLAIN QUERY PLAN {select}"),
        ))
        .fetch_all(pool)
        .await
        .map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|(_, _, _, detail)| detail)
            .collect::<Vec<_>>()
            .join(" | "))
    }
}

/// Restricts each artificial defect and repair to the throwaway fixture.
fn malformed() -> [(&'static str, &'static str, &'static str); 6] {
    [
        (
            "UPDATE vyuh_tasks SET status=0 WHERE idempotency_key='15'",
            "UPDATE vyuh_tasks SET status=3 WHERE idempotency_key='15'",
            "missing_active_owner",
        ),
        (
            "UPDATE vyuh_task_idempotency SET key_value='different' WHERE key_value='12'",
            "UPDATE vyuh_task_idempotency SET key_value='12' WHERE key_value='different'",
            "inconsistent_owner",
        ),
        (
            "UPDATE vyuh_task_idempotency SET expires_at=NULL WHERE key_value='13'",
            "UPDATE vyuh_task_idempotency SET expires_at=(SELECT idempotency_expires_at FROM vyuh_tasks WHERE idempotency_key='13') WHERE key_value='13'",
            "inconsistent_owner",
        ),
        (
            "UPDATE vyuh_task_idempotency SET task_id=(SELECT id FROM vyuh_tasks WHERE idempotency_key='15') WHERE key_value='14'",
            "UPDATE vyuh_task_idempotency SET task_id=(SELECT id FROM vyuh_tasks WHERE idempotency_key='14') WHERE key_value='14'",
            "inconsistent_owner",
        ),
        (
            "UPDATE vyuh_task_idempotency SET task_id=(SELECT id FROM vyuh_task_nodes) WHERE key_value='14'",
            "UPDATE vyuh_task_idempotency SET task_id=(SELECT id FROM vyuh_tasks WHERE idempotency_key='14') WHERE key_value='14'",
            "inconsistent_owner",
        ),
        (
            "UPDATE vyuh_task_idempotency SET expires_at=(SELECT expires_at FROM vyuh_task_idempotency WHERE key_value='14') WHERE key_value='13'",
            "UPDATE vyuh_task_idempotency SET expires_at=(SELECT idempotency_expires_at FROM vyuh_tasks WHERE idempotency_key='13') WHERE key_value='13'",
            "inconsistent_owner",
        ),
    ]
}
