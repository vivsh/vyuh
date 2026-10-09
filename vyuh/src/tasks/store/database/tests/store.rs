use super::*;

/// Verifies rate and lane initialization use conflict-ignore inserts, never no-op upserts.
#[test]
fn initialization_inserts_ignore_conflicts() -> Result<(), String> {
    let now = Utc::now();
    assert_ignore_plan(&rate_plan(now)?, "lane_name")?;
    assert_ignore_plan(&lock_plan(now)?, "lane_name")
}

/// Builds one global-rate insert-if-missing statement.
fn rate_plan(now: chrono::DateTime<Utc>) -> Result<String, String> {
    use crate::db::backend::IgnoreConflictsExt as _;
    let rates = DbTaskStore::rate_table();
    let rate_row = TaskRateRow {
        id: uuid::Uuid::now_v7(),
        lane_name: "default".into(),
        tokens_micros: TOKEN_SCALE,
        updated_at: now,
    };
    Ok(db::from(&rates)
        .insert_many(&[rate_row])
        .ignore_conflicts_on(&rates.lane_name)
        .plan()
        .map_err(|error| error.to_string())?
        .sql)
}

/// Builds one lane-owner insert-if-missing statement.
fn lock_plan(now: chrono::DateTime<Utc>) -> Result<String, String> {
    use crate::db::backend::IgnoreConflictsExt as _;
    let locks = DbTaskStore::lane_lock_table();
    let lock_row = TaskLaneLockRow {
        lane_name: "gpu".into(),
        owner_id: None,
        owner_token: None,
        leased_until: None,
        phase: crate::tasks::LaneOwnerPhase::Active as i16,
        flushing: false,
        empty_since: None,
        generation: 0,
        hook_retry_at: None,
        last_hook_error: None,
        created_at: now,
        updated_at: now,
    };
    Ok(db::from(&locks)
        .insert_many(&[lock_row])
        .ignore_conflicts_on(&locks.lane_name)
        .plan()
        .map_err(|error| error.to_string())?
        .sql)
}

/// Confirms an insert-if-missing plan leaves the existing row unchanged.
fn assert_ignore_plan(sql: &str, target: &str) -> Result<(), String> {
    if !sql.contains(&format!("ON CONFLICT ({target}) DO NOTHING")) {
        return Err(format!(
            "expected conflict-ignore plan for '{target}', got {sql}"
        ));
    }
    Ok(())
}
