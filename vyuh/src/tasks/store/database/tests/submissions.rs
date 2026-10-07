use super::*;

/// Verifies concurrent schedule creation preserves the cursor through conflict-ignore insert.
#[test]
fn schedule_insert_ignores_existing_cursor() -> Result<(), String> {
    use crate::db::backend::IgnoreConflictsExt as _;

    let table = DbTaskStore::schedule_table();
    let row = TaskScheduleRow {
        name: "nightly".into(),
        last_submitted_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let plan = db::from(&table)
        .insert_many(&[row])
        .ignore_conflicts_on(&table.name)
        .plan()
        .map_err(|error| error.to_string())?;
    if !plan.sql.contains("ON CONFLICT (name) DO NOTHING") {
        return Err(format!(
            "expected conflict-ignore schedule insert, got {}",
            plan.sql
        ));
    }
    Ok(())
}
