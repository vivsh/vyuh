use super::*;

/// Verifies task IDs use the canonical UUID string for display, parsing, and serde.
#[test]
fn task_id_round_trips_as_uuid_string() -> Result<(), String> {
    let raw = "018f0f7d-37f6-7c29-8f11-6fbf9f90792a";
    let id = raw.parse::<TaskId>().map_err(|error| error.to_string())?;
    if id.to_string() != raw {
        return Err("task ID display changed".into());
    }
    let encoded = serde_json::to_string(&id).map_err(|error| error.to_string())?;
    let decoded = serde_json::from_str::<TaskId>(&encoded).map_err(|error| error.to_string())?;
    (decoded == id)
        .then_some(())
        .ok_or_else(|| "task ID serde changed its value".into())
}

/// Verifies workflow kinds default to work and retain lowercase wire names.
#[test]
fn task_kind_defaults_and_serializes_stably() -> Result<(), String> {
    if TaskKind::default() != TaskKind::Work {
        return Err("task kind no longer defaults to work".into());
    }
    let work = serde_json::to_string(&TaskKind::Work).map_err(|error| error.to_string())?;
    let flow = serde_json::to_string(&TaskKind::Flow).map_err(|error| error.to_string())?;
    if work != "\"work\"" || flow != "\"flow\"" {
        return Err("task kind wire names changed".into());
    }
    Ok(())
}
