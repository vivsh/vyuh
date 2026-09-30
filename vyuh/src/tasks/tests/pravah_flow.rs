use super::*;
use pravah::{FetchRequest, FetchResponse, Step};

#[path = "pravah_boundaries.rs"]
mod boundaries;

fn fetch_graph(root: pravah::Flow<u32>) -> pravah::Flow<u16> {
    root.map(|_| FetchRequest::new("GET", "https://example.invalid"))
        .fetch()
        .map(|result| result.map(|response| response.status()).unwrap_or(599))
}

/// A pending Fetch cannot be redispatched when the checkpoint has no committed result.
#[test]
fn missing_fetch_result_rejected() -> Result<(), String> {
    let compiled = pravah::compile(fetch_graph).map_err(|e| e.to_string())?;
    let mut runtime = compiled
        .start(1, uuid::Uuid::now_v7())
        .map_err(|e| e.to_string())?;
    assert!(matches!(
        runtime.next().map_err(|e| e.to_string())?,
        Step::Continue
    ));
    assert!(matches!(
        runtime.next().map_err(|e| e.to_string())?,
        Step::Fetch(_)
    ));
    let snapshot = runtime.snapshot().map_err(|e| e.to_string())?;
    let mut restored = compiled.restore(snapshot).map_err(|e| e.to_string())?;
    assert!(deliver(&mut restored, None).is_err());
    Ok(())
}

/// Crash replays preserve Fetch identity and deliver accepted output without another Fetch.
#[test]
fn replay_delivers_once() -> Result<(), String> {
    let compiled = pravah::compile(fetch_graph).map_err(|e| e.to_string())?;
    let id = uuid::Uuid::now_v7();
    let mut runtime = compiled.start(1, id).map_err(|e| e.to_string())?;
    runtime.next().map_err(|e| e.to_string())?;
    let Step::Fetch(fetch) = runtime.next().map_err(|e| e.to_string())? else {
        return Err("missing fetch".into());
    };
    let snapshot = runtime.snapshot().map_err(|e| e.to_string())?;
    let value = serde_json::to_value(FetchResponse::new(202)).map_err(|e| e.to_string())?;
    for _ in 0..2 {
        let mut restored = compiled
            .restore(snapshot.clone())
            .map_err(|e| e.to_string())?;
        assert_eq!(restored.pending_fetch().map(|f| f.id()), Some(fetch.id()));
        deliver(&mut restored, Some(Ok(value.clone()))).map_err(|e| e.to_string())?;
        assert!(restored.pending_fetch().is_none());
        assert!(deliver(&mut restored, Some(Ok(value.clone()))).is_err());
        assert!(matches!(
            restored.next().map_err(|e| e.to_string())?,
            Step::Done(_)
        ));
    }
    let mut replay = compiled.start(1, id).map_err(|e| e.to_string())?;
    replay.next().map_err(|e| e.to_string())?;
    let Step::Fetch(repeated) = replay.next().map_err(|e| e.to_string())? else {
        return Err("missing replay".into());
    };
    assert_eq!(repeated.id(), fetch.id());
    Ok(())
}

/// Only the outer task error becomes a fixed FetchError with structured originating identity.
#[test]
fn failure_contract() -> Result<(), String> {
    let id = TaskId::new(uuid::Uuid::now_v7());
    let error = fetch_failure(crate::tasks::TaskFailure::new(
        Some(id),
        "Provider unavailable",
    ))
    .map_err(|e| e.to_string())?;
    let value = serde_json::to_value(error).map_err(|e| e.to_string())?;
    assert_eq!(
        value.get("code"),
        Some(&serde_json::json!("vyuh_task_failure"))
    );
    assert_eq!(
        value.get("details"),
        Some(&serde_json::json!({"task_id": id}))
    );
    Ok(())
}

/// Domain Result values are forwarded unchanged, while failed task resumes terminate.
#[test]
fn suspension_domain_result() -> Result<(), String> {
    let compiled = pravah::compile(|root: pravah::Flow<u32>| root.suspend::<Result<u32, String>>())
        .map_err(|e| e.to_string())?;
    let mut runtime = compiled
        .start(1, uuid::Uuid::now_v7())
        .map_err(|e| e.to_string())?;
    runtime.next().map_err(|e| e.to_string())?;
    let snapshot = runtime.snapshot().map_err(|e| e.to_string())?;
    deliver(
        &mut runtime,
        Some(Ok(serde_json::json!({"Err":"domain rejection"}))),
    )
    .map_err(|e| e.to_string())?;
    let Step::Done(output) = runtime.next().map_err(|e| e.to_string())? else {
        return Err("not done".into());
    };
    assert_eq!(
        compiled.decode_output(output).map_err(|e| e.to_string())?,
        Err("domain rejection".into())
    );
    let mut failed = compiled.restore(snapshot).map_err(|e| e.to_string())?;
    assert!(
        deliver(
            &mut failed,
            Some(Err(crate::tasks::TaskFailure::new(None, "terminal")))
        )
        .is_err()
    );
    Ok(())
}

/// Invalid successful Work output fails protocol validation without altering the wait.
#[test]
fn malformed_fetch_output() -> Result<(), String> {
    let compiled = pravah::compile(fetch_graph).map_err(|e| e.to_string())?;
    let mut runtime = compiled
        .start(1, uuid::Uuid::now_v7())
        .map_err(|e| e.to_string())?;
    runtime.next().map_err(|e| e.to_string())?;
    runtime.next().map_err(|e| e.to_string())?;
    assert!(deliver(&mut runtime, Some(Ok(serde_json::json!({"Ok":42})))).is_err());
    assert!(runtime.pending_fetch().is_some());
    Ok(())
}
