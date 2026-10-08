use super::*;
use pravah::{AgentResponse, Step};

#[path = "pravah_boundaries.rs"]
mod boundaries;
#[cfg(all(feature = "sqlite", feature = "migrations"))]
#[path = "pravah_sqlite.rs"]
mod sqlite;

fn agent_graph(root: pravah::Flow<u32>) -> pravah::Flow<u16> {
    root.agent(|agent| {
        agent.configure(|input: u32, _: pravah::Context| async move {
            Ok::<_, pravah::GraphError>(pravah::AgentConfig::new(
                "test:///model",
                "Return a number",
                pravah::clients::Message::user(input.to_string()),
            ))
        })
    })
}

/// A pending Agent cannot be redispatched when the checkpoint has no committed result.
#[test]
fn missing_agent_result_rejected() -> Result<(), String> {
    let compiled = pravah::compile(agent_graph).map_err(|e| e.to_string())?;
    let mut runtime = compiled
        .start(1, uuid::Uuid::now_v7())
        .map_err(|e| e.to_string())?;
    assert!(matches!(
        runtime.next().map_err(|e| e.to_string())?,
        Step::Agent(_)
    ));
    let snapshot = runtime.snapshot().map_err(|e| e.to_string())?;
    let mut restored = compiled.restore(snapshot).map_err(|e| e.to_string())?;
    assert!(deliver(&mut restored, None).is_err());
    Ok(())
}

/// Crash replays preserve request identity and accepted configuration is not redispatched.
#[tokio::test]
async fn replay_delivers_once() -> Result<(), String> {
    let compiled = pravah::compile(agent_graph).map_err(|e| e.to_string())?;
    let id = uuid::Uuid::now_v7();
    let mut runtime = compiled.start(1, id).map_err(|e| e.to_string())?;
    let Step::Agent(agent) = runtime.next().map_err(|e| e.to_string())? else {
        return Err("missing agent request".into());
    };
    let snapshot = runtime.snapshot().map_err(|e| e.to_string())?;
    let executor = compiled.prepared().executor(pravah::Context::default());
    let value = serde_json::to_value(executor.execute(&agent).await).map_err(|e| e.to_string())?;
    for _ in 0..2 {
        let mut restored = compiled
            .restore(snapshot.clone())
            .map_err(|e| e.to_string())?;
        assert_eq!(restored.pending_agent().map(|a| a.id()), Some(agent.id()));
        deliver(&mut restored, Some(Ok(value.clone()))).map_err(|e| e.to_string())?;
        assert!(restored.pending_agent().is_none());
        assert!(deliver(&mut restored, Some(Ok(value.clone()))).is_err());
        let accepted = restored.snapshot().map_err(|e| e.to_string())?;
        let mut restored = compiled.restore(accepted).map_err(|e| e.to_string())?;
        assert_next_generation(&mut restored, agent.id())?;
    }
    let mut replay = compiled.start(1, id).map_err(|e| e.to_string())?;
    let Step::Agent(repeated) = replay.next().map_err(|e| e.to_string())? else {
        return Err("missing replay".into());
    };
    assert_eq!(repeated.id(), agent.id());
    Ok(())
}

/// An accepted configure response advances to generation instead of dispatching configure again.
fn assert_next_generation(runtime: &mut pravah::Runtime, prior: uuid::Uuid) -> Result<(), String> {
    for _ in 0..8 {
        if let Step::Agent(next) = runtime.next().map_err(|e| e.to_string())? {
            assert_ne!(next.id(), prior);
            assert_eq!(next.kind(), "generate");
            return Ok(());
        }
    }
    Err("accepted configuration did not advance to generation".into())
}

/// Only the outer task error becomes a fixed AgentError with structured originating identity.
#[test]
fn failure_contract() -> Result<(), String> {
    let id = TaskId::new(uuid::Uuid::now_v7());
    let error = agent_failure(crate::tasks::TaskFailure::new(
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
fn malformed_agent_output() -> Result<(), String> {
    let compiled = pravah::compile(agent_graph).map_err(|e| e.to_string())?;
    let mut runtime = compiled
        .start(1, uuid::Uuid::now_v7())
        .map_err(|e| e.to_string())?;
    runtime.next().map_err(|e| e.to_string())?;
    assert!(deliver(&mut runtime, Some(Ok(serde_json::json!({"Ok":42})))).is_err());
    assert!(runtime.pending_agent().is_some());
    Ok(())
}

/// Wrong identities, versions and fabricated history acknowledgements leave the wait intact.
#[test]
fn invalid_agent_responses() -> Result<(), String> {
    let compiled = pravah::compile(agent_graph).map_err(|e| e.to_string())?;
    let mut runtime = compiled
        .start(1, uuid::Uuid::now_v7())
        .map_err(|e| e.to_string())?;
    let Step::Agent(request) = runtime.next().map_err(|e| e.to_string())? else {
        return Err("missing request".into());
    };
    let response = AgentResponse::new(request.id(), Err(pravah::AgentError::new("test", "failed")));
    let value = serde_json::to_value(response).map_err(|e| e.to_string())?;
    for (key, replacement) in [
        ("id", serde_json::json!(uuid::Uuid::now_v7())),
        ("version", serde_json::json!(0)),
        ("persisted_through", serde_json::json!(999)),
    ] {
        let mut invalid = value.clone();
        let field = invalid.get_mut(key).ok_or("missing response field")?;
        *field = replacement;
        assert!(deliver(&mut runtime, Some(Ok(invalid))).is_err());
        assert_eq!(runtime.pending_agent().map(|r| r.id()), Some(request.id()));
    }
    deliver(
        &mut runtime,
        Some(Err(crate::tasks::TaskFailure::new(None, "cancelled"))),
    )
    .map_err(|e| e.to_string())?;
    assert!(runtime.pending_agent().is_none());
    Ok(())
}
