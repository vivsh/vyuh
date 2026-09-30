use super::*;
use crate::tasks::{TaskFailure, TaskOutcome};

/// Builds an immutable persisted snapshot fixture using the production continuation decoder.
fn continuation(
    snapshot: Option<pravah::Snapshot>,
    resume: Option<Result<serde_json::Value, TaskFailure>>,
) -> Result<Continuation<pravah::Snapshot, serde_json::Value>, String> {
    let mut record = crate::tasks::store::fixtures::flow_record();
    record.state = snapshot
        .map(|s| serde_json::to_string(&s))
        .transpose()
        .map_err(|e| e.to_string())?;
    record.resume_input = resume
        .map(|r| serde_json::to_string(&r))
        .transpose()
        .map_err(|e| e.to_string())?;
    Continuation::decode(&record).map_err(|e| e.to_string())
}

fn outcome<T: serde::Serialize + 'static>(state: FlowState<T>) -> Result<Arc<TaskOutcome>, String> {
    state
        .prepare()
        .erase()
        .downcast_arc::<TaskOutcome>()
        .ok_or_else(|| "not a concrete outcome".into())
}

/// The final permitted instruction emits its boundary instead of an unnecessary sleep.
#[test]
fn final_instruction_boundary() -> Result<(), String> {
    let pure = pravah::compile(|root: pravah::Flow<u32>| root.map(|v| v + 1))
        .map_err(|e| e.to_string())?
        .into_flow(Arc::new(()), 1)
        .map_err(|e| e.to_string())?;
    let state = pure
        .advance(
            TaskId::new(uuid::Uuid::now_v7()),
            Data(Arc::new(3)),
            continuation(None, None)?,
        )
        .map_err(|e| e.to_string())?;
    assert!(
        matches!(outcome(state)?.as_ref(), TaskOutcome::CompleteWith { output } if output == "4")
    );
    let suspended = pravah::compile(|root: pravah::Flow<u32>| root.suspend::<u32>())
        .map_err(|e| e.to_string())?
        .into_flow(Arc::new(()), 1)
        .map_err(|e| e.to_string())?;
    let state = suspended
        .advance(
            TaskId::new(uuid::Uuid::now_v7()),
            Data(Arc::new(3)),
            continuation(None, None)?,
        )
        .map_err(|e| e.to_string())?;
    assert!(matches!(
        outcome(state)?.as_ref(),
        TaskOutcome::Suspend { .. }
    ));
    Ok(())
}

/// Budget snapshots reject spurious input and restore without inventing a suspension.
#[test]
fn budget_resume_contract() -> Result<(), String> {
    let compiled = pravah::compile(|root: pravah::Flow<u32>| root.map(|v| v + 1).map(|v| v * 2))
        .map_err(|e| e.to_string())?;
    let prepared = compiled
        .into_flow(Arc::new(()), 1)
        .map_err(|e| e.to_string())?;
    let id = TaskId::new(uuid::Uuid::now_v7());
    let first = prepared
        .advance(id, Data(Arc::new(3)), continuation(None, None)?)
        .map_err(|e| e.to_string())?;
    let first = outcome(first)?;
    let TaskOutcome::Sleep { state, delay } = first.as_ref() else {
        return Err("missing budget yield".into());
    };
    assert_eq!(*delay, Duration::ZERO);
    let snapshot: pravah::Snapshot = serde_json::from_str(state).map_err(|e| e.to_string())?;
    assert!(
        prepared
            .advance(
                id,
                Data(Arc::new(3)),
                continuation(Some(snapshot.clone()), Some(Ok(serde_json::json!(8))))?
            )
            .is_err()
    );
    let next = prepared
        .advance(id, Data(Arc::new(3)), continuation(Some(snapshot), None)?)
        .map_err(|e| e.to_string())?;
    assert!(
        matches!(outcome(next)?.as_ref(), TaskOutcome::CompleteWith { output } if output == "8")
    );
    Ok(())
}

/// Resume without checkpoint, incompatible graph identity, and wrong suspend value all fail.
#[test]
fn inconsistent_restores_fail() -> Result<(), String> {
    let compiled = pravah::compile(|root: pravah::Flow<u32>| root.suspend::<u32>())
        .map_err(|e| e.to_string())?;
    let execution = uuid::Uuid::now_v7();
    let mut runtime = compiled.start(3, execution).map_err(|e| e.to_string())?;
    runtime.next().map_err(|e| e.to_string())?;
    let snapshot = runtime.snapshot().map_err(|e| e.to_string())?;
    let prepared = compiled
        .into_flow(Arc::new(()), 1)
        .map_err(|e| e.to_string())?;
    let id = TaskId::new(execution);
    assert!(
        prepared
            .advance(
                id,
                Data(Arc::new(3)),
                continuation(None, Some(Ok(serde_json::json!(4))))?
            )
            .is_err()
    );
    assert!(
        prepared
            .advance(
                id,
                Data(Arc::new(3)),
                continuation(Some(snapshot.clone()), None)?
            )
            .is_err()
    );
    assert!(
        prepared
            .advance(
                id,
                Data(Arc::new(3)),
                continuation(Some(snapshot.clone()), Some(Ok(serde_json::json!("wrong"))))?
            )
            .is_err()
    );
    reject_different_graph(snapshot, id)?;
    Ok(())
}

/// Restoration validates the graph fingerprint before delivering any result.
fn reject_different_graph(snapshot: pravah::Snapshot, id: TaskId) -> Result<(), String> {
    let other = pravah::compile(|root: pravah::Flow<u32>| root.map(|v| v + 1))
        .map_err(|e| e.to_string())?
        .into_flow(Arc::new(()), 1)
        .map_err(|e| e.to_string())?;
    assert!(
        other
            .advance(
                id,
                Data(Arc::new(3)),
                continuation(Some(snapshot), Some(Ok(serde_json::json!(4))))?
            )
            .is_err()
    );
    Ok(())
}

/// A valid snapshot from another task cannot reuse its Fetch execution namespace.
#[test]
fn foreign_task_snapshot_rejected() -> Result<(), String> {
    let compiled = pravah::compile(|root: pravah::Flow<u32>| root.suspend::<u32>())
        .map_err(|e| e.to_string())?;
    let mut runtime = compiled
        .start(1, uuid::Uuid::now_v7())
        .map_err(|e| e.to_string())?;
    runtime.next().map_err(|e| e.to_string())?;
    let snapshot = runtime.snapshot().map_err(|e| e.to_string())?;
    let prepared = compiled
        .into_flow(Arc::new(()), 1)
        .map_err(|e| e.to_string())?;
    let error = prepared.advance(
        TaskId::new(uuid::Uuid::now_v7()),
        Data(Arc::new(1)),
        continuation(Some(snapshot), Some(Ok(serde_json::json!(2))))?,
    );
    assert!(
        matches!(error, Err(FlowError::Fail(failure)) if failure.message().contains("another task"))
    );
    Ok(())
}

/// Graph completion goes through the exact same serialized envelope limit as manual Flow.
#[test]
fn completion_envelope_limit() -> Result<(), String> {
    for (size, accepted) in [(32759, true), (32760, false)] {
        let prepared = pravah::Flow::<u32>::root()
            .map(move |_| "x".repeat(size))
            .finish::<u32>()
            .map_err(|e| e.to_string())?
            .into_flow(Arc::new(()), 1)
            .map_err(|e| e.to_string())?;
        let state = prepared
            .advance(
                TaskId::new(uuid::Uuid::now_v7()),
                Data(Arc::new(0)),
                continuation(None, None)?,
            )
            .map_err(|e| e.to_string())?;
        assert_eq!(
            matches!(outcome(state)?.as_ref(), TaskOutcome::CompleteWith { .. }),
            accepted
        );
    }
    Ok(())
}

/// Missing effects are rejected inside nested static graphs, not only at the top level.
#[test]
fn nested_fetch_requires_effects() -> Result<(), String> {
    let compiled = pravah::compile(|root: pravah::Flow<Vec<u32>>| root.each(fetch_graph))
        .map_err(|e| e.to_string())?;
    assert!(matches!(
        compiled.into_flow(Arc::new(()), 256),
        Err(FlowError::MissingEffects)
    ));
    Ok(())
}

/// Every graph-bearing node is inspected, including both Either branches and continuations.
#[test]
fn static_fetch_node_variants() -> Result<(), String> {
    use pravah::graph::NodeKind;
    let pure =
        pravah::compile(|root: pravah::Flow<u32>| root.map(|v| v)).map_err(|e| e.to_string())?;
    let fetch = pravah::compile(fetch_graph).map_err(|e| e.to_string())?;
    let mut graph = pure.graph().clone();
    let NodeKind::PureHandler { key } = &graph.nodes[0].kind else {
        return Err("missing pure handler fixture".into());
    };
    let key = key.clone();
    let child = Box::new(fetch.graph().clone());
    for kind in [
        NodeKind::Subflow {
            graph: child.clone(),
        },
        NodeKind::Each {
            graph: child.clone(),
        },
        NodeKind::Either {
            key: key.clone(),
            left: child.clone(),
            right: Box::new(pure.graph().clone()),
        },
        NodeKind::Either {
            key: key.clone(),
            left: Box::new(pure.graph().clone()),
            right: child,
        },
        NodeKind::Continuation {
            key,
            payload: pravah::graph::to_value(()).map_err(|e| e.to_string())?,
            children: vec![pure.graph().clone(), fetch.graph().clone()],
        },
    ] {
        graph.nodes[0].kind = kind;
        assert!(requires_fetch(&graph));
    }
    assert!(!requires_fetch(pure.graph()));
    Ok(())
}

#[derive(Default)]
struct DynamicFetch;
impl pravah::graph::ContinuationHandler for DynamicFetch {
    fn start(
        &self,
        _: &pravah::graph::Value,
        _: Option<pravah::graph::Value>,
        _: Vec<pravah::graph::Value>,
        _: pravah::graph::ContinuationContext<'_>,
    ) -> Result<pravah::graph::ContinuationTransition, pravah::GraphError> {
        Ok(pravah::graph::ContinuationTransition {
            fetch: Some(FetchRequest::new("GET", "https://example.invalid")),
            checkpoint: Some(
                pravah::graph::to_value(())
                    .map_err(|e| pravah::GraphError::GraphValidation(e.to_string()))?,
            ),
            ..Default::default()
        })
    }
    fn advance(
        &self,
        _: &pravah::graph::Value,
        _: pravah::graph::Value,
        _: pravah::graph::ContinuationEvent,
        _: pravah::graph::ContinuationContext<'_>,
    ) -> Result<pravah::graph::ContinuationTransition, pravah::GraphError> {
        Ok(pravah::graph::ContinuationTransition {
            outputs: vec![
                pravah::graph::to_value(1u32)
                    .map_err(|e| pravah::GraphError::GraphValidation(e.to_string()))?,
            ],
            ..Default::default()
        })
    }
}

/// A dynamic Fetch can pass static checks but must explicitly fail at runtime without policy.
#[test]
fn dynamic_fetch_missing_effects() -> Result<(), String> {
    let builder = pravah::graph::TypedGraphBuilder::<u32>::new();
    let output = builder.continuation::<u32, u32, DynamicFetch, ()>(builder.root(), ());
    let prepared = builder
        .finish(output)
        .map_err(|e| e.to_string())?
        .into_flow(Arc::new(()), 256)
        .map_err(|e| e.to_string())?;
    assert!(matches!(
        prepared.advance(
            TaskId::new(uuid::Uuid::now_v7()),
            Data(Arc::new(0)),
            continuation(None, None)?
        ),
        Err(FlowError::MissingEffects)
    ));
    Ok(())
}
