use super::*;

/// Counts the envelope and accepts exactly 32 KiB, rejecting the next byte.
#[test]
fn exact_result_limit() -> Result<(), TaskRuntimeError> {
    let value = "x".repeat(RESULT_LIMIT - 9);
    assert!(matches!(
        super::super::WorkState::complete(value.clone()).into_outcome(),
        super::super::TaskOutcome::CompleteWith { .. }
    ));
    assert!(matches!(
        super::super::WorkState::complete(format!("{value}x")).into_outcome(),
        super::super::TaskOutcome::Fail { .. }
    ));
    let json = success(&serde_json::to_string(&value)?);
    assert_eq!(json.len(), RESULT_LIMIT);
    validate_resume(&json)
}

/// UTF-8 and escaping are measured as serialized bytes rather than characters.
#[test]
fn escaped_result_limit() -> Result<(), TaskRuntimeError> {
    let output = serde_json::to_string(&"\u{0}é".repeat(9000))?;
    assert!(matches!(
        validate_output(&output),
        Err(TaskRuntimeError::ResultTooLarge { .. })
    ));
    assert!(validate_resume("{\"Unexpected\":null}").is_err());
    assert!(validate_output("invalid").is_err());
    Ok(())
}

/// Generated failures always remain valid, bounded envelopes with the original identity.
#[test]
fn failures_fit_envelope() -> Result<(), TaskRuntimeError> {
    let id = TaskId::new(uuid::Uuid::new_v4());
    let encoded = failure(id, "\u{0}é".repeat(40_000));
    validate_resume(&encoded)?;
    let result = serde_json::from_str::<Result<(), TaskFailure>>(&encoded)?;
    let error = result
        .err()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("expected failure".into()))?;
    assert_eq!(error.task_id(), Some(&id));
    assert!(encoded.len() <= RESULT_LIMIT);
    Ok(())
}
