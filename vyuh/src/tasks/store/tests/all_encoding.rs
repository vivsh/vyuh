use super::*;

/// The complete outer envelope, including separators, determines the fixed limit.
#[test]
fn envelope_boundaries() {
    for (length, accepted) in [(32_750, true), (32_751, false)] {
        let result = format!("{{\"Ok\":\"{}\"}}", "x".repeat(length));
        let mut buffer = String::from("{\"Ok\":[");
        assert_eq!(
            append_result(&mut buffer, Some(&result), true).is_ok(),
            accepted
        );
        if accepted {
            buffer.push_str("]}");
            assert_eq!(buffer.len(), 32_768);
        }
    }
}

/// Malformed, nonterminal, and missing members cannot masquerade as successful results.
#[test]
fn invalid_member_results() {
    for (value, terminal) in [
        (None, true),
        (Some("{\"Ok\":1}"), false),
        (Some("{\"other\":1}"), true),
    ] {
        assert!(append_result(&mut "{\"Ok\":[".into(), value, terminal).is_err());
    }
}

/// Weighted accounting rejects expansion but leaves ordinary low-level calls unchanged.
#[test]
fn expanded_flush_budget() {
    use crate::tasks::store::memory::tests::{commit, flow_record, record, write};
    let group = || {
        commit(
            flow_record().id,
            TaskOutcome::All {
                state: "0".into(),
                children: vec![write(record()); 32],
            },
        )
    };
    assert!(validate_turn(&[group()], 32, 32).is_ok());
    assert!(validate_turn(&[group(), group()], 32, 32).is_err());
}

/// Geometric scratch growth never allocates a parent aggregate beyond its fixed result budget.
#[test]
fn bounded_buffer_capacity() {
    let mut buffer = String::from("{\"Ok\":[");
    while append_result(&mut buffer, Some("{\"Ok\":\"\\u0000é\"}"), true).is_ok() {
        assert!(buffer.capacity() <= crate::tasks::result::RESULT_LIMIT);
    }
    buffer.push_str("]}");
    assert!(buffer.len() <= crate::tasks::result::RESULT_LIMIT);
    assert!(crate::tasks::result::validate_resume(&buffer).is_ok());
}

/// Mixed registered handlers and kinds remain valid without weakening per-child validation.
#[test]
fn mixed_group_validation() {
    use crate::tasks::store::memory::tests::{conf, flow_record, record, write};
    let configuration = conf();
    let valid = vec![write(record()), write(flow_record())];
    assert!(validate_group(&valid, &configuration).is_ok());
    let changes: [fn(&mut TaskWrite); 8] = [
        |child| child.record.name = "unregistered".into(),
        |child| child.record.kind = crate::tasks::TaskKind::Work,
        |child| child.record.lane = "unconfigured".into(),
        |child| child.record.status = TaskStatus::Running,
        |child| child.record.cancelled = true,
        |child| child.ignore_conflicts = true,
        |child| child.record.idempotency_key = Some("missing fingerprint".into()),
        |child| {
            child.initial_delay =
                Some(crate::tasks::config::MAX_TASK_DELAY + std::time::Duration::from_secs(1))
        },
    ];
    for change in changes {
        let mut children = valid.clone();
        if let Some(child) = children.get_mut(1) {
            change(child);
        }
        assert!(validate_group(&children, &configuration).is_err());
    }
    let duplicate = write(record());
    assert!(validate_group(&[duplicate.clone(), duplicate], &configuration).is_err());
}

/// Idempotency uniqueness remains handler-scoped in mixed groups, never group-wide by key alone.
#[test]
fn mixed_group_idempotency() {
    use crate::tasks::store::memory::tests::{conf, flow_record, record, write};
    let mut children = vec![write(record()), write(flow_record())];
    for child in &mut children {
        child.record.idempotency_key = Some("same-key".into());
        child.record.idempotency_fingerprint = Some("fingerprint".into());
    }
    assert!(validate_group(&children, &conf()).is_ok());
    if let Some(child) = children.get_mut(1) {
        child.record.name = "workflow".into();
        child.record.kind = crate::tasks::TaskKind::Work;
    }
    assert!(validate_group(&children, &conf()).is_err());
}
