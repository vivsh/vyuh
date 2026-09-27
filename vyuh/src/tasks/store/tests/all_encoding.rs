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

/// Fan-out limits are deployment policy and cannot diverge between workers.
#[test]
fn join_limit_is_fingerprinted() {
    let mut conf = crate::tasks::store::memory::tests::conf();
    let before = crate::tasks::store::policy_fingerprint(&conf);
    conf.max_all_children += 1;
    assert_ne!(before, crate::tasks::store::policy_fingerprint(&conf));
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
