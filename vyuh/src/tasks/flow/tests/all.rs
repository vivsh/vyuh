use super::*;
use crate::tasks::{FlowState, TaskId};
use serde::Serialize;

#[derive(Serialize, Deserialize, schemars::JsonSchema, Debug, PartialEq)]
struct Image(u32);

#[derive(Serialize, Deserialize, schemars::JsonSchema, Debug, PartialEq)]
struct Document(u32);

/// Mixed builder inputs retain original concrete identities and wire serializers.
#[test]
fn mixed_inputs_preserve_identity() -> Result<(), TaskRuntimeError> {
    type Alias = Image;
    let first: Alias = Image(1);
    let mut children = FlowAllIn::new().with(first).with(Document(2));
    children.push(Image(3));
    assert_eq!(children.len(), 3);
    assert!(!children.is_empty());
    let first = children
        .inputs
        .first()
        .and_then(DataBox::downcast_ref::<Image>);
    assert_eq!(first, Some(&Image(1)));
    assert_eq!(
        children
            .inputs
            .get(1)
            .and_then(DataBox::downcast_ref::<Document>),
        Some(&Document(2))
    );
    assert_eq!(
        children
            .inputs
            .get(2)
            .and_then(DataBox::downcast_ref::<Image>),
        Some(&Image(3))
    );
    assert!(
        children
            .inputs
            .iter()
            .all(|input| input.to_json().is_some())
    );
    FlowState::<()>::all(children, ())?;
    Ok(())
}

/// Every supported tuple arity preserves insertion order without flattening inputs.
#[test]
fn tuple_arities_preserve_order() {
    macro_rules! check {
        ($($value:expr),+) => {{
            let children = FlowAllIn::from(($(Image($value),)+));
            let actual: Vec<_> = children.inputs.iter()
                .filter_map(DataBox::downcast_ref::<Image>).map(|item| item.0).collect();
            assert_eq!(actual, vec![$($value),+]);
        }};
    }
    check!(0);
    check!(0, 1);
    check!(0, 1, 2);
    check!(0, 1, 2, 3);
    check!(0, 1, 2, 3, 4);
    check!(0, 1, 2, 3, 4, 5);
    check!(0, 1, 2, 3, 4, 5, 6);
    check!(0, 1, 2, 3, 4, 5, 6, 7);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
    check!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
}

/// Vector children keep their concrete type, even when that type itself is a tuple.
#[test]
fn vector_and_nested_inputs() -> Result<(), TaskRuntimeError> {
    let children = FlowAllIn::from(vec![(Image(1), Document(2))]);
    assert_eq!(children.len(), 1);
    assert_eq!(
        children
            .inputs
            .first()
            .and_then(DataBox::downcast_ref::<(Image, Document)>),
        Some(&(Image(1), Document(2)))
    );
    let nested = FlowAllIn::from(((Image(3), Document(4)), Image(5)));
    assert_eq!(nested.len(), 2);
    assert!(
        nested
            .inputs
            .first()
            .is_some_and(|input| input.downcast_ref::<(Image, Document)>().is_some())
    );
    assert!(FlowAllIn::default().is_empty());
    assert!(FlowAllIn::from(Vec::<Image>::new()).is_empty());
    FlowState::<()>::all(vec![Image(6)], ())?;
    FlowState::<()>::all((Image(7), Document(8)), ())?;
    Ok(())
}

/// Dynamic result extraction preserves success values, bounded failures, and their task IDs.
#[test]
fn mixed_result_decoding() -> Result<(), TaskRuntimeError> {
    let failure = TaskFailure::new(Some(TaskId::new(uuid::Uuid::from_u128(7))), "cancelled");
    let json = serde_json::to_string(&serde_json::json!([
        {"Ok": {"image": "雪\n\"quoted\""}}, {"Err": failure}, {"Ok": null},
        {"Ok": {"Err": "domain rejection"}}
    ]))?;
    let outputs: FlowAllOut = serde_json::from_str(&json)?;
    assert_eq!(outputs.len(), 4);
    assert!(!outputs.is_empty());
    for _ in 0..2 {
        let image = outputs.decode::<serde_json::Value>(0)?;
        assert_eq!(image, Ok(serde_json::json!({"image": "雪\n\"quoted\""})));
        assert_eq!(outputs.decode::<u32>(1)?, Err(failure.clone()));
    }
    assert_eq!(outputs.decode::<()>(2)?, Ok(()));
    assert_eq!(
        outputs.decode::<Result<(), String>>(3)?,
        Ok(Err("domain rejection".into()))
    );
    Ok(())
}

/// Successful large integers retain their original digits without an intermediate Value tree.
#[test]
fn result_numeric_precision() -> Result<(), TaskRuntimeError> {
    let json = format!(r#"[{{"Ok":{}}},{{"Ok":{}}}]"#, u128::MAX, i128::MIN);
    let outputs: FlowAllOut = serde_json::from_str(&json)?;
    assert_eq!(outputs.decode::<u128>(0)?, Ok(u128::MAX));
    assert_eq!(outputs.decode::<i128>(1)?, Ok(i128::MIN));
    let tuple: (Result<u128, TaskFailure>, Result<i128, TaskFailure>) =
        serde_json::from_str(&json)?;
    assert_eq!(tuple, (Ok(u128::MAX), Ok(i128::MIN)));
    Ok(())
}

/// Extraction errors are explicit, safe for routine diagnostics, and never erase child values.
#[test]
fn result_errors_are_distinct() -> Result<(), TaskRuntimeError> {
    let outputs: FlowAllOut = serde_json::from_str(r#"[{"Ok":"secret-payload"}]"#)?;
    let error = outputs
        .decode::<u32>(0)
        .err()
        .ok_or_else(|| TaskRuntimeError::TaskExecutionError("expected decode error".into()))?;
    assert!(matches!(
        &error,
        TaskRuntimeError::FlowAllDecode { index: 0, .. }
    ));
    assert!(!error.to_string().contains("secret-payload"));
    assert!(std::error::Error::source(&error).is_some());
    assert!(matches!(
        outputs.decode::<()>(1),
        Err(TaskRuntimeError::FlowAllIndex { index: 1, len: 1 })
    ));
    assert!(matches!(
        outputs.decode::<()>(usize::MAX),
        Err(TaskRuntimeError::FlowAllIndex { .. })
    ));
    assert_eq!(outputs.decode::<String>(0)?, Ok("secret-payload".into()));
    let empty: FlowAllOut = serde_json::from_str("[]")?;
    assert!(empty.is_empty());
    assert!(matches!(
        empty.decode::<()>(0),
        Err(TaskRuntimeError::FlowAllIndex { len: 0, .. })
    ));
    Ok(())
}

/// Tuple and dynamic views accept the same envelope and reject malformed or mismatched shapes.
#[test]
fn tuple_and_dynamic_extraction() -> Result<(), TaskRuntimeError> {
    let json = r#"{"Ok":[{"Ok":7},{"Ok":"document"}]}"#;
    type Outputs = (Result<u32, TaskFailure>, Result<String, TaskFailure>);
    let tuple: Result<Outputs, TaskFailure> = serde_json::from_str(json)?;
    assert_eq!(tuple, Ok((Ok(7), Ok("document".into()))));
    let dynamic: Result<FlowAllOut, TaskFailure> = serde_json::from_str(json)?;
    assert!(dynamic.is_ok_and(|items| matches!(items.decode::<u32>(0), Ok(Ok(7)))));
    for invalid in [
        "null",
        "{}",
        r#"[{"Ok":1,"Err":{}}]"#,
        r#"[{"other":1}]"#,
        "[",
    ] {
        assert!(serde_json::from_str::<FlowAllOut>(invalid).is_err());
    }
    assert!(serde_json::from_str::<Outputs>(r#"[{"Ok":7}]"#).is_err());
    assert!(serde_json::from_str::<Outputs>(r#"[{"Ok":7},{"Ok":8}]"#).is_err());
    let outer: Result<FlowAllOut, TaskFailure> =
        serde_json::from_str(r#"{"Err":{"task_id":null,"message":"join overflow"}}"#)?;
    assert!(matches!(outer, Err(error) if error.message() == "join overflow"));
    Ok(())
}

/// Dynamic decoding preserves the complete resume envelope's existing exact byte limit.
#[test]
fn result_envelope_boundaries() -> Result<(), TaskRuntimeError> {
    for (length, accepted) in [(32_750, true), (32_751, false)] {
        let json = format!(r#"{{"Ok":[{{"Ok":"{}"}}]}}"#, "x".repeat(length));
        assert_eq!(
            crate::tasks::result::validate_resume(&json).is_ok(),
            accepted
        );
        if accepted {
            assert_eq!(json.len(), crate::tasks::result::RESULT_LIMIT);
            let outer: Result<FlowAllOut, TaskFailure> = serde_json::from_str(&json)?;
            assert!(outer.is_ok_and(
                |items| matches!(items.decode::<String>(0), Ok(Ok(value)) if value.len() == length)
            ));
        }
    }
    Ok(())
}
