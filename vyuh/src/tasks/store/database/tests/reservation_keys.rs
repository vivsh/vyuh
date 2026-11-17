use super::super::claim::tests::row;
use super::*;

/// Preserves the original handler, escaped key, and full owning UUID in batch order.
#[test]
fn preserves_reservation_identity() -> Result<(), TaskRuntimeError> {
    let mut first = row(5, Utc::now(), None);
    first.name = "handler-a".into();
    first.idempotency_key = Some("CAFÉ \"quoted\" \\".into());
    let mut second = row(7, Utc::now(), None);
    second.name = "handler-b".into();
    second.idempotency_key = first.idempotency_key.clone();
    let ordered = reservation_keys(&[first, second], &[1, 0])?;
    let decoded: Vec<(String, String, uuid::Uuid)> = serde_json::from_str(&ordered)?;
    let key = "CAFÉ \"quoted\" \\";
    assert_eq!(
        decoded,
        [
            ("handler-b".into(), key.into(), uuid::Uuid::from_u128(7)),
            ("handler-a".into(), key.into(), uuid::Uuid::from_u128(5)),
        ]
    );
    Ok(())
}
