use super::*;

/// Only ledger-marked exact predecessors may adopt the typed-return capability protocol.
#[test]
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
fn typed_protocol_requires_migration() {
    let conf = crate::tasks::store::memory::tests::conf();
    let previous = format!("tr-v4:{:.58}", policy_hash(&conf, 4).to_hex());
    assert!(!is_migrated_policy(&conf, &previous));
    let migrated = previous.replacen("tr-v4:", "tr-t4:", 1);
    assert!(is_migrated_policy(&conf, &migrated));
    let mut changed = conf;
    changed
        .handlers
        .push(("extra".into(), crate::tasks::TaskKind::Work));
    assert!(!is_migrated_policy(&changed, &migrated));
}
