use super::*;

/// Only ledger-marked exact predecessors may adopt the typed-return capability protocol.
#[test]
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
fn typed_protocol_requires_migration() {
    let conf = crate::tasks::store::memory::tests::conf();
    let previous = format!("tr-v4:{:.58}", policy_hash(&conf, 4).to_hex());
    assert!(!is_migrated_policy(&conf, &previous));
    let migrated = previous.replacen("tr-v4:", "tr-a4:", 1);
    assert!(is_migrated_policy(&conf, &migrated));
    let mut changed = conf;
    changed
        .handlers
        .push(("extra".into(), crate::tasks::TaskKind::Work));
    assert!(!is_migrated_policy(&changed, &migrated));
}

/// Graph, policy, revision and budget identities fence deployments without extra queries.
#[test]
fn factory_identity_is_fingerprinted() {
    let mut conf = crate::tasks::store::memory::tests::conf();
    conf.flows = vec![("workflow".into(), "graph-policy-revision-budget-a".into())];
    let original = policy_fingerprint(&conf);
    conf.flows = vec![("workflow".into(), "graph-policy-revision-budget-b".into())];
    assert_ne!(original, policy_fingerprint(&conf));
    let predecessor = format!("tr-v6:{:.58}", policy_hash(&conf, 6).to_hex());
    assert!(!is_migrated_policy(&conf, &predecessor));
    assert!(is_migrated_policy(
        &conf,
        &predecessor.replacen("tr-v6:", "tr-a6:", 1)
    ));
}
