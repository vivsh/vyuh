/// Real Pravah snapshots and routed Work use the same transaction contract on SQL stores.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn database_graph_dispatch_contract() -> Result<(), String> {
    use crate::tasks::pravah_flow::dispatch_tests::{bundle, conf, contract};
    let mut store = super::store().await?;
    store.lease_duration = std::time::Duration::from_secs(2);
    let site = crate::Site::test(
        crate::SiteConf::default().log_init(false).tasks(conf()),
        bundle(),
        store.pool.as_sqlx().clone(),
    )
    .await
    .map_err(|e| e.to_string())?;
    contract(&site, std::sync::Arc::new(store)).await
}
