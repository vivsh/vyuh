use super::*;
use crate::tasks::AbstractTaskStore;

#[path = "cancellation_upgrade.rs"]
mod upgrade;

/// SQL stores implement the same cancellation transitions and delivery boundaries as memory.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a disposable dbharness target"
)]
async fn database_cancellation_contract() -> Result<(), String> {
    let store = store().await?;
    crate::tasks::store::memory::tests::cancellation::contract(&store)
        .await
        .map_err(|error| error.to_string())?;
    drop(store);
    crate::tasks::store::memory::tests::cancellation::lanes::contract(&self::store().await?)
        .await
        .map_err(|error| error.to_string())?;
    null_readiness(&self::store().await?)
        .await
        .map_err(|error| error.to_string())?;
    crate::tasks::store::memory::tests::cancellation::lanes::rate(&self::store().await?)
        .await
        .map_err(|error| error.to_string())?;
    let short = DbTaskStore {
        lease_duration: std::time::Duration::from_millis(50),
        ..self::store().await?
    };
    crate::tasks::store::memory::tests::cancellation::lanes::takeover(&short)
        .await
        .map_err(|error| error.to_string())
}

/// SQL NULL readiness is preserved by the conditional cancellation update.
async fn null_readiness(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use crate::tasks::store::memory::tests::{claim, conf, record, write};
    use db::DbSession as _;
    store.initialize(conf()).await?;
    let task = record();
    let id = task.id;
    store.store_tasks(vec![write(task)]).await?;
    store
        .pool
        .clone()
        .execute(db::Statement::raw("UPDATE vyuh_tasks SET ready_at = NULL"))
        .await?;
    assert!(store.cancel(id).await?);
    let task = store
        .get_task(id)
        .await?
        .ok_or_else(|| TaskRuntimeError::TaskNotFound(id.to_string()))?;
    assert!(task.ready_at.is_none());
    assert!(
        store
            .claim_tasks("owner", &[claim()])
            .await?
            .lanes
            .iter()
            .all(|lane| lane.tasks.is_empty())
    );
    Ok(())
}
