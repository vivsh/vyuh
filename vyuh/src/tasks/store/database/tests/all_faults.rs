use super::*;

#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct Counter {
    #[column(primary_key)]
    id: uuid::Uuid,
    remaining_completions: i32,
}

/// Counter damage never rolls back valid children or leaves an endlessly suspended parent.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn underflow_isolated() -> Result<(), String> {
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    underflow(&store).await.map_err(|e| e.to_string())
}

/// Injects an invalid counter through a store transaction, then exercises the normal completion path.
async fn underflow(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    use crate::db::Model as _;
    let (parent, ids) = prepare(store, 2).await?;
    let table = Counter::table();
    db::from(&table)
        .filter(table.id.eq(db::val(parent.into_uuid())))
        .update(&Counter {
            id: parent.into_uuid(),
            remaining_completions: 1,
        })
        .exec(&mut store.pool.clone())
        .await?;
    let commits = ids
        .iter()
        .map(|id| commit(*id, TaskOutcome::Complete))
        .collect::<Vec<_>>();
    store.commit_outcomes("children", &commits).await?;
    let table = DbTaskStore::table();
    let task = db::from(&table)
        .filter(table.id.eq(db::val(parent.into_uuid())))
        .first::<crate::tasks::store::database::model::TaskClaimRow>()
        .exec(&mut store.pool.clone())
        .await?
        .map(crate::tasks::store::database::model::TaskRow::from)
        .ok_or_else(super::missing)?;
    assert!(task.waiting_children.is_none());
    assert_eq!(task.remaining_completions, 0);
    assert!(
        task.resume_input
            .as_deref()
            .is_some_and(|input| input.contains("underflow"))
    );
    for id in ids {
        assert_eq!(
            store.get_task(id).await?.ok_or_else(super::missing)?.status,
            TaskStatus::Succeeded
        );
    }
    Ok(())
}

/// A rollback after every group mutation restores the parent and removes every child and wait.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn group_creation_rollback() -> Result<(), String> {
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    group_rollback(&store).await.map_err(|e| e.to_string())
}

/// Calls production mutation groups inside a transaction deliberately rolled back before acknowledgement.
async fn group_rollback(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    let parent = flow_record();
    let id = parent.id;
    store.store_tasks(vec![write(parent)]).await?;
    store.claim_tasks("parent", &[claim()]).await?;
    let child = record();
    let child_id = child.id;
    let commits = [commit(
        id,
        TaskOutcome::All {
            state: "7".into(),
            children: vec![write(child)],
        },
    )];
    let mut tx = store.pool.begin().await?;
    let now = chrono::Utc::now();
    let (children, deliveries, waits) = store
        .commit_outcomes_tx(&mut tx, "parent", &commits, &conf(), now)
        .await?;
    crate::tasks::store::database::writes::finalize_workflow(
        &mut tx,
        &children,
        deliveries,
        &waits,
        &conf(),
        now,
        32,
    )
    .await?;
    tx.rollback().await?;
    let task = store.get_task(id).await?.ok_or_else(super::missing)?;
    assert_eq!(task.status, TaskStatus::Running);
    assert!(task.state.is_none());
    assert!(store.get_task(child_id).await?.is_none());
    store.commit_outcomes("parent", &commits).await?;
    store.commit_outcomes("parent", &commits).await?;
    assert_eq!(
        store
            .get_task(child_id)
            .await?
            .ok_or_else(super::missing)?
            .parent_id,
        Some(id)
    );
    Ok(())
}

#[derive(db::Record)]
struct ResultPatch {
    last_result: Option<String>,
}

/// Malformed persisted member bytes become an outer error while claim ownership still commits.
#[tokio::test]
#[cfg_attr(not(feature = "sqlite"), ignore = "requires disposable dbharness")]
async fn malformed_result_claim() -> Result<(), String> {
    let store = store().await?;
    store.initialize(conf()).await.map_err(|e| e.to_string())?;
    corrupt_result(&store).await.map_err(|e| e.to_string())
}

/// Corruption is scoped to one selected parent and never decodes into an application type.
async fn corrupt_result(store: &DbTaskStore) -> Result<(), TaskRuntimeError> {
    let (parent, ids) = prepare(store, 1).await?;
    let child = *ids.first().ok_or_else(super::missing)?;
    store
        .commit_outcomes("children", &[commit(child, TaskOutcome::Complete)])
        .await?;
    let table = DbTaskStore::table();
    db::from(&table)
        .filter(table.id.eq(db::val(child.into_uuid())))
        .update(&ResultPatch {
            last_result: Some("{\"bad\":true}".into()),
        })
        .exec(&mut store.pool.clone())
        .await?;
    store.claim_tasks("parent", &[claim()]).await?;
    let task = store.get_task(parent).await?.ok_or_else(super::missing)?;
    assert_eq!(task.status, TaskStatus::Running);
    assert!(
        task.resume_input
            .as_deref()
            .is_some_and(|input| input.contains("malformed"))
    );
    Ok(())
}
