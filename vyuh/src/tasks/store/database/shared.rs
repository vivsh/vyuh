//! Opt-in arbitration and terminal delivery through existing private topic waits.

use std::collections::{HashMap, HashSet};

use super::{
    model::{TaskIdempotencyRow, TaskRow},
    queries::WRITE_CHUNK_SIZE,
    submit::{self, OwnerKey},
    workflow::Deliveries,
};
use crate::{
    db,
    tasks::{
        TaskId, TaskReceipt, TaskRecord, TaskRuntimeError, TaskWrite, models::ShareRole,
        store::sharing,
    },
};
use chrono::{DateTime, Utc};
use db::{DbSession as _, Record as _};
use uuid::Uuid;

pub(super) type Topics = Vec<(Uuid, Option<Vec<u8>>)>;
type PublisherResults = HashMap<Vec<u8>, (i16, String)>;
type PageOwners = HashMap<Vec<u8>, (TaskId, String, Option<Uuid>)>;
type Subscriber = (Vec<u8>, Uuid, Option<Uuid>);

/// Operation-local records and waits; nothing is published until group validation succeeds.
pub(super) struct Resolved {
    pub rows: Vec<TaskRow>,
    pub receipts: Vec<TaskReceipt>,
    pub rejected: HashSet<TaskId>,
    pub topics: Topics,
}

pub(super) fn requested(writes: &[TaskWrite]) -> bool {
    writes
        .iter()
        .any(|write| write.record.share_role == ShareRole::Publisher)
}

/// Arbitrates each distinct key once, including duplicate inputs within a bulk submission.
pub(super) async fn resolve(
    tx: &mut db::DbTransaction<'_>,
    writes: Vec<TaskWrite>,
    now: DateTime<Utc>,
) -> Result<Resolved, TaskRuntimeError> {
    let (prepared, mut keys) = submit::prepare_writes(writes, now)?;
    keys.sort_by(|a, b| (&a.task_name, &a.key_value).cmp(&(&b.task_name, &b.key_value)));
    let ids: HashSet<_> = keys.iter().map(|key| key.task_id).collect();
    let first: HashMap<_, _> = prepared
        .iter()
        .filter(|entry| ids.contains(&entry.write.record.id.into_uuid()))
        .map(|entry| (entry.write.record.id.into_uuid(), &entry.write.record))
        .collect();
    let wanted: HashSet<_> = prepared
        .iter()
        .filter(|entry| entry.write.record.share_role == ShareRole::Publisher)
        .filter_map(|entry| entry.owner_key.as_ref())
        .cloned()
        .collect();
    let holders = reserve(tx, &keys, &first, &wanted).await?;
    let inserted: HashSet<_> = holders
        .values()
        .map(|row| row.id.into_uuid())
        .filter(|id| ids.contains(id))
        .collect();
    let mut result = Resolved {
        rows: Vec::new(),
        receipts: Vec::with_capacity(prepared.len()),
        rejected: HashSet::new(),
        topics: Vec::new(),
    };
    let mut dependencies = HashMap::<TaskId, Vec<TaskId>>::new();
    for entry in prepared {
        let owner = entry
            .owner_key
            .as_ref()
            .map(|key| holders.get(key).ok_or_else(contention))
            .transpose()?;
        let record = entry.write.record;
        resolve_one(
            &mut result,
            &mut dependencies,
            record,
            owner,
            owner.is_some_and(|row| inserted.contains(&row.id.into_uuid())),
            now,
        )?;
    }
    reject_dependents(&mut result.rejected, dependencies);
    discard(tx, &mut result, &inserted).await?;
    Ok(result)
}

/// A held incompatible key leaves no subscriber or waiter behind.
fn resolve_one(
    result: &mut Resolved,
    dependencies: &mut HashMap<TaskId, Vec<TaskId>>,
    mut record: TaskRecord,
    owner: Option<&TaskRecord>,
    provisional: bool,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let id = record.id;
    if let Some(owner) = owner.filter(|owner| owner.id != id) {
        if record.share_role != ShareRole::Publisher || owner.share_role != ShareRole::Publisher {
            result.receipts.push(TaskReceipt::KeyHeld);
            if let Some(parent) = record.parent_id {
                result.rejected.insert(parent);
            }
            return Ok(());
        }
        if let Some(topic) = sharing::subscribe(&mut record, owner, now)? {
            result.topics.push((id.into_uuid(), Some(topic)));
        }
        if provisional
            && let (Some(publisher_parent), Some(subscriber_parent)) =
                (owner.parent_id, record.parent_id)
        {
            dependencies
                .entry(publisher_parent)
                .or_default()
                .push(subscriber_parent);
        }
    }
    result.rows.push(TaskRow::from(record));
    result.receipts.push(TaskReceipt::Queued(id));
    Ok(())
}

/// A provisional publisher in a rejected group cannot be used by another group.
fn reject_dependents(rejected: &mut HashSet<TaskId>, mut edges: HashMap<TaskId, Vec<TaskId>>) {
    let mut pending: Vec<_> = rejected.iter().copied().collect();
    while let Some(parent) = pending.pop() {
        for dependant in edges.remove(&parent).into_iter().flatten() {
            if rejected.insert(dependant) {
                pending.push(dependant);
            }
        }
    }
}

/// Removes only this operation's provisional winners and their staged subscriptions.
async fn discard(
    tx: &mut db::DbTransaction<'_>,
    result: &mut Resolved,
    ids: &HashSet<Uuid>,
) -> Result<(), TaskRuntimeError> {
    if result.rejected.is_empty() {
        return Ok(());
    }
    let mut removed = HashSet::new();
    result.rows.retain(|row| {
        let reject = row
            .parent_id
            .is_some_and(|id| result.rejected.contains(&TaskId::new(id)));
        if reject {
            removed.insert(row.id);
        }
        !reject
    });
    result.topics.retain(|(id, _)| !removed.contains(id));
    let mut owned: Vec<_> = removed.intersection(ids).copied().collect();
    owned.sort_unstable();
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    super::held::delete_provisional(tx, owned, WRITE_CHUNK_SIZE).await?;
    #[cfg(feature = "mysql")]
    for chunk in owned.chunks(WRITE_CHUNK_SIZE) {
        let table = super::DbTaskStore::idempotency_table();
        db::from(&table)
            .filter(table.task_id.in_values(chunk.to_vec()))
            .delete()
            .exec(tx)
            .await?;
    }
    Ok(())
}

/// PostgreSQL and SQLite retain the existing partial index predicate.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn reserve(
    tx: &mut db::DbTransaction<'_>,
    keys: &[TaskIdempotencyRow],
    first: &HashMap<Uuid, &TaskRecord>,
    wanted: &HashSet<OwnerKey>,
) -> Result<HashMap<OwnerKey, TaskRecord>, TaskRuntimeError> {
    let candidates: Vec<_> = keys
        .iter()
        .filter_map(|key| first.get(&key.task_id))
        .map(|record| TaskRow::from((*record).clone()))
        .collect();
    let inserted = insert(tx, &candidates, true).await?;
    let mut holders = HashMap::new();
    let mut unresolved = Vec::new();
    for key in keys {
        let identity = key_identity(key);
        let candidate = first
            .get(&key.task_id)
            .ok_or_else(|| invariant("Missing first shared input"))?;
        if inserted.contains(&key.task_id) {
            holders.insert(identity, (*candidate).clone());
        } else if wanted.contains(&identity) {
            unresolved.push(key);
        } else {
            holders.insert(identity, conflict(candidate));
        }
    }
    for row in lock_keys(tx, &unresolved).await? {
        let key = row
            .idempotency_key
            .clone()
            .ok_or_else(|| invariant("Held key missing"))?;
        holders.insert((row.name.clone(), key), row.try_into()?);
    }
    if holders.len() != keys.len() {
        return Err(contention());
    }
    Ok(holders)
}

/// Existing owner reservations precede the non-waiting publisher primary-key lock.
#[cfg(feature = "mysql")]
async fn reserve(
    tx: &mut db::DbTransaction<'_>,
    keys: &[TaskIdempotencyRow],
    first: &HashMap<Uuid, &TaskRecord>,
    wanted: &HashSet<OwnerKey>,
) -> Result<HashMap<OwnerKey, TaskRecord>, TaskRuntimeError> {
    let mut holders = HashMap::new();
    let mut unresolved = Vec::new();
    for chunk in keys.chunks(WRITE_CHUNK_SIZE) {
        submit::upsert_key_owners(tx, chunk, WRITE_CHUNK_SIZE).await?;
        let owners = submit::load_key_owners(tx, chunk).await?;
        for owner in owners {
            let identity = key_identity(&owner);
            if let Some(record) = first.get(&owner.task_id) {
                holders.insert(identity, (*record).clone());
            } else if wanted.contains(&identity) {
                unresolved.push(owner.task_id);
            } else if let Some(key) = chunk
                .iter()
                .find(|key| key.task_name == owner.task_name && key.key_value == owner.key_value)
                && let Some(candidate) = first.get(&key.task_id)
            {
                holders.insert(identity, conflict(candidate));
            }
        }
    }
    for row in lock_ids(tx, unresolved, false).await? {
        let key = row
            .idempotency_key
            .clone()
            .ok_or_else(|| invariant("Held key missing"))?;
        holders.insert((row.name.clone(), key), row.try_into()?);
    }
    if holders.len() != keys.len() {
        return Err(contention());
    }
    Ok(holders)
}

fn key_identity(key: &TaskIdempotencyRow) -> OwnerKey {
    (key.task_name.clone(), key.key_value.clone())
}

/// Unshared conflicts in a mixed group need no owner lookup or stored payload.
fn conflict(candidate: &TaskRecord) -> TaskRecord {
    let mut record = candidate.clone();
    record.id = TaskId::new(Uuid::nil());
    record.share_role = ShareRole::Unshared;
    record
}

fn invariant(message: &str) -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError(message.into())
}

fn contention() -> TaskRuntimeError {
    db::DbError::Serialization {
        operation: db::DbOperation::FetchAll,
        source: db::sqlx::Error::Protocol(CONTENTION.into()),
    }
    .into()
}

const CONTENTION: &str = "Shared publisher or subscriber changed during arbitration";

pub(super) fn is_contention(error: &TaskRuntimeError) -> bool {
    matches!(error, TaskRuntimeError::StoreError(db::DbError::Serialization { source: db::sqlx::Error::Protocol(message), .. }) if message == CONTENTION)
}

fn param(index: usize) -> String {
    if cfg!(feature = "postgres") {
        format!("${index}")
    } else {
        "?".into()
    }
}

fn slots(offset: usize, len: usize) -> String {
    (offset..offset + len)
        .map(param)
        .collect::<Vec<_>>()
        .join(",")
}

/// Binds only opted-in inserts with the private role and (where present) the key hold.
pub(super) async fn insert(
    tx: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    reserve: bool,
) -> Result<HashSet<Uuid>, TaskRuntimeError> {
    let mut columns = TaskRow::record_insert_column_names();
    columns.push("share_role".into());
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    columns.push("idempotency_held".into());
    let chunk_size = (db::backend::PARAMETER_LIMIT / columns.len()).min(WRITE_CHUNK_SIZE);
    let mut inserted = HashSet::with_capacity(rows.len());
    for chunk in rows.chunks(chunk_size) {
        let args = shared_args(chunk)?;
        let query = insert_sql(&columns, chunk.len(), reserve);
        #[cfg(any(feature = "postgres", feature = "sqlite"))]
        {
            let ids: Vec<(Uuid,)> = tx.fetch_all(db::Statement::new(&query, args)).await?;
            inserted.extend(ids.into_iter().map(|row| row.0));
        }
        #[cfg(feature = "mysql")]
        {
            tx.execute(db::Statement::new(&query, args)).await?;
            inserted.extend(chunk.iter().map(|row| row.id));
        }
    }
    Ok(inserted)
}

fn shared_args(rows: &[TaskRow]) -> Result<db::Arguments<'static>, TaskRuntimeError> {
    use db::sqlx::Arguments as _;
    let mut args = db::Arguments::default();
    for row in rows {
        row.record_bind_insert_values(&mut args)?;
        args.add(row.share_role).map_err(db::sqlx::Error::Encode)?;
        #[cfg(any(feature = "postgres", feature = "sqlite"))]
        args.add(row.idempotency_held)
            .map_err(db::sqlx::Error::Encode)?;
    }
    Ok(args)
}

fn insert_sql(columns: &[String], len: usize, _reserve: bool) -> String {
    let values = (0..len)
        .map(|row| format!("({})", slots(row * columns.len() + 1, columns.len())))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "INSERT INTO vyuh_tasks ({}) VALUES {values}",
        columns.join(",")
    );
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    {
        if _reserve {
            return format!(
                "{sql} ON CONFLICT (name,idempotency_key) WHERE {} DO NOTHING RETURNING id",
                super::held::PREDICATE
            );
        }
        format!("{sql} RETURNING id")
    }
    #[cfg(feature = "mysql")]
    sql
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
const LOCK: &str = "FOR UPDATE NOWAIT";
#[cfg(feature = "sqlite")]
const LOCK: &str = "";

/// Reads only current held rows; a released hold is never resolved by history.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn lock_keys(
    tx: &mut db::DbTransaction<'_>,
    keys: &[&TaskIdempotencyRow],
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    let mut rows = Vec::new();
    for chunk in keys.chunks(256) {
        let mut bindings = Vec::new();
        let mut groups = Vec::new();
        for key in chunk {
            let name = param(bindings.len() + 1);
            let value = param(bindings.len() + 2);
            groups.push(format!("(name={name} AND idempotency_key={value})"));
            bindings.push(key.task_name.clone());
            bindings.push(key.key_value.clone());
        }
        let sql = format!(
            "SELECT {} FROM vyuh_tasks WHERE {} AND ({}) ORDER BY id {LOCK}",
            TaskRow::record_column_names().join(","),
            super::held::PREDICATE,
            groups.join(" OR ")
        );
        let mut stmt = db::Statement::raw(&sql);
        for binding in bindings {
            stmt = stmt.bind(binding);
        }
        rows.extend(tx.fetch_all(stmt).await.map_err(lock_error)?);
    }
    Ok(rows)
}

/// Uses the exact publisher or subscriber UUIDs, never taking a task-table range lock.
#[cfg(feature = "mysql")]
async fn lock_ids(
    tx: &mut db::DbTransaction<'_>,
    mut ids: Vec<Uuid>,
    only_subscribers: bool,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    ids.sort_unstable();
    ids.dedup();
    let guard = if only_subscribers {
        "AND status=2 AND NOT cancelled AND share_role=2"
    } else {
        ""
    };
    let mut rows = Vec::new();
    for chunk in ids.chunks(256) {
        let sql = format!(
            "SELECT {} FROM vyuh_tasks FORCE INDEX (PRIMARY) WHERE id IN ({}) {guard} ORDER BY id {LOCK}",
            TaskRow::record_column_names().join(","),
            slots(1, chunk.len())
        );
        let mut stmt = db::Statement::raw(&sql);
        for id in chunk {
            stmt = stmt.bind(*id);
        }
        rows.extend(tx.fetch_all(stmt).await.map_err(lock_error)?);
    }
    Ok(rows)
}

/// Translates only the NOWAIT operation's native contention, not unrelated SQL errors.
fn lock_error(error: db::DbError) -> TaskRuntimeError {
    use std::error::Error as _;
    if let Some(db::sqlx::Error::Database(native)) = error
        .source()
        .and_then(|err| err.downcast_ref::<db::sqlx::Error>())
    {
        #[cfg(feature = "postgres")]
        if native.code().as_deref() == Some("55P03") {
            return contention();
        }
        #[cfg(feature = "mysql")]
        if native
            .try_downcast_ref::<db::sqlx::mysql::MySqlDatabaseError>()
            .is_some_and(|err| matches!(err.number(), 1205 | 3572))
        {
            return contention();
        }
        #[cfg(feature = "sqlite")]
        let _ = native;
    }
    error.into()
}

/// A shared terminal outcome supplies a result; a drainer refers to that immutable generation.
pub(super) enum PageSource {
    Publisher {
        id: TaskId,
        status: i16,
        result: String,
        lane: String,
    },
    Drainer {
        id: Uuid,
        publisher: TaskId,
        lane: String,
    },
}

/// Reads terminal envelopes once per bounded group, without re-locking published generations.
async fn retained_results(
    tx: &mut db::DbTransaction<'_>,
    sources: &[PageSource],
) -> Result<PublisherResults, TaskRuntimeError> {
    let ids: Vec<_> = sources
        .iter()
        .filter_map(|source| match source {
            PageSource::Drainer { publisher, .. } => Some(publisher.into_uuid()),
            _ => None,
        })
        .collect();
    let mut results = HashMap::new();
    for chunk in ids.chunks(256) {
        let sql = format!(
            "SELECT id,status,last_result FROM vyuh_tasks WHERE id IN ({}) AND share_role=1 AND status IN (3,4)",
            slots(1, chunk.len())
        );
        let mut statement = db::Statement::raw(&sql);
        for id in chunk {
            statement = statement.bind(*id);
        }
        for (id, status, result) in tx.fetch_all::<(Uuid, i16, String)>(statement).await? {
            results.insert(sharing::topic(TaskId::new(id)), (status, result));
        }
    }
    Ok(results)
}

/// Performs independent bounded index seeks per publisher, including one overflow sentinel.
async fn waiters(
    tx: &mut db::DbTransaction<'_>,
    results: &PublisherResults,
) -> Result<Vec<Subscriber>, TaskRuntimeError> {
    let mut topics: Vec<_> = results.iter().collect();
    topics.sort_unstable_by_key(|(topic, _)| *topic);
    let mut rows = Vec::new();
    for chunk in topics.chunks(128) {
        let sql = chunk.iter().enumerate().map(|(i, (_, (_, result)))| {
            let limit = sharing::page_size(result.len()) + 1;
            let key = param(i+1);
            #[cfg(feature="mysql")]
            let select = format!("SELECT topic,task_id,NULL AS parent_id FROM vyuh_task_waits WHERE topic={key} ORDER BY task_id LIMIT {limit} {LOCK}");
            #[cfg(any(feature="sqlite", feature="postgres"))]
            let select = format!("SELECT wait_topic,id,parent_id FROM vyuh_tasks WHERE wait_topic={key} AND status=2 AND NOT cancelled AND share_role=2 ORDER BY id LIMIT {limit} {LOCK}");
            format!("SELECT * FROM ({select}) AS w{i}")
        }).collect::<Vec<_>>().join(" UNION ALL ");
        let mut stmt = db::Statement::raw(&sql);
        for (topic, _) in chunk {
            stmt = stmt.bind((*topic).clone());
        }
        rows.extend(tx.fetch_all(stmt).await.map_err(lock_error)?);
    }
    Ok(rows)
}

#[cfg(not(feature = "postgres"))]
#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct SubscriberWrite {
    #[column(primary_key)]
    id: Uuid,
    status: i16,
    last_result: String,
    updated_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
}

/// Commits one bounded page per generation and schedules only the remaining private waits.
pub(super) async fn publish(
    tx: &mut db::DbTransaction<'_>,
    deliveries: &mut Deliveries,
    lanes: &mut HashSet<String>,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    if deliveries.publishers.is_empty() {
        return Ok(());
    }
    let (results, sources) = page_sources(tx, &mut deliveries.publishers).await?;
    let mut members = waiters(tx, &results).await?;
    let overflow = page_overflow(&mut members, &results);
    #[cfg(feature = "mysql")]
    let members = {
        let tasks = lock_ids(tx, members.iter().map(|member| member.1).collect(), true).await?;
        let mut topics: HashMap<_, _> = members
            .into_iter()
            .map(|(topic, id, _)| (id, topic))
            .collect();
        tasks
            .into_iter()
            .filter_map(|row| {
                topics
                    .remove(&row.id)
                    .map(|topic| (topic, row.id, row.parent_id))
            })
            .collect::<Vec<_>>()
    };
    schedule_pages(tx, sources, overflow, lanes, now).await?;
    #[cfg(feature = "postgres")]
    return publish_postgres(tx, results, members, &mut deliveries.parents, now).await;
    #[cfg(not(feature = "postgres"))]
    publish_rows(tx, results, members, &mut deliveries.parents, now).await
}

/// Maps each private generation to its persisted result and its continuation, if any.
async fn page_sources(
    tx: &mut db::DbTransaction<'_>,
    pending: &mut Vec<PageSource>,
) -> Result<(PublisherResults, PageOwners), TaskRuntimeError> {
    let mut results = retained_results(tx, pending).await?;
    let mut sources = HashMap::new();
    for source in pending.drain(..) {
        let (publisher, lane, drainer) = match source {
            PageSource::Publisher {
                id,
                status,
                result,
                lane,
            } => {
                results.insert(sharing::topic(id), (status, result));
                (id, lane, None)
            }
            PageSource::Drainer {
                id,
                publisher,
                lane,
            } => (publisher, lane, Some(id)),
        };
        let topic = sharing::topic(publisher);
        if !results.contains_key(&topic) {
            return Err(invariant("Shared drainer publisher result missing"));
        }
        sources.insert(topic, (publisher, lane, drainer));
    }
    Ok((results, sources))
}

/// Separates one overflow sentinel per topic; the sentinel remains a waiting subscriber.
fn page_overflow(members: &mut Vec<Subscriber>, results: &PublisherResults) -> Vec<Vec<u8>> {
    let mut seen = HashMap::new();
    let mut overflow = Vec::new();
    members.retain(|member| {
        let count = seen.entry(member.0.clone()).or_insert(0);
        *count += 1;
        let more = results
            .get(&member.0)
            .is_some_and(|(_, result)| *count > sharing::page_size(result.len()));
        if more {
            overflow.push(member.0.clone());
        }
        !more
    });
    overflow
}

/// Inserts or checkpoints exactly one overflow drainer per generation in this transaction.
async fn schedule_pages(
    tx: &mut db::DbTransaction<'_>,
    mut sources: PageOwners,
    overflow: Vec<Vec<u8>>,
    lanes: &mut HashSet<String>,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let mut inserts = Vec::new();
    let mut sleeps = Vec::new();
    for topic in overflow {
        let (publisher, lane, drainer) = sources
            .remove(&topic)
            .ok_or_else(|| invariant("Shared page lost its publisher"))?;
        lanes.insert(lane.clone());
        if let Some(id) = drainer {
            sleeps.push(id);
        } else {
            inserts.push(TaskRow::from(sharing::drainer(publisher, lane, now)));
        }
    }
    if !inserts.is_empty() {
        insert(tx, &inserts, false).await?;
    }
    rearm(tx, &sleeps, now).await
}

/// Re-arms precisely the already-fenced drainer rows, in the subscriber transaction.
async fn rearm(
    tx: &mut db::DbTransaction<'_>,
    ids: &[Uuid],
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    for chunk in ids.chunks(256) {
        let sql = format!(
            "UPDATE vyuh_tasks SET status=0, step_attempts=0, state='null', ready_at={}, completed_at=NULL,last_result=NULL WHERE id IN ({})",
            param(1),
            slots(2, chunk.len())
        );
        let mut stmt = db::Statement::raw(&sql).bind(now);
        for id in chunk {
            stmt = stmt.bind(*id);
        }
        if tx.execute(stmt).await? != chunk.len() as u64 {
            return Err(invariant("Shared checkpoint missed a fenced drainer"));
        }
    }
    Ok(())
}

/// Updates only the subscriber IDs already locked by the membership read.
#[cfg(feature = "postgres")]
async fn publish_postgres(
    tx: &mut db::DbTransaction<'_>,
    results: PublisherResults,
    members: Vec<Subscriber>,
    parents: &mut Vec<(TaskId, String)>,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    if members.is_empty() {
        return Ok(());
    }
    let expected = members.len() as u64;
    let statement = packed_subscribers(results, members, parents, now)?;
    if tx.execute(statement).await? != expected {
        return Err(invariant("Shared subscriber update missed a locked row"));
    }
    Ok(())
}

/// Packs results once per publisher; parent delivery keeps its existing owned result.
#[cfg(feature = "postgres")]
fn packed_subscribers(
    mut results: PublisherResults,
    mut members: Vec<Subscriber>,
    parents: &mut Vec<(TaskId, String)>,
    now: DateTime<Utc>,
) -> Result<db::Statement, TaskRuntimeError> {
    // The existing publication budget bounds the number of publishers and each group
    // has at most PAGE_SIZE members. UNNEST uses a constant number of parameters.
    let mut ids = Vec::with_capacity(members.len());
    let mut member_slots = Vec::with_capacity(members.len());
    let publishers = results.len().min(members.len());
    let mut slots = HashMap::with_capacity(publishers);
    let mut statuses = Vec::with_capacity(publishers);
    let mut outputs = Vec::with_capacity(publishers);
    members.sort_unstable_by_key(|member| member.1);
    for (topic, id, parent) in members {
        let slot = match slots.entry(topic) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let (status, output) = results
                    .remove(entry.key())
                    .ok_or_else(|| invariant("Shared topic lost its result"))?;
                let slot = outputs.len();
                statuses.push(status);
                outputs.push(output);
                *entry.insert(slot)
            }
        };
        if let Some(parent) = parent {
            let output = outputs
                .get(slot)
                .ok_or_else(|| invariant("Shared result slot missing"))?;
            parents.push((TaskId::new(parent), output.clone()));
        }
        ids.push(id);
        member_slots.push(slot as i64 + 1);
    }
    Ok(db::Statement::raw(PACKED_SUBSCRIBERS)
        .bind(ids)
        .bind(member_slots)
        .bind(statuses)
        .bind(outputs)
        .bind(now))
}

#[cfg(feature = "postgres")]
const PACKED_SUBSCRIBERS: &str =
    "UPDATE vyuh_tasks AS t SET status = packed.status, last_result = packed.result,
        wait_topic = NULL, updated_at = $5, completed_at = $5
     FROM unnest($1::uuid[], $2::bigint[]) AS members(id, slot)
     JOIN unnest($3::smallint[], $4::text[]) WITH ORDINALITY AS packed(status, result, slot)
       ON packed.slot = members.slot
     WHERE t.id = members.id";

/// Preserves the existing SQLite and MySQL publication writes.
#[cfg(not(feature = "postgres"))]
async fn publish_rows(
    tx: &mut db::DbTransaction<'_>,
    results: PublisherResults,
    members: Vec<Subscriber>,
    parents: &mut Vec<(TaskId, String)>,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let mut writes = Vec::with_capacity(members.len());
    for (topic, id, parent) in members {
        let (status, result) = results
            .get(&topic)
            .ok_or_else(|| invariant("Shared topic lost its result"))?;
        if let Some(parent) = parent {
            parents.push((TaskId::new(parent), result.clone()));
        }
        writes.push(SubscriberWrite {
            id,
            status: *status,
            last_result: result.clone(),
            updated_at: now,
            completed_at: now,
        });
    }
    if writes.is_empty() {
        return Ok(());
    }
    writes.sort_unstable_by_key(|write| write.id);
    let table = <SubscriberWrite as db::Model>::table();
    db::from(&table)
        .update_many(
            &writes,
            (
                &table.status,
                &table.last_result,
                &table.updated_at,
                &table.completed_at,
            ),
        )
        .batch_size(WRITE_CHUNK_SIZE)
        .exec(tx)
        .await?;
    super::publication::store_changes(tx, writes.into_iter().map(|row| (row.id, None)).collect())
        .await
}
