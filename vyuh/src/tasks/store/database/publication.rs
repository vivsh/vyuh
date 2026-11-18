//! Active topic membership and bounded publication transactions.

use super::DbTaskStore;
use crate::{
    db,
    tasks::{PublishResult, TaskRuntimeError},
};
use db::{DbSession as _, Model as _};
use uuid::Uuid;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct TopicPatch {
    #[column(primary_key)]
    id: Uuid,
    wait_topic: Option<Vec<u8>>,
}

#[cfg(feature = "mysql")]
#[derive(db::Model)]
#[table(name = "vyuh_task_waits")]
pub(super) struct TopicWaitRow {
    #[column(primary_key)]
    task_id: Uuid,
    #[column(type = "varbinary(256)")]
    topic: Vec<u8>,
}

/// Applies only accepted registrations and suspended cancellations in the task transaction.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(super) async fn store_changes(
    tx: &mut db::DbTransaction<'_>,
    changes: Vec<(Uuid, Option<Vec<u8>>)>,
) -> Result<(), TaskRuntimeError> {
    if !changes.is_empty() {
        let rows = changes
            .into_iter()
            .map(|(id, wait_topic)| TopicPatch { id, wait_topic })
            .collect::<Vec<_>>();
        let table = TopicPatch::table();
        db::from(&table)
            .update_many(&rows, &table.wait_topic)
            .batch_size(super::queries::WRITE_CHUNK_SIZE)
            .exec(tx)
            .await?;
    }
    Ok(())
}

/// Stores only active waiters, never touching the table for ordinary outcome batches.
#[cfg(feature = "mysql")]
pub(super) async fn store_changes(
    tx: &mut db::DbTransaction<'_>,
    changes: Vec<(Uuid, Option<Vec<u8>>)>,
) -> Result<(), TaskRuntimeError> {
    if changes.is_empty() {
        return Ok(());
    }
    let mut remove = Vec::new();
    let mut insert = Vec::new();
    for (task_id, topic) in changes {
        if let Some(topic) = topic {
            insert.push(TopicWaitRow { task_id, topic });
        } else {
            remove.push(task_id);
        }
    }
    for ids in remove.chunks(super::queries::WRITE_CHUNK_SIZE) {
        remove_waits(tx, ids).await?;
    }
    if !insert.is_empty() {
        db::from(&TopicWaitRow::table())
            .insert_many(&insert)
            .batch_size(super::queries::WRITE_CHUNK_SIZE)
            .exec(tx)
            .await?;
    }
    Ok(())
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl DbTaskStore {
    /// Uses one atomic statement, including selection of the advisory extra waiter.
    pub(super) async fn publish_bounded(
        &self,
        topic: &[u8],
        input: &str,
        limit: usize,
    ) -> Result<PublishResult, TaskRuntimeError> {
        let sql = publish_sql(limit);
        let statement = db::Statement::raw(&sql)
            .bind(topic.to_vec())
            .bind(input.to_owned());
        let mut pool = self.pool.clone();
        #[cfg(feature = "postgres")]
        {
            let (count, more, _): (i64, bool, i64) = pool.fetch_one(statement).await?;
            Ok(PublishResult {
                resumed: count as u64,
                more,
            })
        }
        #[cfg(feature = "sqlite")]
        {
            let rows: Vec<(bool,)> = pool.fetch_all(statement).await?;
            Ok(PublishResult {
                resumed: rows.len() as u64,
                more: rows.first().is_some_and(|row| row.0),
            })
        }
    }
}

/// Builds an indexed candidate slice; LIMIT + OFFSET tests only the sentinel.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn publish_sql(limit: usize) -> String {
    #[cfg(feature = "postgres")]
    let (topic, data, clock) = ("$1", "$2", "statement_timestamp()");
    #[cfg(feature = "sqlite")]
    let (topic, data, clock) = (
        "?1",
        "?2",
        "strftime('%Y-%m-%dT%H:%M:%S+00:00', CURRENT_TIMESTAMP)",
    );
    let candidates = format!("candidates AS MATERIALIZED (
        SELECT id, attempts FROM vyuh_tasks WHERE wait_topic = {topic} AND status = 2 AND NOT cancelled
        ORDER BY id LIMIT {})", limit + 1);
    let mutation = format!(
        "UPDATE vyuh_tasks SET status = 0, wait_topic = NULL,
        resume_input = {data}, ready_at = {clock}, updated_at = {clock}
        WHERE (id, attempts) IN (SELECT id, attempts FROM candidates ORDER BY id LIMIT {limit})
        AND status = 2 AND NOT cancelled AND wait_topic = {topic}"
    );
    let more = format!("EXISTS (SELECT 1 FROM candidates LIMIT 1 OFFSET {limit})");
    #[cfg(feature = "postgres")]
    {
        let result = format!(
            "SELECT COALESCE((SELECT SUM(1) FROM changed), 0), {more},
            COALESCE((SELECT SUM(1) FROM notified), 0)"
        );
        super::notify::notification_with(
            &format!("{candidates}, "),
            &mutation,
            "node_id, lane_name, status",
            &result,
        )
    }
    #[cfg(feature = "sqlite")]
    format!("WITH {candidates} {mutation} RETURNING {more}")
}

#[cfg(feature = "mysql")]
impl DbTaskStore {
    /// Discovers candidates without locking the topic range, then fences by task attempt.
    pub(super) async fn publish_bounded(
        &self,
        topic: &[u8],
        input: &str,
        limit: usize,
    ) -> Result<PublishResult, TaskRuntimeError> {
        let mut tx = self.pool.begin().await?;
        let sql = format!(
            "SELECT w.task_id, t.attempts FROM vyuh_task_waits w
            JOIN vyuh_tasks t ON t.id = w.task_id
            WHERE w.topic = ? AND t.status = 2 AND NOT t.cancelled
            ORDER BY w.task_id LIMIT {}",
            limit + 1
        );
        let mut candidates: Vec<(Uuid, i32)> = tx
            .fetch_all(db::Statement::raw(&sql).bind(topic.to_vec()))
            .await?;
        let more = candidates.len() > limit;
        candidates.truncate(limit);
        let resumed = if candidates.is_empty() {
            0
        } else {
            publish_mysql(&mut tx, topic, input, &candidates).await?
        };
        tx.commit().await?;
        Ok(PublishResult { resumed, more })
    }
}

/// Locks task IDs before verifying current membership and consuming exactly that set.
#[cfg(feature = "mysql")]
async fn publish_mysql(
    tx: &mut db::DbTransaction<'_>,
    topic: &[u8],
    input: &str,
    candidates: &[(Uuid, i32)],
) -> Result<u64, TaskRuntimeError> {
    let ids = candidates.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let sql = format!(
        "SELECT id, attempts FROM vyuh_tasks FORCE INDEX (PRIMARY)
        WHERE status = 2 AND NOT cancelled AND id IN ({}) ORDER BY id FOR UPDATE",
        slots(ids.len())
    );
    let rows: Vec<(Uuid, i32)> = tx
        .fetch_all(bind_ids(db::Statement::raw(&sql), &ids))
        .await?;
    let ids = rows
        .into_iter()
        .filter(|candidate| candidates.contains(candidate))
        .map(|(id, _)| id)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(0);
    }
    let members = lock_members(tx, topic, &ids).await?;
    if members.is_empty() {
        return Ok(0);
    }
    let sql = format!(
        "UPDATE vyuh_tasks SET status = 0, resume_input = ?,
        ready_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id IN ({})",
        slots(members.len())
    );
    let resumed = tx
        .execute(bind_ids(
            db::Statement::raw(&sql).bind(input.to_owned()),
            &members,
        ))
        .await?;
    remove_waits(tx, &members).await?;
    Ok(resumed)
}

#[cfg(feature = "mysql")]
async fn lock_members(
    tx: &mut db::DbTransaction<'_>,
    topic: &[u8],
    ids: &[Uuid],
) -> Result<Vec<Uuid>, TaskRuntimeError> {
    let sql = format!(
        "SELECT task_id FROM vyuh_task_waits FORCE INDEX (PRIMARY)
        WHERE topic = ? AND task_id IN ({}) ORDER BY task_id FOR UPDATE",
        slots(ids.len())
    );
    let rows: Vec<(Uuid,)> = tx
        .fetch_all(bind_ids(db::Statement::raw(&sql).bind(topic.to_vec()), ids))
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

#[cfg(feature = "mysql")]
async fn remove_waits(
    tx: &mut db::DbTransaction<'_>,
    ids: &[Uuid],
) -> Result<(), TaskRuntimeError> {
    let sql = format!(
        "DELETE FROM vyuh_task_waits WHERE task_id IN ({})",
        slots(ids.len())
    );
    tx.execute(bind_ids(db::Statement::raw(&sql), ids)).await?;
    Ok(())
}

/// Serializes with checkpointing and cancellation before testing for live topic membership.
#[cfg(feature = "mysql")]
pub(super) async fn resume(
    tx: &mut db::DbTransaction<'_>,
    id: crate::tasks::TaskId,
    input: String,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskRuntimeError> {
    let id = id.into_uuid();
    let task: Option<(Uuid,)> = tx.fetch_optional(db::Statement::raw(
        "SELECT id FROM vyuh_tasks WHERE id = ? AND status = 2 AND NOT cancelled FOR UPDATE"
    ).bind(id)).await?;
    if task.is_none() {
        return Ok(false);
    }
    let wait: Option<(Uuid,)> = tx
        .fetch_optional(
            db::Statement::raw("SELECT task_id FROM vyuh_task_waits WHERE task_id = ?").bind(id),
        )
        .await?;
    if wait.is_some() {
        return Ok(false);
    }
    let changed = tx.execute(db::Statement::raw(
        "UPDATE vyuh_tasks SET status = 0, resume_input = ?, ready_at = ?, updated_at = ? WHERE id = ?"
    ).bind(input).bind(now).bind(now).bind(id)).await?;
    Ok(changed > 0)
}

#[cfg(feature = "mysql")]
fn slots(n: usize) -> String {
    std::iter::repeat_n("?", n).collect::<Vec<_>>().join(",")
}

#[cfg(feature = "mysql")]
fn bind_ids(mut statement: db::Statement, ids: &[Uuid]) -> db::Statement {
    for id in ids {
        statement = statement.bind(*id);
    }
    statement
}

#[cfg(all(test, feature = "migrations"))]
#[path = "tests/publication.rs"]
mod tests;
