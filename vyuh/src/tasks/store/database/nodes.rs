//! Node registration, heartbeats, retirement, and explicit claim give-back.

#[cfg(all(test, feature = "migrations"))]
#[path = "tests/nodes.rs"]
mod node_tests;

use super::store::DbTaskStore;
use crate::{db, tasks::TaskRuntimeError};

impl DbTaskStore {
    /// Inserts one runner incarnation without resetting an existing registration.
    pub(super) async fn register_node_impl(
        &self,
        node: &uuid::Uuid,
    ) -> Result<(), TaskRuntimeError> {
        use db::DbSession as _;
        #[cfg(feature = "postgres")]
        let sql = "INSERT INTO vyuh_task_nodes (id, started_at, heartbeat_at) VALUES ($1, statement_timestamp(), statement_timestamp()) ON CONFLICT (id) DO NOTHING";
        #[cfg(feature = "sqlite")]
        let sql = "INSERT INTO vyuh_task_nodes (id, started_at, heartbeat_at) VALUES (?, strftime('%Y-%m-%d %H:%M:%f', 'now'), strftime('%Y-%m-%d %H:%M:%f', 'now')) ON CONFLICT (id) DO NOTHING";
        #[cfg(feature = "mysql")]
        let sql = "INSERT INTO vyuh_task_nodes (id, started_at, heartbeat_at) VALUES (?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE id = id";
        self.pool
            .clone()
            .execute(db::Statement::raw(sql).bind(*node))
            .await?;
        Ok(())
    }

    /// Updates only the existing node; SQLx MySQL FOUND_ROWS reports matched rows.
    pub(super) async fn heartbeat_impl(
        &self,
        node: &uuid::Uuid,
        stale_after: std::time::Duration,
    ) -> Result<bool, TaskRuntimeError> {
        use db::DbSession as _;
        #[cfg(feature = "postgres")]
        let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = statement_timestamp() WHERE id = $1";
        #[cfg(feature = "sqlite")]
        let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?";
        #[cfg(feature = "mysql")]
        let sql = "UPDATE vyuh_task_nodes SET heartbeat_at = CURRENT_TIMESTAMP(6) WHERE id = ?";
        let present = self
            .pool
            .clone()
            .execute(db::Statement::raw(sql).bind(*node))
            .await?
            == 1;
        if present {
            self.retire_nodes(node, stale_after).await?;
        }
        Ok(present)
    }

    /// A separate autocommit delete rechecks peer age without retaining our heartbeat lock.
    async fn retire_nodes(
        &self,
        node: &uuid::Uuid,
        stale_after: std::time::Duration,
    ) -> Result<(), TaskRuntimeError> {
        use db::DbSession as _;
        let micros = i64::try_from(stale_after.as_micros()).map_err(|_| {
            TaskRuntimeError::InvalidOptions(
                "node retirement duration exceeds database range".into(),
            )
        })?;
        #[cfg(feature = "postgres")]
        let sql = "WITH retired AS (DELETE FROM vyuh_task_nodes WHERE id <> $1 AND heartbeat_at < statement_timestamp() - (CAST($2 AS BIGINT) * INTERVAL '1 microsecond') RETURNING id) SELECT pg_notify('vyuh_tasks', 'null') WHERE EXISTS (SELECT 1 FROM retired)";
        #[cfg(feature = "mysql")]
        let sql = "DELETE FROM vyuh_task_nodes WHERE id <> ? AND heartbeat_at < TIMESTAMPADD(MICROSECOND, -?, CURRENT_TIMESTAMP(6))";
        #[cfg(feature = "sqlite")]
        let sql = "DELETE FROM vyuh_task_nodes WHERE id <> ? AND heartbeat_at < strftime('%Y-%m-%d %H:%M:%f', 'now', printf('-%f seconds', ? / 1000000.0))";
        self.pool
            .clone()
            .execute(db::Statement::raw(sql).bind(*node).bind(micros))
            .await?;
        Ok(())
    }

    /// Uses one conditional update at a quiescent lifecycle boundary, with database time.
    /// This neither discovers abandoned nodes nor changes lane leases or workflow state.
    pub(super) async fn return_claims_impl(
        &self,
        node: Option<uuid::Uuid>,
    ) -> Result<u64, TaskRuntimeError> {
        use db::DbSession as _;
        #[cfg(feature = "postgres")]
        let (clock, parameter) = ("statement_timestamp()", "$1");
        #[cfg(feature = "sqlite")]
        let (clock, parameter) = ("strftime('%Y-%m-%d %H:%M:%f', 'now')", "?");
        #[cfg(feature = "mysql")]
        let (clock, parameter) = ("CURRENT_TIMESTAMP(6)", "?");
        let owner = if node.is_some() {
            format!(" AND node_id = {parameter}")
        } else {
            String::new()
        };
        let sql = format!(
            "UPDATE vyuh_tasks SET ready_at = CASE WHEN status = 1 THEN {clock} ELSE ready_at END, \
             status = CASE WHEN status = 1 THEN 0 ELSE status END, node_id = NULL, updated_at = {clock} \
             WHERE (status = 1 OR (status IN (0, 2) AND node_id IS NOT NULL)){owner}"
        );
        #[cfg(feature = "postgres")]
        let sql = super::notify::notifying(&sql);
        let statement = db::Statement::raw(&sql);
        let statement = match node {
            Some(node) => statement.bind(node),
            None => statement,
        };
        #[cfg(feature = "postgres")]
        {
            let (count, _): (i64, i64) = self.pool.clone().fetch_one(statement).await?;
            Ok(count as u64)
        }
        #[cfg(not(feature = "postgres"))]
        {
            Ok(self.pool.clone().execute(statement).await?)
        }
    }
}
