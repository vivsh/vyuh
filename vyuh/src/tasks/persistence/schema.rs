//! Task-owned Mool schema contribution for migration planning.

use crate::db;

use super::model::TaskRow;

/// Builds the validated desired schema for Vyuh's persistent task table.
pub(crate) fn task_schema() -> Result<db::Schema, db::SchemaLoadError> {
    let mut schema = db::schema().model::<TaskRow>().build()?;
    for table in schema.tables.values_mut() {
        append_task_indexes(table);
        #[cfg(feature = "mysql")]
        append_active_identity(table);
    }
    schema.prepare_loaded(dialect())
}

/// Adds the query and identity indexes that preserve task claim performance.
fn append_task_indexes(table: &mut db::Table) {
    table.indexes.extend([
        task_index(
            task_index_name("pending_priority_claim"),
            &["status", "priority", "ready_at", "created_at"],
            false,
            pending_index_predicate(),
        ),
        task_index(
            task_index_name("running_lease"),
            &["status", "leased_until"],
            false,
            running_index_predicate(),
        ),
    ]);
    append_name_status_index(table);
    append_active_identity_index(table);
}

/// Adds the portable name/status lookup index where text columns can be indexed directly.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
fn append_name_status_index(table: &mut db::Table) {
    table.indexes.push(task_index(
        task_index_name("name_status"),
        &["name", "status"],
        false,
        None,
    ));
}

/// Omits the lookup index because Mool correctly rejects unprefixed MySQL TEXT indexes.
#[cfg(feature = "mysql")]
fn append_name_status_index(_table: &mut db::Table) {}

/// Creates one table index using Mool's portable schema metadata.
fn task_index(name: String, columns: &[&str], unique: bool, predicate: Option<&str>) -> db::Index {
    db::Index {
        name,
        columns: columns.iter().map(|column| (*column).to_string()).collect(),
        unique,
        predicate: predicate.map(str::to_string),
        ..Default::default()
    }
}

/// Builds the stable index name for the shared task table.
fn task_index_name(suffix: &str) -> String {
    format!("idx_vyuh_tasks_{suffix}")
}

/// Returns the partial predicate for ready pending task candidates.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
fn pending_index_predicate() -> Option<&'static str> {
    Some("status = 0")
}

/// MySQL expresses the claim predicate through ordinary composite indexes.
#[cfg(feature = "mysql")]
fn pending_index_predicate() -> Option<&'static str> {
    None
}

/// Returns the partial predicate for expired running task candidates.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
fn running_index_predicate() -> Option<&'static str> {
    Some("status = 1")
}

/// MySQL expresses the lease predicate through ordinary composite indexes.
#[cfg(feature = "mysql")]
fn running_index_predicate() -> Option<&'static str> {
    None
}

/// Adds the backend-specific active identity uniqueness representation.
fn append_active_identity_index(table: &mut db::Table) {
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    table.indexes.push(task_index(
        task_index_name("active_identity"),
        &["identity"],
        true,
        Some("identity IS NOT NULL AND status IN (0, 1, 2)"),
    ));

    #[cfg(feature = "mysql")]
    table.indexes.push(task_index(
        task_index_name("identity"),
        &["active_identity"],
        true,
        None,
    ));
}

/// Adds MySQL's generated active-identity column used by its unique index.
#[cfg(feature = "mysql")]
fn append_active_identity(table: &mut db::Table) {
    let generated = db::TableBuilder::new(table.name.clone())
        .column("active_identity", "varchar(255)", |column| {
            column
                .generated("CASE WHEN identity IS NOT NULL AND status IN (0, 1, 2) THEN identity ELSE NULL END")
                .generated_storage(db::schema::GeneratedStorage::Stored)
        })
        .build();
    table.columns.extend(generated.columns);
}

/// Returns the Mool-selected schema dialect for the active task backend.
fn dialect() -> db::Dialect {
    #[cfg(feature = "postgres")]
    {
        return db::Dialect::Postgres;
    }
    #[cfg(feature = "mysql")]
    {
        return db::Dialect::Mysql;
    }
    #[cfg(feature = "sqlite")]
    {
        return db::Dialect::Sqlite;
    }
    #[allow(unreachable_code)]
    db::Dialect::Sqlite
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "postgres")]
    use std::sync::Arc;

    /// Verifies every backend derives the unqualified shared task table name.
    #[test]
    fn task_schema_uses_shared_table_name() -> Result<(), String> {
        let schema = task_schema().map_err(|error| error.to_string())?;
        let table = schema
            .tables
            .get("vyuh_tasks")
            .ok_or_else(|| "task schema did not include vyuh_tasks".to_string())?;

        if table.schema.is_some() {
            return Err("task table must not use a database schema".to_string());
        }
        if schema.tables.contains_key("vyuh.tasks") {
            return Err("task schema must not retain the legacy vyuh.tasks table".to_string());
        }
        Ok(())
    }

    /// Verifies every task index follows the shared table's stable naming convention.
    #[test]
    fn task_schema_uses_shared_index_names() -> Result<(), String> {
        let schema = task_schema().map_err(|error| error.to_string())?;
        let table = schema
            .tables
            .get("vyuh_tasks")
            .ok_or_else(|| "task schema did not include vyuh_tasks".to_string())?;
        let has_invalid_name = table
            .indexes
            .iter()
            .any(|index| !index.name.starts_with("idx_vyuh_tasks_"));

        if has_invalid_name {
            return Err("task schema contains an index outside idx_vyuh_tasks_*".to_string());
        }
        Ok(())
    }

    /// Verifies PostgreSQL migration previews create the shared task table with its identity index.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn task_migration_preview_targets_shared_table() -> Result<(), String> {
        use crate::db::engine::{Config, MakeCommand, MigrationCommand, NativeRunnerFactory};

        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let config = Config::new(
            migration_database_url().to_string(),
            directory.path().to_path_buf(),
            directory.path().join("schema.yaml"),
            dialect(),
        );
        let mut runner =
            NativeRunnerFactory::from_store(config, Arc::new(db::MigrationRegistry::new())).build();
        let command = MigrationCommand::Make(MakeCommand::Generate {
            schema: task_schema().map_err(|error| error.to_string())?,
            name: Some("tasks".to_string()),
            dry_run: true,
            decisions: Vec::new(),
        });
        let result = runner
            .run_command(&command)
            .await
            .map_err(|error| error.to_string())?;
        let yaml = preview_yaml(result)?;

        if !yaml.contains("name: vyuh_tasks")
            || !yaml.contains("name: idx_vyuh_tasks_active_identity")
        {
            return Err(
                "generated task migration does not include vyuh_tasks and its identity index"
                    .to_string(),
            );
        }
        if yaml.contains("table_name: tasks") || yaml.contains("name: tasks") {
            return Err("generated task migration retains the legacy tasks table".to_string());
        }
        Ok(())
    }

    /// Serializes the dry-run migration required for task migration target assertions.
    #[cfg(feature = "postgres")]
    fn preview_yaml(result: crate::db::engine::CommandResult) -> Result<String, String> {
        match result {
            crate::db::engine::CommandResult::Make(crate::db::engine::MakeResult::Preview(
                migration,
            )) => migration
                .to_yaml_string()
                .map_err(|error| error.to_string()),
            _ => Err("task schema generation did not return a migration preview".to_string()),
        }
    }

    /// Provides a syntactically valid selected-backend URL for offline migration planning.
    #[cfg(feature = "postgres")]
    fn migration_database_url() -> &'static str {
        "postgres://localhost/vyuh_task_schema"
    }
}
