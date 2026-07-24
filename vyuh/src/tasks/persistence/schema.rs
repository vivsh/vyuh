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
        task_index(
            task_index_name("name_status"),
            &["name", "status"],
            false,
            None,
        ),
    ]);
    append_active_identity_index(table);
}

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

/// Keeps historical index names stable while task tables differ by backend.
fn task_index_name(suffix: &str) -> String {
    #[cfg(feature = "postgres")]
    return format!("idx_tasks_{suffix}");
    #[cfg(any(feature = "mysql", feature = "sqlite"))]
    return format!("idx_vyuh_tasks_{suffix}");
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
