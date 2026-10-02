//! Source projections that avoid database statement or schema source dumps.

use std::error::Error;

/// Reads typed sources without rendering database query payloads.
pub(super) fn cause<'a>(
    error: &'a (dyn Error + 'static),
) -> (String, Option<&'a (dyn Error + 'static)>) {
    #[cfg(feature = "migrations")]
    if let Some(error) = error.downcast_ref::<crate::db::SchemaLoadError>() {
        return schema_cause(error);
    }
    #[cfg(feature = "migrations")]
    if let Some(error) = error.downcast_ref::<crate::db::MigrationError>() {
        return migration_cause(error);
    }
    if let Some(error) = error.downcast_ref::<crate::db::DbError>() {
        return database_cause(error);
    }
    if let Some(crate::db::sqlx::Error::Database(error)) =
        error.downcast_ref::<crate::db::sqlx::Error>()
    {
        return (
            format!(
                "Database rejected initialization (code: {}). Check database authentication, permissions, and schema compatibility.",
                error.code().as_deref().unwrap_or("unavailable")
            ),
            None,
        );
    }
    if let Some(crate::services::ServiceError::CallError(source)) =
        error.downcast_ref::<crate::services::ServiceError>()
    {
        return ("".into(), Some(source));
    }
    if let Some(crate::callables::CallError::Other(source)) =
        error.downcast_ref::<crate::callables::CallError>()
    {
        return ("".into(), Some(source.as_ref()));
    }
    let source = error.source();
    if source.is_some_and(|source| {
        source.is::<crate::db::DbError>() || source.is::<crate::db::sqlx::Error>()
    }) {
        return ("".into(), source);
    }
    (error.to_string(), source)
}

fn database_cause(error: &crate::db::DbError) -> (String, Option<&(dyn Error + 'static)>) {
    let source = if matches!(error, crate::db::DbError::QuerySet(_)) {
        None
    } else {
        error.source()
    };
    (format!("Database category: {}", error.code()), source)
}

/// Retains schema identities and parser positions without rendering source text.
#[cfg(feature = "migrations")]
fn schema_cause(error: &crate::db::SchemaLoadError) -> (String, Option<&(dyn Error + 'static)>) {
    use crate::db::SchemaLoadError as E;
    match error {
        E::Path { path, source } => (format!("Schema: {path}"), Some(source.as_ref())),
        E::Io(path, source) => (format!("Cannot read schema: {path}"), Some(source)),
        E::Yaml(error) => (match error.location() {
            Some(location) => format!("Invalid YAML at line {}, column {}", location.line(), location.column()),
            None => "Invalid YAML (parser position unavailable).".into(),
        }, None),
        E::Json(error) => (format!("Invalid JSON at line {}, column {}", error.line(), error.column()), None),
        E::Sql(_) => ("SQL schema parsing failed; statement text omitted. Validate the asset with Gaman.".into(), None),
        E::Validation(_) => ("Schema validation failed; inspect the typed validation source or validate the asset with Gaman.".into(), None),
        E::Merge { .. } | E::DuplicateTable(_) => (error.to_string(), None),
    }
}

/// Prevents engine errors from embedding SQL in an outer migration wrapper.
#[cfg(feature = "migrations")]
fn migration_cause(error: &crate::db::MigrationError) -> (String, Option<&(dyn Error + 'static)>) {
    use crate::db::MigrationError as E;
    match error {
        E::SchemaSource { namespace, source } =>
            (format!("Schema contributor: {namespace}"), Some(source)),
        E::Schema(source) => ("Schema composition failed.".into(), Some(source)),
        E::Engine(_) => ("Migration engine initialization failed; inspect the typed engine error with Gaman's diagnostic tooling.".into(), None),
        _ => (error.to_string(), None),
    }
}
