//! Private embedded desired-schema loading for migration-enabled sites.

use std::path::{Component, Path, PathBuf};

use thiserror::Error;

use crate::embed::{self, Dir};

const SCHEMA_DIR: &str = "schema";

/// Errors raised while loading embedded desired-schema assets.
#[derive(Debug, Error)]
pub enum SchemaAssetError {
    #[cfg(not(feature = "migrations"))]
    #[error("schema assets require the `migrations` feature: {path}")]
    MigrationsDisabled { path: PathBuf },
    #[cfg(feature = "migrations")]
    #[error("unsupported schema asset '{path}'; expected .yaml, .yml, or .sql")]
    Unsupported { path: PathBuf },
    #[cfg(feature = "migrations")]
    #[error("cannot read schema asset '{path}': {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[cfg(feature = "migrations")]
    #[error("schema asset '{path}' is not valid UTF-8: {source}")]
    Utf8 {
        path: PathBuf,
        #[source]
        source: std::string::FromUtf8Error,
    },
    #[cfg(feature = "migrations")]
    #[error("cannot determine the schema dialect: {0}")]
    Dialect(String),
    #[cfg(feature = "migrations")]
    #[error("cannot parse schema asset '{path}': {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: crate::db::SchemaLoadError,
    },
    #[cfg(feature = "migrations")]
    #[error("cannot merge schema asset '{path}': {source}")]
    Merge {
        path: PathBuf,
        #[source]
        source: crate::db::SchemaLoadError,
    },
    #[cfg(feature = "migrations")]
    #[error("cannot register schema assets: {0}")]
    Registry(#[source] crate::db::MigrationError),
}

/// Fails instead of silently ignoring schema assets in a non-migration build.
#[cfg(not(feature = "migrations"))]
pub(crate) fn reject(dirs: &[Dir]) -> Result<(), SchemaAssetError> {
    if let Some(path) = schema_files(dirs)
        .into_iter()
        .next()
        .map(|file| file.path().to_path_buf())
    {
        return Err(SchemaAssetError::MigrationsDisabled { path });
    }
    Ok(())
}

/// Registers the desired schema assembled from every overlaid schema asset.
#[cfg(feature = "migrations")]
pub(crate) fn register(
    registry: &mut crate::db::MigrationRegistry,
    dirs: &[Dir],
    database_url: &str,
) -> Result<(), SchemaAssetError> {
    if schema_files(dirs).is_empty() {
        return Ok(());
    }
    let dialect = dialect(database_url)?;
    let Some(schema) = load(dirs, dialect)? else {
        return Ok(());
    };
    let source = crate::db::root_schema_value(schema).map_err(SchemaAssetError::Registry)?;
    registry
        .register_schema(source)
        .map_err(SchemaAssetError::Registry)
}

#[cfg(feature = "migrations")]
fn dialect(database_url: &str) -> Result<crate::db::Dialect, SchemaAssetError> {
    crate::db::engine::Config::dialect_from_database_url(database_url)
        .map_err(|error| SchemaAssetError::Dialect(error.to_string()))
}

#[cfg(feature = "migrations")]
fn load(
    dirs: &[Dir],
    dialect: crate::db::Dialect,
) -> Result<Option<crate::db::Schema>, SchemaAssetError> {
    let mut schema = crate::db::Schema::default();
    let mut found = false;
    for file in schema_files(dirs) {
        found = true;
        let path = file.path().to_path_buf();
        let fragment = parse(&file, dialect)?;
        schema = schema
            .merge(fragment)
            .map_err(|source| SchemaAssetError::Merge { path, source })?;
    }
    Ok(found.then_some(schema))
}

#[cfg(feature = "migrations")]
fn parse(
    file: &embed::File,
    dialect: crate::db::Dialect,
) -> Result<crate::db::Schema, SchemaAssetError> {
    let path = file.path().to_path_buf();
    let bytes = file
        .read_bytes_sync()
        .map_err(|source| SchemaAssetError::Read {
            path: path.clone(),
            source,
        })?;
    let source = String::from_utf8(bytes).map_err(|source| SchemaAssetError::Utf8 {
        path: path.clone(),
        source,
    })?;
    parse_source(&path, &source, dialect)
}

#[cfg(feature = "migrations")]
fn parse_source(
    path: &Path,
    source: &str,
    dialect: crate::db::Dialect,
) -> Result<crate::db::Schema, SchemaAssetError> {
    let parsed = match extension(path) {
        Some("yaml" | "yml") => crate::db::Schema::from_yaml_str(source, dialect),
        Some("sql") => crate::db::Schema::from_sql_str(source, dialect),
        _ => return Err(SchemaAssetError::Unsupported { path: path.into() }),
    };
    parsed.map_err(|source| SchemaAssetError::Parse {
        path: path.into(),
        source,
    })
}

fn schema_files(dirs: &[Dir]) -> Vec<embed::File> {
    embed::DirSet::new(dirs.to_vec())
        .walk_override()
        .filter(|file| schema_path(file.path()))
        .collect()
}

fn schema_path(path: &Path) -> bool {
    path.strip_prefix(SCHEMA_DIR)
        .ok()
        .is_some_and(|rest| !rest.as_os_str().is_empty() && !hidden(rest))
}

fn hidden(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(name) => name.to_string_lossy().starts_with('.'),
        _ => false,
    })
}

#[cfg(feature = "migrations")]
fn extension(path: &Path) -> Option<&str> {
    path.extension().and_then(|extension| extension.to_str())
}

#[cfg(all(test, feature = "migrations"))]
mod tests {
    use std::io;

    use super::*;

    fn asset_dir(path: &Path) -> Result<Dir, io::Error> {
        let root = path
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 asset path"))?;
        Ok(Dir::new(rust_silos::Silo::new(root)))
    }

    fn write(path: &Path, body: &str) -> Result<(), io::Error> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, body)
    }

    fn test_error(error: impl std::fmt::Display) -> io::Error {
        io::Error::other(error.to_string())
    }

    /// Verifies that YAML schema assets contribute desired tables to the registry.
    #[test]
    fn yaml_assets_register_schema() -> Result<(), io::Error> {
        let assets = tempfile::tempdir()?;
        write(
            &assets.path().join("schema/users.yaml"),
            "tables:\n  users:\n    columns:\n      - name: id\n        type: integer\n        primary_key: true\n",
        )?;
        let mut registry = crate::db::MigrationRegistry::new();
        register(
            &mut registry,
            &[asset_dir(assets.path())?],
            "postgres://localhost/example",
        )
        .map_err(test_error)?;

        let schema = registry.schema_for(None).map_err(test_error)?;
        assert!(schema.tables.contains_key("users"));
        Ok(())
    }

    /// Verifies that SQL schema assets are parsed through the selected dialect.
    #[test]
    fn sql_assets_register_schema() -> Result<(), io::Error> {
        let assets = tempfile::tempdir()?;
        write(
            &assets.path().join("schema/audit.sql"),
            "CREATE TABLE audit_log (id integer PRIMARY KEY);",
        )?;
        let mut registry = crate::db::MigrationRegistry::new();
        register(
            &mut registry,
            &[asset_dir(assets.path())?],
            "sqlite::memory:",
        )
        .map_err(test_error)?;

        let schema = registry.schema_for(None).map_err(test_error)?;
        assert!(schema.tables.contains_key("audit_log"));
        Ok(())
    }

    /// Verifies PostgreSQL functions with nested CTE and LATERAL SQL load as schema assets.
    #[test]
    fn postgres_function_asset_loads_without_truncation() -> Result<(), io::Error> {
        let source = r#"
CREATE OR REPLACE FUNCTION dynrs_daily_report(p_working_date date)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
WITH eligible_sessions AS (
    SELECT s.id, s.user_id
    FROM sessions AS s
    WHERE s.working_date = p_working_date
), report_rows AS (
    SELECT session.id, payload.report
    FROM eligible_sessions AS session
    CROSS JOIN LATERAL (
        SELECT jsonb_build_object('user_id', session.user_id) AS report
    ) AS payload
)
SELECT coalesce(jsonb_agg(report), '[]'::jsonb)
FROM report_rows;
$$;
"#;

        let schema = parse_source(
            Path::new("schema/reports.sql"),
            source,
            crate::db::Dialect::Postgres,
        )
        .map_err(test_error)?;

        assert_eq!(schema.functions.len(), 1);
        Ok(())
    }

    /// Verifies malformed SQL retains the asset path and parser location for operators.
    #[test]
    fn sql_asset_parse_error_includes_path_and_location() -> Result<(), io::Error> {
        let error = parse_source(
            Path::new("schema/reports.sql"),
            "CREATE TABLE reports (id integer,);",
            crate::db::Dialect::Postgres,
        )
        .expect_err("invalid SQL must fail");
        let message = error.to_string();

        assert!(message.contains("schema/reports.sql"));
        assert!(message.contains("postgres SQL parse error"));
        assert!(message.contains("line"));
        Ok(())
    }

    /// Verifies that a later asset directory overrides a matching schema file.
    #[test]
    fn later_assets_override_schema_files() -> Result<(), io::Error> {
        let first = tempfile::tempdir()?;
        let second = tempfile::tempdir()?;
        write(
            &first.path().join("schema/users.yaml"),
            "tables:\n  users:\n    columns:\n      - name: id\n        type: integer\n        primary_key: true\n",
        )?;
        write(
            &second.path().join("schema/users.yaml"),
            "tables:\n  accounts:\n    columns:\n      - name: id\n        type: integer\n        primary_key: true\n",
        )?;
        let schema = load(
            &[asset_dir(first.path())?, asset_dir(second.path())?],
            crate::db::Dialect::Postgres,
        )
        .map_err(test_error)?
        .ok_or_else(|| io::Error::other("schema assets were not found"))?;

        assert!(!schema.tables.contains_key("users"));
        assert!(schema.tables.contains_key("accounts"));
        Ok(())
    }

    /// Verifies that unsupported files under schema assets fail with their path.
    #[test]
    fn unsupported_schema_asset_is_reported() -> Result<(), io::Error> {
        let assets = tempfile::tempdir()?;
        write(&assets.path().join("schema/users.txt"), "users")?;

        let error = load(&[asset_dir(assets.path())?], crate::db::Dialect::Postgres)
            .err()
            .ok_or_else(|| io::Error::other("unsupported file unexpectedly loaded"))?;

        assert!(error.to_string().contains("schema/users.txt"));
        Ok(())
    }

    /// Verifies that YAML and SQL schema assets compose into one desired schema.
    #[test]
    fn mixed_schema_assets_merge() -> Result<(), io::Error> {
        let assets = tempfile::tempdir()?;
        write(
            &assets.path().join("schema/users.yaml"),
            "tables:\n  users:\n    columns:\n      - name: id\n        type: integer\n        primary_key: true\n",
        )?;
        write(
            &assets.path().join("schema/audit.sql"),
            "CREATE TABLE audit_log (id integer PRIMARY KEY);",
        )?;

        let schema = load(&[asset_dir(assets.path())?], crate::db::Dialect::Postgres)
            .map_err(test_error)?
            .ok_or_else(|| io::Error::other("schema assets were not found"))?;

        assert!(schema.tables.contains_key("users"));
        assert!(schema.tables.contains_key("audit_log"));
        Ok(())
    }
}

#[cfg(all(test, not(feature = "migrations")))]
mod no_migration_tests {
    use std::{io, path::Path};

    use super::*;

    fn asset_dir(path: &Path) -> Result<Dir, io::Error> {
        let root = path
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 asset path"))?;
        Ok(Dir::new(rust_silos::Silo::new(root)))
    }

    /// Verifies that schema assets cannot be silently ignored without migrations.
    #[test]
    fn schema_assets_require_migrations() -> Result<(), io::Error> {
        let assets = tempfile::tempdir()?;
        std::fs::create_dir_all(assets.path().join("schema"))?;
        std::fs::write(assets.path().join("schema/users.yaml"), "tables: {}")?;

        let error = reject(&[asset_dir(assets.path())?])
            .err()
            .ok_or_else(|| io::Error::other("schema assets were ignored"))?;

        assert!(error.to_string().contains("migrations"));
        Ok(())
    }
}
