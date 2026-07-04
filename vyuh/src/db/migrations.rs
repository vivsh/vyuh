#[cfg(feature = "migrations")]
use std::{collections::BTreeMap, path::PathBuf};

#[cfg(feature = "migrations")]
use thiserror::Error;

#[cfg(feature = "migrations")]
pub use gaman::EmbeddedMigrations;
#[allow(unused_imports)]
pub use gaman::core::Dialect;
#[cfg(feature = "migrations")]
pub use gaman::embedded_migrations;
#[allow(unused_imports)]
pub use gaman::schema::{
    Column, ColumnBuilder, ColumnDesc, ColumnRef, ColumnType, Constraint, ForeignKey, Index,
    IntoTable, Table,
};
#[allow(unused_imports)]
pub use gaman::schema::{Schema, SchemaBuilder, TableBuilder};

/// A crate-level migration history registered through a bundle.
#[cfg(feature = "migrations")]
#[derive(Clone, Copy)]
pub struct MigrationSource {
    namespace: Option<&'static str>,
    migrations: &'static EmbeddedMigrations,
}

/// A schema contribution associated with a migration namespace.
#[cfg(feature = "migrations")]
#[derive(Clone, Copy)]
pub struct SchemaSource {
    namespace: Option<&'static str>,
    build: fn() -> Schema,
}

#[cfg(feature = "migrations")]
impl SchemaSource {
    /// Return the migration namespace this schema contribution belongs to.
    pub fn namespace(&self) -> Option<&'static str> {
        self.namespace
    }

    /// Build the schema contribution.
    pub fn build(&self) -> Schema {
        (self.build)()
    }
}

#[cfg(feature = "migrations")]
impl MigrationSource {
    /// Return the virtual Gaman namespace for this source.
    pub fn namespace(&self) -> Option<&'static str> {
        self.namespace
    }

    /// Return the embedded migration set carried by this source.
    pub fn embedded(&self) -> &'static EmbeddedMigrations {
        self.migrations
    }

    /// Return the source directory baked into `embedded_migrations!`.
    pub fn dir(&self) -> PathBuf {
        PathBuf::from(self.migrations.dir)
    }
}

/// Register the root application migration history.
#[cfg(feature = "migrations")]
pub fn root_migration(migrations: &'static EmbeddedMigrations) -> MigrationSource {
    MigrationSource {
        namespace: None,
        migrations,
    }
}

/// Register a crate migration history under a virtual namespace.
#[cfg(feature = "migrations")]
pub fn crate_migration(
    namespace: &'static str,
    migrations: &'static EmbeddedMigrations,
) -> MigrationSource {
    MigrationSource {
        namespace: Some(namespace),
        migrations,
    }
}

/// Register a root schema contribution.
#[cfg(feature = "migrations")]
pub fn root_schema(build: fn() -> Schema) -> SchemaSource {
    SchemaSource {
        namespace: None,
        build,
    }
}

/// Register a crate schema contribution under a virtual namespace.
#[cfg(feature = "migrations")]
pub fn crate_schema(namespace: &'static str, build: fn() -> Schema) -> SchemaSource {
    SchemaSource {
        namespace: Some(namespace),
        build,
    }
}

/// Errors raised while registering or executing migrations.
#[cfg(feature = "migrations")]
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("duplicate root migration source")]
    DuplicateRoot,
    #[error("duplicate migration namespace '{0}'")]
    DuplicateNamespace(String),
    #[error("invalid migration namespace '{namespace}': {reason}")]
    InvalidNamespace { namespace: String, reason: String },
    #[error("no root migration source is registered")]
    MissingRoot,
    #[error("no migration source registered for namespace '{0}'")]
    MissingNamespace(String),
    #[error("migration engine error: {0}")]
    Engine(#[from] gaman::EngineError),
    #[error("schema merge error: {0}")]
    Schema(String),
}

/// Registry for crate-owned migration sources discovered through bundles.
#[cfg(feature = "migrations")]
#[derive(Clone, Default)]
pub struct MigrationRegistry {
    root: Option<MigrationSource>,
    crates: BTreeMap<&'static str, MigrationSource>,
    root_schema: Vec<SchemaSource>,
    crate_schema: BTreeMap<&'static str, Vec<SchemaSource>>,
}

#[cfg(feature = "migrations")]
impl MigrationRegistry {
    /// Create an empty migration registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a root or crate migration source.
    pub fn register(&mut self, source: MigrationSource) -> Result<(), MigrationError> {
        validate_namespace(source.namespace)?;
        match source.namespace {
            Some(ns) => self.register_crate(ns, source),
            None => self.register_root(source),
        }
    }

    /// Register a schema contribution.
    pub fn register_schema(&mut self, source: SchemaSource) -> Result<(), MigrationError> {
        validate_namespace(source.namespace)?;
        match source.namespace {
            Some(ns) => self.crate_schema.entry(ns).or_default().push(source),
            None => self.root_schema.push(source),
        }
        Ok(())
    }

    /// Merge another registry into this one, rejecting collisions.
    pub fn merge(&mut self, other: MigrationRegistry) -> Result<(), MigrationError> {
        if let Some(source) = other.root {
            self.register(source)?;
        }
        for source in other.crates.into_values() {
            self.register(source)?;
        }
        for source in other.root_schema {
            self.register_schema(source)?;
        }
        for sources in other.crate_schema.into_values() {
            for source in sources {
                self.register_schema(source)?;
            }
        }
        Ok(())
    }

    /// Return the root migration source, if registered.
    pub fn root(&self) -> Option<MigrationSource> {
        self.root
    }

    /// Return a crate migration source by namespace.
    pub fn get(&self, namespace: &str) -> Option<MigrationSource> {
        self.crates.get(namespace).copied()
    }

    /// Iterate over crate migration sources in deterministic namespace order.
    pub fn crates(&self) -> impl Iterator<Item = (&'static str, MigrationSource)> + '_ {
        self.crates.iter().map(|(ns, source)| (*ns, *source))
    }

    /// Build the merged schema for one namespace.
    pub fn schema_for(&self, namespace: Option<&str>) -> Result<Schema, MigrationError> {
        let sources = self.schema_sources(namespace);
        merge_schema(sources)
    }

    fn register_root(&mut self, source: MigrationSource) -> Result<(), MigrationError> {
        if self.root.is_some() {
            return Err(MigrationError::DuplicateRoot);
        }
        self.root = Some(source);
        Ok(())
    }

    fn register_crate(
        &mut self,
        ns: &'static str,
        source: MigrationSource,
    ) -> Result<(), MigrationError> {
        if self.crates.contains_key(ns) {
            return Err(MigrationError::DuplicateNamespace(ns.to_string()));
        }
        self.crates.insert(ns, source);
        Ok(())
    }
}

#[cfg(feature = "migrations")]
fn merge_schema(sources: Vec<SchemaSource>) -> Result<Schema, MigrationError> {
    let mut schema = Schema::default();
    for source in sources {
        schema = schema
            .merge(source.build())
            .map_err(|e| MigrationError::Schema(e.to_string()))?;
    }
    Ok(schema)
}

#[cfg(feature = "migrations")]
impl MigrationRegistry {
    fn schema_sources(&self, namespace: Option<&str>) -> Vec<SchemaSource> {
        match namespace {
            Some(ns) => self
                .crate_schema
                .get(ns)
                .map(|items| items.to_vec())
                .unwrap_or_default(),
            None => self.root_schema.clone(),
        }
    }
}

#[cfg(feature = "migrations")]
fn validate_namespace(namespace: Option<&str>) -> Result<(), MigrationError> {
    let Some(namespace) = namespace else {
        return Ok(());
    };
    if namespace.is_empty() {
        return Err(invalid_ns(namespace, "namespace cannot be empty"));
    }
    if namespace.contains('/') {
        return Err(invalid_ns(namespace, "namespace cannot contain '/'"));
    }
    if !namespace
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(invalid_ns(
            namespace,
            "use lowercase letters, digits, and underscores only",
        ));
    }
    Ok(())
}

#[cfg(feature = "migrations")]
fn invalid_ns(namespace: &str, reason: &str) -> MigrationError {
    MigrationError::InvalidNamespace {
        namespace: namespace.to_string(),
        reason: reason.to_string(),
    }
}
