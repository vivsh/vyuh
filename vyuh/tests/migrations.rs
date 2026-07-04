#![cfg(feature = "migrations")]

use vyuh::prelude::*;

static ROOT: db::EmbeddedMigrations = db::EmbeddedMigrations {
    files: &[],
    dir: "migrations",
    children: &[],
};

static AUTH: db::EmbeddedMigrations = db::EmbeddedMigrations {
    files: &[],
    dir: "migrations",
    children: &[],
};

#[derive(Debug, Clone, db::Model)]
#[table(name = "accounts", schema = "auth")]
struct Account {
    #[column(primary_key, type = "bigserial")]
    id: i64,
    #[column(type = "text")]
    email: String,
    #[column(type = "text")]
    bio: Option<String>,
}

fn root_schema() -> db::Schema {
    db::Schema::builder(db::Dialect::Postgres)
        .table::<Account>()
        .build()
}

#[test]
fn root_migration_registers() {
    let mut registry = db::MigrationRegistry::new();

    assert!(registry.register(db::root_migration(&ROOT)).is_ok());

    assert!(registry.root().is_some());
}

#[test]
fn crate_migration_registers() {
    let mut registry = db::MigrationRegistry::new();

    assert!(
        registry
            .register(db::crate_migration("auth", &AUTH))
            .is_ok()
    );

    assert!(registry.get("auth").is_some());
}

#[test]
fn duplicate_root_errors() {
    let mut registry = db::MigrationRegistry::new();
    assert!(registry.register(db::root_migration(&ROOT)).is_ok());

    assert!(matches!(
        registry.register(db::root_migration(&ROOT)),
        Err(db::MigrationError::DuplicateRoot)
    ));
}

#[test]
fn duplicate_namespace_errors() {
    let mut registry = db::MigrationRegistry::new();
    assert!(
        registry
            .register(db::crate_migration("auth", &AUTH))
            .is_ok()
    );

    assert!(matches!(
        registry.register(db::crate_migration("auth", &AUTH)),
        Err(db::MigrationError::DuplicateNamespace(_))
    ));
}

#[test]
fn invalid_namespace_errors() {
    let mut registry = db::MigrationRegistry::new();

    assert!(matches!(
        registry.register(db::crate_migration("auth/users", &AUTH)),
        Err(db::MigrationError::InvalidNamespace { .. })
    ));
}

#[test]
fn bundle_merge_detects_duplicate_migration_source() {
    let left = bundles::bundle(vec![bundles::migrations(db::root_migration(&ROOT))]);
    let right = bundles::bundle(vec![bundles::migrations(db::root_migration(&ROOT))]);

    let merged = left.merge(right);

    assert!(merged.validate().is_err());
}

#[test]
fn schema_provider_collects_root_schema() {
    let mut registry = db::MigrationRegistry::new();
    assert!(
        registry
            .register_schema(db::root_schema(root_schema))
            .is_ok()
    );

    assert!(matches!(
        registry.schema_for(None),
        Ok(schema) if schema.tables.contains_key("auth.accounts")
    ));
}

#[test]
fn model_derive_generates_table_metadata() {
    let table = <Account as db::IntoTable>::into_table(&db::Dialect::Postgres);

    assert_eq!(table.name, "accounts");
    assert_eq!(table.schema.as_deref(), Some("auth"));
    assert_eq!(table.columns.len(), 3);
    assert!(
        table
            .columns
            .iter()
            .any(|c| c.name == "id" && c.primary_key)
    );
    assert!(table.columns.iter().any(|c| c.name == "bio" && c.nullable));
}
