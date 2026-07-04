use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::{CommandConf, CommandError, CommandRegistry};
use crate::callables::specs::{ArgPart, IntoArgPart};
use crate::callables::{self, Data, FromSite};
use crate::{Error, Site};

// ── SiteRef extractor ─────────────────────────────────────────────────────────

/// Site extractor for use in two-arg command handlers.
pub(crate) struct SiteRef(pub Site);

impl FromSite for SiteRef {
    fn from_site(site: &Site) -> Result<Self, callables::CallError> {
        Ok(SiteRef(site.clone()))
    }
}

impl IntoArgPart for SiteRef {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

// ── registry ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ServeArgs {}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct HealthArgs {}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ShowConfigArgs {
    #[serde(default)]
    pub raw: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CollectStaticArgs {
    #[serde(default = "default_collect_assets_output")]
    pub output: PathBuf,
    #[serde(default)]
    pub clean: bool,
    pub glob: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct StaticExportArgs {
    #[serde(default = "default_collect_pages_output")]
    pub output: PathBuf,
    #[serde(default)]
    pub clean: bool,
    pub glob: Option<String>,
}

#[cfg(feature = "migrations")]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct MakeMigrationArgs {
    pub name: Option<String>,
    #[serde(default)]
    pub empty: bool,
    #[serde(default)]
    pub merge: bool,
    #[serde(default)]
    pub check: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub non_interactive: bool,
}

#[cfg(feature = "migrations")]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct MigrateArgs {
    pub target: Option<String>,
    #[serde(default)]
    pub fake: bool,
    #[serde(default)]
    pub plan: bool,
    #[serde(default)]
    pub check: bool,
}

#[cfg(feature = "migrations")]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ShowMigrationsArgs {}

#[cfg(feature = "migrations")]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SqlMigrateArgs {
    pub id: Option<String>,
    #[serde(default)]
    pub backwards: bool,
}

#[cfg(feature = "migrations")]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct VerifyDbArgs {
    #[serde(default = "default_verify_schema")]
    pub schema: String,
}

#[cfg(feature = "migrations")]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct InspectDbArgs {
    #[serde(default = "default_verify_schema")]
    pub schema: String,
}

fn default_collect_assets_output() -> PathBuf {
    PathBuf::from("dist/assets")
}

fn default_collect_pages_output() -> PathBuf {
    PathBuf::from("dist")
}

#[cfg(feature = "migrations")]
fn default_verify_schema() -> String {
    "public".to_string()
}

pub fn core_registry() -> Result<CommandRegistry, CommandError> {
    let mut registry = CommandRegistry::new();

    let serve =
        super::command::<ServeArgs, _, crate::callables::specs::Tuple2<SiteRef, Data<ServeArgs>>>(
            serve_command,
            CommandConf::new("serve").description("Start the HTTP server."),
        )?;
    registry.register(serve)?;

    let health = super::command::<
        HealthArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<HealthArgs>>,
    >(
        health_command,
        CommandConf::new("health").description("Check basic site health."),
    )?;
    registry.register(health)?;

    let show_config = super::command::<
        ShowConfigArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<ShowConfigArgs>>,
    >(
        show_config_command,
        CommandConf::new("config").description("Print the effective site configuration."),
    )?;
    registry.register(show_config)?;

    let collect_assets = super::command::<
        CollectStaticArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<CollectStaticArgs>>,
    >(
        collect_assets_command,
        CommandConf::new("collect_assets").description("Copy bundled public assets."),
    )?;
    registry.register(collect_assets)?;

    let collect_pages = super::command::<
        StaticExportArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<StaticExportArgs>>,
    >(
        static_export_command,
        CommandConf::new("collect_pages").description("Render selected GET routes to files."),
    )?;
    registry.register(collect_pages)?;

    #[cfg(feature = "migrations")]
    register_migration_commands(&mut registry)?;

    Ok(registry)
}

#[cfg(feature = "migrations")]
fn register_migration_commands(registry: &mut CommandRegistry) -> Result<(), CommandError> {
    let make = super::command::<
        MakeMigrationArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<MakeMigrationArgs>>,
    >(
        make_migration_command,
        CommandConf::new("make_migration").description("Write a migration from schema changes."),
    )?;
    registry.register(make)?;

    let migrate = super::command::<
        MigrateArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<MigrateArgs>>,
    >(
        migrate_command,
        CommandConf::new("migrate").description("Apply or inspect pending migrations."),
    )?;
    registry.register(migrate)?;

    let show = super::command::<
        ShowMigrationsArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<ShowMigrationsArgs>>,
    >(
        show_migrations_command,
        CommandConf::new("show_migrations").description("List migrations and applied status."),
    )?;
    registry.register(show)?;

    let sql = super::command::<
        SqlMigrateArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<SqlMigrateArgs>>,
    >(
        sql_migrate_command,
        CommandConf::new("sql_migrate").description("Print SQL for a migration."),
    )?;
    registry.register(sql)?;

    let verify = super::command::<
        VerifyDbArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<VerifyDbArgs>>,
    >(
        verify_db_command,
        CommandConf::new("verify_db").description("Compare live database with migration state."),
    )?;
    registry.register(verify)?;

    let inspect = super::command::<
        InspectDbArgs,
        _,
        crate::callables::specs::Tuple2<SiteRef, Data<InspectDbArgs>>,
    >(
        inspect_db_command,
        CommandConf::new("inspect_db").description("Print live database schema as JSON."),
    )?;
    registry.register(inspect)?;

    Ok(())
}

async fn show_config_command(
    SiteRef(site): SiteRef,
    Data(args): Data<ShowConfigArgs>,
) -> Result<(), Error> {
    if args.raw {
        if !cfg!(debug_assertions) {
            return Err(Error::invalid(
                "raw config output is disabled in release builds",
            ));
        }
        let json = serde_json::to_string_pretty(site.conf())?;
        println!("{json}");
        return Ok(());
    }
    let json = serde_json::to_string_pretty(&crate::console::redacted_config(&site))?;
    println!("{json}");
    Ok(())
}

// ── handlers ──────────────────────────────────────────────────────────────────

async fn serve_command(SiteRef(site): SiteRef, _args: Data<ServeArgs>) -> Result<(), Error> {
    site.start().await.map_err(Error::other)
}

async fn health_command(SiteRef(site): SiteRef, _args: Data<HealthArgs>) -> Result<(), Error> {
    let uptime = site.uptime().as_secs();
    let db_ok = sqlx::query("SELECT 1")
        .execute(site.db().as_sqlx())
        .await
        .is_ok();
    println!("health: ok");
    println!("database: {}", if db_ok { "ok" } else { "error" });
    println!("uptime_seconds: {}", uptime);
    Ok(())
}

async fn collect_assets_command(
    SiteRef(site): SiteRef,
    Data(args): Data<CollectStaticArgs>,
) -> Result<(), Error> {
    let report = crate::collectors::collect_assets(
        &site,
        crate::collectors::CollectStaticOptions::new(args.output.clone())
            .clean(args.clean)
            .glob(args.glob.clone()),
    )
    .await
    .map_err(Error::other)?;
    println!("Static assets");
    println!("  copied: {}", report.copied);
    println!("  output: {}", report.output.display());
    Ok(())
}

async fn static_export_command(
    SiteRef(site): SiteRef,
    Data(args): Data<StaticExportArgs>,
) -> Result<(), Error> {
    let report = crate::collectors::export_static(
        &site,
        crate::collectors::StaticExportOptions::new(args.output.clone())
            .clean(args.clean)
            .glob(args.glob.clone()),
    )
    .await
    .map_err(Error::other)?;
    println!("Collected pages");
    println!("  pages: {}", report.pages);
    println!("  assets: {}", report.assets);
    println!("  output: {}", report.output.display());
    Ok(())
}

#[cfg(feature = "migrations")]
async fn make_migration_command(
    SiteRef(site): SiteRef,
    Data(args): Data<MakeMigrationArgs>,
) -> Result<(), Error> {
    if !cfg!(debug_assertions) {
        return Err(Error::invalid(
            "make_migration is only available in debug builds",
        ));
    }
    let name = args.name.as_deref().unwrap_or("auto");
    let engine = migration_engine(&site, None)?;
    let schema = site
        .migration_registry()
        .schema_for(None)
        .map_err(Error::other)?;
    if args.empty {
        let migration = engine.make_empty_migration(name).map_err(Error::other)?;
        println!("Created empty migration {}", migration.id);
        return Ok(());
    }
    if args.merge {
        let migration = engine.make_merge_migration(name).map_err(Error::other)?;
        println!("Created merge migration {}", migration.id);
        return Ok(());
    }
    let engine = engine.with_schema(|_| schema).map_err(Error::other)?;
    if args.check {
        engine.make_migration_check().map_err(Error::other)?;
        println!("No migration changes detected.");
        return Ok(());
    }
    let result = if args.dry_run && args.non_interactive {
        engine
            .make_migration_dry_run_non_interactive(args.name.as_deref())
            .map_err(Error::other)?
    } else if args.dry_run {
        engine
            .make_migration_dry_run(args.name.as_deref())
            .map_err(Error::other)?
    } else if args.non_interactive {
        engine
            .make_migration_non_interactive(args.name.as_deref())
            .map_err(Error::other)?
    } else {
        engine
            .make_migration_named(args.name.as_deref())
            .map_err(Error::other)?
    };
    match result {
        Some(migration) => println!("Created migration {}", migration.id),
        None => println!("No changes detected."),
    }
    Ok(())
}

#[cfg(feature = "migrations")]
async fn migrate_command(
    SiteRef(site): SiteRef,
    Data(args): Data<MigrateArgs>,
) -> Result<(), Error> {
    let engine = migration_engine(&site, None)?;
    if args.plan {
        for id in engine.plan().await.map_err(Error::other)? {
            println!("{id}");
        }
        return Ok(());
    }
    if args.check {
        if engine.check().await.map_err(Error::other)? {
            return Err(Error::invalid("pending migrations exist"));
        }
        println!("No pending migrations.");
        return Ok(());
    }
    let applied = if args.fake {
        engine.fake_migrate().await.map_err(Error::other)?
    } else if let Some(target) = args.target.as_deref() {
        engine.migrate_to(target).await.map_err(Error::other)?
    } else {
        engine.migrate().await.map_err(Error::other)?
    };
    println!("Applied {applied} migration(s).");
    Ok(())
}

#[cfg(feature = "migrations")]
async fn show_migrations_command(
    SiteRef(site): SiteRef,
    _args: Data<ShowMigrationsArgs>,
) -> Result<(), Error> {
    let engine = migration_engine(&site, None)?;
    let rows = engine.show_migrations().await.map_err(Error::other)?;
    if rows.is_empty() {
        println!("No migrations found.");
        return Ok(());
    }
    for (id, applied) in rows {
        let marker = if applied { "x" } else { " " };
        println!("[{marker}] {id}");
    }
    Ok(())
}

#[cfg(feature = "migrations")]
async fn sql_migrate_command(
    SiteRef(site): SiteRef,
    Data(args): Data<SqlMigrateArgs>,
) -> Result<(), Error> {
    let engine = migration_engine(&site, None)?;
    let sql = match (args.backwards, args.id.as_deref()) {
        (true, Some(id)) => engine.sql_rollback(&[id]).map_err(Error::other)?,
        (true, None) => engine.sql_rollback(&[]).map_err(Error::other)?,
        (false, Some(id)) => engine.sql_migrate_id(id).map_err(Error::other)?,
        (false, None) => engine.sql_migrate().map_err(Error::other)?,
    };
    for statement in sql {
        println!("{statement}");
    }
    Ok(())
}

#[cfg(feature = "migrations")]
async fn verify_db_command(
    SiteRef(site): SiteRef,
    Data(args): Data<VerifyDbArgs>,
) -> Result<(), Error> {
    let schema = args.schema.clone();
    let engine = migration_engine(&site, None)?;
    let drift = engine.verify(&schema).await.map_err(Error::other)?;
    if drift.is_empty() {
        println!("Database schema matches migration state.");
        return Ok(());
    }
    println!("{}", serde_json::to_string_pretty(&drift)?);
    Err(Error::invalid("database drift detected"))
}

#[cfg(feature = "migrations")]
async fn inspect_db_command(
    SiteRef(site): SiteRef,
    Data(args): Data<InspectDbArgs>,
) -> Result<(), Error> {
    let schema_name = args.schema.clone();
    let engine = migration_engine(&site, None)?;
    let schema = engine
        .inspect_db(&[schema_name.as_str()])
        .await
        .map_err(Error::other)?;
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}

#[cfg(feature = "migrations")]
fn migration_engine(site: &Site, namespace: Option<&str>) -> Result<gaman::MigrationEngine, Error> {
    let registry = site.migration_registry();
    let source = match namespace {
        Some(ns) => registry
            .get(ns)
            .ok_or_else(|| Error::invalid(format!("no migration source for namespace '{ns}'")))?,
        None => registry
            .root()
            .ok_or_else(|| Error::invalid("no root migration source is registered"))?,
    };
    let mut config = gaman::Config::new(
        Some(site.conf().database.url.clone()),
        source.dir(),
        PathBuf::from("schema.yaml"),
    );
    config.tls = gaman::TlsMode::NoTls;
    let mut engine = gaman::MigrationEngine::new(config, source.embedded());
    if let Some(dialect) = configured_dialect() {
        engine = engine.with_dialect(dialect);
    }
    if namespace.is_none() {
        for (ns, source) in registry.crates() {
            engine = engine.add_migrations(ns, source.embedded());
        }
    }
    Ok(engine)
}

#[cfg(feature = "migrations")]
fn configured_dialect() -> Option<gaman::core::Dialect> {
    #[cfg(all(feature = "postgres", not(feature = "sqlite")))]
    {
        return Some(gaman::core::Dialect::Postgres);
    }
    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    {
        return Some(gaman::core::Dialect::Sqlite);
    }
    #[allow(unreachable_code)]
    None
}
