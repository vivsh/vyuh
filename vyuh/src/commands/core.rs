use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
#[cfg(feature = "migrations")]
use std::sync::Arc;

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
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    let db_ok = crate::db::sqlx::query("SELECT 1")
        .execute(site.db().as_sqlx())
        .await
        .is_ok();
    println!("health: ok");
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    println!("database: {}", if db_ok { "ok" } else { "error" });
    #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
    println!("database: not configured");
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
    let schema = site
        .migration_registry()
        .schema_for(None)
        .map_err(Error::other)?;
    match run_migration_command(&site, make_command(&args, schema)?).await? {
        crate::db::engine::CommandResult::Make(result) => print_make_result(result),
        result => Err(unexpected_migration_result("make_migration", result)),
    }
}

#[cfg(feature = "migrations")]
async fn migrate_command(
    SiteRef(site): SiteRef,
    Data(args): Data<MigrateArgs>,
) -> Result<(), Error> {
    match run_migration_command(&site, apply_command(&args)).await? {
        crate::db::engine::CommandResult::Movement(movement) => {
            println!("Applied {} migration(s).", movement.applied);
            Ok(())
        }
        crate::db::engine::CommandResult::Pending(pending) => print_pending_migrations(pending),
        result => Err(unexpected_migration_result("migrate", result)),
    }
}

#[cfg(feature = "migrations")]
async fn show_migrations_command(
    SiteRef(site): SiteRef,
    _args: Data<ShowMigrationsArgs>,
) -> Result<(), Error> {
    match run_migration_command(
        &site,
        crate::db::engine::MigrationCommand::Status {
            reverse: false,
            search: None,
        },
    )
    .await?
    {
        crate::db::engine::CommandResult::Status(rows) => {
            if rows.is_empty() {
                println!("No migrations found.");
            } else {
                for row in rows {
                    let marker = if row.applied { "x" } else { " " };
                    println!("[{marker}] {}", row.id);
                }
            }
            Ok(())
        }
        result => Err(unexpected_migration_result("show_migrations", result)),
    }
}

#[cfg(feature = "migrations")]
async fn sql_migrate_command(
    SiteRef(site): SiteRef,
    Data(args): Data<SqlMigrateArgs>,
) -> Result<(), Error> {
    match run_migration_command(
        &site,
        crate::db::engine::MigrationCommand::Sql {
            id: args.id.clone(),
            backwards: args.backwards,
        },
    )
    .await?
    {
        crate::db::engine::CommandResult::Sql(sql) => {
            for statement in sql {
                println!("{statement}");
            }
            Ok(())
        }
        result => Err(unexpected_migration_result("sql_migrate", result)),
    }
}

#[cfg(feature = "migrations")]
async fn verify_db_command(
    SiteRef(site): SiteRef,
    Data(args): Data<VerifyDbArgs>,
) -> Result<(), Error> {
    match run_migration_command(
        &site,
        crate::db::engine::MigrationCommand::Verify {
            schemas: vec![args.schema.clone()],
        },
    )
    .await?
    {
        crate::db::engine::CommandResult::Verify(drift) if drift.findings.is_empty() => {
            println!("Database schema matches migration state.");
            Ok(())
        }
        crate::db::engine::CommandResult::Verify(drift) => {
            println!("{}", serde_json::to_string_pretty(&drift)?);
            Err(Error::invalid("database drift detected"))
        }
        result => Err(unexpected_migration_result("verify_db", result)),
    }
}

#[cfg(feature = "migrations")]
async fn inspect_db_command(
    SiteRef(site): SiteRef,
    Data(args): Data<InspectDbArgs>,
) -> Result<(), Error> {
    match run_migration_command(
        &site,
        crate::db::engine::MigrationCommand::Inspect {
            schemas: vec![args.schema.clone()],
            filters: Vec::new(),
            table: None,
        },
    )
    .await?
    {
        crate::db::engine::CommandResult::Inspect(schema) => {
            println!("{}", serde_json::to_string_pretty(&schema)?);
            Ok(())
        }
        result => Err(unexpected_migration_result("inspect_db", result)),
    }
}

#[cfg(feature = "migrations")]
/// Concrete Mool runner used for one site-owned migration lifecycle.
pub(crate) type NativeMigrationRunner = crate::db::engine::MigrationRunner<
    crate::db::engine::NativeMigrationStore,
    crate::db::engine::DatabaseTrackingStore,
    crate::db::engine::LazyExecutor,
>;

#[cfg(feature = "migrations")]
/// One serialized Mool migration runner shared by all site commands.
pub(crate) type SharedMigrationRunner = Arc<tokio::sync::Mutex<NativeMigrationRunner>>;

#[cfg(feature = "migrations")]
async fn run_migration_command(
    site: &Site,
    command: crate::db::engine::MigrationCommand,
) -> Result<crate::db::engine::CommandResult, Error> {
    let runner = site
        .migration_runner()
        .ok_or_else(|| Error::invalid("no root migration source is registered"))?;
    let mut runner = runner.lock().await;
    runner.run_command(&command).await.map_err(Error::other)
}

#[cfg(feature = "migrations")]
pub(crate) fn migration_runner(
    registry: crate::db::MigrationRegistry,
    database_url: String,
) -> Result<Option<SharedMigrationRunner>, Error> {
    let registry = Arc::new(registry);
    let Some(source) = registry.root() else {
        return Ok(None);
    };
    let dialect = crate::db::engine::Config::dialect_from_database_url(&database_url)
        .map_err(Error::other)?;
    let config = crate::db::engine::Config::new(
        database_url,
        source.dir(),
        PathBuf::from("schema.yaml"),
        dialect,
    );
    let runner = crate::db::engine::NativeRunnerFactory::from_store(config, registry).build();
    Ok(Some(Arc::new(tokio::sync::Mutex::new(runner))))
}

#[cfg(feature = "migrations")]
fn make_command(
    args: &MakeMigrationArgs,
    schema: crate::db::Schema,
) -> Result<crate::db::engine::MigrationCommand, Error> {
    use crate::db::engine::{MakeCommand, MigrationCommand};

    if args.empty {
        return Ok(MigrationCommand::Make(MakeCommand::Empty {
            name: args.name.clone().unwrap_or_else(|| "auto".to_string()),
        }));
    }
    if args.merge {
        return Ok(MigrationCommand::Make(MakeCommand::Merge {
            name: args.name.clone().unwrap_or_else(|| "auto".to_string()),
        }));
    }
    if args.check {
        return Ok(MigrationCommand::Make(MakeCommand::Check {
            schema,
            decisions: Vec::new(),
        }));
    }
    Ok(MigrationCommand::Make(MakeCommand::Generate {
        schema,
        name: args.name.clone(),
        dry_run: args.dry_run,
        decisions: Vec::new(),
    }))
}

#[cfg(feature = "migrations")]
fn apply_command(args: &MigrateArgs) -> crate::db::engine::MigrationCommand {
    use crate::db::engine::{ApplyCommand, MigrationCommand};

    if args.plan {
        MigrationCommand::Apply(ApplyCommand::Plan)
    } else if args.check {
        MigrationCommand::Apply(ApplyCommand::Check)
    } else {
        MigrationCommand::Apply(ApplyCommand::Execute {
            target: args.target.clone(),
            fake: args.fake,
            fake_verified: false,
            schemas: Vec::new(),
        })
    }
}

#[cfg(feature = "migrations")]
fn print_make_result(result: crate::db::engine::MakeResult) -> Result<(), Error> {
    match result {
        crate::db::engine::MakeResult::Created(migration) => {
            println!("Created migration {}", migration.id);
        }
        crate::db::engine::MakeResult::Preview(migration) => {
            println!("Migration preview {}", migration.id);
        }
        crate::db::engine::MakeResult::NoChanges => println!("No changes detected."),
        crate::db::engine::MakeResult::CheckPassed => println!("No migration changes detected."),
    }
    Ok(())
}

#[cfg(feature = "migrations")]
fn print_pending_migrations(pending: Vec<String>) -> Result<(), Error> {
    if pending.is_empty() {
        println!("No pending migrations.");
        return Ok(());
    }
    for id in pending {
        println!("{id}");
    }
    Ok(())
}

#[cfg(feature = "migrations")]
fn unexpected_migration_result(command: &str, result: crate::db::engine::CommandResult) -> Error {
    Error::invalid(format!("{command} received unexpected result: {result:?}"))
}
