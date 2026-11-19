use tempfile::TempDir;

use crate::{SiteConf, SiteError, conf::ConfError, db, db::DbSession as _};

/// Provisions the real task schema in a file so a subsequent site can open its own pool.
pub(crate) async fn sqlite_task_conf(conf: SiteConf) -> Result<(SiteConf, TempDir), SiteError> {
    let directory = tempfile::tempdir()?;
    let url = format!(
        "sqlite://{}/tasks.db?mode=rwc&max=1",
        directory.path().display()
    );
    let database = db::DbConf::from_url(&url)?;
    let mut pool = db::DbPool::from_conf(&database).await?;
    for statement in task_schema_sql().map_err(ConfError::Other)? {
        pool.execute(db::Statement::raw(&statement)).await?;
    }
    pool.as_sqlx().close().await;
    Ok((conf.database(database), directory))
}

/// Plans fresh SQLite DDL from the framework schema rather than duplicating table definitions.
fn task_schema_sql() -> Result<Vec<String>, String> {
    let planner = db::gaman::OfflinePlanner::new(db::Dialect::Sqlite);
    let schema = crate::tasks::store::database::schema::task_schema().map_err(|e| e.to_string())?;
    let migration = planner
        .make_migration(schema, &[])
        .map_err(|e| e.to_string())?
        .ok_or("empty task schema migration")?;
    planner.sql_migrate(&[migration]).map_err(|e| e.to_string())
}
