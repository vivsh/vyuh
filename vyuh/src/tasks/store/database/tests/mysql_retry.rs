use super::*;
use crate::db::DbSession as _;

/// A native 1020 is retryable only on MariaDB; another HY000 remains permanent.
#[tokio::test]
#[ignore = "requires VYUH_TEST_MYSQL_URL and VYUH_TEST_MYSQL_SERVER"]
async fn checks_native_retry_code() -> Result<(), String> {
    let url = std::env::var("VYUH_TEST_MYSQL_URL").map_err(|e| e.to_string())?;
    let server = std::env::var("VYUH_TEST_MYSQL_SERVER").map_err(|e| e.to_string())?;
    let expected = match server.as_str() {
        "mariadb" => true,
        "mysql" => false,
        _ => return Err("expected a MySQL or MariaDB fixture".into()),
    };
    let conf = db::DbConf::from_url(&url).map_err(|e| e.to_string())?;
    let mut pool = db::DbPool::from_conf(&conf)
        .await
        .map_err(|e| e.to_string())?;
    let task_conf = crate::tasks::TaskRegistry::new()
        .with_config(crate::tasks::TaskConf::default())
        .and_then(|registry| registry.store_conf())
        .map_err(|e| e.to_string())?;
    let store = DbTaskStore::new(
        pool.as_sqlx().clone(),
        256,
        std::time::Duration::from_secs(30),
        task_conf,
    );
    store
        .detect_mysql_server()
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(store.uses_mariadb(), expected);
    for code in [1020, 1105] {
        let sql = format!("SIGNAL SQLSTATE 'HY000' SET MYSQL_ERRNO = {code}");
        let error = pool
            .execute(db::Statement::raw(&sql))
            .await
            .err()
            .ok_or("SIGNAL did not return an error")?;
        assert_eq!(error.code(), "database_error");
        assert_eq!(store.retryable_conflict(&error), expected && code == 1020);
    }
    Ok(())
}
