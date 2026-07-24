#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use crate::bundles::Bundle;
use crate::db::{DbConf, Pool};
use crate::{Site, SiteConf};
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use crate::{SiteError, bundles::IntoBundle};
use axum::Router;
use axum::body::{self, Body, Bytes};
use axum::http::{Method, Request, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{self, Value, value::to_value};
use std::collections::BTreeMap;
use std::ops::Deref;
use tower::ServiceExt;

pub use crate::db::sqlx::test_block_on;

pub fn router(site: &Site) -> Router {
    site.router()
}

pub struct TestClient {
    inner: TestClientInner,
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    database: Option<crate::db::testing::TestDatabase>,
}

struct TestClientInner {
    app: Router,
    site: Site,
}

impl Drop for TestClientInner {
    fn drop(&mut self) {
        self.site.shutdown();
    }
}

impl TestClient {
    pub fn new(site: Site) -> Self {
        let app = router(&site);
        Self {
            inner: TestClientInner { app, site },
            #[cfg(all(
                feature = "test-support",
                any(feature = "postgres", feature = "mysql", feature = "sqlite")
            ))]
            database: None,
        }
    }

    /// Builds an in-process HTTP client from application configuration and a supplied Mool pool.
    ///
    /// The caller retains database ownership and must tear it down after this client has shut
    /// down. Prefer [`Self::from_conf`] when Mool's `test-support` feature is enabled.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub async fn from_pool(
        conf: SiteConf,
        bundle: impl IntoBundle,
        pool: &crate::db::DbPool,
    ) -> Result<Self, SiteError> {
        let site = Site::test(conf, bundle, pool.as_sqlx().clone()).await?;
        Ok(Self::new(site))
    }

    /// Provisions a Mool-owned isolated database, applies bundle migrations, and builds a client.
    ///
    /// The supplied configuration must contain credentials that can create and drop an isolated
    /// database. Call [`Self::teardown`] to shut down the site and report cleanup failures.
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    pub async fn from_conf(
        conf: SiteConf,
        bundle: impl IntoBundle,
    ) -> Result<Self, TestClientError> {
        Self::builder(conf, bundle).build().await
    }

    /// Configures a Mool-owned test database before building an in-process client.
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    pub fn builder(conf: SiteConf, bundle: impl IntoBundle) -> TestClientBuilder {
        TestClientBuilder {
            conf,
            bundle: bundle.into_bundle(),
            apply_migrations: true,
        }
    }

    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    /// Applies registered migrations to a Mool-owned isolated database.
    async fn provision_database(
        conf: &SiteConf,
        bundle: &Bundle,
        apply_migrations: bool,
    ) -> Result<crate::db::testing::TestDatabase, TestClientError> {
        let setup = crate::db::testing::setup(conf.database.clone());
        #[cfg(not(feature = "migrations"))]
        let _ = (bundle, apply_migrations);
        #[cfg(feature = "migrations")]
        let setup = if apply_migrations && bundle.migrations.root().is_some() {
            setup.with_migrations(&bundle.migrations)
        } else {
            setup
        };
        Ok(setup.create().await?)
    }

    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    /// Transfers an isolated Mool database into the client after the site builds successfully.
    async fn from_database(
        conf: SiteConf,
        bundle: Bundle,
        database: crate::db::testing::TestDatabase,
    ) -> Result<Self, TestClientError> {
        let site = Site::test(conf, bundle, database.pool().as_sqlx().clone()).await;
        match site {
            Ok(site) => {
                let app = router(&site);
                Ok(Self {
                    inner: TestClientInner { app, site },
                    database: Some(database),
                })
            }
            Err(site) => Self::cleanup_failed_site(database, site).await,
        }
    }

    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    /// Removes an isolated database when site construction fails.
    async fn cleanup_failed_site(
        database: crate::db::testing::TestDatabase,
        site: SiteError,
    ) -> Result<Self, TestClientError> {
        match database.teardown().await {
            Ok(()) => Err(TestClientError::Site(site)),
            Err(cleanup) => Err(TestClientError::SetupCleanup { site, cleanup }),
        }
    }

    /// Stops background engines before its associated test database is removed.
    pub async fn shutdown_and_wait(self) {
        self.inner.site.shutdown_and_wait().await;
    }

    /// Shuts down the site and deterministically removes its Mool-owned test database.
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    pub async fn teardown(self) -> Result<(), TestClientError> {
        let Self { inner, database } = self;
        inner.site.shutdown_and_wait().await;
        drop(inner);
        if let Some(database) = database {
            database.teardown().await?;
        }
        Ok(())
    }

    /// Returns the built site for test data setup and framework state assertions.
    pub fn site(&self) -> &Site {
        &self.inner.site
    }

    pub fn request(&self, method: Method, path: &str) -> TestRequestBuilder {
        TestRequestBuilder::new(self.inner.app.clone(), method, path)
    }

    pub fn get(&self, path: &str) -> TestRequestBuilder {
        self.request(Method::GET, path)
    }
    pub fn post(&self, path: &str) -> TestRequestBuilder {
        self.request(Method::POST, path)
    }
    pub fn put(&self, path: &str) -> TestRequestBuilder {
        self.request(Method::PUT, path)
    }
    pub fn delete(&self, path: &str) -> TestRequestBuilder {
        self.request(Method::DELETE, path)
    }
    pub fn patch(&self, path: &str) -> TestRequestBuilder {
        self.request(Method::PATCH, path)
    }
}

#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
/// Errors returned while provisioning a Mool-owned database or removing it after a test.
#[derive(Debug, thiserror::Error)]
pub enum TestClientError {
    /// Mool could not provision, migrate, or remove the isolated target.
    #[error(transparent)]
    Database(#[from] crate::db::testing::TestDatabaseError),
    /// The site could not be built after the database was provisioned.
    #[error(transparent)]
    Site(#[from] SiteError),
    /// Site construction and database cleanup both failed.
    #[error("site build failed: {site}; isolated database cleanup also failed: {cleanup}")]
    SetupCleanup {
        /// The site construction failure.
        site: SiteError,
        /// The deterministic test database cleanup failure.
        cleanup: crate::db::testing::TestDatabaseError,
    },
}

#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
/// Configures whether a test client applies registered migrations before site construction.
pub struct TestClientBuilder {
    conf: SiteConf,
    bundle: Bundle,
    apply_migrations: bool,
}

#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
impl TestClientBuilder {
    /// Leaves the isolated database empty instead of applying registered migrations.
    ///
    /// Use this for migration-command tests, schema-negative tests, or fixtures that create
    /// their own schema. The site still uses the configured Mool pool.
    pub fn without_migrations(mut self) -> Self {
        self.apply_migrations = false;
        self
    }

    /// Provisions the isolated database and builds the in-process client.
    pub async fn build(self) -> Result<TestClient, TestClientError> {
        let database =
            TestClient::provision_database(&self.conf, &self.bundle, self.apply_migrations).await?;
        TestClient::from_database(self.conf, self.bundle, database).await
    }
}

pub struct TestRequestBuilder {
    app: Router,
    method: Method,
    path: String,
    headers: Vec<(String, String)>,
    body: Option<Body>,
}

impl TestRequestBuilder {
    pub fn new(app: Router, method: Method, path: &str) -> Self {
        Self {
            app,
            method,
            path: path.to_string(),
            headers: Vec::new(),
            body: None,
        }
    }

    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }

    pub fn body(mut self, body: Body) -> Self {
        self.body = Some(body);
        self
    }

    pub fn json<T: Serialize>(mut self, value: &T) -> Self {
        let json = serde_json::to_vec(value).expect("Failed to serialize JSON");
        self.body = Some(Body::from(json));
        self.headers
            .push(("content-type".to_string(), "application/json".to_string()));
        self
    }

    pub fn query<T: Serialize>(mut self, params: &[(&str, T)]) -> Self {
        let query = TestClient::build_query(params);
        if self.path.contains('?') {
            self.path = format!("{}&{}", self.path, query);
        } else {
            self.path = format!("{}?{}", self.path, query);
        }
        self
    }

    pub async fn send(self) -> TestResponse {
        let mut req = Request::builder().method(self.method).uri(self.path);
        for (k, v) in self.headers {
            req = req.header(&k, &v);
        }
        let req = req
            .body(self.body.unwrap_or_else(|| Body::empty()))
            .unwrap();
        let resp = self.app.clone().oneshot(req).await.unwrap();
        TestResponse { resp }
    }
}

#[derive(Debug)]
pub struct TestResponse {
    resp: Response<Body>,
}

impl TestResponse {
    pub fn status(&self) -> axum::http::StatusCode {
        self.resp.status()
    }
    pub fn header(&self, name: &str) -> Option<&axum::http::HeaderValue> {
        self.resp.headers().get(name)
    }
    pub async fn text(self) -> String {
        let bytes = body::to_bytes(self.resp.into_body(), usize::MAX)
            .await
            .expect("Failed to read body");
        String::from_utf8(bytes.to_vec()).expect("Response was not valid UTF-8")
    }
    pub async fn bytes(self) -> Bytes {
        body::to_bytes(self.resp.into_body(), usize::MAX)
            .await
            .expect("Failed to read body")
    }
    pub async fn json<T: DeserializeOwned>(self) -> T {
        let bytes = body::to_bytes(self.resp.into_body(), usize::MAX)
            .await
            .expect("Failed to read body");
        serde_json::from_slice(&bytes).expect("Response was not valid JSON")
    }
    pub async fn assert_text(self, expected_status: axum::http::StatusCode, expected_body: &str) {
        assert_eq!(self.status(), expected_status);
        let body = self.text().await;
        assert_eq!(body, expected_body);
    }
    pub async fn assert_json<T: DeserializeOwned + PartialEq + std::fmt::Debug>(
        self,
        expected_status: axum::http::StatusCode,
        expected_json: &T,
    ) {
        assert_eq!(self.status(), expected_status);
        let body: T = self.json().await;
        assert_eq!(&body, expected_json);
    }

    pub fn assert_status(self, expected_status: axum::http::StatusCode) -> Self {
        assert_eq!(
            self.status(),
            expected_status,
            "Expected status {}, got {}",
            expected_status,
            self.status()
        );
        self
    }

    pub fn assert_ok(self) -> Self {
        self.assert_status(axum::http::StatusCode::OK)
    }

    pub fn assert_created(self) -> Self {
        self.assert_status(axum::http::StatusCode::CREATED)
    }

    pub fn assert_not_found(self) -> Self {
        self.assert_status(axum::http::StatusCode::NOT_FOUND)
    }

    pub fn assert_bad_request(self) -> Self {
        self.assert_status(axum::http::StatusCode::BAD_REQUEST)
    }
}

impl TestClient {
    pub fn build_query<T: Serialize>(params: &[(&str, T)]) -> String {
        let mut map = BTreeMap::new();
        for (k, v) in params {
            let value: Value = to_value(v).expect("Failed to serialize param");
            let s = match value {
                Value::String(s) => s,
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => value.to_string(),
            };
            map.insert(*k, s);
        }
        serde_urlencoded::to_string(&map).unwrap()
    }
}

/// Creates a minimal mock Site for testing purposes
/// Uses lazy DB (no actual connection) and safe defaults
pub async fn mock_site() -> SiteConf {
    use uuid::Uuid;

    let _test_db_name = format!("vyuh_test_{}", Uuid::now_v7().simple());
    let conf = SiteConf {
        host: "localhost".to_string(),
        port: 8080,
        project_dir: "/tmp/vyuh_test".to_string(),
        database: DbConf::default(),
        secret_key: "test_secret_key_minimum_32_chars!".to_string(),
        media_dir: None,
        templates: crate::templates::TemplateConf::default(),
        touch_reload: None,
        log_init: false,
        tz: Some("UTC".to_string()),
        auth: crate::auth::AuthConf::default(),
        ..Default::default()
    };

    conf
}

/// RAII guard for a test database
/// Automatically drops the database when the guard is dropped
pub struct MockDb {
    pool: Pool,
    pub db_name: String,
    pub base_url: String,
}

impl MockDb {
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

impl Deref for MockDb {
    type Target = Pool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl Drop for MockDb {
    fn drop(&mut self) {
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        let db_name = self.db_name.clone();
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        let base_url = self.base_url.clone();

        #[cfg(feature = "postgres")]
        {
            if !db_name.is_empty() {
                let _ = std::thread::spawn(move || {
                    let rt = tokio::runtime::Runtime::new().ok()?;
                    rt.block_on(async {
                        if let Ok(root_pool) = crate::db::sqlx::PgPool::connect(&base_url).await {
                            let _ = crate::db::sqlx::query(&format!(
                                "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
                                db_name
                            ))
                            .execute(&root_pool)
                            .await;
                            root_pool.close().await;
                        }
                        Some(())
                    })
                })
                .join();
            }
        }

        #[cfg(feature = "mysql")]
        {
            if !db_name.is_empty() {
                let _ = std::thread::spawn(move || {
                    let rt = tokio::runtime::Runtime::new().ok()?;
                    rt.block_on(async {
                        if let Ok(root_pool) = crate::db::sqlx::MySqlPool::connect(&base_url).await
                        {
                            let _ = crate::db::sqlx::query(&format!(
                                "DROP DATABASE IF EXISTS `{}`",
                                db_name
                            ))
                            .execute(&root_pool)
                            .await;
                            root_pool.close().await;
                        }
                        Some(())
                    })
                })
                .join();
            }
        }

        #[cfg(feature = "sqlite")]
        {
            // SQLite uses :memory:, no cleanup needed
        }
    }
}

/// Creates a new isolated database for testing
/// Similar to sqlx test macros, creates a unique database that is cleaned up after use
/// Returns a MockDb guard that derefs to Pool and drops the database on drop
///
/// # Example
/// ```ignore
/// #[tokio::test]
/// async fn test_something() {
///     let db = mock_db().await;
///     // Use db like a Pool - it derefs automatically
///     crate::db::sqlx::query("SELECT 1").execute(&*db).await.unwrap();
///     // Database is dropped when db goes out of scope
/// }
/// ```
pub async fn mock_db() -> MockDb {
    #[cfg(feature = "postgres")]
    {
        let base_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost".to_string());

        let db_name = format!("vyuh_test_{}", uuid::Uuid::now_v7().simple());

        let root_pool = crate::db::sqlx::PgPool::connect(&base_url)
            .await
            .expect("Failed to connect to postgres");

        crate::db::sqlx::query(&format!("CREATE DATABASE \"{}\"", db_name))
            .execute(&root_pool)
            .await
            .expect("Failed to create test database");

        root_pool.close().await;

        let test_url = if base_url.contains('/') {
            let parts: Vec<&str> = base_url.rsplitn(2, '/').collect();
            format!("{}/{}", parts[1], db_name)
        } else {
            format!("{}/{}", base_url, db_name)
        };

        let pool = crate::db::sqlx::PgPool::connect(&test_url)
            .await
            .expect("Failed to connect to test database");

        MockDb {
            pool,
            db_name,
            base_url,
        }
    }

    #[cfg(feature = "mysql")]
    {
        let base_url =
            std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "mysql://localhost".to_string());

        let db_name = format!("vyuh_test_{}", uuid::Uuid::now_v7().simple());

        let root_pool = crate::db::sqlx::MySqlPool::connect(&base_url)
            .await
            .expect("Failed to connect to mysql");

        crate::db::sqlx::query(&format!("CREATE DATABASE `{}`", db_name))
            .execute(&root_pool)
            .await
            .expect("Failed to create test database");

        root_pool.close().await;

        let test_url = if base_url.contains('/') {
            let parts: Vec<&str> = base_url.rsplitn(2, '/').collect();
            format!("{}/{}", parts[1], db_name)
        } else {
            format!("{}/{}", base_url, db_name)
        };

        let pool = crate::db::sqlx::MySqlPool::connect(&test_url)
            .await
            .expect("Failed to connect to test database");

        MockDb {
            pool,
            db_name,
            base_url,
        }
    }

    #[cfg(feature = "sqlite")]
    {
        let pool = crate::db::sqlx::SqlitePool::connect(":memory:")
            .await
            .expect("Failed to create in-memory sqlite database");

        MockDb {
            pool,
            db_name: String::new(),
            base_url: String::new(),
        }
    }

    #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
    {
        MockDb {
            pool: Pool::default(),
            db_name: String::new(),
            base_url: String::new(),
        }
    }
}
