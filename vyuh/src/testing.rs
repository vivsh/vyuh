use crate::auth::{Audience, AuthError, AuthUser, LoginResponse, RequestCredentialLocation};
#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use crate::bundles::Bundle;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use crate::bundles::IntoBundle;
use crate::db::DbConf;
use crate::{Site, SiteConf, SiteError};
use axum::Router;
use axum::body::{self, Body, Bytes};
use axum::extract::ConnectInfo;
use axum::http::{Method, Request, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{self, Value, value::to_value};
use std::{collections::BTreeMap, net::SocketAddr};
use tower::ServiceExt;

pub use crate::db::sqlx::test_block_on;
#[doc(hidden)]
pub use tokio;

/// Returns the router of an already-built site for direct Axum-level tests.
pub fn router(site: &Site) -> Router {
    site.router()
}

/// A complete in-process Vyuh test fixture.
///
/// `TestSite` owns the site and router used by request assertions. When built
/// with [`test_site`], it also owns Mool's isolated database and removes it
/// during [`Self::teardown`].
pub struct TestSite {
    inner: TestSiteInner,
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    database: Option<crate::db::testing::TestDatabase>,
}

struct TestSiteInner {
    app: Router,
    site: Site,
}

impl Drop for TestSiteInner {
    fn drop(&mut self) {
        self.site.shutdown();
    }
}

impl TestSite {
    /// Wraps an already-built site in an in-process test fixture.
    pub fn new(site: Site) -> Self {
        let app = router(&site);
        Self {
            inner: TestSiteInner { app, site },
            #[cfg(all(
                feature = "test-support",
                any(feature = "postgres", feature = "mysql", feature = "sqlite")
            ))]
            database: None,
        }
    }

    /// Starts background runtime engines for an integration test.
    ///
    /// Test sites are inert by default. Call this only when the test needs to
    /// exercise task workers, emitters, PgNotify listeners, or service workers.
    pub async fn start_runtime(&self) -> Result<(), SiteError> {
        self.inner.site.start_runtime().await
    }

    /// Builds an in-process test site from application configuration and a supplied Mool pool.
    ///
    /// The caller retains database ownership and must tear it down after this site has shut
    /// down. Prefer [`test_site`] when Mool's `test-support` feature is enabled.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub async fn from_pool(
        conf: SiteConf,
        bundle: impl IntoBundle,
        pool: &crate::db::DbPool,
    ) -> Result<Self, SiteError> {
        let site = Site::test(conf, bundle, pool.as_sqlx().clone()).await?;
        Ok(Self::new(site))
    }

    /// Provisions a Mool-owned isolated database, applies bundle migrations, and builds a test site.
    ///
    /// The supplied configuration must contain credentials that can create and drop an isolated
    /// database. Call [`Self::teardown`] to shut down the site and report cleanup failures.
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    pub async fn from_conf(conf: SiteConf, bundle: impl IntoBundle) -> Result<Self, TestSiteError> {
        Self::builder(conf, bundle).build().await
    }

    /// Configures a Mool-owned test database before building an in-process client.
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    pub fn builder(conf: SiteConf, bundle: impl IntoBundle) -> TestSiteBuilder {
        TestSiteBuilder {
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
    ) -> Result<crate::db::testing::TestDatabase, TestSiteError> {
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
    ) -> Result<Self, TestSiteError> {
        let site = Site::test(conf, bundle, database.pool().as_sqlx().clone()).await;
        match site {
            Ok(site) => {
                let app = router(&site);
                Ok(Self {
                    inner: TestSiteInner { app, site },
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
    ) -> Result<Self, TestSiteError> {
        match database.teardown().await {
            Ok(()) => Err(TestSiteError::Site(site)),
            Err(cleanup) => Err(TestSiteError::SetupCleanup { site, cleanup }),
        }
    }

    /// Stops any active background engines before its associated test database is removed.
    pub async fn shutdown_and_wait(self) {
        self.inner.site.shutdown_and_wait().await;
    }

    /// Shuts down the site and deterministically removes its Mool-owned test database.
    #[cfg(all(
        feature = "test-support",
        any(feature = "postgres", feature = "mysql", feature = "sqlite")
    ))]
    pub async fn teardown(self) -> Result<(), TestSiteError> {
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

    /// Issues credentials through the site's normal default authentication provider.
    pub async fn login(
        &self,
        user: AuthUser,
        audiences: &[Audience],
    ) -> Result<LoginResponse, AuthError> {
        self.site().auth().login(user, audiences).await
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
pub enum TestSiteError {
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

/// Describes a failure while a macro-owned test site is set up, exercised, or removed.
///
/// The body error remains available to the caller even when deterministic database cleanup also
/// fails. [`finish_test`] constructs this value after the test site has been shut down.
#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
#[derive(Debug, thiserror::Error)]
pub enum TestRunError<E> {
    /// The test configuration factory returned the caller's structured error.
    #[error("test configuration failed")]
    Configuration(E),
    /// The isolated test site could not be provisioned.
    #[error("test site setup failed: {0}")]
    Setup(TestSiteError),
    /// The test body returned an error after its site was built.
    #[error("test body failed")]
    Body(E),
    /// The test site ran successfully but its owned database could not be removed.
    #[error("test site cleanup failed: {0}")]
    Cleanup(TestSiteError),
    /// Both the body and deterministic cleanup failed.
    #[error("test body and test site cleanup both failed")]
    BodyAndCleanup {
        /// The error returned by the test body.
        body: E,
        /// The error returned while removing the macro-owned fixture.
        cleanup: TestSiteError,
    },
}

/// Converts a macro configuration expression into a site configuration.
///
/// `#[vyuh::test]` accepts either a direct [`SiteConf`] or a fallible
/// `Result<SiteConf, E>` factory result. A configuration error is preserved as
/// [`TestRunError::Configuration`] because no site exists yet to tear down.
#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
pub trait TestConfSource<E> {
    /// Returns a test-site configuration or the caller's structured test error.
    fn into_test_conf(self) -> Result<SiteConf, E>;
}

#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
impl<E> TestConfSource<E> for SiteConf {
    fn into_test_conf(self) -> Result<SiteConf, E> {
        Ok(self)
    }
}

#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
impl<E> TestConfSource<E> for Result<SiteConf, E> {
    fn into_test_conf(self) -> Result<SiteConf, E> {
        self
    }
}

/// Combines a test body result with the deterministic teardown result of its owned test site.
///
/// A body failure takes precedence when both operations fail; the cleanup error is retained as a
/// structured secondary value in [`TestRunError::BodyAndCleanup`].
#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
pub fn finish_test<T, E>(
    body: Result<T, E>,
    cleanup: Result<(), TestSiteError>,
) -> Result<(), TestRunError<E>> {
    match (body, cleanup) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(body), Ok(())) => Err(TestRunError::Body(body)),
        (Ok(_), Err(cleanup)) => Err(TestRunError::Cleanup(cleanup)),
        (Err(body), Err(cleanup)) => Err(TestRunError::BodyAndCleanup { body, cleanup }),
    }
}

#[cfg(all(
    test,
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
mod test_run_error_tests {
    use super::*;

    /// Supplies a structured teardown failure without opening a database connection.
    fn cleanup_error() -> TestSiteError {
        TestSiteError::Site(SiteError::TimezoneError("test cleanup failure".to_string()))
    }

    /// Retains a structured setup failure before a test body can be entered.
    #[test]
    fn setup_failure_is_preserved() {
        let result = Err::<(), _>(TestRunError::<&str>::Setup(cleanup_error()));
        assert!(matches!(result, Err(TestRunError::Setup(_))));
    }

    /// Preserves a cleanup failure when an otherwise successful test body completes.
    #[test]
    fn cleanup_failure_is_preserved() {
        let result = finish_test::<(), &str>(Ok(()), Err(cleanup_error()));
        assert!(matches!(result, Err(TestRunError::Cleanup(_))));
    }

    /// Preserves both errors while giving the test-body failure precedence in the result shape.
    #[test]
    fn body_and_cleanup_failures_are_preserved() {
        let result = finish_test::<(), _>(Err("body failure"), Err(cleanup_error()));
        assert!(matches!(
            result,
            Err(TestRunError::BodyAndCleanup {
                body: "body failure",
                ..
            })
        ));
    }
}

#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
/// Configures whether a test site applies registered migrations before site construction.
pub struct TestSiteBuilder {
    conf: SiteConf,
    bundle: Bundle,
    apply_migrations: bool,
}

#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
impl TestSiteBuilder {
    /// Leaves the isolated database empty instead of applying registered migrations.
    ///
    /// Use this for migration-command tests, schema-negative tests, or fixtures that create
    /// their own schema. The site still uses the configured Mool pool.
    pub fn without_migrations(mut self) -> Self {
        self.apply_migrations = false;
        self
    }

    /// Provisions the isolated database and builds the in-process test site.
    pub async fn build(self) -> Result<TestSite, TestSiteError> {
        let database =
            TestSite::provision_database(&self.conf, &self.bundle, self.apply_migrations).await?;
        TestSite::from_database(self.conf, self.bundle, database).await
    }
}

/// Provisions an isolated Mool database, applies registered migrations, and builds a test site.
///
/// Use [`TestSite::builder`] when migrations must remain unapplied, and
/// [`TestSite::from_pool`] when a fixture owns a database shared by several test sites.
#[cfg(all(
    feature = "test-support",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
pub async fn test_site(conf: SiteConf, bundle: impl IntoBundle) -> Result<TestSite, TestSiteError> {
    TestSite::from_conf(conf, bundle).await
}

pub struct TestRequestBuilder {
    app: Router,
    method: Method,
    path: String,
    headers: Vec<(String, String)>,
    body: Option<Body>,
    peer_addr: SocketAddr,
}

impl TestRequestBuilder {
    pub fn new(app: Router, method: Method, path: &str) -> Self {
        Self {
            app,
            method,
            path: path.to_string(),
            headers: Vec::new(),
            body: None,
            peer_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        }
    }

    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }

    /// Applies the issued access credential through its configured request selector.
    ///
    /// This mirrors bearer, custom-header, cookie (including CSRF), and query-token
    /// providers without tests hard-coding their delivery details.
    pub fn with_login<T>(mut self, login: &LoginResponse<T>) -> Self {
        let access = login.credentials().access();
        match login.request_selector() {
            RequestCredentialLocation::Header { name, scheme } => {
                let value = scheme
                    .as_deref()
                    .map(|scheme| format!("{scheme} {access}"))
                    .unwrap_or_else(|| access.to_owned());
                self.headers.push((name.clone(), value));
            }
            RequestCredentialLocation::Cookie { name, csrf } => {
                self.append_cookie(name, access);
                if let Some((cookie, header, token)) = csrf {
                    self.append_cookie(cookie, token);
                    self.headers.push((header.clone(), token.clone()));
                }
            }
            RequestCredentialLocation::Query { name } => self.append_query(name, access),
        }
        self
    }

    pub fn body(mut self, body: Body) -> Self {
        self.body = Some(body);
        self
    }

    /// Sets the connection peer address attached to this in-process request.
    pub fn peer_addr(mut self, peer_addr: SocketAddr) -> Self {
        self.peer_addr = peer_addr;
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
        let query = TestSite::build_query(params);
        if self.path.contains('?') {
            self.path = format!("{}&{}", self.path, query);
        } else {
            self.path = format!("{}?{}", self.path, query);
        }
        self
    }

    /// Adds a cookie to the request while retaining any earlier cookies.
    fn append_cookie(&mut self, name: &str, value: &str) {
        if let Some((_, existing)) = self
            .headers
            .iter_mut()
            .find(|(header, _)| header.eq_ignore_ascii_case("cookie"))
        {
            existing.push_str("; ");
            existing.push_str(name);
            existing.push('=');
            existing.push_str(value);
            return;
        }
        self.headers
            .push(("cookie".to_string(), format!("{name}={value}")));
    }

    /// Adds one encoded query value without replacing existing query parameters.
    fn append_query(&mut self, name: &str, value: &str) {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer.append_pair(name, value);
        let query = serializer.finish();
        let separator = if self.path.contains('?') { '&' } else { '?' };
        self.path.push(separator);
        self.path.push_str(&query);
    }

    pub async fn send(self) -> TestResponse {
        let mut req = Request::builder().method(self.method).uri(self.path);
        for (k, v) in self.headers {
            req = req.header(&k, &v);
        }
        let mut req = req
            .body(self.body.unwrap_or_else(|| Body::empty()))
            .unwrap();
        req.extensions_mut().insert(ConnectInfo(self.peer_addr));
        let resp = self.app.clone().oneshot(req).await.unwrap();
        TestResponse { resp }
    }
}

#[derive(Debug)]
pub struct TestResponse {
    resp: Response<Body>,
}

impl TestResponse {
    /// Returns all response headers, including every repeated `Set-Cookie` value.
    pub fn headers(&self) -> &axum::http::HeaderMap {
        self.resp.headers()
    }

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

impl TestSite {
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
        secret_key_fallbacks: Vec::new(),
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
