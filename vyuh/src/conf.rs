//! Configuration system.
//!
//! Secrets and deployment-specific values go in env vars.
//! Project structure and logic go in source code.

use std::{ffi::OsString, path::PathBuf};

use crate::{
    auth::{AuthConf, CookieSameSite, JwtKeySource},
    channels::ChannelConf,
    console::ConsoleConf,
    db::DbConf,
    emitters::EmitterConf,
    errors::ErrorConf,
    file_storage::UploadConf,
    logging,
    middlewares::HttpConf,
    observability::ObservabilityConf,
    tasks::TaskConf,
    templates::TemplateConf,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfError {
    #[error("missing required field '{field}': {reason}")]
    RequiredField { field: String, reason: String },

    #[error("invalid value for '{field}': {reason}{}", expected.as_ref().map(|e| format!(" (expected: {})", e)).unwrap_or_default())]
    InvalidValue {
        field: String,
        reason: String,
        expected: Option<String>,
    },

    #[error("invalid path for '{field}' at '{path}': {reason}")]
    InvalidPath {
        field: String,
        path: String,
        reason: String,
    },

    #[error("validation failed with {} error(s):\n{}", .0.len(), ConfError::display_many(.0))]
    Many(Vec<ConfError>),

    #[error("missing required field: {0}")]
    MissingField(String),

    #[error("{0}")]
    Other(String),
}

impl ConfError {
    fn display_many(errors: &[ConfError]) -> String {
        errors
            .iter()
            .map(|e| format!("- {}", e))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

pub fn workspace_root(crate_dir: OsString) -> PathBuf {
    let mut dir = PathBuf::from(crate_dir);

    loop {
        let cargo = dir.join("Cargo.toml");

        if cargo.exists() {
            if let Ok(content) = std::fs::read_to_string(&cargo) {
                if content.contains("[workspace]") {
                    return dir;
                }
            }
        }

        if !dir.pop() {
            break;
        }
    }

    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn project_dir() -> PathBuf {
    if let Some(crate_dir) = std::env::var_os("CARGO_MANIFEST_DIR") {
        workspace_root(crate_dir)
    } else {
        std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
    }
}

/// Controls whether configuration is validated for local development or production deployment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    /// Preserve flexible local-development defaults.
    #[default]
    Development,
    /// Require the framework's hardened production configuration.
    Production,
}

fn default_secret_key() -> String {
    format!(
        "dev-secret-{}-replace-before-production",
        env!("CARGO_PKG_NAME")
    )
}

/// Builds the common error for a required production setting.
fn production_required(field: &str) -> ConfError {
    ConfError::InvalidValue {
        field: field.into(),
        reason: "must be enabled in production mode".into(),
        expected: Some("true".into()),
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SiteConf {
    /// Deployment validation mode.
    #[serde(default)]
    pub deployment: DeploymentMode,

    pub host: String,

    pub port: u16,

    pub project_dir: String,

    pub database: DbConf,

    #[serde(default = "default_secret_key")]
    pub secret_key: String,

    /// absolute or relative to project_dir
    pub media_dir: Option<String>,

    /// Template environment behavior. Template files are registered through bundles.
    pub templates: TemplateConf,

    /// absolute or relative to project_dir
    pub touch_reload: Option<String>,

    pub log_init: bool,

    pub tz: Option<String>,

    pub auth: AuthConf,

    pub tasks: TaskConf,

    pub uploads: UploadConf,

    pub channels: ChannelConf,

    pub console: ConsoleConf,

    #[serde(default)]
    pub emitters: EmitterConf,

    pub logging: logging::LoggingConf,

    pub http: HttpConf,

    /// Health probe and Prometheus metrics configuration.
    #[serde(default)]
    pub observability: ObservabilityConf,

    #[serde(skip)]
    pub errors: ErrorConf,
}

impl Default for SiteConf {
    fn default() -> Self {
        let secret_key = default_secret_key();
        Self {
            deployment: DeploymentMode::Development,
            host: "localhost".to_string(),
            port: 8080,
            project_dir: project_dir().as_os_str().to_string_lossy().to_string(),
            database: Default::default(),
            secret_key,
            media_dir: None,
            templates: TemplateConf::default(),
            touch_reload: None,
            log_init: true,
            tz: None,
            auth: AuthConf::default(),
            tasks: TaskConf::default(),
            uploads: UploadConf::default(),
            channels: ChannelConf::default(),
            console: ConsoleConf::default(),
            emitters: EmitterConf::default(),
            logging: logging::LoggingConf::default(),
            http: HttpConf::default(),
            observability: ObservabilityConf::default(),
            errors: ErrorConf::default(),
        }
    }
}

impl SiteConf {
    /// Returns an explicit configuration baseline for production deployments.
    pub fn production() -> Self {
        Self {
            deployment: DeploymentMode::Production,
            console: ConsoleConf::production(),
            http: HttpConf::production(),
            observability: ObservabilityConf::production(),
            ..Self::default()
        }
    }

    /// Apply env vars as patches. Errors if invalid format.
    pub fn with_env(mut self) -> Result<Self, ConfError> {
        apply_env_patches(&mut self, None)?;
        Ok(self)
    }

    /// Parse from loaded env vars, no validation.
    pub fn from_env() -> Result<Self, ConfError> {
        Self::default().with_env()
    }

    /// Load .env files and parse env vars.
    pub fn from_env_with_files() -> Result<Self, ConfError> {
        Self::load_env_files();
        Self::default().with_env()
    }

    /// Load .env files by build config.
    pub fn load_env_files() {
        dotenvy::dotenv().ok();

        #[cfg(test)]
        dotenvy::from_filename_override(".env.test").ok();

        #[cfg(all(debug_assertions, not(test)))]
        dotenvy::from_filename_override(".env.dev").ok();

        #[cfg(not(any(debug_assertions, test)))]
        dotenvy::from_filename_override(".env.prod").ok();
    }

    /// Load .env from path.
    pub fn load_env_file(path: &str) {
        if let Err(e) = dotenvy::from_filename_override(path) {
            tracing::warn!("Failed to load env file {}: {}", path, e);
        }
    }

    // Chainable setter methods

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Sets the deployment validation mode.
    pub fn deployment(mut self, deployment: DeploymentMode) -> Self {
        self.deployment = deployment;
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn project_dir(mut self, dir: impl Into<String>) -> Self {
        self.project_dir = dir.into();
        self
    }

    pub fn database(mut self, database: DbConf) -> Self {
        self.database = database;
        self
    }

    pub fn secret_key(mut self, key: impl Into<String>) -> Self {
        self.secret_key = key.into();
        self
    }

    pub fn media_dir(mut self, dir: impl Into<String>) -> Self {
        self.media_dir = Some(dir.into());
        self
    }

    pub fn templates(mut self, templates: TemplateConf) -> Self {
        self.templates = templates;
        self
    }

    pub fn uploads(mut self, uploads: UploadConf) -> Self {
        self.uploads = uploads;
        self
    }

    pub fn channels(mut self, channels: ChannelConf) -> Self {
        self.channels = channels;
        self
    }

    pub fn console(mut self, console: ConsoleConf) -> Self {
        self.console = console;
        self
    }

    pub fn emitters(mut self, emitters: EmitterConf) -> Self {
        self.emitters = emitters;
        self
    }

    pub fn http(mut self, http: HttpConf) -> Self {
        self.http = http;
        self
    }

    /// Sets health probe and metrics configuration.
    pub fn observability(mut self, observability: ObservabilityConf) -> Self {
        self.observability = observability;
        self
    }

    pub fn touch_reload(mut self, path: impl Into<String>) -> Self {
        self.touch_reload = Some(path.into());
        self
    }

    pub fn log_init(mut self, enable: bool) -> Self {
        self.log_init = enable;
        self
    }

    pub fn timezone(mut self, tz: impl Into<String>) -> Self {
        self.tz = Some(tz.into());
        self
    }

    pub fn auth(mut self, auth: AuthConf) -> Self {
        self.auth = auth;
        self
    }

    pub fn tasks(mut self, tasks: TaskConf) -> Self {
        self.tasks = tasks;
        self
    }

    pub fn logging(mut self, logging: logging::LoggingConf) -> Self {
        self.logging = logging;
        self
    }

    pub fn errors(mut self, errors: ErrorConf) -> Self {
        self.errors = errors;
        self
    }

    /// Validate config. Returns Ok(()) if valid, or Err(ConfError::Many) with all errors.
    pub fn validate(&self) -> Result<(), ConfError> {
        let mut errors = Vec::new();

        self.validate_required(&mut errors);
        self.validate_database(&mut errors);
        self.validate_paths(&mut errors);
        self.console.validate(&mut errors);
        self.observability.validate(&mut errors);
        self.validate_production(&mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfError::Many(errors))
        }
    }

    fn validate_required(&self, errors: &mut Vec<ConfError>) {
        if self.secret_key.is_empty() {
            errors.push(ConfError::RequiredField {
                field: "secret_key".into(),
                reason: "cannot be empty".into(),
            });
        } else if matches!(self.auth.jwt.signing_key, JwtKeySource::SiteSecret)
            && self.secret_key.len() < self.auth.min_secret_len
        {
            errors.push(ConfError::InvalidValue {
                field: "secret_key".into(),
                reason: format!(
                    "must be at least {} characters for auth signing",
                    self.auth.min_secret_len
                ),
                expected: Some(format!("{} or more characters", self.auth.min_secret_len)),
            });
        }
        #[cfg(not(debug_assertions))]
        {
            let default_key = default_secret_key();
            if self.secret_key == default_key {
                errors.push(ConfError::InvalidValue {
                    field: "secret_key".into(),
                    reason: "must not be the default value in release builds".into(),
                    expected: Some("a custom secret key".into()),
                });
            }
        }
        if self.port == 0 {
            errors.push(ConfError::InvalidValue {
                field: "port".into(),
                reason: "must be non-zero".into(),
                expected: Some("1-65535".into()),
            });
        }
        if self.host.is_empty() {
            errors.push(ConfError::RequiredField {
                field: "host".into(),
                reason: "cannot be empty".into(),
            });
        }
    }

    fn validate_database(&self, errors: &mut Vec<ConfError>) {
        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
        {
            #[cfg(not(debug_assertions))]
            if self.database.lazy {
                errors.push(ConfError::InvalidValue {
                    field: "database.lazy".into(),
                    reason: "must be false in release builds".into(),
                    expected: Some("false".into()),
                });
            }

            if self.database.url.is_empty() {
                errors.push(ConfError::RequiredField {
                    field: "database.url".into(),
                    reason: "cannot be empty".into(),
                });
            }
            if self.database.max_connections == 0 {
                errors.push(ConfError::InvalidValue {
                    field: "database.max_connections".into(),
                    reason: "must be non-zero".into(),
                    expected: Some("positive integer".into()),
                });
            }
            if self.database.min_connections > self.database.max_connections {
                errors.push(ConfError::InvalidValue {
                    field: "database.min_connections".into(),
                    reason: format!(
                        "cannot exceed max_connections ({} > {})",
                        self.database.min_connections, self.database.max_connections
                    ),
                    expected: Some(format!("<= {}", self.database.max_connections)),
                });
            }
        }

        #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
        let _ = errors;
    }

    /// Adds errors for configurations that are unsafe for production deployment.
    fn validate_production(&self, errors: &mut Vec<ConfError>) {
        if self.deployment != DeploymentMode::Production {
            return;
        }
        self.validate_production_http(errors);
        self.validate_production_console(errors);
        self.validate_production_cookies(errors);
    }

    /// Validates mandatory transport protections for the production profile.
    fn validate_production_http(&self, errors: &mut Vec<ConfError>) {
        for (field, enabled) in [
            ("http.trace.enabled", self.http.trace.enabled),
            ("http.compression.enabled", self.http.compression.enabled),
            ("http.timeout.enabled", self.http.timeout.enabled),
            ("http.body_limit.enabled", self.http.body_limit.enabled),
            (
                "http.security_headers.enabled",
                self.http.security_headers.enabled,
            ),
        ] {
            if !enabled {
                errors.push(production_required(field));
            }
        }
        if self.http.cors.enabled && self.http.cors.permissive {
            errors.push(ConfError::InvalidValue {
                field: "http.cors.permissive".into(),
                reason: "must be false in production mode".into(),
                expected: Some("application-specific CORS middleware".into()),
            });
        }
    }

    /// Validates the optional administrative console before exposing it in production.
    fn validate_production_console(&self, errors: &mut Vec<ConfError>) {
        if self.console.enabled && !self.console.secure_cookie {
            errors.push(ConfError::InvalidValue {
                field: "console.secure_cookie".into(),
                reason: "must be true when console is enabled in production mode".into(),
                expected: Some("true".into()),
            });
        }
    }

    /// Validates cookie transport and browser isolation settings in production.
    fn validate_production_cookies(&self, errors: &mut Vec<ConfError>) {
        for (field, cookie) in [
            ("auth.access_cookie", self.auth.access_cookie.as_ref()),
            ("auth.refresh_cookie", self.auth.refresh_cookie.as_ref()),
        ] {
            let Some(cookie) = cookie else {
                continue;
            };
            if !cookie.secure || !cookie.http_only {
                errors.push(ConfError::InvalidValue {
                    field: field.into(),
                    reason: "must use secure and HTTP-only cookies in production mode".into(),
                    expected: Some("secure=true, http_only=true".into()),
                });
            }
            if matches!(cookie.same_site, CookieSameSite::None) && !cookie.secure {
                errors.push(ConfError::InvalidValue {
                    field: field.into(),
                    reason: "SameSite=None requires secure cookies".into(),
                    expected: Some("secure=true".into()),
                });
            }
        }
    }

    fn validate_paths(&self, errors: &mut Vec<ConfError>) {
        let base = PathBuf::from(&self.project_dir);

        validate_dir_readable(&base, "project_dir", errors);

        if let Some(ref dir) = self.media_dir {
            validate_dir_writable(&base, dir, "media_dir", errors);
        }
        validate_upload_dir(&base, &self.uploads.dir, "uploads.dir", errors);
        if let Some(ref dir) = self.uploads.temp_dir {
            validate_upload_dir(&base, dir, "uploads.temp_dir", errors);
        }
        if let Some(ref file) = self.touch_reload {
            validate_file_writable(&base, file, "touch_reload", errors);
        }
    }
}

fn apply_env_patches(conf: &mut SiteConf, prefix: Option<&str>) -> Result<(), ConfError> {
    let strip_prefix = |key: &str, pref: Option<&str>| -> String {
        pref.and_then(|p| key.strip_prefix(p))
            .unwrap_or(key)
            .to_lowercase()
    };

    for (key, value) in std::env::vars() {
        if let Some(pref) = prefix {
            if !key.starts_with(pref) {
                continue;
            }
        }

        let field_name = strip_prefix(&key, prefix);

        match field_name.as_str() {
            "database_url" => match DbConf::from_url(&value) {
                Ok(db) => conf.database = db,
                Err(e) => {
                    return Err(ConfError::Other(format!("Database config error: {}", e)));
                }
            },
            "secret_key" => conf.secret_key = value,
            "host" => conf.host = value,
            "port" => match value.parse::<u16>() {
                Ok(p) => conf.port = p,
                Err(_) => {
                    return Err(ConfError::Other(format!(
                        "PORT must be a valid u16, got: {}",
                        value
                    )));
                }
            },
            "tz" => conf.tz = Some(value),
            "log_init" => match value.parse::<bool>() {
                Ok(b) => conf.log_init = b,
                Err(_) => {
                    return Err(ConfError::Other(format!(
                        "LOG_INIT must be 'true' or 'false', got: {}",
                        value
                    )));
                }
            },
            _ => {} // Ignore unknown fields
        }
    }
    Ok(())
}

fn validate_dir_readable(path: &PathBuf, field: &str, errors: &mut Vec<ConfError>) {
    if !path.exists() {
        errors.push(ConfError::InvalidPath {
            field: field.into(),
            path: path.display().to_string(),
            reason: "directory does not exist".into(),
        });
        return;
    }
    if !path.is_dir() {
        errors.push(ConfError::InvalidPath {
            field: field.into(),
            path: path.display().to_string(),
            reason: "not a directory".into(),
        });
        return;
    }
    if let Err(e) = std::fs::read_dir(path) {
        errors.push(ConfError::InvalidPath {
            field: field.into(),
            path: path.display().to_string(),
            reason: format!("cannot read directory: {}", e),
        });
    }
}

fn validate_dir_writable(base: &PathBuf, dir: &str, field: &str, errors: &mut Vec<ConfError>) {
    if dir.is_empty() {
        return;
    }
    let path = base.join(dir);
    validate_dir_readable(&path, field, errors);

    if path.exists() && path.is_dir() {
        let test_file = path.join(format!(".vyuh_dir_write_{}", std::process::id()));
        if std::fs::write(&test_file, b"").is_err() {
            errors.push(ConfError::InvalidPath {
                field: field.into(),
                path: path.display().to_string(),
                reason: "directory is not writable".into(),
            });
        } else {
            let _ = std::fs::remove_file(test_file);
        }
    }
}

fn validate_upload_dir(base: &PathBuf, dir: &str, field: &str, errors: &mut Vec<ConfError>) {
    if dir.is_empty() {
        errors.push(ConfError::RequiredField {
            field: field.into(),
            reason: "cannot be empty".into(),
        });
        return;
    }
    let path = base.join(dir);
    if path.exists() {
        validate_dir_writable(base, dir, field, errors);
        return;
    }
    let Some(parent) = path.parent() else {
        return;
    };
    if !parent.exists() {
        errors.push(ConfError::InvalidPath {
            field: field.into(),
            path: parent.display().to_string(),
            reason: "parent directory does not exist".into(),
        });
    }
}

fn validate_file_writable(base: &PathBuf, file: &str, field: &str, errors: &mut Vec<ConfError>) {
    if file.is_empty() {
        return;
    }
    let path = base.join(file);

    if let Some(parent) = path.parent() {
        if !parent.exists() {
            errors.push(ConfError::InvalidPath {
                field: field.into(),
                path: parent.display().to_string(),
                reason: "parent directory does not exist".into(),
            });
            return;
        }
        if !parent.is_dir() {
            errors.push(ConfError::InvalidPath {
                field: field.into(),
                path: parent.display().to_string(),
                reason: "parent is not a directory".into(),
            });
            return;
        }

        let test_file = parent.join(format!(".vyuh_touch_write_{}", std::process::id()));
        if std::fs::write(&test_file, b"").is_err() {
            errors.push(ConfError::InvalidPath {
                field: field.into(),
                path: parent.display().to_string(),
                reason: "parent directory is not writable".into(),
            });
        } else {
            let _ = std::fs::remove_file(test_file);
        }
    }
}
