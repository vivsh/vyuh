//! Configuration system.
//!
//! Secrets and deployment-specific values go in env vars.
//! Project structure and logic go in source code.

use std::{ffi::OsString, fmt, path::PathBuf};

use crate::{
    auth::AuthConf, cache::CacheConf, channels::ChannelConf, console::ConsoleConf, db::DbConf,
    email::MailConf, emitters::EmitterConf, errors::ErrorConf, file_storage::UploadConf, logging,
    middlewares::HttpConf, observability::ObservabilityConf, tasks::TaskConf,
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

fn default_static_url() -> String {
    crate::assets::DEFAULT_STATIC_URL.to_string()
}

/// Builds the common error for a required production setting.
fn production_required(field: &str) -> ConfError {
    ConfError::InvalidValue {
        field: field.into(),
        reason: "must be enabled in production mode".into(),
        expected: Some("true".into()),
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SiteConf {
    /// Deployment validation mode.
    #[serde(default)]
    pub deployment: DeploymentMode,

    pub host: String,

    pub port: u16,

    /// Public URL base used for bundle-owned `public/**` assets.
    #[serde(default = "default_static_url")]
    pub static_url: String,

    pub project_dir: String,

    pub database: DbConf,

    #[serde(default = "default_secret_key", skip_serializing)]
    pub secret_key: String,

    /// Previous application secrets accepted only while verifying credentials.
    #[serde(default, skip_serializing)]
    pub secret_key_fallbacks: Vec<String>,

    /// absolute or relative to project_dir
    pub media_dir: Option<String>,

    /// Template environment behavior. Template files are registered through bundles.
    pub templates: TemplateConf,

    /// Outbound SMTP email configuration.
    pub mail: MailConf,

    /// absolute or relative to project_dir
    pub touch_reload: Option<String>,

    pub log_init: bool,

    pub tz: Option<String>,

    #[serde(skip, default)]
    pub auth: AuthConf,

    /// Source-owned cache providers and their default selection.
    #[serde(skip, default)]
    pub cache: CacheConf,

    #[serde(skip, default)]
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

impl fmt::Debug for SiteConf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SiteConf")
            .field("deployment", &self.deployment)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("static_url", &self.static_url)
            .field("project_dir", &self.project_dir)
            .field("secret_key", &"<redacted>")
            .field("secret_key_fallbacks", &self.secret_key_fallbacks.len())
            .field("auth", &self.auth)
            .field("cache", &self.cache)
            .field("console", &self.console)
            .field("observability", &self.observability)
            .finish_non_exhaustive()
    }
}

impl Default for SiteConf {
    fn default() -> Self {
        let secret_key = default_secret_key();
        Self {
            deployment: DeploymentMode::Development,
            host: "localhost".to_string(),
            port: 8080,
            static_url: default_static_url(),
            project_dir: project_dir().as_os_str().to_string_lossy().to_string(),
            database: Default::default(),
            secret_key,
            secret_key_fallbacks: Vec::new(),
            media_dir: None,
            templates: TemplateConf::default(),
            mail: MailConf::default(),
            touch_reload: None,
            log_init: true,
            tz: None,
            auth: AuthConf::default(),
            cache: CacheConf::default(),
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

    /// Sets the root-relative or absolute public URL used for bundled assets.
    pub fn static_url(mut self, static_url: impl Into<String>) -> Self {
        self.static_url = static_url.into();
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

    /// Sets previous secrets retained temporarily for credential verification.
    pub fn secret_key_fallbacks<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.secret_key_fallbacks = keys.into_iter().map(Into::into).collect();
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

    /// Sets outbound SMTP email configuration.
    pub fn mail(mut self, mail: MailConf) -> Self {
        self.mail = mail;
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

    /// Sets source-owned cache provider configuration.
    pub fn cache(mut self, cache: CacheConf) -> Self {
        self.cache = cache;
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
        self.validate_static_url(&mut errors);
        self.validate_database(&mut errors);
        self.validate_paths(&mut errors);
        self.validate_auth(&mut errors);
        self.validate_cache(&mut errors);
        self.validate_tasks(&mut errors);
        self.console.validate(&mut errors);
        self.observability.validate(&mut errors);
        self.mail.validate(&mut errors);
        self.validate_logging(&mut errors);
        self.validate_production(&mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfError::Many(errors))
        }
    }

    fn validate_tasks(&self, errors: &mut Vec<ConfError>) {
        if let Err(error) = self.tasks.validate() {
            errors.push(ConfError::InvalidValue {
                field: "tasks".into(),
                reason: error.to_string(),
                expected: Some("valid task lanes, batching, and polling limits".into()),
            });
        }
    }

    /// Adds accumulated cache configuration failures to the site validation report.
    fn validate_cache(&self, errors: &mut Vec<ConfError>) {
        if let Err(error) = self.cache.validate() {
            errors.push(ConfError::InvalidValue {
                field: "cache".into(),
                reason: error.to_string(),
                expected: Some("unique configured providers and one valid default provider".into()),
            });
        }
    }

    fn validate_logging(&self, errors: &mut Vec<ConfError>) {
        if let Err(error) = self.logging.validate() {
            errors.push(ConfError::InvalidValue {
                field: "logging".into(),
                reason: error.to_string(),
                expected: Some("valid logging rules and sinks".into()),
            });
        }
        if let Err(error) = self
            .logging
            .validate_mail_admins(self.log_init, self.mail.enabled)
        {
            errors.push(ConfError::InvalidValue {
                field: "logging.mail_admins".into(),
                reason: error.to_string(),
                expected: Some("enabled logging and outbound mail".into()),
            });
        }
    }

    fn validate_required(&self, errors: &mut Vec<ConfError>) {
        if self.secret_key.is_empty() {
            errors.push(ConfError::RequiredField {
                field: "secret_key".into(),
                reason: "cannot be empty".into(),
            });
        } else if self.secret_key.len() < self.auth.minimum_secret_length() {
            let minimum = self.auth.minimum_secret_length();
            errors.push(ConfError::InvalidValue {
                field: "secret_key".into(),
                reason: format!("must be at least {minimum} characters for authentication"),
                expected: Some(format!("{minimum} or more characters")),
            });
        }
        if self.deployment == DeploymentMode::Production {
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

    fn validate_auth(&self, errors: &mut Vec<ConfError>) {
        if let Err(error) = self.auth.validate_provider_names() {
            errors.push(ConfError::InvalidValue {
                field: "auth.providers".into(),
                reason: error.to_string(),
                expected: Some("an application provider ID without the 'vyuh-' prefix".into()),
            });
        }
    }

    fn validate_static_url(&self, errors: &mut Vec<ConfError>) {
        if let Err(error) = crate::assets::AssetUrls::parse(&self.static_url) {
            errors.push(ConfError::InvalidValue {
                field: "static_url".into(),
                reason: error.to_string(),
                expected: Some("a root-relative path or absolute HTTP(S) URL".into()),
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

    /// Validates cookie transport and browser isolation settings in production.
    fn validate_production_cookies(&self, errors: &mut Vec<ConfError>) {
        if let Err(error) = self.auth.validate_production() {
            errors.push(ConfError::InvalidValue {
                field: "auth.providers".into(),
                reason: error.to_string(),
                expected: Some("secure credential cookies with usable CSRF policy".into()),
            });
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
            "secret_key_fallbacks" => {
                conf.secret_key_fallbacks = parse_secret_key_fallbacks(&value)?;
            }
            "host" => conf.host = value,
            "static_url" => conf.static_url = value,
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
            "smtp_url" => {
                conf.mail = MailConf::from_url(&value)?;
            }
            "mail_enabled" => {
                conf.mail.enabled = value.parse::<bool>().map_err(|_| {
                    ConfError::Other(format!(
                        "MAIL_ENABLED must be 'true' or 'false', got: {value}"
                    ))
                })?;
            }
            "mail_host" => conf.mail.host = value,
            "mail_port" => {
                conf.mail.port = value.parse::<u16>().map_err(|_| {
                    ConfError::Other(format!("MAIL_PORT must be a valid u16, got: {value}"))
                })?;
            }
            "mail_username" => conf.mail.username = Some(value),
            "mail_password" => conf.mail.password = Some(value),
            "mail_sender" => conf.mail.sender = Some(value),
            "mail_tls" => {
                conf.mail.tls =
                    serde_json::from_value(serde_json::Value::String(value)).map_err(|_| {
                        ConfError::Other("MAIL_TLS must be 'start_tls', 'tls', or 'none'".into())
                    })?;
            }
            "mail_timeout_seconds" => {
                conf.mail.timeout_seconds = value.parse::<u64>().map_err(|_| {
                    ConfError::Other(format!(
                        "MAIL_TIMEOUT_SECONDS must be a valid u64, got: {value}"
                    ))
                })?;
            }
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

/// Parses Django-style secret fallbacks from JSON or a comma-separated env value.
fn parse_secret_key_fallbacks(value: &str) -> Result<Vec<String>, ConfError> {
    if value.trim_start().starts_with('[') {
        return serde_json::from_str(value).map_err(|_| {
            ConfError::Other(
                "SECRET_KEY_FALLBACKS must be a JSON string array or comma-separated list".into(),
            )
        });
    }
    Ok(value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect())
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

#[cfg(test)]
mod auth_secret_tests {
    use super::{SiteConf, parse_secret_key_fallbacks};
    use crate::logging::{LogRule, LogSink, LoggingConf, MailAdmins};

    /// Verifies env fallbacks accept both conventional comma and JSON list forms.
    #[test]
    fn parses_secret_key_fallback_env_forms() {
        assert_eq!(
            parse_secret_key_fallbacks("old-one, old-two").ok(),
            Some(vec!["old-one".into(), "old-two".into()])
        );
        assert_eq!(
            parse_secret_key_fallbacks(r#"["old-one","old-two"]"#).ok(),
            Some(vec!["old-one".into(), "old-two".into()])
        );
    }

    /// Verifies even debug-only raw configuration serialization cannot expose secret rings.
    #[test]
    fn serialized_site_configuration_omits_auth_secrets() -> Result<(), serde_json::Error> {
        let value = serde_json::to_string(
            &SiteConf::default()
                .secret_key("active-production-secret-value")
                .secret_key_fallbacks(["retired-production-secret-value"]),
        )?;
        assert!(!value.contains("active-production-secret-value"));
        assert!(!value.contains("retired-production-secret-value"));
        assert!(!value.contains("secret_key"));
        Ok(())
    }

    /// Verifies a configured administrator sink cannot be disabled by inert logging setup.
    #[test]
    fn mail_admins_requires_site_logging() {
        let conf = SiteConf::default().log_init(false).logging(LoggingConf {
            env_prefix: None,
            rules: vec![LogRule {
                name: "ADMINS".into(),
                sink: LogSink::mail_admins(MailAdmins::new(["ops@example.com"])),
                default_filter: "error".into(),
            }],
        });
        assert!(conf.validate().is_err());
    }
}
