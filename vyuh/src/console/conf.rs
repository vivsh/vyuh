use serde::{Deserialize, Serialize};

use crate::conf::ConfError;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleBootstrapMode {
    LocalOnly,
    Always,
    Disabled,
}

impl Default for ConsoleBootstrapMode {
    fn default() -> Self {
        Self::LocalOnly
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleConf {
    pub enabled: bool,
    pub path: String,
    pub bootstrap_token_ttl_seconds: u64,
    pub session_ttl_seconds: u64,
    pub print_bootstrap_url: ConsoleBootstrapMode,
    pub cookie_name: String,
    /// Whether the console session cookie is restricted to HTTPS.
    #[serde(default)]
    pub secure_cookie: bool,
    pub page_size_default: usize,
    pub page_size_max: usize,
    pub status_cache_ttl_seconds: u64,
}

impl Default for ConsoleConf {
    fn default() -> Self {
        Self {
            enabled: cfg!(debug_assertions),
            path: "/console".to_string(),
            bootstrap_token_ttl_seconds: 300,
            session_ttl_seconds: 28_800,
            print_bootstrap_url: ConsoleBootstrapMode::LocalOnly,
            cookie_name: "vyuh_console".to_string(),
            secure_cookie: false,
            page_size_default: 50,
            page_size_max: 250,
            status_cache_ttl_seconds: 5,
        }
    }
}

impl ConsoleConf {
    /// Returns the console configuration used by the production site profile.
    pub fn production() -> Self {
        Self {
            enabled: false,
            secure_cookie: true,
            ..Self::default()
        }
    }
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    pub fn bootstrap_token_ttl_seconds(mut self, seconds: u64) -> Self {
        self.bootstrap_token_ttl_seconds = seconds;
        self
    }

    pub fn session_ttl_seconds(mut self, seconds: u64) -> Self {
        self.session_ttl_seconds = seconds;
        self
    }

    pub fn print_bootstrap_url(mut self, mode: ConsoleBootstrapMode) -> Self {
        self.print_bootstrap_url = mode;
        self
    }

    pub fn cookie_name(mut self, name: impl Into<String>) -> Self {
        self.cookie_name = name.into();
        self
    }

    /// Configures whether the console session cookie requires HTTPS.
    pub fn secure_cookie(mut self, secure: bool) -> Self {
        self.secure_cookie = secure;
        self
    }

    pub fn page_size_default(mut self, size: usize) -> Self {
        self.page_size_default = size;
        self
    }

    pub fn page_size_max(mut self, size: usize) -> Self {
        self.page_size_max = size;
        self
    }

    pub fn status_cache_ttl_seconds(mut self, seconds: u64) -> Self {
        self.status_cache_ttl_seconds = seconds;
        self
    }

    pub(crate) fn validate(&self, errors: &mut Vec<ConfError>) {
        if !self.enabled {
            return;
        }
        if !self.path.starts_with('/') || self.path.len() <= 1 {
            errors.push(ConfError::InvalidValue {
                field: "console.path".into(),
                reason: "must be an absolute non-root path".into(),
                expected: Some("a path such as /console".into()),
            });
        }
        if self.path.ends_with('/') {
            errors.push(ConfError::InvalidValue {
                field: "console.path".into(),
                reason: "must not end with '/'".into(),
                expected: Some("a path such as /console".into()),
            });
        }
        if self.bootstrap_token_ttl_seconds == 0 {
            errors.push(ConfError::InvalidValue {
                field: "console.bootstrap_token_ttl_seconds".into(),
                reason: "must be greater than zero".into(),
                expected: Some("a positive duration in seconds".into()),
            });
        }
        if self.session_ttl_seconds == 0 {
            errors.push(ConfError::InvalidValue {
                field: "console.session_ttl_seconds".into(),
                reason: "must be greater than zero".into(),
                expected: Some("a positive duration in seconds".into()),
            });
        }
        if self.cookie_name.trim().is_empty() {
            errors.push(ConfError::InvalidValue {
                field: "console.cookie_name".into(),
                reason: "must not be empty".into(),
                expected: Some("a cookie name".into()),
            });
        }
        if self.page_size_default == 0 || self.page_size_max == 0 {
            errors.push(ConfError::InvalidValue {
                field: "console.page_size".into(),
                reason: "default and max page sizes must be greater than zero".into(),
                expected: Some("positive page sizes".into()),
            });
        }
        if self.page_size_default > self.page_size_max {
            errors.push(ConfError::InvalidValue {
                field: "console.page_size_default".into(),
                reason: "must not exceed page_size_max".into(),
                expected: Some(format!("at most {}", self.page_size_max)),
            });
        }
        if self.status_cache_ttl_seconds == 0 {
            errors.push(ConfError::InvalidValue {
                field: "console.status_cache_ttl_seconds".into(),
                reason: "must be greater than zero".into(),
                expected: Some("a positive duration in seconds".into()),
            });
        }
    }
}
