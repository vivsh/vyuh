use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::conf::ConfError;

use super::ConsoleAccess;

/// Source-owned configuration for the built-in operational console.
#[derive(Clone, Serialize, Deserialize)]
pub struct ConsoleConf {
    pub enabled: bool,
    pub path: String,
    pub page_size_default: usize,
    pub page_size_max: usize,
    pub status_cache_ttl_seconds: u64,
    #[serde(skip)]
    access: Option<Arc<dyn ConsoleAccess>>,
}

impl fmt::Debug for ConsoleConf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsoleConf")
            .field("enabled", &self.enabled)
            .field("path", &self.path)
            .field("page_size_default", &self.page_size_default)
            .field("page_size_max", &self.page_size_max)
            .field("status_cache_ttl_seconds", &self.status_cache_ttl_seconds)
            .field("access", &self.access.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl Default for ConsoleConf {
    fn default() -> Self {
        Self {
            enabled: cfg!(debug_assertions),
            path: "/console".to_string(),
            page_size_default: 50,
            page_size_max: 250,
            status_cache_ttl_seconds: 5,
            access: None,
        }
    }
}

impl ConsoleConf {
    /// Returns the console configuration used by the production site profile.
    pub fn production() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Enables or disables console route registration.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Sets the console route prefix.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Configures the application policy used to authorize console requests.
    pub fn access(mut self, policy: impl ConsoleAccess) -> Self {
        self.access = Some(Arc::new(policy));
        self
    }

    /// Sets the default number of rows shown by console list pages.
    pub fn page_size_default(mut self, size: usize) -> Self {
        self.page_size_default = size;
        self
    }

    /// Sets the maximum number of rows accepted by console list pages.
    pub fn page_size_max(mut self, size: usize) -> Self {
        self.page_size_max = size;
        self
    }

    /// Sets the in-process duration used to cache console status snapshots.
    pub fn status_cache_ttl_seconds(mut self, seconds: u64) -> Self {
        self.status_cache_ttl_seconds = seconds;
        self
    }

    /// Returns whether this debug-only console accepts requests without authentication.
    pub(crate) fn development_access(&self) -> bool {
        self.enabled && self.access.is_none() && cfg!(debug_assertions)
    }

    /// Returns the application-owned console authorization policy.
    pub(crate) fn access_policy(&self) -> Option<&Arc<dyn ConsoleAccess>> {
        self.access.as_ref()
    }

    /// Returns the safe access mode shown by console diagnostics.
    pub(crate) fn access_mode(&self) -> &'static str {
        if self.development_access() {
            "development"
        } else {
            "policy"
        }
    }

    pub(crate) fn validate(&self, errors: &mut Vec<ConfError>) {
        if !self.enabled {
            return;
        }
        if let Err(reason) = crate::bundles::validate_route_prefix(&self.path) {
            errors.push(ConfError::InvalidValue {
                field: "console.path".into(),
                reason,
                expected: Some("a path such as /console".into()),
            });
        }
        if !cfg!(debug_assertions) && self.access.is_none() {
            errors.push(ConfError::InvalidValue {
                field: "console.access".into(),
                reason: "an enabled release console requires an access policy".into(),
                expected: Some("ConsoleConf::access(policy)".into()),
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
