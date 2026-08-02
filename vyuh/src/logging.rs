use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Error)]
pub enum LoggingError {
    #[error("Failed to create log directory: {0}")]
    DirCreation(#[from] std::io::Error),

    #[error(
        "Failed to set global subscriber: {0}\nReason: Tracing subscriber already initialized. Ensure init_tracing() is only called once per process."
    )]
    SubscriberInit(#[from] tracing_subscriber::util::TryInitError),

    #[error(
        "Invalid log rule name '{name}': {reason}\nExpected: Name must start with a letter (A-Z or a-z), followed by letters, digits, or underscores. Max length 48 characters."
    )]
    InvalidRuleName { name: String, reason: String },

    #[error(
        "Invalid env prefix '{prefix}': {reason}\nExpected: Must start with uppercase letter (A-Z), followed by uppercase letters, digits, or underscores. Max length 48 characters."
    )]
    InvalidEnvPrefix { prefix: String, reason: String },

    #[error(
        "Duplicate log rule name '{0}'\nReason: Each rule must have a unique name as it's used for environment variables and file prefixes."
    )]
    DuplicateRuleName(String),

    #[error(
        "Invalid log filter '{value}': {source}\nExpected: Valid tracing filter directive (e.g., 'info', 'my_crate=debug', 'warn,my_crate::module=trace') or 'off' to disable."
    )]
    FilterParse {
        value: String,
        source: tracing_subscriber::filter::ParseError,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Rotation {
    Daily,
    Hourly,
    Minutely,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "TRACE" => Some(LogLevel::Trace),
            "DEBUG" => Some(LogLevel::Debug),
            "INFO" => Some(LogLevel::Info),
            "WARN" => Some(LogLevel::Warn),
            "ERROR" => Some(LogLevel::Error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogSink {
    File { dir: String, rotation: Rotation },
    Stdout { pretty: bool },
    Stderr { pretty: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRule {
    /// Rule name used for environment variable lookup and file prefix.
    ///
    /// Validation rules:
    /// - Must start with an ASCII letter (A-Z or a-z)
    /// - Followed by letters, digits (0-9), or underscores (_)
    /// - Length: 1-48 characters
    /// - Pattern: `^[A-Za-z][A-Za-z0-9_]{0,47}$`
    ///
    /// The name is used as-is for file prefixes but uppercased for environment variables.
    ///
    /// Examples:
    /// - Valid: `APP`, `ServerLog`, `api_trace`, `MyApp`, `db_queries`
    /// - Invalid: `1app` (starts with digit), `app-log` (hyphen), `_app` (starts with underscore)
    ///
    /// Environment variable format: `<ENV_PREFIX>_<UPPERCASE_NAME>`
    /// For example, `MyApp` with prefix `RUST_LOG` creates `RUST_LOG_MYAPP`
    pub name: String,
    pub sink: LogSink,
    pub default_filter: String, // off means disabled.
}

impl LogRule {
    pub fn validate(&self) -> Result<(), LoggingError> {
        // ^[A-Za-z][A-Za-z0-9_]{0,47}$
        let s = self.name.as_str();
        if s.is_empty() {
            return Err(LoggingError::InvalidRuleName {
                name: self.name.clone(),
                reason: "Name cannot be empty".to_string(),
            });
        }
        if s.len() > 48 {
            return Err(LoggingError::InvalidRuleName {
                name: self.name.clone(),
                reason: format!("Name too long ({} characters, max 48)", s.len()),
            });
        }
        let mut chars = s.chars();
        let Some(first) = chars.next() else {
            return Err(LoggingError::InvalidRuleName {
                name: self.name.clone(),
                reason: "Name cannot be empty".to_string(),
            });
        };
        if !first.is_ascii_alphabetic() {
            return Err(LoggingError::InvalidRuleName {
                name: self.name.clone(),
                reason: format!("Must start with a letter (A-Z or a-z), found '{}'", first),
            });
        }
        if let Some(invalid_char) = chars.find(|c| !(c.is_ascii_alphanumeric() || *c == '_')) {
            return Err(LoggingError::InvalidRuleName {
                name: self.name.clone(),
                reason: format!(
                    "Invalid character '{}'. Only letters, digits, and underscores allowed",
                    invalid_char
                ),
            });
        }

        let default_filter = self.default_filter.trim();

        // Validate default_filter parses (allow "off" but reject invalid directives)
        if !is_off_string(default_filter) {
            EnvFilter::try_new(default_filter).map_err(|e| LoggingError::FilterParse {
                value: default_filter.to_string(),
                source: e,
            })?;
        }

        Ok(())
    }

    /// Returns filter from env var or default. Returns None if disabled.
    fn build_filter(
        &self,
        env_prefix: &str,
        global_filter: Option<&String>,
    ) -> Result<Option<EnvFilter>, LoggingError> {
        let rule_env_var = format!("{}_{}", env_prefix, &self.name.to_ascii_uppercase());

        let filter_str = if let Some(rule_filter) = std::env::var(&rule_env_var).ok() {
            rule_filter
        } else {
            if let Some(gfilter) = global_filter {
                gfilter.clone()
            } else {
                self.default_filter.clone()
            }
        }
        .trim()
        .to_string();

        if is_off_string(&filter_str) {
            return Ok(None);
        }

        EnvFilter::try_new(&filter_str)
            .map(Some)
            .map_err(|e| LoggingError::FilterParse {
                value: filter_str,
                source: e,
            })
    }
}

fn is_off_string(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "off" | "0" | "false" | "no"
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Configuration for VYUH tracing/logging.
///
/// Env resolution (PREFIX defaults to `RUST_LOG`):
/// 1) `<PREFIX>_<UPPERCASE_RULE_NAME>`
/// 2) `<PREFIX>` (global fallback)
/// 3) rule `default_filter`
///
/// Note: Rule names are automatically uppercased when building environment variable names.
/// For example, a rule named "MyApp" with prefix "RUST_LOG" will check "RUST_LOG_MYAPP".
///
/// If the resolved env value (after trimming) is one of:
/// `off`, `0`, `false`, `no` (case-insensitive),
/// the rule is disabled (no layer is installed).
///
/// Otherwise the value must be a valid `tracing_subscriber::EnvFilter` directive string.
pub struct LoggingConf {
    /// Optional prefix for environment variables (should be uppercase)
    /// Defaults to RUST_LOG if not specified
    pub env_prefix: Option<String>,
    /// Logging rules where each rule defines a sink and default filter
    pub rules: Vec<LogRule>,
}

impl Default for LoggingConf {
    fn default() -> Self {
        if cfg!(debug_assertions) {
            return Self {
                env_prefix: None,
                rules: vec![LogRule {
                    name: "VYUH".into(),
                    sink: LogSink::Stdout { pretty: true },
                    default_filter: "debug,sqlx=error".into(),
                }],
            };
        }
        Self {
            env_prefix: None,
            rules: vec![],
        }
    }
}

impl LoggingConf {
    pub fn resolved_env_prefix(&self) -> &str {
        self.env_prefix.as_deref().unwrap_or("RUST_LOG")
    }

    pub fn validate(&self) -> Result<(), LoggingError> {
        // Validate env_prefix if present
        if let Some(prefix) = &self.env_prefix {
            let s = prefix.as_str();
            if s.is_empty() {
                return Err(LoggingError::InvalidEnvPrefix {
                    prefix: prefix.clone(),
                    reason: "Prefix cannot be empty".to_string(),
                });
            }
            if s.len() > 48 {
                return Err(LoggingError::InvalidEnvPrefix {
                    prefix: prefix.clone(),
                    reason: format!("Prefix too long ({} characters, max 48)", s.len()),
                });
            }
            let mut chars = s.chars();
            let Some(first) = chars.next() else {
                return Err(LoggingError::InvalidEnvPrefix {
                    prefix: prefix.clone(),
                    reason: "Prefix cannot be empty".to_string(),
                });
            };
            if !first.is_ascii_uppercase() {
                return Err(LoggingError::InvalidEnvPrefix {
                    prefix: prefix.clone(),
                    reason: format!("Must start with uppercase letter (A-Z), found '{}'", first),
                });
            }
            if let Some(invalid_char) =
                chars.find(|c| !(c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_'))
            {
                return Err(LoggingError::InvalidEnvPrefix {
                    prefix: prefix.clone(),
                    reason: format!(
                        "Invalid character '{}'. Only uppercase letters, digits, and underscores allowed (env vars should be uppercase)",
                        invalid_char
                    ),
                });
            }
        }

        let mut seen = std::collections::HashSet::new();
        for r in &self.rules {
            r.validate()?;
            if !seen.insert(r.name.clone()) {
                return Err(LoggingError::DuplicateRuleName(r.name.clone()));
            }
        }
        Ok(())
    }
}

/// Returned guard must be kept alive until shutdown
pub struct LoggingGuard {
    _file_guards: Vec<WorkerGuard>,
}

impl LoggingGuard {
    pub(crate) fn noop() -> Self {
        Self {
            _file_guards: Vec::new(),
        }
    }
}

fn resolve_log_dir(project_dir: &Path, dir: &str) -> PathBuf {
    let path = Path::new(dir);
    if path.is_relative() {
        project_dir.join(path)
    } else {
        path.to_path_buf()
    }
}

pub(crate) fn init_tracing(
    project_dir: &Path,
    conf: &LoggingConf,
) -> Result<LoggingGuard, LoggingError> {
    if conf.rules.is_empty() {
        return Ok(LoggingGuard {
            _file_guards: vec![],
        });
    }

    conf.validate()?;

    let env_prefix = conf.resolved_env_prefix();
    let global_filter = std::env::var(env_prefix).ok();

    let mut guards = Vec::new();
    type BoxedLayer = Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>;

    let mut layers: Vec<BoxedLayer> = Vec::new();

    for rule in &conf.rules {
        let Some(filter) = rule.build_filter(env_prefix, global_filter.as_ref())? else {
            continue;
        };

        match &rule.sink {
            LogSink::File { dir, rotation } => {
                let log_dir = resolve_log_dir(project_dir, dir);
                fs::create_dir_all(&log_dir)?;

                let appender = match rotation {
                    Rotation::Daily => tracing_appender::rolling::daily(&log_dir, &rule.name),
                    Rotation::Hourly => tracing_appender::rolling::hourly(&log_dir, &rule.name),
                    Rotation::Minutely => tracing_appender::rolling::minutely(&log_dir, &rule.name),
                };

                let (writer, guard) = tracing_appender::non_blocking(appender);
                guards.push(guard);

                let layer = fmt::layer()
                    .json()
                    .with_writer(writer)
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_target(true)
                    .with_file(true)
                    .with_line_number(true)
                    .with_thread_ids(true)
                    .with_thread_names(true)
                    .with_timer(fmt::time::UtcTime::rfc_3339());
                layers.push(layer.with_filter(filter).boxed());
            }
            LogSink::Stdout { pretty } => {
                if *pretty {
                    let layer = fmt::layer()
                        .pretty()
                        .with_ansi(true)
                        .with_writer(std::io::stdout);
                    layers.push(layer.with_filter(filter).boxed());
                } else {
                    let layer = fmt::layer()
                        .json()
                        .with_writer(std::io::stdout)
                        .with_current_span(true)
                        .with_span_list(true)
                        .with_target(true)
                        .with_timer(fmt::time::UtcTime::rfc_3339());
                    layers.push(layer.with_filter(filter).boxed());
                }
            }
            LogSink::Stderr { pretty } => {
                if *pretty {
                    let layer = fmt::layer()
                        .pretty()
                        .with_ansi(true)
                        .with_writer(std::io::stderr);
                    layers.push(layer.with_filter(filter).boxed());
                } else {
                    let layer = fmt::layer()
                        .json()
                        .with_writer(std::io::stderr)
                        .with_current_span(true)
                        .with_span_list(true)
                        .with_target(true)
                        .with_timer(fmt::time::UtcTime::rfc_3339());
                    layers.push(layer.with_filter(filter).boxed());
                }
            }
        }
    }

    if layers.is_empty() {
        return Ok(LoggingGuard {
            _file_guards: vec![],
        });
    }

    tracing_subscriber::registry().with(layers).try_init()?;

    Ok(LoggingGuard {
        _file_guards: guards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_off_string() {
        assert!(is_off_string("off"));
        assert!(is_off_string("OFF"));
        assert!(is_off_string("Off"));
        assert!(is_off_string("  off  "));
        assert!(is_off_string("0"));
        assert!(is_off_string("false"));
        assert!(is_off_string("FALSE"));
        assert!(is_off_string("no"));
        assert!(is_off_string("NO"));

        assert!(!is_off_string(""));
        assert!(!is_off_string("on"));
        assert!(!is_off_string("info"));
        assert!(!is_off_string("disabled"));
    }

    #[test]
    fn test_resolve_log_dir_relative() {
        let project = Path::new("/project");
        let result = resolve_log_dir(project, "logs");
        assert_eq!(result, PathBuf::from("/project/logs"));

        let result = resolve_log_dir(project, "./logs");
        assert_eq!(result, PathBuf::from("/project/./logs"));
    }

    #[test]
    fn test_resolve_log_dir_absolute() {
        let project = Path::new("/project");
        let result = resolve_log_dir(project, "/var/log");
        assert_eq!(result, PathBuf::from("/var/log"));
    }

    #[test]
    fn test_log_rule_name_validation_valid() {
        let rule = LogRule {
            name: "APP".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        assert!(rule.validate().is_ok());

        let rule = LogRule {
            name: "SERVER_LOG".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        assert!(rule.validate().is_ok());

        // Mixed case is now allowed
        let rule = LogRule {
            name: "MyApp".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        assert!(rule.validate().is_ok());

        let rule = LogRule {
            name: "serverLog".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        assert!(rule.validate().is_ok());

        let rule = LogRule {
            name: "api_trace_2".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "debug".into(),
        };
        assert!(rule.validate().is_ok());

        let rule = LogRule {
            name: "API_TRACE_2".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "trace".into(),
        };
        assert!(rule.validate().is_ok());

        let rule = LogRule {
            name: "A".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "off".into(),
        };
        assert!(rule.validate().is_ok());
    }

    #[test]
    fn test_log_rule_name_validation_invalid() {
        // Empty name
        let rule = LogRule {
            name: "".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        let err = rule.validate().unwrap_err();
        assert!(matches!(err, LoggingError::InvalidRuleName { .. }));
        assert!(err.to_string().contains("empty"));

        // Starts with digit
        let rule = LogRule {
            name: "1APP".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        let err = rule.validate().unwrap_err();
        assert!(matches!(err, LoggingError::InvalidRuleName { .. }));

        // Contains hyphen
        let rule = LogRule {
            name: "APP-LOG".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        let err = rule.validate().unwrap_err();
        assert!(matches!(err, LoggingError::InvalidRuleName { .. }));
        assert!(err.to_string().contains("'-'"));

        // Too long (>48 chars)
        let rule = LogRule {
            name: "A".repeat(49),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };
        let err = rule.validate().unwrap_err();
        assert!(matches!(err, LoggingError::InvalidRuleName { .. }));
        assert!(err.to_string().contains("too long"));
    }

    #[test]
    fn test_log_rule_filter_validation() {
        // Valid filter
        let rule = LogRule {
            name: "APP".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info,my_crate=debug".into(),
        };
        assert!(rule.validate().is_ok());

        // "off" is valid
        let rule = LogRule {
            name: "APP".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "off".into(),
        };
        assert!(rule.validate().is_ok());

        // Invalid filter syntax
        let rule = LogRule {
            name: "APP".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "invalid[filter".into(),
        };
        let err = rule.validate().unwrap_err();
        assert!(matches!(err, LoggingError::FilterParse { .. }));
    }

    #[test]
    fn test_logging_conf_env_prefix_validation_valid() {
        let conf = LoggingConf {
            env_prefix: Some("MY_APP".into()),
            rules: vec![],
        };
        assert!(conf.validate().is_ok());

        let conf = LoggingConf {
            env_prefix: Some("LOG_2".into()),
            rules: vec![],
        };
        assert!(conf.validate().is_ok());

        let conf = LoggingConf {
            env_prefix: None,
            rules: vec![],
        };
        assert!(conf.validate().is_ok());
    }

    #[test]
    fn test_logging_conf_env_prefix_validation_invalid() {
        // Empty prefix
        let conf = LoggingConf {
            env_prefix: Some("".into()),
            rules: vec![],
        };
        let err = conf.validate().unwrap_err();
        assert!(matches!(err, LoggingError::InvalidEnvPrefix { .. }));

        // Lowercase
        let conf = LoggingConf {
            env_prefix: Some("my_app".into()),
            rules: vec![],
        };
        let err = conf.validate().unwrap_err();
        assert!(matches!(err, LoggingError::InvalidEnvPrefix { .. }));

        // Too long
        let conf = LoggingConf {
            env_prefix: Some("A".repeat(49)),
            rules: vec![],
        };
        let err = conf.validate().unwrap_err();
        assert!(matches!(err, LoggingError::InvalidEnvPrefix { .. }));
    }

    #[test]
    fn test_logging_conf_duplicate_rule_names() {
        let conf = LoggingConf {
            env_prefix: None,
            rules: vec![
                LogRule {
                    name: "APP".into(),
                    sink: LogSink::Stdout { pretty: false },
                    default_filter: "info".into(),
                },
                LogRule {
                    name: "APP".into(),
                    sink: LogSink::Stderr { pretty: false },
                    default_filter: "debug".into(),
                },
            ],
        };
        let err = conf.validate().unwrap_err();
        assert!(matches!(err, LoggingError::DuplicateRuleName(_)));
        assert!(err.to_string().contains("APP"));
    }

    #[test]
    fn test_logging_conf_resolved_env_prefix() {
        let conf = LoggingConf {
            env_prefix: Some("MY_APP".into()),
            rules: vec![],
        };
        assert_eq!(conf.resolved_env_prefix(), "MY_APP");

        let conf = LoggingConf {
            env_prefix: None,
            rules: vec![],
        };
        assert_eq!(conf.resolved_env_prefix(), "RUST_LOG");
    }

    #[test]
    fn test_log_rule_mixed_case_env_var() {
        let rule = LogRule {
            name: "MyApp".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };

        // Test that mixed case name is uppercased for env var
        unsafe {
            std::env::set_var("TEST_PREFIX_MYAPP", "debug");
        }
        let result = rule.build_filter("TEST_PREFIX", None);
        unsafe {
            std::env::remove_var("TEST_PREFIX_MYAPP");
        }

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_log_rule_build_filter_with_env_var() {
        // Set up test env var
        let rule = LogRule {
            name: "TEST_RULE".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };

        // Test with rule-specific env var
        unsafe {
            std::env::set_var("TEST_PREFIX_TEST_RULE", "debug");
        }
        let result = rule.build_filter("TEST_PREFIX", None);
        unsafe {
            std::env::remove_var("TEST_PREFIX_TEST_RULE");
        }

        assert!(result.is_ok());
        let filter = result.unwrap();
        assert!(filter.is_some());
    }

    #[test]
    fn test_log_rule_build_filter_with_global() {
        let rule = LogRule {
            name: "TEST_RULE".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "info".into(),
        };

        // No rule-specific var, uses global
        let result = rule.build_filter("TEST_PREFIX", Some(&"warn".to_string()));
        assert!(result.is_ok());
        let filter = result.unwrap();
        assert!(filter.is_some());
    }

    #[test]
    fn test_log_rule_build_filter_default() {
        let rule = LogRule {
            name: "TEST_RULE".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "error".into(),
        };

        // No env vars, uses default
        let result = rule.build_filter("TEST_PREFIX", None);
        assert!(result.is_ok());
        let filter = result.unwrap();
        assert!(filter.is_some());
    }

    #[test]
    fn test_log_rule_build_filter_off() {
        // Use unique prefix to avoid env var conflicts with other tests
        unsafe {
            std::env::remove_var("OFF_TEST_PREFIX_TEST_RULE");
            std::env::remove_var("OFF_TEST_PREFIX_LOG");
        }

        let rule = LogRule {
            name: "TEST_RULE".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "off".into(),
        };

        let result = rule.build_filter("OFF_TEST_PREFIX", None);
        assert!(result.is_ok());
        let filter = result.unwrap();
        assert!(filter.is_none());

        // Test with "0"
        let rule = LogRule {
            name: "TEST_RULE".into(),
            sink: LogSink::Stdout { pretty: false },
            default_filter: "0".into(),
        };
        let result = rule.build_filter("OFF_TEST_PREFIX", None);
        assert!(result.is_ok());
        let filter = result.unwrap();
        assert!(filter.is_none());
    }

    #[test]
    fn test_log_level_from_str() {
        assert!(matches!(LogLevel::from_str("TRACE"), Some(LogLevel::Trace)));
        assert!(matches!(LogLevel::from_str("trace"), Some(LogLevel::Trace)));
        assert!(matches!(LogLevel::from_str("DEBUG"), Some(LogLevel::Debug)));
        assert!(matches!(LogLevel::from_str("INFO"), Some(LogLevel::Info)));
        assert!(matches!(LogLevel::from_str("WARN"), Some(LogLevel::Warn)));
        assert!(matches!(LogLevel::from_str("ERROR"), Some(LogLevel::Error)));
        assert!(LogLevel::from_str("invalid").is_none());
    }

    /// Verifies debug builds keep Vyuh diagnostics while limiting SQLx to errors.
    #[test]
    fn test_logging_conf_default_in_debug_mode() {
        let conf = LoggingConf::default();

        if cfg!(debug_assertions) {
            assert_eq!(conf.rules.len(), 1);
            assert_eq!(conf.rules[0].name, "VYUH");
            assert_eq!(conf.rules[0].default_filter, "debug,sqlx=error");
            assert!(matches!(
                conf.rules[0].sink,
                LogSink::Stdout { pretty: true }
            ));
        } else {
            assert_eq!(conf.rules.len(), 0);
        }
    }

    #[test]
    fn test_error_messages_contain_guidance() {
        // InvalidRuleName should explain what's expected
        let err = LoggingError::InvalidRuleName {
            name: "bad".into(),
            reason: "test".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Expected:"));
        assert!(msg.contains("letter") || msg.contains("uppercase"));

        // InvalidEnvPrefix should explain format
        let err = LoggingError::InvalidEnvPrefix {
            prefix: "bad".into(),
            reason: "test".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Expected:"));

        // FilterParse should explain valid formats
        let filter_err = EnvFilter::try_new("invalid[").unwrap_err();
        let err = LoggingError::FilterParse {
            value: "invalid[".into(),
            source: filter_err,
        };
        let msg = err.to_string();
        assert!(msg.contains("Expected:"));
        assert!(msg.contains("off"));

        // DuplicateRuleName should explain why it matters
        let err = LoggingError::DuplicateRuleName("APP".into());
        let msg = err.to_string();
        assert!(msg.contains("unique"));
        assert!(msg.contains("environment"));
    }

    #[test]
    fn test_rotation_variants() {
        // Ensure all rotation variants exist and can be created
        let _ = Rotation::Daily;
        let _ = Rotation::Hourly;
        let _ = Rotation::Minutely;
    }

    #[test]
    fn test_log_sink_variants() {
        let _ = LogSink::File {
            dir: "logs".into(),
            rotation: Rotation::Daily,
        };
        let _ = LogSink::Stdout { pretty: true };
        let _ = LogSink::Stderr { pretty: false };
    }
}
