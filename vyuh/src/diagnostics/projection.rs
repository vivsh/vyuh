use crate::{SiteError, auth::AuthBuildError, conf::ConfError};

use super::{BuildDiagnostic as D, registrations, subsystems};

impl SiteError {
    /// Derives actionable diagnostics without consuming this error or its sources.
    ///
    /// Existing error lists are flattened in validation order. Causes are
    /// sanitized and bounded in both debug and release builds.
    pub fn diagnostics(&self) -> Vec<D> {
        match self {
            Self::ConfError(error) => configuration(error),
            Self::BundleError(error) => registrations::bundle(error),
            Self::AuthBuildError(error) => vec![auth_build(error)],
            Self::ServiceError(error) => vec![subsystems::service(error)],
            Self::TaskRuntimeError(error) => vec![subsystems::task(error)],
            Self::LoggingError(error) => vec![subsystems::logging(error)],
            Self::EmitterError(error) => vec![subsystems::emitter(error)],
            Self::SchemaAsset(error) => vec![registrations::schema(error)],
            Self::DatabaseError(error) => vec![D::new("Database initialization failed.",
                "Check SiteConf.database, the selected backend feature, connectivity, and database permissions.").causes(error)],
            Self::TemplateError(error) => vec![D::new("Template initialization failed.",
                "Check the named template and embedded template assets; correct its path or syntax.").causes(error)],
            Self::SignalError(error) => vec![D::new("Signal registration failed.",
                "Check the registered signal handler's input type and reported cause.").causes(error)],
            Self::IOError(error) => vec![D::new("Site initialization encountered an I/O failure.",
                "Check the reported operating-system error and access permissions.").causes(error)],
            Self::ServeError(error) => vec![D::new("HTTP serving failed.",
                "Check the reported transport error.").causes(error)],
            Self::CommandError(error) => vec![D::new(error.to_string(), "Check the command's reported error.")],
            other => vec![site_text(other)],
        }
    }
}

/// Selects preparation versus network-provider initialization context.
fn auth_build(error: &AuthBuildError) -> D {
    match error {
        AuthBuildError::Configuration(source) => subsystems::auth(source),
        AuthBuildError::ProviderInitialization(source) => D::new(
            "Authentication provider initialization failed.",
            "Check the named provider's configuration, endpoint connectivity, and key availability.",
        ).causes(source),
        AuthBuildError::WorkerFailure => D::new(
            "Authentication initialization worker failed.",
            "Check runtime shutdown or worker failure logs; collect a reproducer if initialization repeatedly fails.",
        ),
    }
}

/// Projects site-owned text errors without recursively invoking SiteError Display.
fn site_text(error: &SiteError) -> D {
    match error {
        SiteError::ServiceNotFound(name) => D::new("A required service is not registered.",
            "Register its factory with bundles::service or expose the requested interface through Service::expose.").detail(name),
        SiteError::TimezoneError(zone) => D::new("The configured timezone is invalid.",
            "Set SiteConf.tz to an IANA timezone such as Asia/Kolkata or UTC.").detail(zone),
        SiteError::TemplateFileError(reason) => D::new("A template file could not be loaded.",
            "Check the embedded template path and file contents.").detail(reason),
        SiteError::AddressResolutionError(reason) => D::new("The listen address could not be resolved.",
            "Check SiteConf.host and SiteConf.port and the host's DNS configuration.").detail(reason),
        SiteError::FileWatchError(reason) => D::new("The file watcher could not initialize.",
            "Check watched paths, permissions, and operating-system watcher limits.").detail(reason),
        _ => D::new("Site initialization failed.", "Inspect the typed source error."),
    }
}

/// Flattens existing validation lists without adding another validation pass.
pub(crate) fn configuration(error: &ConfError) -> Vec<D> {
    let mut pending = vec![error];
    let mut result = Vec::new();
    while let Some(error) = pending.pop() {
        match error {
            ConfError::Many(errors) => pending.extend(errors.iter().rev()),
            ConfError::Auth(error) => result.push(subsystems::auth(error)),
            ConfError::Cache(error) => result.push(subsystems::cache(error)),
            ConfError::Task(error) => result.push(subsystems::task(error)),
            ConfError::Logging(error) => result.push(subsystems::logging(error)),
            error => result.push(config_field(error)),
        }
    }
    result
}

/// Keeps configuration field identity while withholding secret-bearing values.
fn config_field(error: &ConfError) -> D {
    match error {
        ConfError::RequiredField { field, reason } => {
            field_diagnostic(field, reason, "Set this required SiteConf field.", None)
        }
        ConfError::InvalidValue {
            field,
            reason,
            expected,
        } => field_diagnostic(
            field,
            reason,
            "Correct this SiteConf field to satisfy the reported constraint.",
            expected.as_deref(),
        ),
        ConfError::InvalidPath {
            field,
            path,
            reason,
        } => {
            let diagnostic = field_diagnostic(
                field,
                reason,
                "Correct this configured path and check its existence and permissions.",
                None,
            );
            if super::sanitize::sensitive(field) {
                diagnostic
            } else {
                diagnostic.detail(path)
            }
        }
        ConfError::MissingField(field) => field_diagnostic(
            field,
            "A required value is missing.",
            "Set this required SiteConf field.",
            None,
        ),
        ConfError::Other(reason) => D::new(
            "Site configuration is invalid.",
            "Correct the configuration identified by the reported cause.",
        )
        .detail(reason),
        _ => D::new(
            "Site configuration is invalid.",
            "Inspect the typed configuration error.",
        ),
    }
}

/// Combines a field constraint and remedy without dumping configuration values.
fn field_diagnostic(field: &str, reason: &str, hint: &str, expected: Option<&str>) -> D {
    let mut diagnostic = D::new(format!("Invalid configuration field '{field}'."), hint);
    if super::sanitize::sensitive(field) {
        return diagnostic.detail("A required secret is missing or does not satisfy its configuration policy; its value is omitted.");
    }
    diagnostic = diagnostic.detail(reason);
    if let Some(expected) = expected {
        diagnostic = diagnostic.detail(format!("Expected: {expected}"));
    }
    diagnostic
}

#[cfg(test)]
#[path = "tests/projection.rs"]
mod tests;
