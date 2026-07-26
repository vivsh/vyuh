#[cfg(feature = "migrations")]
use crate::errors::ErrorSource;
use crate::{
    Error, callables,
    errors::{ErrorKind, ErrorSourceKind, ErrorView},
    validation::ValidationReport,
};
use axum::http::StatusCode;
use std::borrow::Cow;

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// Emitted when `--help` or a parse error causes early exit; output is in the message.
    #[error("{0}")]
    Exit(String),

    #[error("Command not found: {0}")]
    NotFound(String),

    #[error("Command not found: {0}. Use 'help' to list available commands.")]
    UnknownCommand(String),

    #[error("Command already exists: {0}")]
    AlreadyExists(String),

    #[error("Unsupported command argument schema: {0}")]
    UnsupportedSchema(String),

    #[error("Unsupported type for command argument: {0}")]
    UnsupportedType(String),

    #[error("Unknown flag for command '{command}': --{flag}")]
    UnknownFlag { command: String, flag: String },

    #[error("Unexpected argument for command '{command}': {argument}")]
    UnexpectedArgument { command: String, argument: String },

    #[error("Missing required argument: --{flag}")]
    MissingRequired { flag: String },

    #[error("--{flag} expects at least one value")]
    MissingValue { flag: String },

    #[error("--{flag} expects exactly one value, got {count}")]
    TooManyValues { flag: String, count: usize },

    #[error("Duplicate flag for command '{command}': --{flag}")]
    DuplicateFlag { command: String, flag: String },

    #[error("Failed to parse --{flag} value '{value}' as {expected_type}: {error}")]
    ParseError {
        flag: String,
        value: String,
        expected_type: String,
        error: String,
    },

    #[error("Failed to deserialize arguments: {0}")]
    DeserializeError(String),

    #[error(transparent)]
    Validation(ValidationReport),

    #[error(transparent)]
    Handler(#[from] Error),

    #[error(transparent)]
    CallError(callables::CallError),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl CommandError {
    pub(crate) fn to_view(&self) -> ErrorView {
        match self {
            CommandError::Validation(report) => ErrorView::from_validation(report.clone()),
            CommandError::Handler(error) => handler_error_view(error),
            CommandError::UnknownCommand(_) | CommandError::NotFound(_) => ErrorView {
                status: StatusCode::NOT_FOUND,
                source: ErrorSourceKind::Framework,
                kind: ErrorKind::NotFound,
                code: Cow::Borrowed("unknown_command"),
                message: Cow::Owned(self.to_string()),
                errors: None,
                validation: None,
            },
            CommandError::AlreadyExists(_) => ErrorView {
                status: StatusCode::CONFLICT,
                source: ErrorSourceKind::Framework,
                kind: ErrorKind::Conflict,
                code: Cow::Borrowed("duplicate_command"),
                message: Cow::Owned(self.to_string()),
                errors: None,
                validation: None,
            },
            CommandError::UnknownFlag { .. }
            | CommandError::UnexpectedArgument { .. }
            | CommandError::MissingRequired { .. }
            | CommandError::MissingValue { .. }
            | CommandError::TooManyValues { .. }
            | CommandError::DuplicateFlag { .. }
            | CommandError::ParseError { .. }
            | CommandError::DeserializeError(_)
            | CommandError::UnsupportedSchema(_)
            | CommandError::UnsupportedType(_) => ErrorView {
                status: StatusCode::BAD_REQUEST,
                source: ErrorSourceKind::Parse,
                kind: ErrorKind::BadRequest,
                code: Cow::Borrowed("command_parse_error"),
                message: Cow::Owned(self.to_string()),
                errors: None,
                validation: None,
            },
            CommandError::CallError(_) | CommandError::Other(_) | CommandError::Exit(_) => {
                ErrorView {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    source: ErrorSourceKind::Framework,
                    kind: ErrorKind::Other,
                    code: Cow::Borrowed("command_error"),
                    message: Cow::Owned(self.to_string()),
                    errors: None,
                    validation: None,
                }
            }
        }
    }
}

/// Builds the command-only error view without changing HTTP error redaction.
fn handler_error_view(error: &Error) -> ErrorView {
    #[cfg(feature = "migrations")]
    if let Some(view) = migration_command_view(error) {
        return view;
    }
    ErrorView::from_error(error)
}

/// Preserves Mool/Gaman's sanitized migration diagnostic for terminal commands only.
#[cfg(feature = "migrations")]
fn migration_command_view(error: &Error) -> Option<ErrorView> {
    let ErrorSource::Other(source) = error.source.as_ref()? else {
        return None;
    };
    let migration = source.downcast_ref::<crate::db::engine::MigrationCommandError>()?;
    let diagnostic = migration.diagnostic();
    let mut view = ErrorView::from_error(error);
    view.message = Cow::Owned(render_migration_diagnostic(&diagnostic));
    Some(view)
}

/// Renders Gaman's structured diagnostic in the same concise terminal shape as its CLI.
#[cfg(feature = "migrations")]
fn render_migration_diagnostic(diagnostic: &crate::db::engine::CommandDiagnostic) -> String {
    let mut output = diagnostic.summary.clone();
    for detail in &diagnostic.details {
        output.push_str("\n  ");
        output.push_str(detail);
    }
    if let Some(hint) = &diagnostic.hint {
        output.push_str("\n  hint: ");
        output.push_str(hint);
    }
    output
}

impl From<callables::CallError> for CommandError {
    fn from(err: callables::CallError) -> Self {
        match err {
            callables::CallError::Validation(report) => CommandError::Validation(report),
            other => CommandError::CallError(other),
        }
    }
}

#[cfg(all(test, feature = "migrations"))]
mod tests {
    use super::*;

    /// Verifies migration command views retain Gaman's sanitized execution summary and hint.
    #[test]
    fn migration_command_view_preserves_diagnostic() {
        let migration = crate::db::engine::MigrationCommandError::Execution(
            crate::db::engine::ExecutorError::Connect(
                "postgres://gaman:secret@localhost/app password=hunter2".to_string(),
            ),
        );
        let error = Error::wrap(ErrorKind::Other, migration)
            .with_context("migration command failed (ExecutionFailed)");
        let view = CommandError::Handler(error).to_view();
        let rendered = crate::errors::ErrorConf::default().render_command(
            crate::errors::ErrorCommandContext {
                command: "migrate".to_string(),
                args: Vec::new(),
            },
            view,
        );

        assert!(rendered.contains("Error: database operation failed"));
        assert!(rendered.contains("hint: check database connectivity"));
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("Internal server error"));
    }

    /// Verifies structured migration details retain their ordering and terminal formatting.
    #[test]
    fn migration_diagnostic_renders_details_and_hint() {
        let diagnostic = crate::db::engine::CommandDiagnostic {
            code: crate::db::engine::DiagnosticCode::ExecutionFailed,
            summary: "database operation failed".to_string(),
            details: vec![
                "migration: 0001_report".to_string(),
                "apply statement 1: CREATE FUNCTION report()".to_string(),
                "execute failed [42702]: column reference is ambiguous".to_string(),
            ],
            hint: Some("inspect migration '0001_report' with `gaman show 0001_report`".to_string()),
            retryable: false,
        };

        assert_eq!(
            render_migration_diagnostic(&diagnostic),
            "database operation failed\n  migration: 0001_report\n  apply statement 1: CREATE FUNCTION report()\n  execute failed [42702]: column reference is ambiguous\n  hint: inspect migration '0001_report' with `gaman show 0001_report`"
        );
    }
}
