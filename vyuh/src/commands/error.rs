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

    let mut view = ErrorView::from_error(error);
    view.message = Cow::Owned(error.display_verbose());
    view
}

#[cfg(feature = "migrations")]
/// Preserves Mool migration diagnostics for terminal commands without HTTP error redaction.
fn migration_command_view(error: &Error) -> Option<ErrorView> {
    let ErrorSource::Other(source) = error.source.as_deref()? else {
        return None;
    };
    let migration = source.downcast_ref::<crate::db::engine::MigrationCommandError>()?;
    let mut view = ErrorView::from_error(error);
    view.message = Cow::Owned(render_migration_diagnostic(migration));
    Some(view)
}

#[cfg(feature = "migrations")]
/// Formats Mool's diagnostic and prompt definition for a non-interactive terminal host.
fn render_migration_diagnostic(error: &crate::db::engine::MigrationCommandError) -> String {
    let diagnostic = error.diagnostic();
    let mut output = diagnostic.summary;
    for detail in diagnostic.details {
        output.push_str("\n  ");
        output.push_str(&detail);
    }
    let clarifications = error.failure().clarifications;
    if !clarifications.is_empty() {
        output.push_str("\n  clarification input is required:");
        for clarification in &clarifications {
            output.push_str("\n  - ");
            output.push_str(&clarification.id);
            output.push_str(": ");
            output.push_str(&super::migration_prompt::render_clarification(
                clarification,
            ));
        }
    }
    if let Some(hint) = diagnostic.hint {
        output.push_str("\n  hint: ");
        output.push_str(&hint);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies command rendering keeps the native Mool error chain and local context.
    #[test]
    fn command_view_keeps_native_error_chain() {
        let error = Error::from(crate::db::DbError::Mock {
            operation: "fetch users",
            reason: "table users does not exist".to_string(),
        })
        .with_context("rebuild user index");
        let http_view = ErrorView::from_error(&error);
        let view = CommandError::Handler(error).to_view();
        let rendered = crate::errors::ErrorConf::default().render_command(
            crate::errors::ErrorCommandContext {
                command: "users:reindex".to_string(),
                args: Vec::new(),
            },
            view,
        );

        assert!(rendered.contains("Internal server error"));
        assert!(rendered.contains("rebuild user index"));
        assert!(rendered.contains("mock fetch users failed: table users does not exist"));
        assert!(!http_view.message.contains("mock fetch users failed"));
    }

    /// Verifies migration command errors use their native causal-chain display.
    #[cfg(feature = "migrations")]
    #[test]
    fn migration_command_uses_native_error_chain() {
        let migration = crate::db::engine::MigrationCommandError::Execution(
            crate::db::engine::ExecutorError::Connect("database unavailable".to_string()),
        );
        let expected = migration.to_string();
        let view = CommandError::Handler(Error::wrap(ErrorKind::Other, migration)).to_view();
        let rendered = crate::errors::ErrorConf::default().render_command(
            crate::errors::ErrorCommandContext {
                command: "migrate".to_string(),
                args: Vec::new(),
            },
            view,
        );

        assert!(rendered.contains(&expected));
    }

    /// Verifies a non-interactive clarification reports Mool's canonical question and choice.
    #[cfg(feature = "migrations")]
    #[test]
    fn migration_clarification_keeps_prompt_context() {
        let migration = crate::db::engine::MigrationCommandError::NeedsInput(vec![
            crate::db::engine::Clarification {
                id: "rename_column:users:email".to_string(),
                severity: crate::db::engine::Severity::Suggestion,
                kind: crate::db::engine::ClarificationKind::RenameColumn {
                    table: "users".to_string(),
                    old: "email".to_string(),
                    candidates: vec!["email_address".to_string()],
                },
            },
        ]);
        let view = CommandError::Handler(Error::wrap(ErrorKind::Invalid, migration)).to_view();
        let rendered = crate::errors::ErrorConf::default().render_command(
            crate::errors::ErrorCommandContext {
                command: "make_migration".to_string(),
                args: Vec::new(),
            },
            view,
        );

        assert!(rendered.contains("Column 'email' was removed from 'users'"));
        assert!(rendered.contains("email_address"));
        assert!(!rendered.contains("Validation failed"));
    }
}
