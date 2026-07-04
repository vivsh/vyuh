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
            CommandError::Handler(error) => ErrorView::from_error(error),
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

impl From<callables::CallError> for CommandError {
    fn from(err: callables::CallError) -> Self {
        match err {
            callables::CallError::Validation(report) => CommandError::Validation(report),
            other => CommandError::CallError(other),
        }
    }
}
