use std::sync::Arc;

use crate::signals::SignalError;

#[derive(Debug, thiserror::Error, Clone)]
pub enum BundleError {
    #[error(transparent)]
    Signal(#[from] Arc<SignalError>),

    #[error(transparent)]
    Task(#[from] Arc<crate::tasks::TaskError>),

    #[error(transparent)]
    Emitter(#[from] Arc<crate::emitters::EmitterError>),

    #[error(transparent)]
    Service(#[from] Arc<crate::services::ServiceError>),

    #[error(transparent)]
    Command(Arc<crate::commands::CommandError>),

    #[cfg(feature = "migrations")]
    #[error(transparent)]
    Migration(Arc<crate::db::MigrationError>),

    #[error("Multiple errors occurred: {0:?}")]
    ErrorList(Vec<BundleError>),

    #[error("API doc generation failed: {0}")]
    DocGen(String),

    #[error("invalid route path for '{name}': {reason}")]
    InvalidRoutePath {
        name: String,
        path: String,
        reason: String,
    },

    #[error("invalid route name: {reason}")]
    InvalidRouteName { name: String, reason: String },

    #[error("invalid route prefix '{prefix}': {reason}")]
    InvalidRoutePrefix { prefix: String, reason: String },

    #[error("duplicate route name '{name}'")]
    DuplicateRouteName { name: String },

    #[error("duplicate route path/method: {methods} {path}")]
    DuplicateRoutePathMethod { path: String, methods: String },
}
