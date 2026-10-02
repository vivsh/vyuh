use std::sync::Arc;

use crate::signals::SignalError;

#[derive(Debug, thiserror::Error, Clone)]
pub enum BundleError {
    Config(String),
    Auth(String),
    Signal(#[from] Arc<SignalError>),

    Task(#[from] Arc<crate::tasks::TaskRuntimeError>),

    Emitter(#[from] Arc<crate::emitters::EmitterError>),

    Service(#[from] Arc<crate::services::ServiceError>),

    Command(#[source] Arc<crate::commands::CommandError>),

    #[cfg(feature = "migrations")]
    Migration(#[source] Arc<crate::db::MigrationError>),

    ErrorList(Vec<BundleError>),

    DocGen(String),

    #[cfg(feature = "mcp")]
    Mcp(String),

    RouteRegistry(String),

    InvalidRoutePath {
        name: String,
        path: String,
        reason: String,
    },

    InvalidRouteName {
        name: String,
        reason: String,
    },

    InvalidRoutePrefix {
        prefix: String,
        reason: String,
    },

    DuplicateRouteName {
        name: String,
    },

    DuplicateRoutePathMethod {
        path: String,
        methods: String,
    },

    MissingAuthAudience {
        name: String,
        path: String,
    },
}

impl std::fmt::Display for BundleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::diagnostics::render(
            formatter,
            "Bundle validation failed",
            &crate::diagnostics::bundle(self),
        )
    }
}
