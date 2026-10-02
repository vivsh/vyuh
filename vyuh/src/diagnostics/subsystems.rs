use super::BuildDiagnostic as D;
use crate::{
    auth::AuthError, cache::CacheError, emitters::EmitterError, logging::LoggingError,
    services::ServiceError, tasks::TaskRuntimeError,
};

/// Chooses remedies from auth variants rather than matching rendered text.
pub(super) fn auth(error: &AuthError) -> D {
    let hint = match error {
        AuthError::DuplicateProvider(_) => {
            "Register each provider name once across SiteConf.auth and all bundle contributions."
        }
        AuthError::ReservedProviderId(_) => {
            "Choose an application provider name without the reserved 'vyuh-' prefix."
        }
        AuthError::InvalidProviderId(_) => {
            "Use a non-empty provider name containing only ASCII letters, digits, hyphens, or underscores."
        }
        AuthError::ProviderNotFound(_) => {
            "Register the selected provider in SiteConf.auth before referring to it."
        }
        AuthError::InvalidAudience(_) => {
            "Use an Audience of 1–128 ASCII bytes containing letters, digits, hyphens, underscores, dots, colons, or slashes."
        }
        AuthError::AmbiguousProvider(_) => {
            "Give overlapping providers distinct credential selectors or disjoint audiences."
        }
        AuthError::DuplicateLoginMethod(_) => "Register each login method name only once.",
        AuthError::InvalidLoginMethod(_) => "Correct the login method identifier.",
        AuthError::LoginMethodNotFound(_) => {
            "Register the selected login method with AuthConf::method."
        }
        AuthError::ProviderUnavailable => {
            "Check the provider endpoint, network connectivity, and signing-key availability."
        }
        AuthError::InvalidProviderConfig(_) => {
            "Correct the named provider setting described by the cause; retain the required audience and security constraints."
        }
        AuthError::UnsupportedProviderCapability | AuthError::UnsupportedLocationCapability => {
            "Select a provider and credential location supporting the configured authentication lifecycle."
        }
        AuthError::Internal(_) => {
            "Collect a minimal reproducer and report this authentication invariant failure."
        }
        _ => "Check the authentication configuration and the reported cause.",
    };
    D::new("Authentication configuration failed.", hint).causes(error)
}

/// Explains provider/default selection using the existing cache builders.
pub(super) fn cache(error: &CacheError) -> D {
    let hint = match error {
        CacheError::DuplicateProvider => {
            "Register each CacheName only once; CacheConf::default() already registers 'default'."
        }
        CacheError::MissingDefaultProvider => {
            "Select a registered provider with CacheConf::default_provider(...)."
        }
        CacheError::InvalidDefaultProvider | CacheError::ProviderNotFound => {
            "Register the selected provider with CacheConf::provider(...) before choosing it."
        }
        CacheError::InvalidProviderName => {
            "Use a cache name of 1–64 ASCII letters, digits, hyphens, or underscores."
        }
        CacheError::InvalidTtl => {
            "Configure a positive cache TTL or explicitly select CacheTtl::Forever."
        }
        _ => "Check this cache provider's configuration and reported constraint.",
    };
    D::new("Cache configuration failed.", hint).causes(error)
}

/// Identifies task registration and lane corrections without starting workers.
pub(super) fn task(error: &TaskRuntimeError) -> D {
    let hint = match error {
        TaskRuntimeError::UnknownLane(_) => {
            "Configure the named TaskLaneConf or correct the task's lane selection."
        }
        TaskRuntimeError::AlreadyExists(_) => {
            "Register the task once or give conflicting task definitions distinct names."
        }
        TaskRuntimeError::TaskNotFound(_) => {
            "Include the referenced Work or Flow registration in the site's bundle."
        }
        TaskRuntimeError::TypeMismatch(_, _) => {
            "Check task input registration; report a reproducer if valid declarations produce a type mismatch."
        }
        TaskRuntimeError::InvalidConfig(_) => {
            "Correct the named task setting to satisfy the reported constraint in TaskConf or TaskLaneConf."
        }
        _ => "Check the task registration and reported initialization cause.",
    };
    D::new("Task initialization failed.", hint).causes(error)
}

/// Unwraps initialization context without repeating service wrappers in causes.
pub(super) fn service(error: &ServiceError) -> D {
    let mut current = error;
    let mut names = Vec::new();
    while let ServiceError::Initialization { service, source } = current {
        if names.len() == 8 {
            return D::new(
                "Service initialization failed.",
                "Inspect the typed service source.",
            )
            .detail("[service context truncated]");
        }
        names.push(*service);
        current = source;
    }
    let hint = match current {
        ServiceError::AlreadyRegistered(_) => {
            "Register each concrete service and exposed facade only once."
        }
        ServiceError::NotFound(_) => {
            "Register the required service factory or expose the requested interface through Service::expose."
        }
        ServiceError::ArcShared => {
            "Keep the factory result exclusively owned during initialization; do not publish service ownership before assembly completes."
        }
        ServiceError::UnexpectedOutput | ServiceError::FacadeDowncast => {
            "Collect a minimal reproducer and report this service type invariant failure."
        }
        _ => {
            "Correct the reported factory, facade exposure, or worker-registration failure before restarting."
        }
    };
    let mut diagnostic = D::new("Service initialization failed.", hint);
    for name in names {
        diagnostic = diagnostic.detail(format!("Service: {name}"));
    }
    diagnostic.causes(current)
}

/// Distinguishes logging ownership conflicts from invalid sink configuration.
pub(super) fn logging(error: &LoggingError) -> D {
    let hint = match error {
        LoggingError::SubscriberInit(_) => {
            "Initialize logging once, or use SiteConf::log_init(false) when the application owns tracing."
        }
        LoggingError::DirCreation(_) => {
            "Check the log directory and its parent directory permissions."
        }
        LoggingError::DuplicateRuleName(_) => "Give each logging rule a unique name.",
        LoggingError::InvalidRuleName { .. } => {
            "Use a rule name starting with a letter and containing only letters, digits, or underscores (at most 48 characters)."
        }
        LoggingError::InvalidEnvPrefix { .. } => {
            "Use an uppercase environment prefix starting with a letter (at most 48 characters)."
        }
        LoggingError::FilterParse { .. } => "Use a tracing filter such as info or my_crate=debug.",
        LoggingError::MailAdminsFeature => {
            "Enable Vyuh's email feature to use mail-admins logging."
        }
        LoggingError::MailAdminsMailDisabled => {
            "Enable outbound mail before configuring mail-admins logging."
        }
        LoggingError::MailAdminsLoggingDisabled => {
            "Enable SiteConf::log_init(true) to install the mail-admins logging sink."
        }
        _ => "Correct the mail-admins setting identified by the reported constraint.",
    };
    D::new("Logging initialization failed.", hint).causes(error)
}

/// Explains emitter scheduling and registration failures without guessing causes.
pub(super) fn emitter(error: &EmitterError) -> D {
    let hint = match error {
        EmitterError::AlreadyExists => "Register each emitter type only once.",
        EmitterError::MissingTaskTarget { .. } => {
            "Include the scheduled task's Work registration in the site bundle."
        }
        EmitterError::CronError(_) => "Correct the cron expression in the emitter's schedule.",
        EmitterError::InvalidDebounce(_) => "Correct the emitter debounce duration and mode.",
        EmitterError::InvalidSchedule(_) => {
            "Correct the schedule setting identified by the reported constraint."
        }
        _ => "Check the emitter declaration and reported initialization cause.",
    };
    D::new("Emitter initialization failed.", hint).causes(error)
}
