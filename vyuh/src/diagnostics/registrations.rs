use super::{BuildDiagnostic as D, subsystems};
use crate::{bundles::BundleError, schema_assets::SchemaAssetError};

/// Projects existing bundle failures in order without recursively formatting lists.
pub(crate) fn bundle(error: &BundleError) -> Vec<D> {
    let mut pending = vec![error];
    let mut result = Vec::new();
    while let Some(error) = pending.pop() {
        match error {
            BundleError::ErrorList(errors) => pending.extend(errors.iter().rev()),
            BundleError::Service(error) => result.push(subsystems::service(error)),
            BundleError::Task(error) => result.push(subsystems::task(error)),
            BundleError::Emitter(error) => result.push(subsystems::emitter(error)),
            error => result.push(registration(error)),
        }
    }
    result
}

/// Supplies route-specific context and actionable path or audience corrections.
fn registration(error: &BundleError) -> D {
    match error {
        BundleError::DuplicateRoutePathMethod { path, methods } =>
            D::new("A route is registered more than once.",
                "Keep one registration for this method and normalized path, or give the operations distinct paths.")
                .detail(format!("Route: {methods} {path}"))
                .detail("Paths differing only by one terminal slash use the same internal routing path."),
        BundleError::DuplicateRouteName { name } => D::new("A route name is registered more than once.",
            "Give each operation a unique name, or remove the duplicate registration.").detail(name),
        BundleError::MissingAuthAudience { name, path } => D::new("An authenticated operation has no audience.",
            "Declare its bundle audience with .with_conf(bundles::conf().audience(...)).")
            .detail(format!("Operation: {name} at {path}")),
        BundleError::InvalidRoutePath { name, path, reason } => D::new("A route path is invalid.",
            "Correct the declared route path using Vyuh's route syntax; check path parameters and the trim option.")
            .detail(format!("Operation: {name} at {path}")).detail(reason),
        BundleError::InvalidRouteName { name, reason } => D::new("A route name is invalid.",
            "Correct the operation name to satisfy the reported naming constraint.").detail(name).detail(reason),
        BundleError::InvalidRoutePrefix { prefix, reason } => D::new("A bundle prefix is invalid.",
            "Correct the path passed to Bundle::with_prefix.").detail(prefix).detail(reason),
        other => bundle_source(other),
    }
}

/// Preserves opaque subsystem reasons without guessing their specific failure.
fn bundle_source(error: &BundleError) -> D {
    match error {
        BundleError::Config(reason) => D::new("Bundle configuration failed.",
            "Correct the reported declaration in BundleConf or the operation configuration.").detail(reason),
        BundleError::Auth(reason) => D::new("Bundle authentication configuration failed.",
            "Correct the reported audience, scope, or provider declaration. Bundle providers must cover exactly their bundle audience; shared providers belong in SiteConf.auth.").detail(reason),
        BundleError::DocGen(reason) => D::new("OpenAPI generation failed.",
            "Check the reported schema or OpenAPI endpoint declaration and its audience.").detail(reason),
        BundleError::RouteRegistry(reason) => D::new("Route registry construction failed.",
            "Check route names and normalized paths for conflicting registrations.").detail(reason),
        #[cfg(feature = "mcp")]
        BundleError::Mcp(reason) => D::new("MCP configuration failed.",
            "Check the reported MCP declaration, tool ownership, audience, and endpoint/resource collisions.").detail(reason),
        #[cfg(feature = "migrations")]
        BundleError::Migration(error) => D::new("Migration registration failed.",
            "Check migration registrations; compose schema contributors beneath one root migration history.").causes(error.as_ref()),
        BundleError::Signal(error) => D::new("Signal registration failed.",
            "Check the signal handler input type and reported cause.").causes(error.as_ref()),
        BundleError::Command(error) => D::new("Command registration failed.",
            "Check the command name, arguments, and registration.").causes(error.as_ref()),
        _ => D::new("Bundle validation failed.", "Inspect the typed bundle error."),
    }
}

/// Presents schema loading failures without exposing authored SQL.
pub(super) fn schema(error: &SchemaAssetError) -> D {
    match error {
        #[cfg(not(feature = "migrations"))]
        SchemaAssetError::MigrationsDisabled { path } => D::new("Schema assets require migration support.",
            "Enable Vyuh's migrations feature, or remove the embedded schema assets.").detail(path.display().to_string()),
        #[cfg(feature = "migrations")]
        SchemaAssetError::Unsupported { path } => D::new("The schema asset format is unsupported.",
            "Use a .yaml, .yml, or .sql schema asset.").detail(path.display().to_string()),
        #[cfg(feature = "migrations")]
        SchemaAssetError::Parse { path, source } | SchemaAssetError::Merge { path, source } =>
            D::new("An embedded schema could not be loaded.",
                "Validate this schema asset with Gaman and correct its syntax or conflicting definitions.")
                .detail(path.display().to_string()).causes(source),
        #[cfg(feature = "migrations")]
        SchemaAssetError::Registry(error) => D::new("Schema registration failed.",
            "Correct the reported migration history or schema contributor registration.").causes(error),
        #[cfg(feature = "migrations")]
        other => D::new("Schema asset initialization failed.",
            "Check the named schema asset, selected database dialect, and migration registration.").causes(other),
    }
}
