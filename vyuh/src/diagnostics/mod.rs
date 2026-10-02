//! Failure-only presentation for site assembly; no runtime state is retained.

mod model;
mod projection;
mod registrations;
mod sanitize;
mod sources;
mod subsystems;

pub use model::BuildDiagnostic;
pub(crate) use model::render;
pub(crate) use projection::configuration;
pub(crate) use registrations::bundle;
