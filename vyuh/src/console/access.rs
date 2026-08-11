//! Console audience and request guard backed by the application's authentication.

use axum::{extract::FromRequestParts, http::request::Parts};

use crate::{
    Site,
    auth::{Audience, AuthError, AuthUser},
    callables::{IntoArgPart, specs::ArgPart},
};

/// Audience required by credentials used to access the built-in console.
pub const CONSOLE_AUDIENCE: Audience = Audience::new("vyuh-console");

/// Decides whether an authenticated user may access the built-in console.
///
/// The policy is synchronous and receives `None` only when no credential was
/// presented. Invalid credentials continue through Vyuh's normal authentication
/// failure path and never reach this policy.
pub trait ConsoleAccess: Send + Sync + 'static {
    /// Returns whether this request may use the console.
    fn allows(&self, site: &Site, user: Option<&AuthUser>) -> bool;
}

/// Private extractor that applies console development or policy access.
pub(crate) struct ConsoleGuard(Option<AuthUser>);

impl ConsoleGuard {
    /// Returns the authenticated user when a credential was presented.
    pub(crate) fn user(&self) -> Option<&AuthUser> {
        self.0.as_ref()
    }
}

impl FromRequestParts<Site> for ConsoleGuard {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        if site.conf().console.development_access() {
            return Ok(Self(None));
        }
        let user = optional_user(parts, site).await?;
        let policy =
            site.conf().console.access_policy().ok_or_else(|| {
                AuthError::Internal("console access policy is unavailable".into())
            })?;
        policy
            .allows(site, user.as_ref())
            .then_some(Self(user))
            .ok_or(AuthError::Forbidden)
    }
}

/// Extracts an optional user while keeping console audience failures opaque.
async fn optional_user(parts: &mut Parts, site: &Site) -> Result<Option<AuthUser>, AuthError> {
    match <Option<AuthUser> as FromRequestParts<Site>>::from_request_parts(parts, site).await {
        Err(error) if matches!(error.error(), AuthError::AudienceMismatch) => {
            Err(AuthError::InvalidCredential)
        }
        Err(error) => Err(error.into_error()),
        Ok(user) => Ok(user),
    }
}

impl IntoArgPart for ConsoleGuard {
    fn into_arg_part() -> ArgPart {
        ArgPart::Composite(vec![
            ArgPart::Authentication,
            ArgPart::Response(vec![
                crate::callables::ReturnSpec::error(401, "Unauthorized."),
                crate::callables::ReturnSpec::error(403, "Forbidden."),
                crate::callables::ReturnSpec::error(500, "Authentication service failed."),
                crate::callables::ReturnSpec::error(503, "Authentication provider unavailable."),
            ]),
        ])
    }
}
