//! Request extraction and callable metadata for authenticated identities.

use std::sync::Arc;

use axum::{
    extract::{FromRequestParts, OptionalFromRequestParts},
    http::{HeaderValue, header, request::Parts},
    response::{IntoResponse, Response},
};

use super::{AuthError, AuthUser};
use crate::{
    Site,
    callables::{IntoArgPart, specs::ArgPart},
};

#[derive(Clone)]
struct AcceptedIdentity(AuthUser);

/// Safe HTTP rejection produced while extracting an authenticated identity.
pub struct AuthRejection {
    error: AuthError,
    challenges: Option<Arc<[HeaderValue]>>,
}

impl AuthRejection {
    pub(crate) fn new(error: AuthError, challenges: Option<Arc<[HeaderValue]>>) -> Self {
        Self { error, challenges }
    }

    pub(crate) fn plain(error: AuthError) -> Self {
        Self::new(error, None)
    }

    pub(crate) fn into_error(self) -> AuthError {
        self.error
    }

    /// Returns the underlying structured authentication failure.
    pub const fn error(&self) -> &AuthError {
        &self.error
    }
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let mut response = self.error.into_response();
        if let Some(challenges) = self.challenges {
            for challenge in challenges.iter() {
                response
                    .headers_mut()
                    .append(header::WWW_AUTHENTICATE, challenge.clone());
            }
        }
        response
    }
}

impl FromRequestParts<Site> for AuthUser {
    type Rejection = AuthRejection;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        if let Some(user) = parts.extensions.get::<AcceptedIdentity>() {
            return Ok(user.0.clone());
        }
        let context = parts
            .extensions
            .get::<crate::bundles::BundleRequestContext>();
        let audience = context
            .and_then(|value| value.audience.as_ref())
            .or_else(|| {
                parts
                    .extensions
                    .get::<crate::OperationId>()
                    .and_then(|id| site.operation_audience(*id))
            })
            .or_else(|| site.auth().default_audience())
            .ok_or_else(|| AuthRejection::plain(AuthError::MissingAudienceContext))?;
        let user = site
            .auth()
            .authenticate(parts, audience)
            .await
            .map_err(|error| site.auth().rejection(error, audience))?;
        user.validate()
            .map_err(|error| site.auth().rejection(error, audience))?;
        parts.extensions.insert(AcceptedIdentity(user.clone()));
        Ok(user)
    }
}

impl OptionalFromRequestParts<Site> for AuthUser {
    type Rejection = AuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        site: &Site,
    ) -> Result<Option<Self>, Self::Rejection> {
        match <Self as FromRequestParts<Site>>::from_request_parts(parts, site).await {
            Ok(user) => Ok(Some(user)),
            Err(error) if matches!(error.error, AuthError::NoCredential) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl IntoArgPart for AuthUser {
    fn into_arg_part() -> ArgPart {
        ArgPart::Composite(vec![
            ArgPart::Authentication,
            ArgPart::Security {
                scheme: "vyuhAuth".into(),
                scopes: Vec::new(),
                join_all: false,
            },
            ArgPart::Response(vec![
                crate::callables::ReturnSpec::error(401, "Unauthorized."),
                crate::callables::ReturnSpec::error(403, "Forbidden."),
                crate::callables::ReturnSpec::error(500, "Authentication service failed."),
                crate::callables::ReturnSpec::error(503, "Authentication provider unavailable."),
            ]),
        ])
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::FromRequestParts;

    use super::*;

    /// Verifies a public `AuthUser` extension cannot bypass provider authentication.
    #[tokio::test]
    async fn public_identity_extension_is_not_an_authentication_cache()
    -> Result<(), Box<crate::SiteError>> {
        let site = Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::Bundle::default(),
        )
        .await
        .map_err(Box::new)?;
        let request = axum::http::Request::new(axum::body::Body::empty());
        let (mut parts, _) = request.into_parts();
        parts.extensions.insert(AuthUser::new("injected-user"));

        let result =
            <AuthUser as FromRequestParts<Site>>::from_request_parts(&mut parts, &site).await;
        assert!(
            result
                .as_ref()
                .is_err_and(|error| matches!(error.error(), AuthError::NoCredential))
        );
        Ok(())
    }
}
