//! Request extraction and callable metadata for authenticated identities.

use axum::{
    extract::{FromRequestParts, OptionalFromRequestParts},
    http::request::Parts,
};

use super::{AuthError, AuthUser};
use crate::{
    Site,
    callables::{IntoArgPart, specs::ArgPart},
};

#[derive(Clone)]
struct AcceptedIdentity(AuthUser);

impl FromRequestParts<Site> for AuthUser {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        if let Some(user) = parts.extensions.get::<AcceptedIdentity>() {
            return Ok(user.0.clone());
        }
        let context = parts
            .extensions
            .get::<crate::bundles::BundleRequestContext>();
        let audience = context
            .and_then(|value| value.audience.as_ref())
            .or_else(|| site.auth().default_audience())
            .ok_or(AuthError::MissingAudienceContext)?;
        let user = site.auth().authenticate(parts, audience).await?;
        user.validate()?;
        parts.extensions.insert(AcceptedIdentity(user.clone()));
        Ok(user)
    }
}

impl OptionalFromRequestParts<Site> for AuthUser {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        site: &Site,
    ) -> Result<Option<Self>, Self::Rejection> {
        match <Self as FromRequestParts<Site>>::from_request_parts(parts, site).await {
            Ok(user) => Ok(Some(user)),
            Err(AuthError::NoCredential) => Ok(None),
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
        assert!(matches!(result, Err(AuthError::NoCredential)));
        Ok(())
    }
}
