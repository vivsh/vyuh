//! Public identity-token provider configuration and application mapping.

use std::{future::Future, sync::Arc};

use futures::future::BoxFuture;

use crate::auth::{
    Audience, AudienceId, AuthError, AuthUser, CookieConf, CredentialLocation, CsrfConf,
    UnsafeQueryCredentials,
};

const GOOGLE_ISSUER: &str = "https://accounts.google.com";
const MAX_RESOURCES: usize = 64;

/// Cryptographically verified claims from one external identity token.
#[derive(Clone)]
pub struct IdTokenClaims {
    pub(crate) subject: String,
    pub(crate) issuer: String,
    pub(crate) audiences: Vec<String>,
    pub(crate) issued_at: chrono::DateTime<chrono::Utc>,
    pub(crate) expires_at: chrono::DateTime<chrono::Utc>,
    pub(crate) token_id: Option<String>,
    pub(crate) raw: serde_json::Map<String, serde_json::Value>,
}

impl std::fmt::Debug for IdTokenClaims {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdTokenClaims")
            .field("audience_count", &self.audiences.len())
            .field("claim_count", &self.raw.len())
            .field("has_token_id", &self.token_id.is_some())
            .finish_non_exhaustive()
    }
}

impl IdTokenClaims {
    /// Returns the stable issuer-local subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the verified token issuer.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the protocol-level token audiences.
    pub fn audiences(&self) -> &[String] {
        &self.audiences
    }

    /// Returns the verified token issue time.
    pub const fn issued_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.issued_at
    }

    /// Returns the verified token expiry.
    pub const fn expires_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.expires_at
    }

    /// Returns the token identifier when supplied by the issuer.
    pub fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    /// Returns one authenticated provider-specific claim.
    pub fn claim(&self, name: &str) -> Option<&serde_json::Value> {
        self.raw.get(name)
    }

    /// Returns all bounded provider-specific claims.
    pub fn claims(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.raw
    }
}

/// Maps a verified external identity token to an application identity.
pub trait IdTokenMapper: Send + Sync + 'static {
    /// Applies application identity and account policy after protocol validation.
    fn map(
        &self,
        claims: &IdTokenClaims,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send;
}

pub(crate) trait ErasedIdTokenMapper: Send + Sync {
    fn map<'a>(&'a self, claims: &'a IdTokenClaims) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: IdTokenMapper> ErasedIdTokenMapper for T {
    fn map<'a>(&'a self, claims: &'a IdTokenClaims) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(IdTokenMapper::map(self, claims))
    }
}

/// A discovery-backed, verify-only JWT identity-token provider.
#[derive(Clone)]
pub struct IdToken {
    pub(crate) issuer: String,
    pub(crate) issuer_aliases: Vec<String>,
    pub(crate) resources: Vec<(Audience, String)>,
    pub(crate) mapper: Option<Arc<dyn ErasedIdTokenMapper>>,
    pub(crate) location: CredentialLocation,
    pub(crate) csrf: Option<CsrfConf>,
}

impl std::fmt::Debug for IdToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdToken")
            .field("issuer", &self.issuer)
            .field("resources", &self.resources.len())
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

impl IdToken {
    /// Creates an identity-token provider using OpenID discovery.
    pub fn discovery(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            issuer_aliases: Vec::new(),
            resources: Vec::new(),
            mapper: None,
            location: CredentialLocation::bearer(),
            csrf: None,
        }
    }

    /// Creates a Google identity-token provider with Google's issuer policy.
    pub fn google() -> Self {
        let mut value = Self::discovery(GOOGLE_ISSUER);
        value.issuer_aliases.push("accounts.google.com".into());
        value
    }

    /// Maps one Vyuh route audience to the exact external JWT audience.
    pub fn resource(mut self, audience: Audience, token_audience: impl Into<String>) -> Self {
        self.resources.push((audience, token_audience.into()));
        self
    }

    /// Sets the required application identity mapper.
    pub fn mapper(mut self, mapper: impl IdTokenMapper) -> Self {
        self.mapper = Some(Arc::new(mapper));
        self
    }

    /// Restores conventional `Authorization: Bearer` extraction.
    pub fn from_bearer(mut self) -> Self {
        self.location = CredentialLocation::bearer();
        self.csrf = None;
        self
    }

    /// Extracts the identity token from one header.
    pub fn from_header(mut self, name: impl Into<String>) -> Self {
        self.location = CredentialLocation::header(name);
        self.csrf = None;
        self
    }

    /// Extracts the identity token from a header with a scheme.
    pub fn from_header_with_scheme(
        mut self,
        name: impl Into<String>,
        scheme: impl Into<String>,
    ) -> Self {
        self.location = CredentialLocation::header_with_scheme(name, scheme);
        self.csrf = None;
        self
    }

    /// Extracts the identity token from an authentication cookie.
    pub fn from_cookie(mut self, cookie: impl Into<CookieConf>) -> Self {
        self.location = CredentialLocation::cookie(cookie);
        self.csrf = self.location.default_csrf();
        self
    }

    /// Replaces the default double-submit policy for cookie credentials.
    pub fn csrf(mut self, value: CsrfConf) -> Self {
        self.csrf = Some(value);
        self
    }

    /// Explicitly disables CSRF checks for a cookie identity token.
    pub fn without_csrf(mut self) -> Self {
        self.csrf = None;
        self
    }

    /// Extracts the identity token from an explicitly acknowledged query parameter.
    pub fn from_query(mut self, name: impl Into<String>, risk: UnsafeQueryCredentials) -> Self {
        self.location = CredentialLocation::query(name, risk);
        self.csrf = None;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), AuthError> {
        super::super::oauth::http::validate_remote_url(&self.issuer)?;
        self.location.validate()?;
        if let Some(csrf) = &self.csrf {
            csrf.validate()?;
        }
        validate_resources(&self.resources)?;
        if self.mapper.is_none() {
            return Err(AuthError::InvalidProviderConfig(
                "identity-token providers require an application mapper".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_production(&self) -> Result<(), AuthError> {
        self.validate()?;
        self.location.validate_production_cookie()?;
        if self.location.is_cookie() && self.csrf.is_none() {
            return Err(AuthError::InvalidProviderConfig(
                "cookie credentials require CSRF validation in production".into(),
            ));
        }
        Ok(())
    }
}

fn validate_resources(values: &[(Audience, String)]) -> Result<(), AuthError> {
    if values.is_empty() || values.len() > MAX_RESOURCES {
        return Err(AuthError::InvalidProviderConfig(
            "identity-token providers require between 1 and 64 resources".into(),
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for (audience, token_audience) in values {
        let id = AudienceId::declared(*audience)?;
        if !names.insert(id.as_str().to_owned()) || !valid_token_audience(token_audience) {
            return Err(AuthError::InvalidProviderConfig(
                "identity-token resources require unique audiences and bounded token audiences"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn valid_token_audience(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value
            .bytes()
            .all(|byte| !byte.is_ascii_control() && !byte.is_ascii_whitespace())
}
