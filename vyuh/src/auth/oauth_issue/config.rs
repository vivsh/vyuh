//! Site-owned issuer, client, and account policy.

use std::{future::Future, sync::Arc};

use futures::future::BoxFuture;
use url::Url;

use crate::{
    Site,
    auth::{Audience, AuthError, AuthUser, KeySource, Scope},
};

/// Resolves an existing account at authorization, exchange, refresh, and resource access.
pub trait OAuthGrantVerifier: Send + Sync + 'static {
    /// Returns the current identity and grants, or rejects the account.
    fn verify<'a>(
        &'a self,
        site: &'a Site,
        subject: &'a str,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

pub(super) trait AccountVerifier: Send + Sync {
    fn verify<'a>(
        &'a self,
        site: &'a Site,
        subject: &'a str,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: OAuthGrantVerifier> AccountVerifier for T {
    fn verify<'a>(
        &'a self,
        site: &'a Site,
        subject: &'a str,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(OAuthGrantVerifier::verify(self, site, subject))
    }
}

/// A pre-registered OAuth client with exact redirect URIs.
#[derive(Clone)]
pub struct OAuthClient {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) redirects: Vec<String>,
    pub(super) secret: Option<KeySource>,
}

impl OAuthClient {
    /// Registers a public PKCE client with a user-facing display name.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            redirects: Vec::new(),
            secret: None,
        }
    }

    /// Adds one exact redirect URI, validated at site construction.
    pub fn redirect_uri(mut self, value: impl Into<String>) -> Self {
        self.redirects.push(value.into());
        self
    }

    /// Requires confidential client authentication with a deployment-owned secret.
    pub fn secret(mut self, value: KeySource) -> Self {
        self.secret = Some(value);
        self
    }

    pub(super) fn validate(&self) -> Result<(), AuthError> {
        if !bounded(&self.id, 256)
            || !bounded(&self.name, 128)
            || !(1..=16).contains(&self.redirects.len())
        {
            return Err(invalid("invalid OAuth client"));
        }
        for uri in &self.redirects {
            let url = clean_url(uri, true)
                .filter(|_| uri.len() <= 1024)
                .ok_or_else(|| invalid("invalid OAuth client redirect URI"))?;
            if url.query_pairs().any(|(key, _)| {
                matches!(
                    key.as_ref(),
                    "code" | "error" | "error_description" | "state" | "iss"
                )
            }) {
                return Err(invalid("reserved OAuth redirect query parameter"));
            }
        }
        Ok(())
    }
}

/// Configures a local OAuth issuer for protected MCP endpoints.
#[derive(Clone)]
pub struct OAuthIssuer {
    pub(super) issuer: String,
    pub(super) authorize_route: Option<String>,
    pub(super) browser_audience: Option<Audience>,
    pub(super) clients: Vec<OAuthClient>,
    pub(super) resources: Vec<(Audience, Vec<Scope>)>,
    pub(super) verifier: Option<Arc<dyn AccountVerifier>>,
}

impl std::fmt::Debug for OAuthIssuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthIssuer")
            .field("resources", &self.resources.len())
            .field("clients", &self.clients.len())
            .finish_non_exhaustive()
    }
}

impl OAuthIssuer {
    /// Starts configuration with the issuer's canonical HTTPS origin.
    pub fn new(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            authorize_route: None,
            browser_audience: None,
            clients: Vec::new(),
            resources: Vec::new(),
            verifier: None,
        }
    }

    /// Selects the named application GET route that handles sign-in and consent.
    pub fn authorize_route(mut self, name: impl Into<String>) -> Self {
        self.authorize_route = Some(name.into());
        self
    }

    /// Selects the existing CSRF-protected access-cookie audience for browser authorization.
    pub fn browser_audience(mut self, audience: Audience) -> Self {
        self.browser_audience = Some(audience);
        self
    }

    /// Registers a client allowed to request consent.
    pub fn client(mut self, client: OAuthClient) -> Self {
        self.clients.push(client);
        self
    }

    /// Allows delegation to the canonical MCP URL with explicitly listed scopes.
    pub fn resource(mut self, audience: Audience, scopes: impl IntoIterator<Item = Scope>) -> Self {
        self.resources
            .push((audience, scopes.into_iter().collect()));
        self
    }

    /// Uses the application's current account policy to verify delegated identity.
    pub fn verifier(mut self, verifier: impl OAuthGrantVerifier) -> Self {
        self.verifier = Some(Arc::new(verifier));
        self
    }
}

pub(super) fn bounded(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

/// Validates front-channel URLs and allows HTTP only for explicit loopback redirects.
pub(super) fn clean_url(value: &str, loopback: bool) -> Option<Url> {
    let url = Url::parse(value).ok()?;
    let https = url.scheme() == "https";
    let local = loopback
        && url.scheme() == "http"
        && matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"));
    ((https || local)
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && !value.contains('\\')
        && !value.chars().any(char::is_control))
    .then_some(url)
}

pub(super) fn invalid(reason: &str) -> AuthError {
    AuthError::InvalidProviderConfig(reason.into())
}
