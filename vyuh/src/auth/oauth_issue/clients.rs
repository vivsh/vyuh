//! Resolves each client through exactly one source and checks its consent binding.

use crate::{
    Site,
    auth::{AuthError, Scope},
};

use super::{
    IssuerRuntime, OAuthCimdAuth, OAuthDcrMethod, OAuthIssueError,
    browser::parse_scopes,
    cimd,
    config::exact,
    protocol::store_failure,
    registration::{RegistrationPolicy, registration_id},
    runtime::Client,
    store::{self, Proof},
};

const BAD: OAuthIssueError = OAuthIssueError::InvalidClient;

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
pub(super) enum ClientBinding {
    CimdPublic = 1,
    CimdSigned = 2,
    Registered = 3,
}

impl From<OAuthCimdAuth> for ClientBinding {
    fn from(value: OAuthCimdAuth) -> Self {
        match value {
            OAuthCimdAuth::Public => Self::CimdPublic,
            OAuthCimdAuth::PrivateKeyJwt => Self::CimdSigned,
        }
    }
}

pub(super) enum Resolved<'a> {
    Static(&'a Client),
    Cimd(&'a super::OAuthCimdClient, cimd::CimdMetadata),
    Registered(&'a RegistrationPolicy, RegisteredClient),
}

pub(super) struct RegisteredClient {
    redirects: Vec<String>,
    pub(super) method: OAuthDcrMethod,
    pub(super) secret: Option<blake3::Hash>,
    refresh: bool,
    scopes: Vec<Scope>,
}

impl Resolved<'_> {
    pub(super) fn binding(&self) -> Option<ClientBinding> {
        match self {
            Self::Static(_) => None,
            Self::Cimd(rule, _) => Some(rule.auth.into()),
            Self::Registered(_, _) => Some(ClientBinding::Registered),
        }
    }

    pub(super) fn name(&self) -> String {
        match self {
            Self::Static(client) => client.name.clone(),
            Self::Cimd(rule, _) => {
                let host = cimd::document_url(&rule.client_id)
                    .and_then(|url| url.host_str().map(str::to_owned))
                    .unwrap_or_else(|| rule.client_id.clone());
                format!("{} ({host})", rule.name)
            }
            Self::Registered(policy, _) => policy.name.clone(),
        }
    }

    pub(super) fn permits_redirect(&self, value: &str) -> bool {
        exact(
            match self {
                Self::Static(client) => &client.redirects,
                Self::Cimd(_, metadata) => &metadata.redirects,
                Self::Registered(_, client) => &client.redirects,
            },
            value,
        )
    }

    pub(super) fn permits_scopes(&self, mut scopes: impl Iterator<Item = impl AsRef<str>>) -> bool {
        let allowed = match self {
            Self::Static(_) => return true,
            Self::Cimd(_, metadata) => metadata.scopes.as_deref(),
            Self::Registered(_, client) => Some(client.scopes.as_slice()),
        };
        allowed.is_none_or(|allowed| {
            scopes.all(|scope| {
                allowed
                    .binary_search_by(|value| value.as_str().cmp(scope.as_ref()))
                    .is_ok()
            })
        })
    }

    pub(super) fn permits_refresh(&self) -> bool {
        match self {
            Self::Static(_) => true,
            Self::Cimd(_, metadata) => metadata.refresh,
            Self::Registered(_, client) => client.refresh,
        }
    }

    pub(super) fn matches_binding(&self, binding: Option<i16>) -> bool {
        self.binding().map(|binding| binding as i16) == binding
    }

    /// Enforces the immutable consent binding and the current rules for non-static clients.
    pub(super) fn permits_proof(&self, proof: &Proof) -> bool {
        self.matches_binding(proof.client_binding)
            && (matches!(self, Self::Static(_))
                || (self.permits_redirect(&proof.redirect)
                    && self.permits_scopes(proof.scopes.split_ascii_whitespace())))
    }
}

/// Resolves a static, metadata, or registered client without downgrade on any failure.
pub(super) async fn resolve<'a>(
    site: &Site,
    issuer: &'a IssuerRuntime,
    id: &str,
) -> Result<Resolved<'a>, OAuthIssueError> {
    if let Some(client) = issuer.clients.get(id) {
        return Ok(Resolved::Static(client));
    }
    if let Some(rule) = issuer.cimd.get(id) {
        let transport = issuer
            .cimd_http
            .as_ref()
            .ok_or(OAuthIssueError::Unavailable)?;
        return cimd::resolve(transport, rule)
            .await
            .map(|metadata| Resolved::Cimd(rule, metadata));
    }
    let policy = issuer.dcr.as_ref().ok_or(BAD)?;
    registered(site, issuer, policy, id)
        .await
        .map(|client| Resolved::Registered(policy, client))
}

/// Checks the consent binding without fetching metadata on the bearer hot path.
pub(super) async fn access(
    site: &Site,
    issuer: &IssuerRuntime,
    id: &str,
    binding: Option<i16>,
    redirect: &str,
) -> Result<(), AuthError> {
    let valid = match binding {
        None => issuer.clients.contains_key(id),
        Some(binding) if binding == ClientBinding::Registered as i16 => {
            if issuer.clients.contains_key(id) || issuer.cimd.contains_key(id) {
                return Err(AuthError::InvalidCredential);
            }
            let policy = issuer.dcr.as_ref().ok_or(AuthError::InvalidCredential)?;
            let client =
                registered(site, issuer, policy, id)
                    .await
                    .map_err(|error| match error {
                        OAuthIssueError::Unavailable => AuthError::ProviderUnavailable,
                        _ => AuthError::InvalidCredential,
                    })?;
            exact(&client.redirects, redirect)
        }
        Some(binding) => issuer.cimd.get(id).is_some_and(|rule| {
            ClientBinding::from(rule.auth) as i16 == binding && exact(&rule.redirects, redirect)
        }),
    };
    valid.then_some(()).ok_or(AuthError::InvalidCredential)
}

/// Decodes and rechecks immutable registration terms under the current operator policy.
async fn registered(
    site: &Site,
    issuer: &IssuerRuntime,
    policy: &RegistrationPolicy,
    id: &str,
) -> Result<RegisteredClient, OAuthIssueError> {
    let id = registration_id(id).ok_or(BAD)?;
    let (redirects, method, secret, refresh, scopes) =
        store::registration(site.db(), &issuer.issuer, id)
            .await
            .map_err(store_failure("registration_lookup"))?
            .ok_or(BAD)?;
    let redirects: Vec<String> = serde_json::from_str(&redirects).map_err(|_| BAD)?;
    let method = OAuthDcrMethod::parse(&method).ok_or(BAD)?;
    let secret = secret
        .as_deref()
        .map(blake3::Hash::from_hex)
        .transpose()
        .map_err(|_| BAD)?;
    let scopes = if scopes.is_empty() {
        Vec::new()
    } else {
        parse_scopes(&scopes).map_err(|_| BAD)?
    };
    if (method == OAuthDcrMethod::None) != secret.is_none()
        || !policy.permits(&redirects, method, refresh, &scopes)
    {
        return Err(BAD);
    }
    Ok(RegisteredClient {
        redirects,
        method,
        secret,
        refresh,
        scopes,
    })
}
