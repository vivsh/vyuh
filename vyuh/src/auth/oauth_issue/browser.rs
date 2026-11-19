//! Application-owned sign-in and consent through the normal cookie provider.

use axum::{
    http::{HeaderValue, Method, StatusCode, header, request::Parts},
    response::Response,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::{
    Site,
    auth::{AuthError, AuthUser, Scope},
};

use super::{
    IssuerRuntime, OAuthIssueError,
    clients::{self, ClientBinding, Resolved},
    config::{bounded, clean_url, exact},
    protocol::{Parameters, no_store, store_failure},
    registration::registration_id,
    runtime::{CODE_TTL, CONSENT_TTL, GRANT_TTL, credential_digest, now, random_credential},
    store::{self, CredentialRow, GrantRow},
};

/// The next step the application must present in its own sign-in or consent UI.
pub enum OAuthConsent {
    /// Return this protocol error redirect unchanged; its callback is registered.
    Redirect(Response),
    /// Send the browser to the existing application login with this local return path.
    SignIn {
        /// A framework-generated relative path and the original authorization parameters.
        return_to: String,
    },
    /// Render the application consent page, then POST the opaque continuation.
    Ready {
        /// The operator-provided label for the client, including the document host for CIMD.
        client_name: String,
        /// The exact MCP resource the client requested.
        resource: String,
        /// The requested resource scopes. Approval also checks current account grants.
        requested_scopes: Vec<Scope>,
        /// Short-lived, single-use proof bound to the current application access cookie.
        continuation: String,
    },
}

/// A borrowed facade over the site's configured OAuth issuer; no UI is provided.
pub struct OAuthIssue<'a> {
    pub(super) site: &'a Site,
    pub(super) issuer: &'a IssuerRuntime,
}

#[derive(Serialize, Deserialize)]
struct Authorization {
    client: String,
    #[serde(default)]
    client_binding: Option<ClientBinding>,
    resource: String,
    redirect: String,
    challenge: String,
    scopes: Vec<Scope>,
    state: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Pending {
    id: Uuid,
    issuer: String,
    authorization: Authorization,
    subject: String,
    provider: String,
    cookie: String,
    expires_at: i64,
}

impl Site {
    /// Returns the configured OAuth issuer, or an error when issuance is disabled.
    pub fn oauth(&self) -> Result<OAuthIssue<'_>, OAuthIssueError> {
        let issuer = self
            .auth()
            .oauth_issuer
            .as_deref()
            .ok_or(OAuthIssueError::NotConfigured)?;
        Ok(OAuthIssue { issuer, site: self })
    }
}

impl OAuthIssue<'_> {
    /// Validates a front-channel GET and returns application sign-in or consent data.
    ///
    /// Render the result without caching or logging its contents. No grant is written by a GET.
    pub async fn request(&self, parts: &Parts) -> Result<OAuthConsent, OAuthIssueError> {
        if parts.method != Method::GET {
            return Err(OAuthIssueError::InvalidRequest);
        }
        let query = parts.uri.query().ok_or(OAuthIssueError::InvalidRequest)?;
        let (authorization, client_name) =
            match authorization(self.site, self.issuer, query).await? {
                Ok(result) => result,
                Err(response) => return Ok(OAuthConsent::Redirect(response)),
            };
        let (user, cookie) = match self.browser(parts).await {
            Ok(result) => result,
            Err(AuthError::NoCredential | AuthError::ExpiredCredential) => {
                let path = self
                    .site
                    .routes()
                    .reverse_url(&self.issuer.authorize_route, &[])
                    .ok_or(OAuthIssueError::Unavailable)?;
                return Ok(OAuthConsent::SignIn {
                    return_to: format!("{path}?{query}"),
                });
            }
            Err(error) => return Err(error.into()),
        };
        self.issuer
            .verify_account(self.site, user.subject())
            .await?;
        let pending = Pending {
            id: Uuid::new_v4(),
            issuer: self.issuer.issuer.clone(),
            authorization,
            subject: user.subject().into(),
            provider: user.provider().ok_or(OAuthIssueError::Unavailable)?.into(),
            cookie,
            expires_at: now() + CONSENT_TTL,
        };
        Ok(OAuthConsent::Ready {
            client_name,
            resource: pending.authorization.resource.clone(),
            requested_scopes: pending.authorization.scopes.clone(),
            continuation: self.issuer.codec.seal(&pending)?,
        })
    }

    /// Consumes the consent in a CSRF-checked POST and redirects with a short-lived code.
    ///
    /// Granted scopes must be a subset of the request and the account's current grants.
    pub async fn approve(
        &self,
        parts: &Parts,
        continuation: &str,
        scopes: &[Scope],
    ) -> Result<Response, OAuthIssueError> {
        let (pending, browser) = self.open(parts, continuation).await?;
        let account = self
            .issuer
            .verify_account(self.site, &pending.subject)
            .await?;
        let mut scopes = scopes.to_vec();
        crate::scopes::validate_scopes(&scopes).map_err(|_| OAuthIssueError::InvalidScope)?;
        scopes.sort_unstable();
        if scopes.windows(2).any(|pair| pair[0] == pair[1])
            || !account.has_all(&scopes)
            || !browser.has_all(&scopes)
            || !scopes
                .iter()
                .all(|scope| pending.authorization.scopes.binary_search(scope).is_ok())
        {
            return Err(OAuthIssueError::InvalidScope);
        }
        let at = now();
        if pending.expires_at <= at {
            return Err(OAuthIssueError::InvalidRequest);
        }
        let raw = random_credential()?;
        let code = CredentialRow {
            digest: credential_digest(&raw).ok_or(OAuthIssueError::Unavailable)?,
            grant_id: pending.id,
            kind: store::CODE,
            generation: 0,
            expires_at: at + CODE_TTL,
        };
        let grant = pending.grant(Some((scopes, code.expires_at)), at);
        let response = redirect(&pending.authorization, &self.issuer.issuer, ("code", &raw))?;
        if !store::decide(self.site.db(), &grant, Some(&code))
            .await
            .map_err(store_failure("approve"))?
        {
            return Err(OAuthIssueError::InvalidRequest);
        }
        Ok(response)
    }

    /// Consumes the consent in a CSRF-checked POST and redirects with `access_denied`.
    pub async fn deny(
        &self,
        parts: &Parts,
        continuation: &str,
    ) -> Result<Response, OAuthIssueError> {
        let (pending, _) = self.open(parts, continuation).await?;
        let at = now();
        if pending.expires_at <= at {
            return Err(OAuthIssueError::InvalidRequest);
        }
        let response = redirect(
            &pending.authorization,
            &self.issuer.issuer,
            ("error", "access_denied"),
        )?;
        let grant = pending.grant(None, at);
        if !store::decide(self.site.db(), &grant, None)
            .await
            .map_err(store_failure("deny"))?
        {
            return Err(OAuthIssueError::InvalidRequest);
        }
        Ok(response)
    }

    /// Deletes a bounded batch of expired records. Call only from trusted maintenance code.
    ///
    /// The supplied limit must be between 1 and 1,000. No background cleanup is started.
    pub async fn prune(&self, limit: u32) -> Result<u64, OAuthIssueError> {
        if !(1..=1_000).contains(&limit) {
            return Err(OAuthIssueError::InvalidRequest);
        }
        store::prune(self.site.db(), &self.issuer.issuer, now(), limit)
            .await
            .map_err(store_failure("prune"))
    }

    /// Permanently removes a dynamic registration and denies subsequent use of its grants.
    ///
    /// Call only from trusted operator code. Returns false for an absent registration.
    /// Malformed IDs and database errors are returned to the caller.
    pub async fn disable_registration(&self, client_id: &str) -> Result<bool, OAuthIssueError> {
        let id = registration_id(client_id).ok_or(OAuthIssueError::InvalidClient)?;
        store::disable(self.site.db(), &self.issuer.issuer, id)
            .await
            .map_err(store_failure("disable_registration"))
    }

    /// Deletes up to 1,000 expired assertion replay claims from this issuer.
    ///
    /// Call from trusted maintenance. Limits outside 1–1,000 and storage failures return errors.
    pub async fn prune_assertions(&self, limit: u32) -> Result<u64, OAuthIssueError> {
        if !(1..=1_000).contains(&limit) {
            return Err(OAuthIssueError::InvalidRequest);
        }
        store::prune_assertions(self.site.db(), &self.issuer.issuer, now(), limit)
            .await
            .map_err(store_failure("prune_assertions"))
    }

    /// Opens an issuer-specific continuation only for the exact accepted cookie and account.
    async fn open(
        &self,
        parts: &Parts,
        continuation: &str,
    ) -> Result<(Pending, AuthUser), OAuthIssueError> {
        if parts.method != Method::POST {
            return Err(OAuthIssueError::InvalidRequest);
        }
        let pending: Pending = self
            .issuer
            .codec
            .open(continuation)
            .map_err(|_| OAuthIssueError::InvalidRequest)?;
        if pending.issuer != self.issuer.issuer || pending.expires_at <= now() {
            return Err(OAuthIssueError::InvalidRequest);
        }
        let authorization = &pending.authorization;
        if authorization.client_binding.is_none() {
            let client = self
                .issuer
                .clients
                .get(&authorization.client)
                .ok_or(OAuthIssueError::InvalidClient)?;
            if !exact(&client.redirects, &authorization.redirect) {
                return Err(OAuthIssueError::InvalidRequest);
            }
            validate_resource(self.issuer, authorization)?;
        }
        let (user, cookie) = self.browser(parts).await?;
        if pending.subject != user.subject()
            || user.provider() != Some(&pending.provider)
            || !bool::from(cookie.as_bytes().ct_eq(pending.cookie.as_bytes()))
        {
            return Err(OAuthIssueError::AccessDenied);
        }
        if authorization.client_binding.is_some() {
            let client = clients::resolve(self.site, self.issuer, &authorization.client).await?;
            validate_registration(self.issuer, &client, authorization)?;
        }
        Ok((pending, user))
    }

    async fn browser(&self, parts: &Parts) -> Result<(crate::auth::AuthUser, String), AuthError> {
        self.site
            .auth()
            .authenticate_oauth_browser(self.site, parts, &self.issuer.browser)
            .await
    }
}

impl Pending {
    /// Constructs the one durable decision, retaining no principal or redirect on denial.
    fn grant(&self, approved: Option<(Vec<Scope>, i64)>, at: i64) -> GrantRow {
        let mut row = GrantRow {
            id: self.id,
            issuer: self.issuer.clone(),
            client_id: None,
            client_binding: None,
            subject: None,
            resource: None,
            scopes: None,
            redirect_uri: None,
            pkce_challenge: None,
            code_expires_at: None,
            expires_at: self.expires_at,
            generation: None,
            revoked_at: None,
            approved: approved.is_some(),
        };
        if let Some((scopes, code_expiry)) = approved {
            row.client_id = Some(self.authorization.client.clone());
            row.client_binding = self
                .authorization
                .client_binding
                .map(|binding| binding as i16);
            row.subject = Some(self.subject.clone());
            row.resource = Some(self.authorization.resource.clone());
            row.scopes = Some(
                scopes
                    .iter()
                    .map(Scope::as_str)
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            row.redirect_uri = Some(self.authorization.redirect.clone());
            row.pkce_challenge = Some(self.authorization.challenge.clone());
            row.code_expires_at = Some(code_expiry);
            row.expires_at = at + GRANT_TTL;
        }
        row
    }
}

/// Authorizes redirects only after an exact registered client and callback match.
async fn authorization(
    site: &Site,
    issuer: &IssuerRuntime,
    query: &str,
) -> Result<Result<(Authorization, String), Response>, OAuthIssueError> {
    let params = Parameters::parse(query.as_bytes())?;
    let client = params
        .get("client_id")
        .ok_or(OAuthIssueError::InvalidClient)?;
    let uri = params
        .get("redirect_uri")
        .ok_or(OAuthIssueError::InvalidRequest)?;
    let resolved = clients::resolve(site, issuer, client).await?;
    if !resolved.permits_redirect(uri) {
        return Err(OAuthIssueError::InvalidRequest);
    }
    let state = params.get("state");
    if state.is_some_and(|value| !bounded(value, 512))
        || ["client_secret", "client_assertion", "client_assertion_type"]
            .iter()
            .any(|param| params.get(param).is_some())
    {
        return Err(OAuthIssueError::InvalidRequest);
    }
    let outcome = authorization_details(issuer, &params, &resolved, client, uri, state);
    match outcome {
        Ok(value) => Ok(Ok((value, resolved.name()))),
        Err(error) => Ok(Err(callback(
            uri,
            state,
            &issuer.issuer,
            ("error", error.code()),
        )?)),
    }
}

/// Validates the PKCE parameters, exact resource, and finite application scopes.
fn authorization_details(
    issuer: &IssuerRuntime,
    params: &Parameters,
    resolved: &Resolved<'_>,
    client: &str,
    uri: &str,
    state: Option<&str>,
) -> Result<Authorization, OAuthIssueError> {
    let required = |field| params.get(field).ok_or(OAuthIssueError::InvalidRequest);
    if required("response_type")? != "code" {
        return Err(OAuthIssueError::UnsupportedResponseType);
    }
    let challenge = required("code_challenge")?;
    if params.get("code_challenge_method") != Some("S256")
        || challenge.len() != 43
        || !URL_SAFE_NO_PAD
            .decode(challenge)
            .is_ok_and(|bytes| bytes.len() == 32)
    {
        return Err(OAuthIssueError::InvalidRequest);
    }
    if params
        .get("response_mode")
        .is_some_and(|value| value != "query")
    {
        return Err(OAuthIssueError::InvalidRequest);
    }
    let scopes = params
        .get("scope")
        .map(parse_scopes)
        .transpose()?
        .unwrap_or_default();
    let value = Authorization {
        client: client.into(),
        client_binding: resolved.binding(),
        redirect: uri.into(),
        resource: params
            .get("resource")
            .ok_or(OAuthIssueError::InvalidTarget)?
            .into(),
        challenge: challenge.into(),
        scopes,
        state: state.map(str::to_owned),
    };
    validate_registration(issuer, resolved, &value)?;
    Ok(value)
}

pub(super) fn parse_scopes(value: &str) -> Result<Vec<Scope>, OAuthIssueError> {
    if value.len() > 8192 || value.bytes().filter(|byte| *byte == b' ').count() >= 128 {
        return Err(OAuthIssueError::InvalidScope);
    }
    let mut scopes = value
        .split(' ')
        .map(|value| Scope::new(value.to_owned()))
        .collect::<Vec<_>>();
    crate::scopes::validate_scopes(&scopes).map_err(|_| OAuthIssueError::InvalidScope)?;
    scopes.sort_unstable();
    if scopes.windows(2).any(|pair| pair.first() == pair.get(1)) {
        return Err(OAuthIssueError::InvalidScope);
    }
    Ok(scopes)
}

fn validate_registration(
    issuer: &IssuerRuntime,
    client: &Resolved<'_>,
    value: &Authorization,
) -> Result<(), OAuthIssueError> {
    if !client.matches_binding(value.client_binding.map(|binding| binding as i16)) {
        return Err(OAuthIssueError::InvalidClient);
    }
    if !client.permits_redirect(&value.redirect) {
        return Err(OAuthIssueError::InvalidRequest);
    }
    validate_resource(issuer, value)?;
    if !client.permits_scopes(value.scopes.iter().map(Scope::as_str)) {
        return Err(OAuthIssueError::InvalidScope);
    }
    Ok(())
}

fn validate_resource(issuer: &IssuerRuntime, value: &Authorization) -> Result<(), OAuthIssueError> {
    let scopes = issuer
        .resource_scopes(&value.resource)
        .ok_or(OAuthIssueError::InvalidTarget)?;
    if !value
        .scopes
        .iter()
        .all(|scope| scopes.binary_search(scope).is_ok())
    {
        return Err(OAuthIssueError::InvalidScope);
    }
    Ok(())
}

fn redirect(
    request: &Authorization,
    issuer: &str,
    outcome: (&str, &str),
) -> Result<Response, OAuthIssueError> {
    callback(&request.redirect, request.state.as_deref(), issuer, outcome)
}

/// Builds a no-store redirect only from a previously validated exact callback.
fn callback(
    uri: &str,
    state: Option<&str>,
    issuer: &str,
    outcome: (&str, &str),
) -> Result<Response, OAuthIssueError> {
    let mut url = clean_url(uri, true).ok_or(OAuthIssueError::InvalidRequest)?;
    url.query_pairs_mut()
        .append_pair(outcome.0, outcome.1)
        .append_pair("iss", issuer);
    if let Some(state) = state {
        url.query_pairs_mut().append_pair("state", state);
    }
    let location = HeaderValue::from_str(url.as_str()).map_err(|_| OAuthIssueError::Unavailable)?;
    Ok(no_store((
        StatusCode::SEE_OTHER,
        [(header::LOCATION, location)],
    )))
}
