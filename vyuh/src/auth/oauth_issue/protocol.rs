//! Headless OAuth metadata, token exchange, and revocation endpoints.

use std::collections::BTreeMap;

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde::Serialize;
use serde_json::json;
use subtle::ConstantTimeEq;

use crate::{
    Site,
    auth::{AuthError, Scope},
    db::DbError,
};

use super::{
    IssuerRuntime,
    config::{bounded, invalid},
    runtime::{ACCESS_TTL, credential_digest, now, random_credential},
    store::{self, CredentialRow, Proof},
};

const METADATA_PATH: &str = "/.well-known/oauth-authorization-server";
const TOKEN_PATH: &str = "/oauth/token";
const REVOCATION_PATH: &str = "/oauth/revoke";
const MAX_FORM: usize = 16 * 1024;

/// Safe, protocol-shaped authorization or token failure.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[non_exhaustive]
pub enum OAuthIssueError {
    /// A parameter or the consent continuation was invalid.
    #[error("invalid OAuth request")]
    InvalidRequest,
    /// The client failed authentication or is unregistered.
    #[error("invalid OAuth client")]
    InvalidClient,
    /// The credential is expired, invalid, or already used.
    #[error("invalid OAuth grant")]
    InvalidGrant,
    /// Requested scopes were not allowed.
    #[error("invalid OAuth scope")]
    InvalidScope,
    /// The resource is not configured for this issuer.
    #[error("invalid OAuth target")]
    InvalidTarget,
    /// The request did not use the authorization-code or refresh grant.
    #[error("unsupported OAuth grant type")]
    UnsupportedGrantType,
    /// Only the authorization-code response type is supported.
    #[error("unsupported OAuth response type")]
    UnsupportedResponseType,
    /// The browser credential was missing or rejected.
    #[error("OAuth browser authentication required")]
    AuthenticationRequired,
    /// The current account or browser request cannot authorize this grant.
    #[error("OAuth access denied")]
    AccessDenied,
    /// The issuer has not been enabled.
    #[error("OAuth issuer is not configured")]
    NotConfigured,
    /// The issuer could not complete the operation, without disclosing the cause.
    #[error("OAuth issuer temporarily unavailable")]
    Unavailable,
}

impl From<AuthError> for OAuthIssueError {
    fn from(error: AuthError) -> Self {
        match error.status() {
            StatusCode::UNAUTHORIZED => Self::AuthenticationRequired,
            StatusCode::FORBIDDEN => Self::AccessDenied,
            _ => Self::Unavailable,
        }
    }
}

impl IntoResponse for OAuthIssueError {
    fn into_response(self) -> Response {
        use OAuthIssueError::*;
        let status = match self {
            InvalidClient | AuthenticationRequired => StatusCode::UNAUTHORIZED,
            AccessDenied => StatusCode::FORBIDDEN,
            NotConfigured => StatusCode::NOT_FOUND,
            Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::BAD_REQUEST,
        };
        let mut response = no_store((status, Json(json!({ "error": self.code() }))));
        if matches!(self, InvalidClient) {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"OAuth\""),
            );
        }
        response
    }
}

impl OAuthIssueError {
    pub(super) fn code(self) -> &'static str {
        use OAuthIssueError::*;
        match self {
            InvalidRequest | NotConfigured => "invalid_request",
            InvalidClient => "invalid_client",
            InvalidGrant => "invalid_grant",
            InvalidScope => "invalid_scope",
            InvalidTarget => "invalid_target",
            UnsupportedGrantType => "unsupported_grant_type",
            UnsupportedResponseType => "unsupported_response_type",
            AuthenticationRequired | AccessDenied => "access_denied",
            Unavailable => "temporarily_unavailable",
        }
    }
}

/// Keeps an MCP resource from replacing an OAuth protocol route.
pub(super) fn reserved_path(path: &str) -> bool {
    matches!(path, METADATA_PATH | TOKEN_PATH | REVOCATION_PATH)
}

/// Prevents OAuth credentials and front-channel continuations from being cached.
pub(super) fn no_store(value: impl IntoResponse) -> Response {
    let mut response = value.into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    response
}

/// Installs headless endpoints after verifying the application-owned authorize route.
pub(crate) fn setup(
    router: &mut Router<Site>,
    issuer: &IssuerRuntime,
    routes: &crate::routes::RouteRegistry,
) -> Result<(), AuthError> {
    let routes = crate::routes::Routes::new(routes);
    let authorize = routes
        .reverse_url(&issuer.authorize_route, &[])
        .filter(|path| {
            path.starts_with('/')
                && !path.starts_with("//")
                && !path.contains(['?', '#', '\\'])
                && routes.resolve_url(axum::http::Method::GET, path).is_some()
        })
        .ok_or_else(|| invalid("OAuth authorization route must be a named static GET route"))?;
    for path in [METADATA_PATH, TOKEN_PATH, REVOCATION_PATH] {
        if routes.resolve_url(axum::http::Method::GET, path).is_some()
            || routes.resolve_url(axum::http::Method::POST, path).is_some()
        {
            return Err(invalid(
                "OAuth protocol path conflicts with an application route",
            ));
        }
    }
    let metadata = metadata(issuer, &authorize);
    *router = std::mem::take(router)
        .route(METADATA_PATH, get(move || async move { Json(metadata) }))
        .route(TOKEN_PATH, post(token))
        .route(REVOCATION_PATH, post(revocation));
    Ok(())
}

/// Publishes only configured, pre-registered-client capabilities.
fn metadata(issuer: &IssuerRuntime, authorize: &str) -> serde_json::Value {
    let mut scopes = issuer
        .resources
        .values()
        .flatten()
        .map(Scope::as_str)
        .collect::<Vec<_>>();
    scopes.sort_unstable();
    scopes.dedup();
    json!({
        "issuer": issuer.issuer,
        "authorization_endpoint": format!("{}{authorize}", issuer.issuer),
        "token_endpoint": format!("{}{TOKEN_PATH}", issuer.issuer),
        "revocation_endpoint": format!("{}{REVOCATION_PATH}", issuer.issuer),
        "response_types_supported": ["code"], "response_modes_supported": ["query"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"], "scopes_supported": scopes,
        "token_endpoint_auth_methods_supported": ["none", "client_secret_basic", "client_secret_post"],
        "revocation_endpoint_auth_methods_supported": ["none", "client_secret_basic", "client_secret_post"],
        "authorization_response_iss_parameter_supported": true
    })
}

async fn token(State(site): State<Site>, request: Request) -> Result<Response, OAuthIssueError> {
    let issuer = site.oauth()?.issuer;
    let (headers, form) = read_form(request).await?;
    let client = authenticate_client(issuer, &headers, &form)?;
    let proof = match form.get("grant_type") {
        Some("authorization_code") => {
            find_proof(&site, issuer, client, &form, "code", store::CODE).await?
        }
        Some("refresh_token") => {
            find_proof(
                &site,
                issuer,
                client,
                &form,
                "refresh_token",
                store::REFRESH,
            )
            .await?
        }
        Some(_) => return Err(OAuthIssueError::UnsupportedGrantType),
        None => return Err(OAuthIssueError::InvalidRequest),
    };
    let resource = form.get("resource").ok_or(OAuthIssueError::InvalidTarget)?;
    if resource != proof.resource || issuer.resource_scopes(resource).is_none() {
        return Err(OAuthIssueError::InvalidTarget);
    }
    if proof.kind == store::CODE && !valid_code(&proof, &form) {
        return Err(OAuthIssueError::InvalidGrant);
    }
    if let Some(scope) = form.get("scope") {
        let scopes = super::browser::parse_scopes(scope)?;
        if !scopes
            .iter()
            .map(Scope::as_str)
            .eq(proof.scopes.split_ascii_whitespace())
        {
            return Err(OAuthIssueError::InvalidScope);
        }
    }
    Ok(no_store(Json(exchange(&site, issuer, proof).await?)))
}

/// Verifies the submitted PKCE proof and exact redirect before any replay action.
fn valid_code(proof: &Proof, form: &Parameters) -> bool {
    let Some(verifier) = form.get("code_verifier") else {
        return false;
    };
    if form.get("redirect_uri") != Some(proof.redirect.as_str())
        || !(43..=128).contains(&verifier.len())
        || !verifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
    {
        return false;
    }
    let challenge = URL_SAFE_NO_PAD.encode(ring::digest::digest(
        &ring::digest::SHA256,
        verifier.as_bytes(),
    ));
    bool::from(challenge.as_bytes().ct_eq(proof.challenge.as_bytes()))
}

async fn find_proof(
    site: &Site,
    issuer: &IssuerRuntime,
    client: &str,
    form: &Parameters,
    field: &str,
    kind: i16,
) -> Result<Proof, OAuthIssueError> {
    let raw = form.get(field).ok_or(OAuthIssueError::InvalidRequest)?;
    let digest = credential_digest(raw).ok_or(OAuthIssueError::InvalidGrant)?;
    store::lookup(site.db(), &issuer.issuer, digest)
        .await
        .map_err(store_failure("token_lookup"))?
        .filter(|proof| proof.client == client && proof.kind == kind)
        .ok_or(OAuthIssueError::InvalidGrant)
}

#[derive(Serialize)]
struct TokenReply {
    access_token: String,
    refresh_token: String,
    token_type: &'static str,
    expires_in: i64,
    scope: String,
}

/// Revalidates the account before acquiring any write lock; only the winner returns credentials.
async fn exchange(
    site: &Site,
    issuer: &IssuerRuntime,
    proof: Proof,
) -> Result<TokenReply, OAuthIssueError> {
    if proof.revoked.is_some() || proof.grant_expiry <= now() {
        return Err(OAuthIssueError::InvalidGrant);
    }
    let consumed = match proof.kind {
        store::CODE => proof.generation.is_some(),
        _ => proof.generation != Some(proof.credential_generation),
    };
    if consumed {
        store::revoke(site.db(), proof.grant, now())
            .await
            .map_err(store_failure("replay"))?;
        return Err(OAuthIssueError::InvalidGrant);
    }
    if proof.credential_expiry <= now() || (proof.kind == store::CODE && proof.code_expiry <= now())
    {
        return Err(OAuthIssueError::InvalidGrant);
    }
    issuer
        .verify_account(site, &proof.subject)
        .await
        .map_err(|error| match error.status() {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => OAuthIssueError::InvalidGrant,
            _ => OAuthIssueError::Unavailable,
        })?;
    let at = now();
    let (reply, rows) = credentials(&proof, at)?;
    if !store::advance(site.db(), &proof, &rows, at)
        .await
        .map_err(store_failure("token_advance"))?
    {
        reject_after_race(site, issuer, &proof, at).await?;
        return Err(OAuthIssueError::InvalidGrant);
    }
    Ok(reply)
}

/// Revokes only when a validated proof lost to a committed generation advance.
async fn reject_after_race(
    site: &Site,
    issuer: &IssuerRuntime,
    proof: &Proof,
    at: i64,
) -> Result<(), OAuthIssueError> {
    let current = store::lookup(site.db(), &issuer.issuer, proof.digest.clone())
        .await
        .map_err(store_failure("replay_lookup"))?;
    if current.is_some_and(|row| {
        row.grant == proof.grant
            && row.revoked.is_none()
            && row
                .generation
                .is_some_and(|generation| generation > proof.credential_generation)
    }) {
        store::revoke(site.db(), proof.grant, at)
            .await
            .map_err(store_failure("replay"))?;
    }
    Ok(())
}

/// Keeps opaque credentials outside SQL and bounds access lifetime by the absolute grant expiry.
fn credentials(
    proof: &Proof,
    at: i64,
) -> Result<(TokenReply, [CredentialRow; 2]), OAuthIssueError> {
    let expiry = (at + ACCESS_TTL).min(proof.grant_expiry);
    if expiry <= at {
        return Err(OAuthIssueError::InvalidGrant);
    }
    let access = random_credential()?;
    let refresh = random_credential()?;
    let row = |raw: &str, kind, expires_at| -> Result<_, OAuthIssueError> {
        Ok(CredentialRow {
            digest: credential_digest(raw).ok_or(OAuthIssueError::Unavailable)?,
            grant_id: proof.grant,
            kind,
            generation: proof
                .credential_generation
                .checked_add(1)
                .ok_or(OAuthIssueError::Unavailable)?,
            expires_at,
        })
    };
    let rows = [
        row(&access, store::ACCESS, expiry)?,
        row(&refresh, store::REFRESH, proof.grant_expiry)?,
    ];
    Ok((
        TokenReply {
            access_token: access,
            refresh_token: refresh,
            token_type: "Bearer",
            expires_in: expiry - at,
            scope: proof.scopes.clone(),
        },
        rows,
    ))
}

async fn revocation(
    State(site): State<Site>,
    request: Request,
) -> Result<Response, OAuthIssueError> {
    let issuer = site.oauth()?.issuer;
    let (headers, form) = read_form(request).await?;
    let client = authenticate_client(issuer, &headers, &form)?;
    let raw = form
        .get("token")
        .filter(|raw| !raw.is_empty())
        .ok_or(OAuthIssueError::InvalidRequest)?;
    if let Some(digest) = credential_digest(raw) {
        let proof = store::lookup(site.db(), &issuer.issuer, digest)
            .await
            .map_err(store_failure("revoke_lookup"))?;
        if let Some(proof) =
            proof.filter(|proof| proof.client == client && proof.kind != store::CODE)
        {
            store::revoke(site.db(), proof.grant, now())
                .await
                .map_err(store_failure("revoke"))?;
        }
    }
    Ok(no_store(StatusCode::OK))
}

/// Requires bounded UTF-8 form data without accepting JSON or URL-query client secrets.
async fn read_form(request: Request) -> Result<(HeaderMap, Parameters), OAuthIssueError> {
    let (parts, body) = request.into_parts();
    let mut types = parts.headers.get_all(header::CONTENT_TYPE).iter();
    let valid = types
        .next()
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|mime| {
                mime.trim()
                    .eq_ignore_ascii_case("application/x-www-form-urlencoded")
            })
        })
        && types.next().is_none();
    if !valid {
        return Err(OAuthIssueError::InvalidRequest);
    }
    let bytes = to_bytes(body, MAX_FORM)
        .await
        .map_err(|_| OAuthIssueError::InvalidRequest)?;
    Ok((parts.headers, Parameters::parse(&bytes)?))
}

pub(super) struct Parameters(BTreeMap<String, String>);

impl Parameters {
    /// Rejects duplicate fields and bounds work before URL decoding.
    pub(super) fn parse(input: &[u8]) -> Result<Self, OAuthIssueError> {
        if input.len() > MAX_FORM || !valid_form_encoding(input) {
            return Err(OAuthIssueError::InvalidRequest);
        }
        let mut fields = BTreeMap::new();
        for (key, value) in url::form_urlencoded::parse(input) {
            if fields.len() >= 24
                || !bounded(&key, 64)
                || value.chars().any(char::is_control)
                || fields
                    .insert(key.into_owned(), value.into_owned())
                    .is_some()
            {
                return Err(OAuthIssueError::InvalidRequest);
            }
        }
        Ok(Self(fields))
    }

    pub(super) fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }
}

/// Rejects malformed percent escapes and invalid UTF-8 before the lossy URL decoder.
fn valid_form_encoding(input: &[u8]) -> bool {
    input.iter().enumerate().all(|(index, byte)| {
        *byte != b'%'
            || input
                .get(index + 1..index + 3)
                .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
    }) && percent_encoding::percent_decode(input)
        .decode_utf8()
        .is_ok()
}

/// Selects one client authentication method; a body and Basic header cannot be mixed.
fn authenticate_client<'a>(
    issuer: &'a IssuerRuntime,
    headers: &HeaderMap,
    params: &Parameters,
) -> Result<&'a str, OAuthIssueError> {
    let mut authorization = headers.get_all(header::AUTHORIZATION).iter();
    let basic = authorization.next().map(decode_basic).transpose()?;
    if authorization.next().is_some()
        || (basic.is_some()
            && (params.get("client_id").is_some() || params.get("client_secret").is_some()))
    {
        return Err(OAuthIssueError::InvalidClient);
    }
    let (id, presented) = if let Some((id, secret)) = &basic {
        (id.as_str(), Some(secret.as_str()))
    } else {
        (
            params
                .get("client_id")
                .ok_or(OAuthIssueError::InvalidClient)?,
            params.get("client_secret"),
        )
    };
    let (name, client) = issuer
        .clients
        .get_key_value(id)
        .ok_or(OAuthIssueError::InvalidClient)?;
    match (client.secret.as_ref(), presented) {
        (None, None) => Ok(name),
        (Some(expected), Some(secret))
            if bool::from(
                expected
                    .as_bytes()
                    .ct_eq(blake3::hash(secret.as_bytes()).as_bytes()),
            ) =>
        {
            Ok(name)
        }
        _ => Err(OAuthIssueError::InvalidClient),
    }
}

/// Decodes RFC 6749's form-encoded Basic username and password, without logging them.
fn decode_basic(header: &HeaderValue) -> Result<(String, String), OAuthIssueError> {
    let bad = OAuthIssueError::InvalidClient;
    let header = header.to_str().map_err(|_| bad)?;
    let (scheme, encoded) = header.split_once(' ').ok_or(bad)?;
    if !scheme.eq_ignore_ascii_case("basic") || encoded.len() > 6 * 1024 {
        return Err(bad);
    }
    let decoded = STANDARD.decode(encoded).map_err(|_| bad)?;
    let (name, secret) = std::str::from_utf8(&decoded)
        .map_err(|_| bad)?
        .split_once(':')
        .ok_or(bad)?;
    let decode = |part: &str| {
        if !valid_form_encoding(part.as_bytes()) {
            return Err(bad);
        }
        let value = format!("v={part}");
        let mut values = url::form_urlencoded::parse(value.as_bytes());
        let (_, decoded) = values.next().ok_or(bad)?;
        if values.next().is_some() {
            return Err(bad);
        }
        Ok(decoded.into_owned())
    };
    Ok((decode(name)?, decode(secret)?))
}

pub(super) fn store_failure(operation: &'static str) -> impl FnOnce(DbError) -> OAuthIssueError {
    move |error| {
        log_store(operation, &error);
        OAuthIssueError::Unavailable
    }
}

pub(super) fn log_store(operation: &'static str, error: &DbError) {
    // Database details can contain application-owned inputs. Emit only the safe class.
    let class = match error {
        DbError::Connection { .. } => "connection",
        DbError::Timeout { .. } => "timeout",
        DbError::Integrity { .. } => "integrity",
        DbError::Serialization { .. } | DbError::Deadlock { .. } => "concurrency",
        _ => "database",
    };
    tracing::warn!(operation, class, "OAuth issuer store operation failed");
}
