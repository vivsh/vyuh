//! Bounded RFC 7591 registration and normalized operator policy.

use axum::{
    Json,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::{
    Site,
    auth::{AuthError, Scope, SecretRing, codecs::unique_json_fields},
};

use super::{
    IssuerRuntime, OAuthDcr, OAuthDcrAdmission, OAuthDcrMethod, OAuthIssueError,
    browser::parse_scopes,
    config::{bounded, exact, invalid, validate_redirect},
    protocol::{no_store, store_failure},
    runtime::{fill_random, now, random_credential},
    store::{self, RegistrationRow},
};

pub(super) const PATH: &str = "/oauth/register";
const PREFIX: &str = "vyuh_dcr_";
const MAX_BODY: usize = 24 * 1024;
const SLOT_ATTEMPTS: usize = 8;

pub(super) struct RegistrationPolicy {
    pub(super) name: String,
    pub(super) methods: Vec<OAuthDcrMethod>,
    redirects: Vec<String>,
    scopes: Vec<Scope>,
    allow_refresh: bool,
    initial_token: Option<blake3::Hash>,
}

impl RegistrationPolicy {
    /// Freezes policy and resolves the optional dedicated admission credential at build time.
    pub(super) fn build(
        config: &OAuthDcr,
        secrets: &SecretRing,
        resource_scopes: impl Fn(&Scope) -> bool,
    ) -> Result<Self, AuthError> {
        let valid = bounded(&config.name, 128)
            && valid_redirects(&config.redirects)
            && (1..=3).contains(&config.methods.len())
            && distinct(&config.methods)
            && distinct(&config.scopes)
            && crate::scopes::validate_scopes(&config.scopes).is_ok()
            && config.scopes.iter().all(resource_scopes);
        if !valid {
            return Err(invalid("invalid OAuth dynamic registration policy"));
        }
        let initial_token = match &config.admission {
            OAuthDcrAdmission::Unauthenticated => None,
            OAuthDcrAdmission::InitialAccessToken(source) => {
                if source.is_site_secret() {
                    return Err(invalid(
                        "OAuth registration must use a dedicated initial token",
                    ));
                }
                let secret = secrets.active(source)?;
                if !valid_bearer(&secret) {
                    return Err(invalid("invalid OAuth registration initial token"));
                }
                Some(blake3::hash(&secret))
            }
        };
        let mut scopes = config.scopes.clone();
        scopes.sort_unstable();
        Ok(Self {
            name: config.name.clone(),
            methods: config.methods.clone(),
            redirects: config.redirects.clone(),
            scopes,
            allow_refresh: config.allow_refresh,
            initial_token,
        })
    }

    pub(super) fn permits(
        &self,
        redirects: &[String],
        method: OAuthDcrMethod,
        refresh: bool,
        scopes: &[Scope],
    ) -> bool {
        self.permits_redirects(redirects)
            && self.methods.contains(&method)
            && (!refresh || self.allow_refresh)
            && scopes
                .iter()
                .all(|scope| self.scopes.binary_search(scope).is_ok())
    }

    fn permits_redirects(&self, redirects: &[String]) -> bool {
        valid_redirects(redirects) && redirects.iter().all(|uri| exact(&self.redirects, uri))
    }

    fn admits(&self, headers: &HeaderMap) -> bool {
        let mut headers = headers.get_all(header::AUTHORIZATION).iter();
        let first = headers.next();
        if headers.next().is_some() {
            return false;
        }
        match (self.initial_token, first) {
            (None, None) => true,
            (Some(digest), Some(value)) => {
                value.to_str().ok().and_then(bearer).is_some_and(|secret| {
                    bool::from(
                        digest
                            .as_bytes()
                            .ct_eq(blake3::hash(secret.as_bytes()).as_bytes()),
                    )
                })
            }
            _ => false,
        }
    }
}

pub(super) fn distinct<T: PartialEq>(values: &[T]) -> bool {
    values.iter().enumerate().all(|(index, item)| {
        !values
            .get(..index)
            .is_some_and(|previous| previous.contains(item))
    })
}

pub(super) fn valid_redirects(redirects: &[String]) -> bool {
    (1..=16).contains(&redirects.len())
        && distinct(redirects)
        && redirects.iter().all(|value| dynamic_redirect(value))
}

fn dynamic_redirect(value: &str) -> bool {
    value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_whitespace())
        && value.split_once("://").is_some_and(|(_, remainder)| {
            remainder
                .split(['/', '?', '#'])
                .next()
                .is_some_and(|authority| !authority.contains('@'))
        })
        && validate_redirect(value).is_ok()
}

pub(super) fn registration_id(value: &str) -> Option<Uuid> {
    let raw = value.strip_prefix(PREFIX)?;
    (raw.len() == 36).then_some(())?;
    let id = Uuid::parse_str(raw).ok()?;
    (id.hyphenated().encode_lower(&mut Uuid::encode_buffer()) == raw).then_some(id)
}

fn valid_bearer(secret: &[u8]) -> bool {
    (32..=1024).contains(&secret.len())
        && secret
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/=".contains(byte))
}

fn bearer(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && valid_bearer(token.as_bytes())).then_some(token)
}

#[derive(Deserialize)]
struct RegistrationRequest {
    #[serde(default)]
    redirect_uris: Vec<String>,
    token_endpoint_auth_method: Option<String>,
    grant_types: Option<Vec<String>>,
    response_types: Option<Vec<String>>,
    scope: Option<String>,
}

#[derive(Serialize)]
struct RegistrationReply<'a> {
    client_id: String,
    client_id_issued_at: i64,
    client_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_secret_expires_at: Option<u8>,
    redirect_uris: Vec<String>,
    token_endpoint_auth_method: &'static str,
    grant_types: &'static [&'static str],
    response_types: [&'static str; 1],
    #[serde(skip_serializing_if = "String::is_empty")]
    scope: String,
}

/// Validates an RFC 7591 request before allocating any identifier or credential.
pub(super) async fn register(State(site): State<Site>, request: Request) -> Response {
    let Ok(oauth) = site.oauth() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let issuer = oauth.issuer;
    let Some(policy) = &issuer.dcr else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !policy.admits(request.headers()) {
        return admission_error(policy.initial_token.is_some());
    }
    let input = match read_request(request).await {
        Ok(input) => input,
        Err(error) => return bad_request(error),
    };
    match registration(&site, issuer, policy, input).await {
        Ok(reply) => no_store((StatusCode::CREATED, Json(reply))),
        Err(error) => error,
    }
}

/// Requires JSON with unique members and rejects extensions that request unsupported proof modes.
async fn read_request(request: Request) -> Result<RegistrationRequest, &'static str> {
    let (parts, body) = request.into_parts();
    let mut types = parts.headers.get_all(header::CONTENT_TYPE).iter();
    let kind = types.next().and_then(|value| value.to_str().ok());
    if !kind.is_some_and(|kind| {
        kind.split(';')
            .next()
            .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
    }) || types.next().is_some()
    {
        return Err("invalid_client_metadata");
    }
    let bytes = to_bytes(body, MAX_BODY)
        .await
        .map_err(|_| "invalid_client_metadata")?;
    decode_request(&bytes)
}

/// Validates registration metadata types and rejects unsupported credential declarations.
fn decode_request(bytes: &[u8]) -> Result<RegistrationRequest, &'static str> {
    unique_json_fields(bytes).map_err(|_| "invalid_client_metadata")?;
    let fields: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| "invalid_client_metadata")?;
    if fields.get("software_statement").is_some() {
        return Err("unapproved_software_statement");
    }
    if [
        "client_secret",
        "jwks",
        "jwks_uri",
        "token_endpoint_auth_methods_supported",
    ]
    .iter()
    .any(|name| fields.get(name).is_some())
    {
        return Err("invalid_client_metadata");
    }
    if fields.get("redirect_uris").is_some_and(|value| {
        !value
            .as_array()
            .is_some_and(|uris| uris.iter().all(serde_json::Value::is_string))
    }) {
        return Err("invalid_redirect_uri");
    }
    if [
        "token_endpoint_auth_method",
        "grant_types",
        "response_types",
        "scope",
    ]
    .iter()
    .any(|field| fields.get(field).is_some_and(serde_json::Value::is_null))
    {
        return Err("invalid_client_metadata");
    }
    serde_json::from_value(fields).map_err(|_| "invalid_client_metadata")
}

/// Validates effective metadata and constructs the one-time credential response.
async fn registration<'a>(
    site: &Site,
    issuer: &IssuerRuntime,
    policy: &'a RegistrationPolicy,
    input: RegistrationRequest,
) -> Result<RegistrationReply<'a>, Response> {
    let invalid = || bad_request("invalid_client_metadata");
    let (method, refresh, scope) = validated(&input, policy).map_err(bad_request)?;
    let issued_at = now();
    let mut row = RegistrationRow {
        id: Uuid::nil(),
        issuer: issuer.issuer.clone(),
        slot: 0,
        redirects: serde_json::to_string(&input.redirect_uris).map_err(|_| invalid())?,
        auth_method: method.wire().into(),
        secret_digest: None,
        allow_refresh: refresh,
        scopes: scope.clone(),
        issued_at,
    };
    let secret = allocate(site, issuer, &mut row, method).await?;
    Ok(RegistrationReply {
        client_id: format!("{PREFIX}{}", row.id),
        client_id_issued_at: issued_at,
        client_name: &policy.name,
        client_secret_expires_at: secret.as_ref().map(|_| 0),
        client_secret: secret,
        redirect_uris: input.redirect_uris,
        token_endpoint_auth_method: method.wire(),
        grant_types: if refresh {
            &["authorization_code", "refresh_token"]
        } else {
            &["authorization_code"]
        },
        response_types: ["code"],
        scope,
    })
}

/// Applies the operator's policy to effective values, including RFC defaults.
fn validated(
    input: &RegistrationRequest,
    policy: &RegistrationPolicy,
) -> Result<(OAuthDcrMethod, bool, String), &'static str> {
    let invalid = "invalid_client_metadata";
    if !policy.permits_redirects(&input.redirect_uris) {
        return Err("invalid_redirect_uri");
    }
    let method = OAuthDcrMethod::parse(
        input
            .token_endpoint_auth_method
            .as_deref()
            .unwrap_or("client_secret_basic"),
    )
    .ok_or(invalid)?;
    let refresh = grant_types(input.grant_types.as_deref()).ok_or(invalid)?;
    if input
        .response_types
        .as_deref()
        .is_some_and(|types| types != ["code"])
    {
        return Err(invalid);
    }
    let requested = input
        .scope
        .as_deref()
        .map(parse_scopes)
        .transpose()
        .map_err(|_| invalid)?;
    let scopes = requested.as_deref().unwrap_or(&policy.scopes);
    if !policy.permits(&input.redirect_uris, method, refresh, scopes) {
        return Err(invalid);
    }
    let scope = scopes
        .iter()
        .map(Scope::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    Ok((method, refresh, scope))
}

fn grant_types(types: Option<&[String]>) -> Option<bool> {
    let Some(types) = types else {
        return Some(false);
    };
    (types.len() <= 2
        && distinct(types)
        && exact(types, "authorization_code")
        && types
            .iter()
            .all(|value| matches!(value.as_str(), "authorization_code" | "refresh_token")))
    .then(|| exact(types, "refresh_token"))
}

/// Allocates a durable slot using bounded retries on acknowledged uniqueness conflicts only.
async fn allocate(
    site: &Site,
    issuer: &IssuerRuntime,
    row: &mut RegistrationRow,
    method: OAuthDcrMethod,
) -> Result<Option<String>, Response> {
    let secret = (method != OAuthDcrMethod::None)
        .then(random_credential)
        .transpose()
        .map_err(|_| OAuthIssueError::Unavailable.into_response())?;
    row.secret_digest = secret
        .as_ref()
        .map(|raw| blake3::hash(raw.as_bytes()).to_hex().to_string());
    for _ in 0..SLOT_ATTEMPTS {
        row.slot = store::free_slot(site.db(), &issuer.issuer)
            .await
            .map_err(|error| store_failure("registration_slot")(error).into_response())?
            .ok_or_else(|| OAuthIssueError::Unavailable.into_response())?;
        let mut id = [0; 16];
        fill_random(&mut id).map_err(|_| OAuthIssueError::Unavailable.into_response())?;
        row.id = uuid::Builder::from_random_bytes(id).into_uuid();
        if issuer.clients.contains_key(&format!("{PREFIX}{}", row.id)) {
            continue;
        }
        if store::register(site.db(), row)
            .await
            .map_err(|error| store_failure("register")(error).into_response())?
        {
            return Ok(secret);
        }
    }
    Err(OAuthIssueError::Unavailable.into_response())
}

fn bad_request(error: &'static str) -> Response {
    no_store((
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": error })),
    ))
}

fn admission_error(protected: bool) -> Response {
    if !protected {
        return bad_request("invalid_client_metadata");
    }
    no_store((
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"OAuth registration\""),
        )],
        Json(serde_json::json!({ "error": "invalid_token" })),
    ))
}
