//! Exact, allowlisted HTTPS metadata resolution for OAuth clients.

use std::{io, net::IpAddr, time::Duration};

use futures::StreamExt;
use reqwest::{Client, StatusCode, dns, header, redirect};
use serde::Deserialize;
use serde_json::{Value, value::RawValue};
use tokio::{
    net::lookup_host,
    time::{Instant, timeout_at},
};
use url::{Host, Url};

use crate::auth::{AuthError, Scope, codecs::unique_json_fields};

use super::{
    OAuthCimdAuth, OAuthCimdClient, OAuthIssueError,
    assertion::{ALGORITHMS, key_set},
    browser::parse_scopes,
    config::{bounded, exact, invalid},
    registration::{distinct, valid_redirects},
};

const LIMIT: usize = 32 * 1024;
pub(super) const DEADLINE: Duration = Duration::from_secs(5);
const BAD: OAuthIssueError = OAuthIssueError::InvalidClient;
const UNAVAILABLE: OAuthIssueError = OAuthIssueError::Unavailable;

pub(super) struct CimdMetadata {
    pub(super) redirects: Vec<String>,
    pub(super) refresh: bool,
    pub(super) scopes: Option<Vec<Scope>>,
    pub(super) algorithm: Option<String>,
    keys: Option<SigningKeys>,
}

enum SigningKeys {
    Inline(Vec<Value>),
    Remote(Url, Instant),
}

impl CimdMetadata {
    /// Fetches the current remote key set inside the original metadata operation's deadline.
    pub(super) async fn keys(
        &self,
        client: &Client,
    ) -> Result<std::borrow::Cow<'_, [Value]>, OAuthIssueError> {
        match self.keys.as_ref().ok_or(BAD)? {
            SigningKeys::Inline(keys) => Ok(std::borrow::Cow::Borrowed(keys)),
            SigningKeys::Remote(url, deadline) => {
                let raw = fetch(client, url.clone(), *deadline).await?;
                key_set(&raw).map(std::borrow::Cow::Owned)
            }
        }
    }
}

#[derive(Deserialize)]
struct Metadata {
    client_id: String,
    redirect_uris: Vec<String>,
    token_endpoint_auth_methods_supported: Option<Vec<String>>,
    token_endpoint_auth_method: Option<String>,
    token_endpoint_auth_signing_alg: Option<String>,
    grant_types: Option<Vec<String>>,
    response_types: Option<Vec<String>>,
    scope: Option<String>,
    jwks_uri: Option<String>,
    jwks: Option<Box<RawValue>>,
}

/// Builds the dedicated HTTPS connector using a DNS resolver that enforces egress policy.
pub(super) fn transport() -> Result<Client, AuthError> {
    Client::builder()
        .https_only(true)
        .no_proxy()
        .redirect(redirect::Policy::none())
        .retry(reqwest::retry::never())
        .pool_max_idle_per_host(0)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .timeout(DEADLINE)
        .dns_resolver2(PublicDns)
        .build()
        .map_err(|_| invalid("OAuth client metadata transport could not be built"))
}

/// Validates one exact trust rule before any request can use it.
pub(super) fn validate(rule: &OAuthCimdClient) -> Result<(), AuthError> {
    if document_url(&rule.client_id).is_none()
        || !bounded(&rule.name, 128)
        || !valid_redirects(&rule.redirects)
    {
        return Err(invalid("invalid OAuth client metadata rule"));
    }
    Ok(())
}

/// Resolves only the configured document under the dedicated HTTPS policy.
pub(super) async fn resolve(
    client: &Client,
    rule: &OAuthCimdClient,
) -> Result<CimdMetadata, OAuthIssueError> {
    let url = document_url(&rule.client_id).ok_or(BAD)?;
    let deadline = Instant::now() + DEADLINE;
    let raw = fetch(client, url.clone(), deadline).await?;
    parse(&raw, rule, &url, deadline)
}

/// Validates metadata and establishes the exact callback and authentication policy intersection.
fn parse(
    raw: &[u8],
    rule: &OAuthCimdClient,
    url: &Url,
    deadline: Instant,
) -> Result<CimdMetadata, OAuthIssueError> {
    let mut metadata = decode_metadata(raw)?;
    if metadata.client_id != rule.client_id
        || !valid_redirects(&metadata.redirect_uris)
        || !lists(&metadata)
        || !auth_method(&metadata, rule.auth)
    {
        return Err(BAD);
    }
    metadata
        .redirect_uris
        .retain(|uri| exact(&rule.redirects, uri));
    if metadata.redirect_uris.is_empty() {
        return Err(BAD);
    }
    let keys = signing_keys(&metadata, url, deadline)?;
    if rule.auth == OAuthCimdAuth::PrivateKeyJwt
        && (keys.is_none()
            || metadata
                .token_endpoint_auth_signing_alg
                .as_deref()
                .is_some_and(|value| !ALGORITHMS.contains(&value)))
    {
        return Err(BAD);
    }
    Ok(CimdMetadata {
        redirects: metadata.redirect_uris,
        refresh: metadata
            .grant_types
            .as_deref()
            .is_some_and(|grants| exact(grants, "refresh_token")),
        scopes: metadata
            .scope
            .as_deref()
            .map(parse_scopes)
            .transpose()
            .map_err(|_| BAD)?,
        algorithm: metadata.token_endpoint_auth_signing_alg,
        keys,
    })
}

/// Rejects ambiguous metadata and properties that could introduce unsupported credentials.
fn decode_metadata(raw: &[u8]) -> Result<Metadata, OAuthIssueError> {
    unique_json_fields(raw).map_err(|_| BAD)?;
    let fields: Value = serde_json::from_slice(raw).map_err(|_| BAD)?;
    if [
        "client_secret",
        "client_secret_expires_at",
        "software_statement",
    ]
    .iter()
    .any(|key| fields.get(key).is_some())
    {
        return Err(BAD);
    }
    if [
        "token_endpoint_auth_methods_supported",
        "token_endpoint_auth_method",
        "token_endpoint_auth_signing_alg",
        "grant_types",
        "response_types",
        "scope",
        "jwks_uri",
        "jwks",
    ]
    .iter()
    .any(|key| fields.get(key).is_some_and(Value::is_null))
    {
        return Err(BAD);
    }
    serde_json::from_slice(raw).map_err(|_| BAD)
}

fn signing_keys(
    metadata: &Metadata,
    document: &Url,
    deadline: Instant,
) -> Result<Option<SigningKeys>, OAuthIssueError> {
    match (metadata.jwks.as_deref(), metadata.jwks_uri.as_deref()) {
        (None, None) => Ok(None),
        (Some(jwks), None) => key_set(jwks.get().as_bytes())
            .map(SigningKeys::Inline)
            .map(Some),
        (None, Some(uri)) => {
            let url = document_url(uri)
                .filter(|url| url.origin() == document.origin())
                .ok_or(BAD)?;
            Ok(Some(SigningKeys::Remote(url, deadline)))
        }
        (Some(_), Some(_)) => Err(BAD),
    }
}

fn lists(metadata: &Metadata) -> bool {
    metadata
        .grant_types
        .as_deref()
        .is_none_or(|values| valid_list(values) && exact(values, "authorization_code"))
        && metadata
            .response_types
            .as_deref()
            .is_none_or(|values| valid_list(values) && exact(values, "code"))
}

fn valid_list(values: &[String]) -> bool {
    (1..=16).contains(&values.len())
        && distinct(values)
        && values.iter().all(|value| bounded(value, 256))
}

/// Applies singular/plural compatibility and selects only the operator's method.
fn auth_method(metadata: &Metadata, method: OAuthCimdAuth) -> bool {
    let allowed = match method {
        OAuthCimdAuth::Public => "none",
        OAuthCimdAuth::PrivateKeyJwt => "private_key_jwt",
    };
    let singular = metadata.token_endpoint_auth_method.as_deref();
    match metadata.token_endpoint_auth_methods_supported.as_deref() {
        None => singular.unwrap_or("none") == allowed,
        Some(offered) => {
            valid_list(offered)
                && offered
                    .iter()
                    .all(|method| !method.starts_with("client_secret"))
                && singular.is_none_or(|method| exact(offered, method))
                && exact(offered, allowed)
        }
    }
}

/// Accepts unambiguous DNS-hosted HTTPS URLs while keeping identifier comparison byte-for-byte.
pub(super) fn document_url(raw: &str) -> Option<Url> {
    if !bounded(raw, 1024) || !raw.is_ascii() || raw.contains(['\\', '?', '#']) {
        return None;
    }
    let (authority, path) = raw.strip_prefix("https://")?.split_once('/')?;
    if authority.contains('@') || path.is_empty() || path.split('/').any(dot_segment) {
        return None;
    }
    let url = Url::parse(raw).ok()?;
    let Host::Domain(name) = url.host()? else {
        return None;
    };
    if !name.contains('.')
        || name.ends_with('.')
        || name.ends_with(".localhost")
        || url.port() == Some(0)
    {
        return None;
    }
    if url.as_str() != raw {
        let base = authority.strip_suffix(":443")?;
        if url.as_str() != format!("https://{base}/{path}") {
            return None;
        }
    }
    Some(url)
}

fn dot_segment(raw: &str) -> bool {
    matches!(
        percent_encoding::percent_decode_str(raw)
            .decode_utf8()
            .as_deref(),
        Ok("." | "..")
    )
}

/// Sends one bounded JSON GET without forwarding request headers or following redirects.
async fn fetch(client: &Client, url: Url, deadline: Instant) -> Result<Vec<u8>, OAuthIssueError> {
    #[cfg(test)]
    if let Some(body) = tests::document(&url) {
        return body;
    }
    timeout_at(deadline, async {
        let response = client
            .get(url)
            .header(header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| UNAVAILABLE)?;
        read_json(response).await
    })
    .await
    .map_err(|_| UNAVAILABLE)?
}

/// Requires a successful JSON response and limits actual streamed bytes.
pub(super) async fn read_json(response: reqwest::Response) -> Result<Vec<u8>, OAuthIssueError> {
    if response.status() != StatusCode::OK {
        return Err(if response.status().is_server_error() {
            UNAVAILABLE
        } else {
            BAD
        });
    }
    let mut types = response.headers().get_all(header::CONTENT_TYPE).iter();
    let json = types
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|mime| {
            mime.eq_ignore_ascii_case("application/json")
                || mime.eq_ignore_ascii_case("application/jwk-set+json")
        });
    if !json
        || types.next().is_some()
        || response
            .content_length()
            .is_some_and(|length| length > LIMIT as u64)
    {
        return Err(BAD);
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| UNAVAILABLE)?;
        if bytes.len().saturating_add(chunk.len()) > LIMIT {
            return Err(BAD);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

struct PublicDns;

impl dns::Resolve for PublicDns {
    /// Supplies the connector with the vetted addresses directly. Mixed answers fail closed.
    fn resolve(&self, name: dns::Name) -> dns::Resolving {
        Box::pin(async move {
            let mut addrs = Vec::new();
            for addr in lookup_host((name.as_str(), 0)).await? {
                if addrs.len() == 32 || !public_address(addr.ip()) {
                    return Err(io::Error::other("invalid OAuth metadata destination").into());
                }
                addrs.push(addr);
            }
            if addrs.is_empty() {
                return Err(io::Error::other("no OAuth metadata destination").into());
            }
            Ok(Box::new(addrs.into_iter()) as dns::Addrs)
        })
    }
}

/// Allows public unicast only. IPv6 transition, mapped and special-use prefixes are excluded.
fn public_address(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(ip) => !matches!(
            ip.octets(),
            [0 | 10 | 127 | 224..=255, ..]
                | [100, 64..=127, ..]
                | [169, 254, ..]
                | [172, 16..=31, ..]
                | [192, 0, 0 | 2, _]
                | [192, 31, 196, _]
                | [192, 52, 193, _]
                | [192, 88, 99, _]
                | [192, 168, ..]
                | [192, 175, 48, _]
                | [198, 18 | 19, ..]
                | [198, 51, 100, _]
                | [203, 0, 113, _]
        ),
        IpAddr::V6(ip) => {
            let [a, b, c, ..] = ip.segments();
            (0x2000..=0x3fff).contains(&a)
                && !matches!(
                    (a, b, c),
                    (0x2001, 0..=0x01ff | 0x0db8, _)
                        | (0x2002, _, _)
                        | (0x2620, 0x004f, 0x8000)
                        | (0x3fff, 0..=0x0fff, _)
                )
        }
    }
}

#[cfg(test)]
#[path = "tests/cimd.rs"]
pub(super) mod tests;
