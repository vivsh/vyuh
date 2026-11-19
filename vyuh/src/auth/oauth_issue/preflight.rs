//! Opt-in, read-only diagnostics for a deployed OAuth-protected MCP endpoint.

use std::future::Future;

use reqwest::{Client, RequestBuilder, Response, StatusCode, header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncWriteExt;

use crate::{
    Data, Error, Site,
    auth::{Scope, codecs::unique_json_fields},
    bundles::{self, BundlePart},
    commands::CommandConf,
    mcp::{CURRENT_VERSION, protected_resource_url},
};

use super::{
    IssuerRuntime, OAuthCimdAuth, OAuthDcrMethod, assertion,
    cimd::{self, CimdMetadata, DEADLINE},
    config::{bounded, exact},
    protocol::{METADATA_PATH, REVOCATION_PATH, TOKEN_PATH},
    registration,
};

const SIGNED: &str = "signed-cimd";
const DCR: &str = "dcr";
const LOCAL: &str = "local_policy";
const CHALLENGE: &str = "mcp_challenge";
const RESOURCE: &str = "resource_metadata";
const ISSUER: &str = "issuer_metadata";
const DOCUMENT: &str = "cimd_document";
const KEYS: &str = "cimd_keys";
const REGISTRATION: &str = "dcr_advertisement";
const JWT: &str = "private_key_jwt";

/// Registers the opt-in `mcp:preflight` command for a configured OAuth issuer.
///
/// The command validates local policy and probes the configured public HTTPS resource.
/// Invalid policy, unavailable discovery, and metadata mismatches fail the command.
pub fn mcp_preflight() -> BundlePart {
    bundles::command(
        preflight,
        CommandConf::new("mcp:preflight")
            .description("Check a deployed OAuth-protected MCP resource."),
    )
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct PreflightArgs {
    /// Exact configured HTTPS MCP resource URL.
    resource: String,
    /// Registration mode, either signed-cimd or dcr.
    mode: String,
    /// Exact trusted metadata document URL for signed CIMD.
    client_id: Option<String>,
    /// Exact registered or operator-approved callback.
    redirect_uri: String,
    /// DCR token authentication method, using its OAuth wire name.
    token_auth_method: Option<String>,
    /// Scopes expected on the resource and selected client.
    #[serde(default)]
    scopes: Vec<String>,
    /// Require refresh support in discovery and the selected client policy.
    #[serde(default)]
    require_refresh: bool,
    /// Emit a JSON report for automated checks.
    #[serde(default)]
    json: bool,
}

#[derive(Serialize)]
struct PreflightReport {
    resource: String,
    mode: String,
    checks: Vec<PreflightCheck>,
}

#[derive(Serialize)]
struct PreflightCheck {
    id: &'static str,
    status: CheckStatus,
    detail: &'static str,
}

#[derive(Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Pass,
    Fail,
    Skipped,
}

impl PreflightReport {
    /// Initializes relevant stages so unsatisfied prerequisites remain visible in the report.
    fn new(mode: &str) -> Self {
        let ids: &[&str] = match mode {
            SIGNED => &[LOCAL, CHALLENGE, RESOURCE, ISSUER, DOCUMENT, KEYS],
            DCR => &[LOCAL, CHALLENGE, RESOURCE, ISSUER, REGISTRATION],
            _ => &[LOCAL],
        };
        Self {
            resource: String::new(),
            mode: match mode {
                SIGNED => SIGNED,
                DCR => DCR,
                _ => "",
            }
            .into(),
            checks: ids
                .iter()
                .map(|&id| PreflightCheck {
                    id,
                    status: CheckStatus::Skipped,
                    detail: "prerequisite_failed",
                })
                .collect(),
        }
    }

    /// Records a stage and returns its value only when the stage passes.
    fn record<T>(
        &mut self,
        id: &'static str,
        result: Result<T, &'static str>,
        detail: &'static str,
    ) -> Option<T> {
        if let Some(check) = self.checks.iter_mut().find(|check| check.id == id) {
            match result {
                Ok(_) => {
                    check.status = CheckStatus::Pass;
                    check.detail = detail;
                }
                Err(reason) => {
                    check.status = CheckStatus::Fail;
                    check.detail = reason;
                }
            }
        }
        result.ok()
    }

    fn result(&self) -> Result<(), Error> {
        if self
            .checks
            .iter()
            .any(|check| check.status != CheckStatus::Pass)
        {
            return Err(Error::invalid("MCP preflight checks failed"));
        }
        Ok(())
    }

    /// Renders the same ordered results for operators and scripts.
    fn render(&self, json: bool) -> Result<String, Error> {
        if json {
            return Ok(serde_json::to_string(self)?);
        }
        Ok(self
            .checks
            .iter()
            .map(|check| {
                let status = match check.status {
                    CheckStatus::Pass => "pass",
                    CheckStatus::Fail => "fail",
                    CheckStatus::Skipped => "skipped",
                };
                format!("{} {status} {}", check.id, check.detail)
            })
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

async fn preflight(site: Site, Data(args): Data<PreflightArgs>) -> Result<(), Error> {
    let report = inspect(&site, &args, send).await;
    let mut output = report.render(args.json)?;
    output.push('\n');
    let mut stdout = tokio::io::stdout();
    stdout.write_all(output.as_bytes()).await?;
    stdout.flush().await?;
    report.result()
}

async fn send(request: RequestBuilder) -> Result<Response, &'static str> {
    #[cfg(all(test, feature = "sqlite"))]
    if let Some(site) = tests::fixture_site() {
        return tests::route(&site, request).await;
    }
    request.send().await.map_err(|_| "https_probe_failed")
}

/// Checks local policy before constructing or sending any outbound request.
async fn inspect<F>(
    site: &Site,
    args: &PreflightArgs,
    send: impl Fn(RequestBuilder) -> F + Copy + Send + Sync,
) -> PreflightReport
where
    F: Future<Output = Result<Response, &'static str>> + Send,
{
    let mut report = PreflightReport::new(&args.mode);
    let local = site
        .oauth()
        .map_err(|_| "oauth_issuer_not_configured")
        .and_then(|oauth| local_policy(oauth.issuer, args).map(|_| oauth.issuer));
    let Some(issuer) = report.record(LOCAL, local, "approved_resource_and_client") else {
        return report;
    };
    report.resource.clone_from(&args.resource);
    let Some(client) = report.record(
        CHALLENGE,
        cimd::transport().map_err(|_| "https_setup_failed"),
        "bearer_challenge_verified",
    ) else {
        return report;
    };
    let Some(discovery) = discover(site, issuer, args, &client, send, &mut report).await else {
        return report;
    };
    if args.mode == SIGNED {
        check_cimd(issuer, args, &client, &discovery, &mut report).await;
    } else {
        report.record(
            REGISTRATION,
            check_dcr(issuer, args, &discovery),
            "registration_advertised",
        );
    }
    report
}

/// Probes only URLs derived from the issuer and resource validated by local policy.
async fn discover<F>(
    site: &Site,
    issuer: &IssuerRuntime,
    args: &PreflightArgs,
    client: &Client,
    send: impl Fn(RequestBuilder) -> F + Copy + Send + Sync,
    report: &mut PreflightReport,
) -> Option<Value>
where
    F: Future<Output = Result<Response, &'static str>> + Send,
{
    let challenge = probe_challenge(client, &args.resource, send).await;
    let metadata_url = report.record(CHALLENGE, challenge, "bearer_challenge_verified")?;
    let document = read_discovery(client, &metadata_url, send)
        .await
        .and_then(|value| check_resource(&value, args, issuer));
    report.record(RESOURCE, document, "resource_metadata_verified")?;
    let server_url = format!("{}{METADATA_PATH}", issuer.issuer);
    let discovery = read_discovery(client, &server_url, send)
        .await
        .and_then(|value| check_issuer(site, issuer, args, &value).map(|()| value));
    report.record(ISSUER, discovery, "issuer_metadata_verified")
}

/// Validates the input against the issuer that owns the deployment's OAuth routes.
fn local_policy(issuer: &IssuerRuntime, args: &PreflightArgs) -> Result<(), &'static str> {
    let resource = issuer
        .resource_scopes(&args.resource)
        .ok_or("resource_not_configured")?;
    let scopes: Vec<_> = args
        .scopes
        .iter()
        .map(|scope| Scope::new(scope.as_str()))
        .collect();
    if crate::scopes::validate_scopes(&scopes).is_err()
        || !registration::distinct(&scopes)
        || !scopes
            .iter()
            .all(|scope| resource.binary_search(scope).is_ok())
    {
        return Err("scope_not_configured");
    }
    match args.mode.as_str() {
        SIGNED if args.token_auth_method.is_none() => {
            let id = args.client_id.as_deref().ok_or("client_id_required")?;
            let rule = issuer.cimd.get(id).ok_or("client_id_not_trusted")?;
            if rule.auth != OAuthCimdAuth::PrivateKeyJwt {
                return Err("signed_client_required");
            }
            if !exact(&rule.redirects, &args.redirect_uri) {
                return Err("callback_not_trusted");
            }
        }
        DCR if args.client_id.is_none() => {
            let method = requested_dcr_method(args)?;
            let policy = issuer.dcr.as_ref().ok_or("dcr_not_configured")?;
            if !policy.permits(
                std::slice::from_ref(&args.redirect_uri),
                method,
                args.require_refresh,
                &scopes,
            ) {
                return Err("dcr_policy_rejected");
            }
        }
        _ => return Err("invalid_mode_or_arguments"),
    }
    Ok(())
}

fn requested_dcr_method(args: &PreflightArgs) -> Result<OAuthDcrMethod, &'static str> {
    args.token_auth_method
        .as_deref()
        .and_then(OAuthDcrMethod::parse)
        .ok_or("dcr_auth_method_required")
}

/// Confirms the deployed endpoint challenges without executing an MCP tool.
async fn probe_challenge<F>(
    client: &Client,
    resource: &str,
    send: impl Fn(RequestBuilder) -> F + Copy + Send + Sync,
) -> Result<String, &'static str>
where
    F: Future<Output = Result<Response, &'static str>> + Send,
{
    let expected = protected_resource_url(resource).map_err(|_| "invalid_resource_url")?;
    let response = send(
        client
            .post(resource)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", CURRENT_VERSION)
            .body(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#),
    )
    .await?;
    if response.status() != StatusCode::UNAUTHORIZED {
        return Err("expected_mcp_bearer_challenge");
    }
    let mut challenges = response
        .headers()
        .get_all(header::WWW_AUTHENTICATE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(bearer_params);
    let params = challenges.next().ok_or("missing_bearer_challenge")?;
    if challenges.next().is_some() || !callback_metadata(params, &expected) {
        return Err("resource_metadata_challenge_mismatch");
    }
    Ok(expected)
}

fn bearer_params(value: &str) -> Option<&str> {
    let (scheme, params) = value.trim().split_once([' ', '\t'])?;
    scheme.eq_ignore_ascii_case("bearer").then_some(params)
}

/// Reads the expected quoted parameter exactly once, including when other values contain commas.
fn callback_metadata(mut params: &str, expected: &str) -> bool {
    let mut found = false;
    loop {
        let Some((key, remainder)) = params.split_once('=') else {
            return false;
        };
        let key = key.trim();
        if key.is_empty() || !key.bytes().all(token_byte) {
            return false;
        }
        let Some((value, tail)) = challenge_value(remainder.trim_start()) else {
            return false;
        };
        if key.eq_ignore_ascii_case("resource_metadata") {
            if found || value != expected {
                return false;
            }
            found = true;
        }
        if tail.is_empty() {
            return found;
        }
        let Some(next) = tail.strip_prefix(',') else {
            return false;
        };
        params = next.trim_start();
    }
}

/// Returns a challenge parameter's raw value and the remaining parameter list.
fn challenge_value(value: &str) -> Option<(&str, &str)> {
    if let Some(quoted) = value.strip_prefix('"') {
        let mut escaping = false;
        for (index, byte) in quoted.bytes().enumerate() {
            match (byte, escaping) {
                (b'"', false) => {
                    return Some((quoted.get(..index)?, quoted.get(index + 1..)?.trim_start()));
                }
                (b'\\', false) => escaping = true,
                _ => escaping = false,
            }
        }
        return None;
    }
    let (token, tail) = value.split_at(value.find(',').unwrap_or(value.len()));
    let token = token.trim_end();
    (!token.is_empty() && token.bytes().all(token_byte)).then_some((token, tail))
}

fn token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^`_|~".contains(&byte)
}

/// Bounds discovery response processing and rejects ambiguous top-level metadata.
async fn read_discovery<F>(
    client: &Client,
    url: &str,
    send: impl Fn(RequestBuilder) -> F + Copy + Send + Sync,
) -> Result<Value, &'static str>
where
    F: Future<Output = Result<Response, &'static str>> + Send,
{
    tokio::time::timeout(DEADLINE, async {
        let response = send(client.get(url).header(header::ACCEPT, "application/json")).await?;
        let raw = cimd::read_json(response)
            .await
            .map_err(|_| "discovery_response_invalid")?;
        unique_json_fields(&raw).map_err(|_| "discovery_json_invalid")?;
        serde_json::from_slice(&raw).map_err(|_| "discovery_json_invalid")
    })
    .await
    .map_err(|_| "discovery_timeout")?
}

/// Requires the resource document to name exactly the configured issuer and requested scopes.
fn check_resource(
    value: &Value,
    args: &PreflightArgs,
    issuer: &IssuerRuntime,
) -> Result<(), &'static str> {
    let issuer_matches = value
        .get("authorization_servers")
        .and_then(Value::as_array)
        .is_some_and(|issuers| {
            issuers.len() == 1
                && issuers.first().and_then(Value::as_str) == Some(issuer.issuer.as_str())
        });
    if field(value, "resource") != Some(args.resource.as_str()) || !issuer_matches {
        return Err("resource_or_issuer_mismatch");
    }
    if !contains(value, "bearer_methods_supported", "header")
        || !supports_scopes(value, &args.scopes)
    {
        return Err("resource_scope_or_bearer_mismatch");
    }
    Ok(())
}

/// Compares deployed endpoints with exact URLs from the locally resolved authorization route.
fn check_issuer(
    site: &Site,
    issuer: &IssuerRuntime,
    args: &PreflightArgs,
    value: &Value,
) -> Result<(), &'static str> {
    let authorize = site
        .routes()
        .reverse_url(&issuer.authorize_route, &[])
        .ok_or("authorization_route_missing")?;
    if field(value, "issuer") != Some(issuer.issuer.as_str())
        || field(value, "authorization_endpoint") != Some(&format!("{}{authorize}", issuer.issuer))
        || field(value, "token_endpoint") != Some(&format!("{}{TOKEN_PATH}", issuer.issuer))
        || field(value, "revocation_endpoint")
            != Some(&format!("{}{REVOCATION_PATH}", issuer.issuer))
    {
        return Err("issuer_or_endpoint_mismatch");
    }
    if !contains(value, "code_challenge_methods_supported", "S256") {
        return Err("s256_not_advertised");
    }
    if !contains(value, "grant_types_supported", "authorization_code")
        || (args.require_refresh && !contains(value, "grant_types_supported", "refresh_token"))
        || !supports_scopes(value, &args.scopes)
        || value.get("authorization_response_iss_parameter_supported") != Some(&Value::Bool(true))
    {
        return Err("issuer_capability_mismatch");
    }
    Ok(())
}

/// Resolves the configured CIMD client and examines key eligibility without a signed assertion.
async fn check_cimd(
    issuer: &IssuerRuntime,
    args: &PreflightArgs,
    client: &Client,
    value: &Value,
    report: &mut PreflightReport,
) {
    let document = resolve_cimd(issuer, args, client, value).await;
    let Some(document) = report.record(DOCUMENT, document, "trusted_cimd_document_verified") else {
        return;
    };
    let keys = document
        .keys(client)
        .await
        .map_err(|_| "jwks_unavailable_or_invalid")
        .and_then(|keys| {
            keys.iter()
                .any(|key| {
                    field(key, "kid").is_some_and(|kid| bounded(kid, 128))
                        && assertion::ALGORITHMS.iter().any(|alg| {
                            document
                                .algorithm
                                .as_deref()
                                .is_none_or(|value| value == *alg)
                                && assertion::decoding_key(key, alg).is_ok()
                        })
                })
                .then_some(())
                .ok_or("no_usable_signing_key")
        });
    report.record(KEYS, keys, "signing_key_available");
}

/// Ensures the selected document permits the exact requested callback and scopes.
async fn resolve_cimd(
    issuer: &IssuerRuntime,
    args: &PreflightArgs,
    client: &Client,
    value: &Value,
) -> Result<CimdMetadata, &'static str> {
    if value.get("client_id_metadata_document_supported") != Some(&Value::Bool(true))
        || !contains(value, "token_endpoint_auth_methods_supported", JWT)
    {
        return Err("signed_cimd_not_advertised");
    }
    let rule = args
        .client_id
        .as_deref()
        .and_then(|id| issuer.cimd.get(id))
        .ok_or("client_id_not_trusted")?;
    let document = cimd::resolve(client, rule)
        .await
        .map_err(|_| "cimd_document_unavailable_or_invalid")?;
    if !exact(&document.redirects, &args.redirect_uri)
        || (args.require_refresh && !document.refresh)
        || document.scopes.as_ref().is_some_and(|scopes| {
            !args
                .scopes
                .iter()
                .all(|scope| scopes.iter().any(|candidate| candidate.as_str() == scope))
        })
    {
        return Err("cimd_client_policy_mismatch");
    }
    Ok(document)
}

/// Verifies the published registration URL and method without contacting the endpoint.
fn check_dcr(
    issuer: &IssuerRuntime,
    args: &PreflightArgs,
    value: &Value,
) -> Result<(), &'static str> {
    let method = requested_dcr_method(args)?;
    if field(value, "registration_endpoint")
        != Some(&format!("{}{}", issuer.issuer, registration::PATH))
        || !contains(
            value,
            "token_endpoint_auth_methods_supported",
            method.wire(),
        )
    {
        return Err("dcr_advertisement_mismatch");
    }
    Ok(())
}

fn field<'a>(value: &'a Value, name: &str) -> Option<&'a str> {
    value.get(name).and_then(Value::as_str)
}

fn contains(value: &Value, name: &str, expected: &str) -> bool {
    value
        .get(name)
        .and_then(Value::as_array)
        .is_some_and(|values| {
            values.iter().all(Value::is_string)
                && values.iter().any(|value| value.as_str() == Some(expected))
        })
}

fn supports_scopes(value: &Value, scopes: &[String]) -> bool {
    scopes
        .iter()
        .all(|scope| contains(value, "scopes_supported", scope))
}

#[cfg(test)]
#[path = "tests/preflight.rs"]
mod tests;
