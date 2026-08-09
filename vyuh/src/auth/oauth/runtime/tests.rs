//! Signed-token and discovery tests for the Huskarl-backed runtime.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, Request, StatusCode, header::AUTHORIZATION};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use huskarl_resource_server::core::{
    Error as HuskarlError, ErrorKind,
    http::{HttpClient, HttpResponse, Idempotency},
    platform::MaybeSendBoxFuture,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair as _},
};
use rsa::{RsaPrivateKey, pkcs1::EncodeRsaPrivateKey, traits::PublicKeyParts};
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;

use super::*;
use crate::auth::{Audience, AuthError, OAuthIdentityMapper, OAuthJwtAlgorithm, Scope};

const API: Audience = Audience::new("api");
const TOKEN_AUDIENCE: &str = "https://api.example.com";
const REPORTS_TOKEN_AUDIENCE: &str = "https://api.example.com/reports";
const ISSUER: &str = "https://issuer.example.com";
const OAUTH_METADATA: &str = "https://issuer.example.com/.well-known/oauth-authorization-server";
const OIDC_METADATA: &str = "https://issuer.example.com/.well-known/openid-configuration";
const JWKS: &str = "https://issuer.example.com/jwks";
const APP_READ: Scope = Scope::of("app:read");

struct ScopedMapper;

impl OAuthIdentityMapper for ScopedMapper {
    async fn map(&self, claims: &OAuthClaims) -> Result<AuthUser, AuthError> {
        Ok(AuthUser::new(&claims.subject).with_scope(APP_READ))
    }
}

struct InvalidMapper;

impl OAuthIdentityMapper for InvalidMapper {
    async fn map(&self, claims: &OAuthClaims) -> Result<AuthUser, AuthError> {
        Ok(AuthUser::new(&claims.subject).with_scope(Scope::new("invalid scope")))
    }
}

#[derive(Clone)]
enum Reply {
    Json(Bytes),
    Sequence(Vec<Bytes>),
    Status(StatusCode),
}

#[derive(Clone)]
struct FakeClient {
    replies: Arc<BTreeMap<String, Reply>>,
    calls: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl FakeClient {
    fn new(replies: BTreeMap<String, Reply>) -> Self {
        Self {
            replies: Arc::new(replies),
            calls: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn calls(&self, uri: &str) -> Result<usize, TestError> {
        let calls = self.calls.lock().map_err(|_| TestError::Poisoned)?;
        Ok(calls.get(uri).copied().unwrap_or_default())
    }
}

impl HttpClient for FakeClient {
    fn execute(
        &self,
        request: Request<Bytes>,
        _idempotency: Idempotency,
    ) -> MaybeSendBoxFuture<'_, Result<HttpResponse, HuskarlError>> {
        Box::pin(async move {
            let uri = request.uri().to_string();
            let call = {
                let mut calls = self
                    .calls
                    .lock()
                    .map_err(|_| test_error(TestError::Poisoned))?;
                let count = calls.entry(uri.clone()).or_default();
                let current = *count;
                *count += 1;
                current
            };
            match self.replies.get(&uri) {
                Some(Reply::Json(body)) => Ok(HttpResponse {
                    status: StatusCode::OK,
                    headers: HeaderMap::new(),
                    body: body.clone(),
                }),
                Some(Reply::Sequence(values)) => values
                    .get(call)
                    .or_else(|| values.last())
                    .cloned()
                    .map(|body| HttpResponse {
                        status: StatusCode::OK,
                        headers: HeaderMap::new(),
                        body,
                    })
                    .ok_or_else(|| test_error(TestError::UnexpectedUri)),
                Some(Reply::Status(status)) => Err(super::super::http::status_error(*status)),
                None => Err(test_error(TestError::UnexpectedUri)),
            }
        })
    }
}

#[derive(Debug, Error)]
enum TestError {
    #[error("test HTTP state was poisoned")]
    Poisoned,
    #[error("test HTTP client received an unexpected URI")]
    UnexpectedUri,
    #[error("test signing key could not be generated")]
    KeyGeneration,
    #[error("test token could not be encoded")]
    TokenEncoding,
    #[error("test signing key could not be exported")]
    KeyExport,
    #[error("test clock is before the Unix epoch")]
    Clock,
}

fn test_error(source: TestError) -> HuskarlError {
    HuskarlError::new(ErrorKind::Config, source)
}

struct TestKey {
    encoding: EncodingKey,
    public_x: String,
}

struct AlgorithmKey {
    encoding: EncodingKey,
    jwk: String,
    jwt: Algorithm,
    configured: OAuthJwtAlgorithm,
}

impl TestKey {
    fn generate() -> Result<Self, TestError> {
        let document = ring::signature::Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map_err(|_| TestError::KeyGeneration)?;
        let pair = ring::signature::Ed25519KeyPair::from_pkcs8(document.as_ref())
            .map_err(|_| TestError::KeyGeneration)?;
        Ok(Self {
            encoding: EncodingKey::from_ed_der(document.as_ref()),
            public_x: URL_SAFE_NO_PAD.encode(pair.public_key().as_ref()),
        })
    }
}

fn rsa_key() -> Result<AlgorithmKey, TestError> {
    let private =
        RsaPrivateKey::new(&mut rand08::rngs::OsRng, 2048).map_err(|_| TestError::KeyGeneration)?;
    let document = private.to_pkcs1_der().map_err(|_| TestError::KeyExport)?;
    let n = URL_SAFE_NO_PAD.encode(private.n().to_bytes_be());
    let e = URL_SAFE_NO_PAD.encode(private.e().to_bytes_be());
    Ok(AlgorithmKey {
        encoding: EncodingKey::from_rsa_der(document.as_bytes()),
        jwk: format!(
            r#"{{"kty":"RSA","n":"{n}","e":"{e}","kid":"key-1","use":"sig","alg":"RS256"}}"#
        ),
        jwt: Algorithm::RS256,
        configured: OAuthJwtAlgorithm::Rs256,
    })
}

fn ec_key() -> Result<AlgorithmKey, TestError> {
    let random = SystemRandom::new();
    let document = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &random)
        .map_err(|_| TestError::KeyGeneration)?;
    let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, document.as_ref())
        .map_err(|_| TestError::KeyGeneration)?;
    let point = pair.public_key().as_ref();
    if point.len() != 65 || point.first() != Some(&4) {
        return Err(TestError::KeyExport);
    }
    let x = point.get(1..33).ok_or(TestError::KeyExport)?;
    let y = point.get(33..65).ok_or(TestError::KeyExport)?;
    let x = URL_SAFE_NO_PAD.encode(x);
    let y = URL_SAFE_NO_PAD.encode(y);
    Ok(AlgorithmKey {
        encoding: EncodingKey::from_ec_der(document.as_ref()),
        jwk: format!(
            r#"{{"kty":"EC","crv":"P-256","x":"{x}","y":"{y}","kid":"key-1","use":"sig","alg":"ES256"}}"#
        ),
        jwt: Algorithm::ES256,
        configured: OAuthJwtAlgorithm::Es256,
    })
}

#[derive(Serialize)]
struct Claims<'a> {
    sub: &'a str,
    iss: &'a str,
    aud: &'a str,
    scope: &'a str,
    exp: u64,
    tenant: &'a str,
}

fn metadata() -> Bytes {
    Bytes::from(format!(
        r#"{{"issuer":"{ISSUER}","token_endpoint":"{ISSUER}/token","jwks_uri":"{JWKS}","response_types_supported":["code"]}}"#
    ))
}

fn jwk(key: &TestKey, kid: &str) -> String {
    format!(
        r#"{{"kty":"OKP","crv":"Ed25519","x":"{}","kid":"{kid}","use":"sig","alg":"EdDSA"}}"#,
        key.public_x
    )
}

fn jwks(keys: &[(&TestKey, &str)]) -> Bytes {
    let values = keys
        .iter()
        .map(|(key, kid)| jwk(key, kid))
        .collect::<Vec<_>>()
        .join(",");
    Bytes::from(format!(r#"{{"keys":[{values}]}}"#))
}

fn client(key: &TestKey, oauth_status: Option<StatusCode>) -> FakeClient {
    let mut replies = BTreeMap::from([
        (OIDC_METADATA.to_owned(), Reply::Json(metadata())),
        (JWKS.to_owned(), Reply::Json(jwks(&[(key, "key-1")]))),
    ]);
    let oauth = oauth_status.map_or_else(|| Reply::Json(metadata()), Reply::Status);
    replies.insert(OAUTH_METADATA.to_owned(), oauth);
    FakeClient::new(replies)
}

fn provider(required: &[&str]) -> OAuthResourceServer {
    let resource = OAuthResource::new(TOKEN_AUDIENCE)
        .advertise_scopes(["api.read", "api.write"])
        .require_scopes(required.iter().copied());
    OAuthResourceServer::discovery(ISSUER)
        .resource(API, resource)
        .algorithms([OAuthJwtAlgorithm::EdDsa])
}

fn now() -> Result<u64, TestError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| TestError::Clock)
}

fn token(key: &TestKey, kid: Option<&str>, scope: &str, exp: u64) -> Result<String, TestError> {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = kid.map(str::to_owned);
    encode(
        &header,
        &Claims {
            sub: "oauth-user",
            iss: ISSUER,
            aud: TOKEN_AUDIENCE,
            scope,
            exp,
            tenant: "one",
        },
        &key.encoding,
    )
    .map_err(|_| TestError::TokenEncoding)
}

fn custom_token(key: &TestKey, claims: &Value) -> Result<String, TestError> {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("key-1".to_owned());
    encode(&header, claims, &key.encoding).map_err(|_| TestError::TokenEncoding)
}

fn base_claims(exp: u64) -> Value {
    json!({
        "sub": "oauth-user",
        "iss": ISSUER,
        "aud": TOKEN_AUDIENCE,
        "scope": "api.read",
        "exp": exp,
    })
}

fn substituted_algorithm_token(exp: u64) -> Result<String, TestError> {
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("key-1".to_owned());
    encode(
        &header,
        &base_claims(exp),
        &EncodingKey::from_secret(b"untrusted-test-secret"),
    )
    .map_err(|_| TestError::TokenEncoding)
}

fn algorithm_token(key: &AlgorithmKey, exp: u64) -> Result<String, TestError> {
    let mut header = Header::new(key.jwt);
    header.kid = Some("key-1".to_owned());
    encode(&header, &base_claims(exp), &key.encoding).map_err(|_| TestError::TokenEncoding)
}

fn algorithm_client(key: &AlgorithmKey) -> FakeClient {
    let keys = Bytes::from(format!(r#"{{"keys":[{}]}}"#, key.jwk));
    FakeClient::new(BTreeMap::from([
        (OAUTH_METADATA.to_owned(), Reply::Json(metadata())),
        (JWKS.to_owned(), Reply::Json(keys)),
    ]))
}

fn token_with_duplicate_subject(key: &TestKey, exp: u64) -> Result<String, TestError> {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"EdDSA","kid":"key-1"}"#);
    let claims = URL_SAFE_NO_PAD.encode(format!(
        r#"{{"sub":"one","sub":"two","iss":"{ISSUER}","aud":"{}","exp":{exp}}}"#,
        TOKEN_AUDIENCE
    ));
    let input = format!("{header}.{claims}");
    let signature = jsonwebtoken::crypto::sign(input.as_bytes(), &key.encoding, Algorithm::EdDSA)
        .map_err(|_| TestError::TokenEncoding)?;
    Ok(format!("{input}.{signature}"))
}

fn token_with_critical_header(key: &TestKey, exp: u64) -> Result<String, TestError> {
    let header = URL_SAFE_NO_PAD
        .encode(r#"{"alg":"EdDSA","kid":"key-1","crit":["unsupported"],"unsupported":true}"#);
    let claims = URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&base_claims(exp)).map_err(|_| TestError::TokenEncoding)?);
    let input = format!("{header}.{claims}");
    let signature = jsonwebtoken::crypto::sign(input.as_bytes(), &key.encoding, Algorithm::EdDSA)
        .map_err(|_| TestError::TokenEncoding)?;
    Ok(format!("{input}.{signature}"))
}

fn parts(token: &str) -> Result<axum::http::request::Parts, TestError> {
    let request = Request::builder()
        .uri("/resource")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(())
        .map_err(|_| TestError::UnexpectedUri)?;
    Ok(request.into_parts().0)
}

async fn runtime(conf: OAuthResourceServer, client: FakeClient) -> Result<OAuthRuntime, AuthError> {
    OAuthRuntime::build_with_client(ProviderId::new("oauth")?, conf, client).await
}

async fn authenticate_token(
    runtime: &OAuthRuntime,
    token: &str,
) -> Result<Result<AuthUser, AuthError>, TestError> {
    let audience = AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?;
    Ok(runtime.authenticate(token, &parts(token)?, &audience).await)
}

/// RFC 8414 discovery initializes one shared JWKS verifier and validates a signed token.
#[tokio::test]
async fn validates_signed_oauth_token() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let client = client(&key, None);
    let runtime = runtime(provider(&["api.read"]), client.clone())
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&key, Some("key-1"), "api.read", now()?.saturating_add(300))?;
    let user = runtime
        .authenticate(
            &token,
            &parts(&token)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    assert_eq!(user.key.to_string(), "oauth-user");
    assert!(user.scopes().is_empty());
    assert_eq!(client.calls(OAUTH_METADATA)?, 1);
    assert_eq!(client.calls(JWKS)?, 1);
    Ok(())
}

/// RSA and EC providers validate real signatures through the shared private verifier.
#[tokio::test]
async fn validates_rsa_and_ec_oauth_tokens() -> Result<(), TestError> {
    for key in [rsa_key()?, ec_key()?] {
        let conf = OAuthResourceServer::discovery(ISSUER)
            .resource(API, OAuthResource::new(TOKEN_AUDIENCE))
            .algorithms([key.configured]);
        let runtime = runtime(conf, algorithm_client(&key))
            .await
            .map_err(|_| TestError::UnexpectedUri)?;
        let token = algorithm_token(&key, now()?.saturating_add(300))?;
        assert!(authenticate_token(&runtime, &token).await?.is_ok());
    }
    Ok(())
}

/// Missing RFC 8414 endpoints fall back narrowly to OIDC discovery.
#[tokio::test]
async fn falls_back_to_oidc_only_for_missing_oauth_metadata() -> Result<(), TestError> {
    for status in [StatusCode::NOT_FOUND, StatusCode::GONE] {
        let key = TestKey::generate()?;
        let client = client(&key, Some(status));
        runtime(provider(&[]), client.clone())
            .await
            .map_err(|_| TestError::UnexpectedUri)?;
        assert_eq!(client.calls(OAUTH_METADATA)?, 1);
        assert_eq!(client.calls(OIDC_METADATA)?, 1);
    }
    Ok(())
}

/// Server failures do not trigger the OIDC compatibility fallback.
#[tokio::test]
async fn does_not_fallback_after_server_failure() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let client = client(&key, Some(StatusCode::INTERNAL_SERVER_ERROR));
    let result = runtime(provider(&[]), client.clone()).await;
    assert!(matches!(
        result,
        Err(AuthError::InvalidProviderConfig(message))
            if message.contains("oauth") && message.contains("metadata discovery")
    ));
    assert_eq!(client.calls(OIDC_METADATA)?, 0);
    Ok(())
}

/// Malformed RFC 8414 metadata fails startup without trying OIDC discovery.
#[tokio::test]
async fn does_not_fallback_after_malformed_metadata() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let replies = BTreeMap::from([
        (
            OAUTH_METADATA.to_owned(),
            Reply::Json(Bytes::from_static(b"{")),
        ),
        (OIDC_METADATA.to_owned(), Reply::Json(metadata())),
        (JWKS.to_owned(), Reply::Json(jwks(&[(&key, "key-1")]))),
    ]);
    let client = FakeClient::new(replies);
    assert!(runtime(provider(&[]), client.clone()).await.is_err());
    assert_eq!(client.calls(OIDC_METADATA)?, 0);
    Ok(())
}

/// A single compatible JWK accepts a token without a key ID.
#[tokio::test]
async fn accepts_missing_kid_when_key_is_unambiguous() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&key, None, "", now()?.saturating_add(300))?;
    assert!(
        runtime
            .authenticate(
                &token,
                &parts(&token)?,
                &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
            )
            .await
            .is_ok()
    );
    Ok(())
}

/// Expired credentials retain Vyuh's specific safe error classification.
#[tokio::test]
async fn maps_expired_tokens() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&key, Some("key-1"), "", now()?.saturating_sub(60))?;
    let result = runtime
        .authenticate(
            &token,
            &parts(&token)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await;
    assert!(matches!(result, Err(AuthError::ExpiredCredential)));
    Ok(())
}

/// Issuer, audience, not-before, and subject checks remain inside Huskarl validation.
#[tokio::test]
async fn rejects_invalid_registered_claims() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let expiry = now()?.saturating_add(300);
    let cases = invalid_claim_cases(expiry);
    for claims in cases {
        let token = custom_token(&key, &claims)?;
        assert!(matches!(
            authenticate_token(&runtime, &token).await?,
            Err(AuthError::InvalidCredential)
        ));
    }
    Ok(())
}

fn invalid_claim_cases(expiry: u64) -> [Value; 5] {
    let issuer = registered_claims(
        Some("oauth-user"),
        "https://other.example.com",
        TOKEN_AUDIENCE,
        expiry,
        None,
    );
    let audience = registered_claims(
        Some("oauth-user"),
        ISSUER,
        "https://other.example.com/api",
        expiry,
        None,
    );
    let not_before = registered_claims(
        Some("oauth-user"),
        ISSUER,
        TOKEN_AUDIENCE,
        expiry,
        Some(expiry),
    );
    let subject = registered_claims(None, ISSUER, TOKEN_AUDIENCE, expiry, None);
    let mut expiration = base_claims(expiry);
    if let Some(values) = expiration.as_object_mut() {
        values.remove("exp");
    }
    [issuer, audience, not_before, subject, expiration]
}

fn registered_claims(
    subject: Option<&str>,
    issuer: &str,
    audience: &str,
    expiry: u64,
    not_before: Option<u64>,
) -> Value {
    let mut claims = json!({
        "iss": issuer,
        "aud": audience,
        "scope": "api.read",
        "exp": expiry,
    });
    if let Some(values) = claims.as_object_mut() {
        if let Some(subject) = subject {
            values.insert("sub".into(), json!(subject));
        }
        if let Some(not_before) = not_before {
            values.insert("nbf".into(), json!(not_before));
        }
    }
    claims
}

/// Tokens using an algorithm outside the configured allowlist fail before mapping.
#[tokio::test]
async fn rejects_algorithm_substitution() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = substituted_algorithm_token(now()?.saturating_add(300))?;
    assert!(matches!(
        authenticate_token(&runtime, &token).await?,
        Err(AuthError::InvalidCredential)
    ));
    Ok(())
}

/// Malformed Bearer values fail as credentials and never become provider outages.
#[tokio::test]
async fn rejects_malformed_jwt() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    assert!(matches!(
        authenticate_token(&runtime, "not-a-jwt").await?,
        Err(AuthError::InvalidCredential)
    ));
    Ok(())
}

/// Upstream OAuth scopes are checked before application identity mapping.
#[tokio::test]
async fn rejects_insufficient_upstream_scope() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&["api.write"]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&key, Some("key-1"), "api.read", now()?.saturating_add(300))?;
    let result = runtime
        .authenticate(
            &token,
            &parts(&token)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await;
    assert!(matches!(result, Err(AuthError::InsufficientScope)));
    Ok(())
}

/// Duplicate claim names are rejected before Huskarl's normal Serde decoding.
#[tokio::test]
async fn rejects_duplicate_claim_names() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token_with_duplicate_subject(&key, now()?.saturating_add(300))?;
    let result = runtime
        .authenticate(
            &token,
            &parts(&token)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await;
    assert!(matches!(result, Err(AuthError::InvalidCredential)));
    Ok(())
}

/// Unrecognized critical JOSE headers are rejected by the private validator.
#[tokio::test]
async fn rejects_unrecognized_critical_headers() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token_with_critical_header(&key, now()?.saturating_add(300))?;
    assert!(matches!(
        authenticate_token(&runtime, &token).await?,
        Err(AuthError::InvalidCredential)
    ));
    Ok(())
}

/// A forged signature for a known key does not trigger a JWKS refresh.
#[tokio::test]
async fn invalid_signature_does_not_refresh_jwks() -> Result<(), TestError> {
    let trusted = TestKey::generate()?;
    let forged = TestKey::generate()?;
    let client = client(&trusted, None);
    let runtime = runtime(provider(&[]), client.clone())
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&forged, Some("key-1"), "", now()?.saturating_add(300))?;
    let result = runtime
        .authenticate(
            &token,
            &parts(&token)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await;
    assert!(matches!(result, Err(AuthError::InvalidCredential)));
    assert_eq!(client.calls(JWKS)?, 1);
    Ok(())
}

/// Multiple audience validators share one initial JWKS verifier and fetch.
#[tokio::test]
async fn resources_share_initial_jwks_fetch() -> Result<(), TestError> {
    const REPORTS: Audience = Audience::new("https://api.example.com/reports");
    let key = TestKey::generate()?;
    let client = client(&key, None);
    let conf = provider(&[]).resource(REPORTS, OAuthResource::new(REPORTS_TOKEN_AUDIENCE));
    runtime(conf, client.clone())
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    assert_eq!(client.calls(JWKS)?, 1);
    Ok(())
}

/// Multiple algorithm-compatible keys require a key ID for unambiguous selection.
#[tokio::test]
async fn rejects_missing_kid_with_ambiguous_keys() -> Result<(), TestError> {
    let first = TestKey::generate()?;
    let second = TestKey::generate()?;
    let replies = BTreeMap::from([
        (OAUTH_METADATA.to_owned(), Reply::Json(metadata())),
        (
            JWKS.to_owned(),
            Reply::Json(jwks(&[(&first, "key-1"), (&second, "key-2")])),
        ),
    ]);
    let runtime = runtime(provider(&[]), FakeClient::new(replies))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&first, None, "", now()?.saturating_add(300))?;
    let result = runtime
        .authenticate(
            &token,
            &parts(&token)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await;
    assert!(matches!(result, Err(AuthError::InvalidCredential)));
    Ok(())
}

/// An unknown key ID refreshes once and accepts a newly published signing key.
#[tokio::test]
async fn refreshes_jwks_for_unknown_key() -> Result<(), TestError> {
    let first = TestKey::generate()?;
    let second = TestKey::generate()?;
    let replies = BTreeMap::from([
        (OAUTH_METADATA.to_owned(), Reply::Json(metadata())),
        (
            JWKS.to_owned(),
            Reply::Sequence(vec![
                jwks(&[(&first, "key-1")]),
                jwks(&[(&first, "key-1"), (&second, "key-2")]),
            ]),
        ),
    ]);
    let client = FakeClient::new(replies);
    let runtime = runtime(provider(&[]), client.clone())
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&second, Some("key-2"), "", now()?.saturating_add(300))?;
    assert!(
        runtime
            .authenticate(
                &token,
                &parts(&token)?,
                &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
            )
            .await
            .is_ok()
    );
    assert_eq!(client.calls(JWKS)?, 2);
    Ok(())
}

/// Concurrent unknown keys share one refresh and subsequent attempts are throttled.
#[tokio::test]
async fn coalesces_and_throttles_unknown_key_refresh() -> Result<(), TestError> {
    let trusted = TestKey::generate()?;
    let unknown = TestKey::generate()?;
    let client = client(&trusted, None);
    let runtime = runtime(provider(&[]), client.clone())
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&unknown, Some("unknown"), "", now()?.saturating_add(300))?;
    let attempts = (0..8).map(|_| authenticate_token(&runtime, &token));
    let results = futures::future::join_all(attempts).await;
    assert!(
        results
            .into_iter()
            .all(|result| matches!(result, Ok(Err(AuthError::InvalidCredential))))
    );
    assert!(matches!(
        authenticate_token(&runtime, &token).await?,
        Err(AuthError::InvalidCredential)
    ));
    assert_eq!(client.calls(JWKS)?, 2);
    Ok(())
}

/// A custom mapper deliberately grants application scopes after upstream validation.
#[tokio::test]
async fn custom_mapper_assigns_application_scopes() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let conf = provider(&[]).mapper(ScopedMapper);
    let runtime = runtime(conf, client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&key, Some("key-1"), "api.read", now()?.saturating_add(300))?;
    let user = runtime
        .authenticate(
            &token,
            &parts(&token)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    assert!(user.has_scope(&APP_READ));
    Ok(())
}

/// Invalid application identities are rejected after successful OAuth verification.
#[tokio::test]
async fn rejects_invalid_mapper_identity() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let conf = provider(&[]).mapper(InvalidMapper);
    let runtime = runtime(conf, client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let token = token(&key, Some("key-1"), "", now()?.saturating_add(300))?;
    assert!(matches!(
        authenticate_token(&runtime, &token).await?,
        Err(AuthError::InvalidCredential)
    ));
    Ok(())
}

/// Encoded credentials are bounded before duplicate checking or cryptographic parsing.
#[tokio::test]
async fn rejects_oversized_credentials_before_validation() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(provider(&[]), client(&key, None))
        .await
        .map_err(|_| TestError::UnexpectedUri)?;
    let presented = "x".repeat(MAX_OAUTH_CREDENTIAL_BYTES + 1);
    let placeholder = token(&key, Some("key-1"), "", now()?.saturating_add(300))?;
    let result = runtime
        .authenticate(
            &presented,
            &parts(&placeholder)?,
            &AudienceId::declared(API).map_err(|_| TestError::UnexpectedUri)?,
        )
        .await;
    assert!(matches!(result, Err(AuthError::InvalidCredential)));
    Ok(())
}
