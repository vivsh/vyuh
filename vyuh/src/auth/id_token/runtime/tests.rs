//! Signed-token tests for the external identity-token runtime.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{
    HeaderMap, Method, Request, StatusCode,
    header::{AUTHORIZATION, COOKIE, SET_COOKIE},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use huskarl_resource_server::core::{
    Error as HuskarlError, ErrorKind,
    http::{HttpClient, HttpResponse, Idempotency},
    platform::MaybeSendBoxFuture,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rsa::{RsaPrivateKey, pkcs1::EncodeRsaPrivateKey, traits::PublicKeyParts};
use serde::Serialize;
use thiserror::Error;

use super::*;
use crate::auth::{Audience, AuthProvider, CookieConf, IdTokenMapper};

const API: Audience = Audience::new("api");
const ISSUER: &str = "https://identity.example.com";
const TOKEN_AUDIENCE: &str = "https://app.example.com/system-token";
const METADATA: &str = "https://identity.example.com/.well-known/openid-configuration";
const JWKS: &str = "https://identity.example.com/jwks";

#[derive(Clone)]
struct FakeClient(Arc<BTreeMap<String, Bytes>>);

impl HttpClient for FakeClient {
    fn execute(
        &self,
        request: Request<Bytes>,
        _idempotency: Idempotency,
    ) -> MaybeSendBoxFuture<'_, Result<HttpResponse, HuskarlError>> {
        Box::pin(async move {
            let body = self
                .0
                .get(&request.uri().to_string())
                .cloned()
                .ok_or_else(unexpected_request)?;
            Ok(HttpResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body,
            })
        })
    }
}

struct Mapper;

impl IdTokenMapper for Mapper {
    async fn map(&self, claims: &IdTokenClaims) -> Result<AuthUser, AuthError> {
        if claims.claim("email").and_then(serde_json::Value::as_str) != Some("system@example.com") {
            return Err(AuthError::InvalidCredential);
        }
        Ok(AuthUser::new(claims.subject()))
    }
}

struct TestKey {
    encoding: EncodingKey,
    jwk: String,
}

impl TestKey {
    fn generate() -> Result<Self, TestError> {
        let private = RsaPrivateKey::new(&mut rand08::rngs::OsRng, 2048)
            .map_err(|_| TestError::KeyGeneration)?;
        let document = private.to_pkcs1_der().map_err(|_| TestError::KeyExport)?;
        let modulus = URL_SAFE_NO_PAD.encode(private.n().to_bytes_be());
        let exponent = URL_SAFE_NO_PAD.encode(private.e().to_bytes_be());
        Ok(Self {
            encoding: EncodingKey::from_rsa_der(document.as_bytes()),
            jwk: format!(
                r#"{{"kty":"RSA","n":"{modulus}","e":"{exponent}","kid":"key-1","use":"sig","alg":"RS256"}}"#
            ),
        })
    }
}

#[derive(Serialize)]
struct Claims<'a> {
    sub: &'a str,
    iss: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
    email: &'a str,
}

fn fake_client(key: &TestKey) -> FakeClient {
    let metadata = Bytes::from(format!(
        r#"{{"issuer":"{ISSUER}","token_endpoint":"{ISSUER}/token","jwks_uri":"{JWKS}","response_types_supported":["code"]}}"#
    ));
    let jwks = Bytes::from(format!(r#"{{"keys":[{}]}}"#, key.jwk));
    FakeClient(Arc::new(BTreeMap::from([
        (METADATA.to_owned(), metadata),
        (JWKS.to_owned(), jwks),
    ])))
}

fn token(key: &TestKey, audience: &str, expiry: u64) -> Result<String, TestError> {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("key-1".into());
    encode(
        &header,
        &Claims {
            sub: "system-user",
            iss: ISSUER,
            aud: audience,
            exp: expiry,
            iat: now()?,
            email: "system@example.com",
        },
        &key.encoding,
    )
    .map_err(|_| TestError::TokenEncoding)
}

fn parts(token: &str) -> Result<axum::http::request::Parts, TestError> {
    Request::builder()
        .uri("/system")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(())
        .map(|request| request.into_parts().0)
        .map_err(|_| TestError::Request)
}

fn now() -> Result<u64, TestError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| TestError::Clock)
}

async fn runtime(key: &TestKey) -> Result<IdTokenRuntime, AuthError> {
    let conf = IdToken::discovery(ISSUER)
        .resource(API, TOKEN_AUDIENCE)
        .mapper(Mapper);
    runtime_with_conf(key, conf).await
}

async fn runtime_with_conf(key: &TestKey, conf: IdToken) -> Result<IdTokenRuntime, AuthError> {
    IdTokenRuntime::build_with_client(
        ProviderId::new(AuthProvider::new("system").as_str())?,
        conf,
        fake_client(key),
    )
    .await
}

/// A signed external token is normalized and mapped without exposing its credential.
#[tokio::test]
async fn authenticates_signed_identity_token() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(&key).await.map_err(|_| TestError::Runtime)?;
    let token = token(&key, TOKEN_AUDIENCE, now()?.saturating_add(300))?;
    let audience = AudienceId::declared(API).map_err(|_| TestError::Runtime)?;
    let user = runtime
        .authenticate(&token, &parts(&token)?, &audience)
        .await
        .map_err(|_| TestError::Runtime)?;
    assert_eq!(user.key.to_string(), "system-user");
    Ok(())
}

/// Cookie identity-token logout validates presented values and clears credential state.
#[tokio::test]
async fn cookie_logout_is_validated_and_response_ready() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let conf = IdToken::discovery(ISSUER)
        .resource(API, TOKEN_AUDIENCE)
        .mapper(Mapper)
        .from_cookie(CookieConf::new("system_token"));
    let runtime = runtime_with_conf(&key, conf)
        .await
        .map_err(|_| TestError::Runtime)?;
    let token = token(&key, TOKEN_AUDIENCE, now()?.saturating_add(300))?;
    let request = Request::builder()
        .method(Method::POST)
        .uri("/logout")
        .header(
            COOKIE,
            format!("system_token={token}; system_token_csrf=csrf-value"),
        )
        .header("x-csrf-token", "csrf-value")
        .body(())
        .map_err(|_| TestError::Request)?;
    let headers = runtime
        .logout_headers(&request.into_parts().0)
        .map_err(|_| TestError::Runtime)?;
    assert_eq!(headers.len(), 2);
    assert!(headers.iter().all(|(name, _)| name == SET_COOKIE));

    let malformed = Request::builder()
        .uri("/logout")
        .header(COOKIE, "system_token=malformed")
        .body(())
        .map_err(|_| TestError::Request)?;
    assert!(matches!(
        runtime.logout_headers(&malformed.into_parts().0),
        Err(AuthError::InvalidCredential)
    ));
    Ok(())
}

/// External audience and expiry checks run before application identity mapping.
#[tokio::test]
async fn rejects_wrong_audience_and_expiry() -> Result<(), TestError> {
    let key = TestKey::generate()?;
    let runtime = runtime(&key).await.map_err(|_| TestError::Runtime)?;
    let audience = AudienceId::declared(API).map_err(|_| TestError::Runtime)?;
    let wrong = token(
        &key,
        "https://other.example.com",
        now()?.saturating_add(300),
    )?;
    let expired = token(&key, TOKEN_AUDIENCE, now()?.saturating_sub(60))?;
    assert!(matches!(
        runtime
            .authenticate(&wrong, &parts(&wrong)?, &audience)
            .await,
        Err(AuthError::InvalidCredential)
    ));
    assert!(matches!(
        runtime
            .authenticate(&expired, &parts(&expired)?, &audience)
            .await,
        Err(AuthError::ExpiredCredential)
    ));
    Ok(())
}

fn unexpected_request() -> HuskarlError {
    HuskarlError::new(ErrorKind::Config, TestError::Request)
}

#[derive(Debug, Error)]
enum TestError {
    #[error("test signing key generation failed")]
    KeyGeneration,
    #[error("test signing key export failed")]
    KeyExport,
    #[error("test token encoding failed")]
    TokenEncoding,
    #[error("test request construction failed")]
    Request,
    #[error("test clock failed")]
    Clock,
    #[error("test runtime failed")]
    Runtime,
}
