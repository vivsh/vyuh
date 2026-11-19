use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header, request::Parts};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use huskarl_resource_server::core::{
    RetryAdvice,
    http::{HttpResponse, Idempotency},
};
use serde_json::{Value, json};

use super::*;
use crate::{
    Site, SiteConf,
    auth::{Audience, KeySource, OAuthIdentityMapper, SecretRing},
};

const API: Audience = Audience::new("partner");
const ISSUER: &str = "https://issuer.example.test";
const ENDPOINT: &str = "https://issuer.example.test/introspect";
const TOKEN_AUDIENCE: &str = "https://api.example.test";
const TOKEN: &str = "opaque.token-with~symbols";
const SECRET: &str = "fixture-secret:with+symbols";

#[derive(Clone)]
struct Issuer {
    metadata: Bytes,
    reply: Bytes,
    status: StatusCode,
    calls: Arc<AtomicUsize>,
    discovery: Arc<AtomicUsize>,
}

impl Issuer {
    fn new(reply: impl Into<Bytes>) -> Self {
        Self {
            metadata: document(ENDPOINT, Some("client_secret_basic")),
            reply: reply.into(),
            status: StatusCode::OK,
            calls: Arc::default(),
            discovery: Arc::default(),
        }
    }
}

impl HttpClient for Issuer {
    fn execute(
        &self,
        request: Request<Bytes>,
        _idempotency: Idempotency,
    ) -> MaybeSendBoxFuture<'_, Result<HttpResponse, HuskarlError>> {
        Box::pin(async move {
            let uri = request.uri();
            let headers = HeaderMap::from_iter([(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )]);
            if request.method() == Method::GET
                && uri.path() == "/.well-known/oauth-authorization-server"
            {
                self.discovery.fetch_add(1, Ordering::Relaxed);
                return Ok(HttpResponse {
                    status: StatusCode::OK,
                    headers,
                    body: self.metadata.clone(),
                });
            }
            if request.method() != Method::POST || uri.to_string() != ENDPOINT {
                return Err(HuskarlError::new(
                    RetryAdvice::No,
                    "unexpected OAuth transport call",
                ));
            }
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert_request(&request);
            if !self.status.is_success() {
                return Err(super::super::http::status_error(self.status));
            }
            super::super::http::validate_introspection_body(&headers, &self.reply)?;
            Ok(HttpResponse {
                status: self.status,
                headers,
                body: self.reply.clone(),
            })
        })
    }
}

/// Checks the Basic and form boundaries without printing test credentials.
fn assert_request(request: &Request<Bytes>) {
    let expected = format!(
        "Basic {}",
        STANDARD.encode("vyuh:fixture-secret%3Awith%2Bsymbols")
    );
    assert!(
        request
            .headers()
            .get(header::AUTHORIZATION)
            .is_some_and(|value| value == expected.as_str())
    );
    assert!(request.uri().query().is_none());
    let form = url::form_urlencoded::parse(request.body()).collect::<BTreeMap<_, _>>();
    assert!(form.get("token").is_some_and(|value| value == TOKEN));
    assert!(
        form.get("token_type_hint")
            .is_some_and(|value| value == "access_token")
    );
    assert_eq!(form.len(), 2);
}

/// Advertises a JWKS URL to prove introspection never invokes the JWT verifier.
fn document(endpoint: &str, auth: Option<&str>) -> Bytes {
    let mut metadata = json!({
        "issuer": ISSUER, "introspection_endpoint": endpoint,
        "jwks_uri": format!("{ISSUER}/jwks"),
        "token_endpoint": format!("{ISSUER}/token"),
        "response_types_supported": ["code"],
    });
    if let Some(auth) = auth {
        metadata["introspection_endpoint_auth_methods_supported"] = json!([auth]);
    }
    Bytes::from(metadata.to_string())
}

fn provider() -> OAuthResourceServer {
    OAuthResourceServer::discovery(ISSUER)
        .introspection("vyuh", KeySource::inline(SECRET))
        .resource(
            API,
            OAuthResource::new(TOKEN_AUDIENCE).require_scopes(["api.read"]),
        )
}

fn active() -> Value {
    json!({"active": true, "aud": TOKEN_AUDIENCE, "sub": "person", "scope": "api.read"})
}

async fn runtime(conf: OAuthResourceServer, client: Issuer) -> Result<OAuthRuntime, AuthError> {
    let secrets = SecretRing::new(
        "fixture-site-secret-for-introspection",
        &[],
        std::path::Path::new("."),
        32,
    )?;
    let secret = conf.prepare_secret(&secrets)?;
    OAuthRuntime::build_with_client(ProviderId::new("partner")?, conf, client, secret).await
}

async fn site() -> Result<Site, String> {
    Site::build(
        SiteConf::default().timezone("Asia/Kolkata").log_init(false),
        crate::bundles::Bundle::default(),
    )
    .await
    .map_err(err)
}

fn parts(raw: &str) -> Result<Parts, String> {
    Ok(Request::builder()
        .uri("/resource")
        .header(header::AUTHORIZATION, format!("Bearer {raw}"))
        .body(())
        .map_err(err)?
        .into_parts()
        .0)
}

async fn authenticate(
    runtime: &OAuthRuntime,
    site: &Site,
    raw: &str,
) -> Result<AuthUser, AuthError> {
    let parts = parts(raw).map_err(|_| AuthError::InvalidCredential)?;
    runtime
        .authenticate(site, raw, &parts, &AudienceId::declared(API)?)
        .await
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

struct Mapped;

impl OAuthIdentityMapper for Mapped {
    async fn map(&self, site: &Site, claims: &OAuthClaims) -> Result<AuthUser, AuthError> {
        assert_eq!(site.timezone(), chrono_tz::Asia::Kolkata);
        assert_eq!(claims.issuer, ISSUER);
        assert_eq!(claims.raw.get("tenant"), Some(&json!("alpha")));
        assert!(claims.raw.get("token").is_none());
        Ok(AuthUser::new(&claims.subject).with_scope(crate::auth::Scope::of("app:read")))
    }
}

/// Verifies Basic transport, authenticated mapping, and no caching.
#[tokio::test]
async fn authenticates_opaque_tokens() -> Result<(), String> {
    let mut claims = active();
    claims["tenant"] = json!("alpha");
    claims["token"] = json!(TOKEN);
    claims["iat"] = json!(20);
    let client = Issuer::new(claims.to_string());
    let runtime = runtime(provider().mapper(Mapped), client.clone())
        .await
        .map_err(err)?;
    let site = site().await?;
    assert_eq!(client.calls.load(Ordering::Relaxed), 0);
    for _ in 0..2 {
        let user = authenticate(&runtime, &site, TOKEN).await.map_err(err)?;
        assert_eq!(user.subject(), "person");
        assert_eq!(user.provider(), Some("partner"));
        assert!(user.authentication().auth_time().is_none());
        assert!(user.has_scope(&crate::auth::Scope::of("app:read")));
    }
    assert_eq!(client.discovery.load(Ordering::Relaxed), 1);
    assert_eq!(client.calls.load(Ordering::Relaxed), 2);
    let mut duplicate = parts(TOKEN)?;
    duplicate.headers.append(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer repeated"),
    );
    assert!(runtime.location.extract(&duplicate).is_err());
    assert!(
        runtime
            .location
            .extract(&Request::new(()).into_parts().0)
            .map_err(err)?
            .is_none()
    );
    for raw in [
        "mal formed",
        "comma,token",
        &"a".repeat(MAX_OAUTH_CREDENTIAL_BYTES + 1),
    ] {
        assert!(matches!(
            authenticate(&runtime, &site, raw).await,
            Err(AuthError::InvalidCredential)
        ));
    }
    assert_eq!(client.calls.load(Ordering::Relaxed), 2);
    Ok(())
}

/// Rejects unsuitable tokens before mapping, including active tokens for other audiences.
#[tokio::test]
async fn checks_remote_claims() -> Result<(), String> {
    let site = site().await?;
    for (field, value, expected) in [
        ("active", json!(false), 401),
        ("aud", json!("other"), 401),
        ("aud", Value::Null, 401),
        ("iss", json!("https://other.example.test"), 401),
        ("sub", Value::Null, 401),
        ("token_type", json!("refresh_token"), 401),
        ("exp", json!(1), 401),
        ("nbf", json!(4_102_444_800_u64), 401),
        ("scope", json!("api.write"), 403),
    ] {
        let mut value_set = active();
        if value.is_null() {
            value_set
                .as_object_mut()
                .ok_or("invalid fixture")?
                .remove(field);
        } else {
            value_set[field] = value;
        }
        let runtime = runtime(
            provider().mapper(Mapped),
            Issuer::new(value_set.to_string()),
        )
        .await
        .map_err(err)?;
        let error = authenticate(&runtime, &site, TOKEN)
            .await
            .err()
            .ok_or("invalid credential accepted")?;
        assert_eq!(error.status().as_u16(), expected, "{field}");
    }
    let runtime = runtime(provider(), Issuer::new(active().to_string()))
        .await
        .map_err(err)?;
    let user = authenticate(&runtime, &site, TOKEN).await.map_err(err)?;
    assert!(user.scopes().is_empty());
    Ok(())
}

/// Separates service errors from bad credentials and never retries failed requests.
#[tokio::test]
async fn fails_closed_on_outage() -> Result<(), String> {
    let site = site().await?;
    for (status, body) in [
        (StatusCode::UNAUTHORIZED, String::new()),
        (StatusCode::TEMPORARY_REDIRECT, String::new()),
        (StatusCode::GATEWAY_TIMEOUT, String::new()),
        (StatusCode::OK, r#"{"aud":"one","aud":"two"}"#.to_owned()),
        (StatusCode::OK, "bad document".to_owned()),
        (StatusCode::OK, "a".repeat(64 * 1024 + 1)),
    ] {
        let mut client = Issuer::new(body);
        client.status = status;
        let runtime = runtime(provider(), client.clone()).await.map_err(err)?;
        assert!(matches!(
            authenticate(&runtime, &site, TOKEN).await,
            Err(AuthError::ProviderUnavailable)
        ));
        assert_eq!(client.calls.load(Ordering::Relaxed), 1);
    }
    Ok(())
}

/// Configuration fails before sending a token, and JWT-only options cannot silently no-op.
#[tokio::test]
async fn validates_startup_policy() -> Result<(), String> {
    assert!(
        provider()
            .algorithms([OAuthJwtAlgorithm::Rs256])
            .validate()
            .is_err()
    );
    assert!(
        OAuthResourceServer::discovery(ISSUER)
            .algorithms([OAuthJwtAlgorithm::Rs256])
            .introspection("vyuh", KeySource::inline(SECRET))
            .resource(API, OAuthResource::new(TOKEN_AUDIENCE))
            .validate()
            .is_err()
    );
    assert!(
        provider()
            .introspection("vyuh", KeySource::site_secret())
            .validate()
            .is_err()
    );
    assert!(!format!("{:?}", provider()).contains(SECRET));
    for metadata in [
        document("http://localhost/introspect", None),
        document(ENDPOINT, Some("private_key_jwt")),
        Bytes::from(
            json!({"issuer": ISSUER, "token_endpoint": format!("{ISSUER}/token")}).to_string(),
        ),
    ] {
        let mut client = Issuer::new(active().to_string());
        client.metadata = metadata;
        assert!(matches!(
            runtime(provider(), client.clone()).await,
            Err(AuthError::InvalidProviderConfig(_))
        ));
        assert_eq!(client.calls.load(Ordering::Relaxed), 0);
    }
    Ok(())
}

/// OpenAPI, console, and MCP retain resource metadata without advertising the credential as JWT.
#[tokio::test]
async fn describes_introspection() -> Result<(), String> {
    let conf = crate::auth::AuthConf::default()
        .provider(crate::auth::AuthProvider::new("partner"), provider());
    let summary = conf.summary();
    let metadata = summary
        .providers
        .first()
        .ok_or("missing provider summary")?;
    assert_eq!(metadata.format, "OAuth Bearer");
    let runtime = runtime(provider(), Issuer::new(active().to_string()))
        .await
        .map_err(err)?;
    assert!(matches!(
        runtime.openapi().credential_type,
        crate::auth::CredentialType::Token(Some(format)) if format == "OAuth Bearer"
    ));
    #[cfg(feature = "mcp")]
    {
        let audience = AudienceId::declared(API).map_err(err)?;
        let metadata = runtime
            .protected_resource(&audience)
            .ok_or("missing MCP metadata")?;
        assert_eq!(metadata.issuer, ISSUER);
        assert_eq!(metadata.required_scopes, ["api.read"]);
        assert_eq!(metadata.advertised_scopes, ["api.read"]);
    }
    Ok(())
}
