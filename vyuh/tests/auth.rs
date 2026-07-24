use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use vyuh::{
    SiteConf,
    auth::{
        ApiKey, ApiKeyConf, ApiKeyPrincipal, ApiKeyVerifier, AuthAudiencePolicy, AuthConf,
        AuthError, AuthUser, BitRole, JWTClaim, JwtAlgorithm, JwtConf, JwtKeySource, TokenKind,
        permit,
    },
    bundles, routes,
    routes::{Json, StatusCode},
    testing::TestSite,
};

fn test_conf() -> SiteConf {
    SiteConf {
        secret_key: "auth-test-secret-minimum-32-chars".to_string(),
        log_init: false,
        logging: vyuh::logging::LoggingConf {
            env_prefix: None,
            rules: vec![],
        },
        ..SiteConf::default()
    }
}

const HMAC_SECRET: &str = "jwt-test-secret-with-at-least-32-characters";

const RSA_PRIVATE_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL
UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V
rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H
oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI
Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/
by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd
WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ
Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j
E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn
LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS
bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE
8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl
xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY
2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1
9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0
7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ
mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7
MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4
t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam
QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA
2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ
4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn
PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH
UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8
oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==
-----END RSA PRIVATE KEY-----"#;

const RSA_PUBLIC_KEY: &str = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4
l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyW
yj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG
/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4l
QzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h
3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQAB
-----END RSA PUBLIC KEY-----"#;

const EC_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgWTFfCGljY6aw3Hrt
kHmPRiazukxPLb6ilpRAewjW8nihRANCAATDskChT+Altkm9X7MI69T3IUmrQU0L
950IxEzvw/x5BMEINRMrXLBJhqzO9Bm+d6JbqA21YQmd1Kt4RzLJR1W+
-----END PRIVATE KEY-----"#;

const EC_PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEw7JAoU/gJbZJvV+zCOvU9yFJq0FN
C/edCMRM78P8eQTBCDUTK1ywSYaszvQZvneiW6gNtWEJndSreEcyyUdVvg==
-----END PUBLIC KEY-----"#;

#[derive(BitRole)]
enum TestRole {
    Manager,
    Viewer,
}

#[derive(Debug, Serialize, JsonSchema)]
struct WhoAmI {
    key: String,
    roles: u64,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
struct KeyInfo {
    key_id: String,
    subject: Option<String>,
    roles: u64,
}

struct StaticApiKeyVerifier;

impl ApiKeyVerifier for StaticApiKeyVerifier {
    async fn verify(&self, presented: &str) -> Result<ApiKeyPrincipal, AuthError> {
        if presented == "valid-key" {
            Ok(ApiKeyPrincipal::new("key-1")
                .subject("service-1")
                .roles(TestRole::Viewer.to_role_type()))
        } else {
            Err(AuthError::InvalidApiKey)
        }
    }
}

fn jwt_conf(algorithm: JwtAlgorithm) -> JwtConf {
    JwtConf {
        algorithm,
        signing_key: JwtKeySource::Inline(HMAC_SECRET.to_string()),
        verifying_key: None,
        key_id: Some("test-key".to_string()),
    }
}

#[bundles::route(path = "/public")]
async fn public() -> Json<&'static str> {
    Json("ok")
}

#[bundles::route(path = "/me")]
async fn me(user: AuthUser) -> Json<WhoAmI> {
    Json(WhoAmI {
        key: user.key.to_string(),
        roles: user.roles,
    })
}

#[bundles::route(path = "/api-key")]
async fn api_key_route(key: ApiKey) -> Json<KeyInfo> {
    Json(KeyInfo {
        key_id: key.key_id.to_string(),
        subject: key.subject.as_ref().map(ToString::to_string),
        roles: key.roles,
    })
}

#[bundles::route(path = "/secure")]
async fn secure(_permit: permit!(TestRole, Manager)) -> Json<WhoAmI> {
    Json(WhoAmI {
        key: "manager".to_string(),
        roles: TestRole::Manager.to_role_type(),
    })
}

#[tokio::test]
async fn default_jwt_uses_hs256_with_site_secret() {
    let site = vyuh::Site::build(test_conf(), bundles::Bundle::new())
        .await
        .unwrap();
    let token = site
        .auth()
        .create_token_pair(AuthUser::new("user-1", 0), &[])
        .unwrap()
        .access_token;
    let header = jsonwebtoken::decode_header(&token).unwrap();

    assert_eq!(header.alg, jsonwebtoken::Algorithm::HS256);
    site.auth().decode(&token).unwrap();

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn hmac_jwt_algorithms_sign_and_verify() {
    for (jwt_algorithm, expected) in [
        (JwtAlgorithm::HS256, jsonwebtoken::Algorithm::HS256),
        (JwtAlgorithm::HS384, jsonwebtoken::Algorithm::HS384),
        (JwtAlgorithm::HS512, jsonwebtoken::Algorithm::HS512),
    ] {
        let conf = test_conf().auth(AuthConf::default().jwt(jwt_conf(jwt_algorithm)));
        let site = vyuh::Site::build(conf, bundles::Bundle::new())
            .await
            .unwrap();
        let token = site
            .auth()
            .create_token_pair(AuthUser::new("user-1", 0), &[])
            .unwrap()
            .access_token;
        let header = jsonwebtoken::decode_header(&token).unwrap();

        assert_eq!(header.alg, expected);
        assert_eq!(header.kid.as_deref(), Some("test-key"));
        site.auth().decode(&token).unwrap();

        site.shutdown_and_wait().await;
    }
}

#[tokio::test]
async fn rsa_jwt_algorithm_signs_with_private_key_and_verifies_with_public_key() {
    let jwt = JwtConf::rs256(
        JwtKeySource::Inline(RSA_PRIVATE_KEY.to_string()),
        JwtKeySource::Inline(RSA_PUBLIC_KEY.to_string()),
    )
    .key_id("rsa-key");
    let site = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(AuthUser::new("user-1", 0), &[])
        .unwrap()
        .access_token;
    let header = jsonwebtoken::decode_header(&token).unwrap();

    assert_eq!(header.alg, jsonwebtoken::Algorithm::RS256);
    assert_eq!(header.kid.as_deref(), Some("rsa-key"));
    site.auth().decode(&token).unwrap();

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn es256_jwt_algorithm_signs_with_private_key_and_verifies_with_public_key() {
    let jwt = JwtConf {
        algorithm: JwtAlgorithm::ES256,
        signing_key: JwtKeySource::Inline(EC_PRIVATE_KEY.to_string()),
        verifying_key: Some(JwtKeySource::Inline(EC_PUBLIC_KEY.to_string())),
        key_id: Some("ec-key".to_string()),
    };
    let site = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(AuthUser::new("user-1", 0), &[])
        .unwrap()
        .access_token;
    let header = jsonwebtoken::decode_header(&token).unwrap();

    assert_eq!(header.alg, jsonwebtoken::Algorithm::ES256);
    assert_eq!(header.kid.as_deref(), Some("ec-key"));
    site.auth().decode(&token).unwrap();

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn rsa_jwt_keys_can_be_loaded_from_project_relative_files() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("jwt-private.pem"), RSA_PRIVATE_KEY).unwrap();
    std::fs::write(temp.path().join("jwt-public.pem"), RSA_PUBLIC_KEY).unwrap();
    let jwt = JwtConf::rs256(
        JwtKeySource::File("jwt-private.pem".to_string()),
        JwtKeySource::File("jwt-public.pem".to_string()),
    );
    let site = vyuh::Site::build(
        test_conf()
            .project_dir(temp.path().to_string_lossy())
            .auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(AuthUser::new("user-1", 0), &[])
        .unwrap()
        .access_token;

    site.auth().decode(&token).unwrap();
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn jwt_rejects_tokens_signed_with_wrong_algorithm() {
    let hs384_site = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt_conf(JwtAlgorithm::HS384))),
        bundles::Bundle::new(),
    )
    .await
    .unwrap();
    let token = hs384_site
        .auth()
        .create_token_pair(AuthUser::new("user-1", 0), &[])
        .unwrap()
        .access_token;
    let hs512_site = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt_conf(JwtAlgorithm::HS512))),
        bundles::Bundle::new(),
    )
    .await
    .unwrap();

    let err = hs512_site.auth().decode(&token).unwrap_err();
    assert!(matches!(err, AuthError::InvalidToken));

    hs384_site.shutdown_and_wait().await;
    hs512_site.shutdown_and_wait().await;
}

#[tokio::test]
async fn missing_jwt_env_key_fails_site_build() {
    let jwt = JwtConf::hs512(JwtKeySource::Env(
        "VYUH_TEST_MISSING_JWT_SECRET".to_string(),
    ));
    let err = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("JWT key env var"));
}

#[tokio::test]
async fn missing_jwt_file_fails_site_build() {
    let jwt = JwtConf::rs256(
        JwtKeySource::File("missing-private.pem".to_string()),
        JwtKeySource::File("missing-public.pem".to_string()),
    );
    let err = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("failed to read JWT key file"));
}

#[tokio::test]
async fn invalid_jwt_pem_fails_site_build() {
    let jwt = JwtConf::rs256(
        JwtKeySource::Inline("not a private pem".to_string()),
        JwtKeySource::Inline("not a public pem".to_string()),
    );
    let err = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("failed to parse JWT signing key"));
}

#[tokio::test]
async fn asymmetric_jwt_config_requires_verifying_key() {
    let jwt = JwtConf {
        algorithm: JwtAlgorithm::RS256,
        signing_key: JwtKeySource::Inline(RSA_PRIVATE_KEY.to_string()),
        verifying_key: None,
        key_id: None,
    };
    let err = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("require a verifying_key"));
}

#[tokio::test]
async fn hmac_jwt_config_rejects_short_inline_secret() {
    let jwt = JwtConf::hs512(JwtKeySource::Inline("short".to_string()));
    let err = vyuh::Site::build(
        test_conf().auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("JWT signing key"));
}

#[tokio::test]
async fn external_hmac_jwt_key_does_not_use_site_secret_length() {
    let jwt = JwtConf::hs512(JwtKeySource::Inline(HMAC_SECRET.to_string()));
    let site = vyuh::Site::build(
        test_conf()
            .secret_key("short")
            .auth(AuthConf::default().jwt(jwt)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(AuthUser::new("user-1", 0), &[])
        .unwrap()
        .access_token;

    site.auth().decode(&token).unwrap();
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auth_accepts_bearer_authorization_header() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            me,
        },
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(
            AuthUser::new("user-1", TestRole::Viewer.to_role_type()),
            &[],
        )
        .unwrap()
        .access_token;
    let client = TestSite::new(site.clone());

    client
        .get("/me")
        .header("authorization", &format!("Bearer {token}"))
        .send()
        .await
        .assert_status(StatusCode::OK);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn public_route_does_not_require_auth() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            public,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    client
        .get("/public")
        .send()
        .await
        .assert_status(StatusCode::OK);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auth_accepts_legacy_jwt_authorization_header() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            me,
        },
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(
            AuthUser::new("user-1", TestRole::Viewer.to_role_type()),
            &[],
        )
        .unwrap()
        .access_token;
    let client = TestSite::new(site.clone());

    client
        .get("/me")
        .header("authorization", &format!("JWT {token}"))
        .send()
        .await
        .assert_status(StatusCode::OK);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auth_missing_token_returns_unauthorized() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            me,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    client
        .get("/me")
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auth_permit_rejects_missing_role() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            secure,
        },
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(
            AuthUser::new("user-1", TestRole::Viewer.to_role_type()),
            &[],
        )
        .unwrap()
        .access_token;
    let client = TestSite::new(site.clone());

    client
        .get("/secure")
        .header("authorization", &format!("Bearer {token}"))
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auth_user_rejects_refresh_token() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            me,
        },
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(
            AuthUser::new("user-1", TestRole::Viewer.to_role_type()),
            &[],
        )
        .unwrap()
        .refresh_token;
    let client = TestSite::new(site.clone());

    client
        .get("/me")
        .header("authorization", &format!("Bearer {token}"))
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn refresh_rejects_access_token() {
    let site = vyuh::Site::build(test_conf(), bundles::Bundle::new())
        .await
        .unwrap();
    let token = site
        .auth()
        .create_token_pair(
            AuthUser::new("user-1", TestRole::Viewer.to_role_type()),
            &[],
        )
        .unwrap()
        .access_token;
    let req = routes::Request::builder()
        .header("authorization", format!("Bearer {token}"))
        .body(routes::Body::empty())
        .unwrap();
    let (parts, _) = req.into_parts();

    let err = site.auth().refresh(&parts, &[]).unwrap_err();
    assert!(matches!(err, AuthError::WrongTokenKind));

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn audience_required_rejects_route_without_audience() {
    let conf = test_conf().auth(AuthConf::default().audience(AuthAudiencePolicy::Required));
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            me,
        },
    )
    .await
    .unwrap();
    let token = site
        .auth()
        .create_token_pair(AuthUser::new("user-1", 0), &["web"])
        .unwrap()
        .access_token;
    let client = TestSite::new(site.clone());

    client
        .get("/me")
        .header("authorization", &format!("Bearer {token}"))
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn issuer_must_match_when_configured() {
    let conf = test_conf().auth(AuthConf::default().issuer("expected"));
    let site = vyuh::Site::build(conf, bundles::Bundle::new())
        .await
        .unwrap();
    let claims = JWTClaim::new(
        &AuthUser::new("user-1", 0),
        "",
        Some("wrong".to_string()),
        vec![],
        3600,
        TokenKind::Access,
    );
    let token = site.auth().encode(&claims).unwrap();

    let err = site.auth().decode(&token).unwrap_err();
    assert!(matches!(
        err,
        AuthError::InvalidToken | AuthError::InternalError(_)
    ));

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn leeway_allows_recently_expired_tokens() {
    let conf = test_conf().auth(AuthConf::default().leeway_seconds(30));
    let site = vyuh::Site::build(conf, bundles::Bundle::new())
        .await
        .unwrap();
    let claims = JWTClaim::new(
        &AuthUser::new("user-1", 0),
        "",
        None,
        vec![],
        -10,
        TokenKind::Access,
    );
    let token = site.auth().encode(&claims).unwrap();

    site.auth().decode(&token).unwrap();

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn configured_minimum_secret_length_is_validated() {
    let err = vyuh::Site::build(
        SiteConf::default()
            .secret_key("short")
            .log_init(false)
            .auth(AuthConf::default().min_secret_len(32)),
        bundles::Bundle::new(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("secret_key"));
}

#[tokio::test]
async fn default_cookies_are_disabled() {
    let site = vyuh::Site::build(test_conf(), bundles::Bundle::new())
        .await
        .unwrap();
    let mut response = routes::Response::new(routes::Body::empty());
    site.auth()
        .login_user(AuthUser::new("user-1", 0), &[], &mut response)
        .unwrap();

    assert!(
        response
            .headers()
            .get_all("set-cookie")
            .iter()
            .next()
            .is_none()
    );

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn opt_in_cookies_are_written() {
    let conf = test_conf().auth(AuthConf::cookie_pair("access_token", "refresh_token"));
    let site = vyuh::Site::build(conf, bundles::Bundle::new())
        .await
        .unwrap();
    let mut response = routes::Response::new(routes::Body::empty());
    site.auth()
        .login_user(AuthUser::new("user-1", 0), &[], &mut response)
        .unwrap();

    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn api_key_extracts_from_configured_header() {
    let conf = test_conf()
        .auth(AuthConf::default().api_keys(ApiKeyConf::default().verifier(StaticApiKeyVerifier)));
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            api_key_route,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    client
        .get("/api-key")
        .header("x-api-key", "valid-key")
        .send()
        .await
        .assert_json(
            StatusCode::OK,
            &KeyInfo {
                key_id: "key-1".to_string(),
                subject: Some("service-1".to_string()),
                roles: TestRole::Viewer.to_role_type(),
            },
        )
        .await;

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn api_key_authorization_scheme_works_when_configured() {
    let conf = test_conf()
        .auth(AuthConf::default().api_keys(ApiKeyConf::default().verifier(StaticApiKeyVerifier)));
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            api_key_route,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    client
        .get("/api-key")
        .header("authorization", "ApiKey valid-key")
        .send()
        .await
        .assert_status(StatusCode::OK);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn api_key_query_param_is_explicit_opt_in() {
    let disabled_conf = test_conf()
        .auth(AuthConf::default().api_keys(ApiKeyConf::default().verifier(StaticApiKeyVerifier)));
    let disabled_site = vyuh::Site::build(
        disabled_conf,
        bundles::bundle! {
            api_key_route,
        },
    )
    .await
    .unwrap();
    let disabled_client = TestSite::new(disabled_site.clone());

    disabled_client
        .get("/api-key?api_key=valid-key")
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
    disabled_site.shutdown_and_wait().await;

    let enabled_conf = test_conf().auth(
        AuthConf::default().api_keys(
            ApiKeyConf::default()
                .allow_query_param(true)
                .verifier(StaticApiKeyVerifier),
        ),
    );
    let enabled_site = vyuh::Site::build(
        enabled_conf,
        bundles::bundle! {
            api_key_route,
        },
    )
    .await
    .unwrap();
    let enabled_client = TestSite::new(enabled_site.clone());

    enabled_client
        .get("/api-key?api_key=valid-key")
        .send()
        .await
        .assert_status(StatusCode::OK);

    enabled_site.shutdown_and_wait().await;
}

#[tokio::test]
async fn api_key_missing_verifier_returns_server_error() {
    let conf = test_conf().auth(AuthConf::default().api_keys(ApiKeyConf::default().enabled(true)));
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            api_key_route,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    client
        .get("/api-key")
        .header("x-api-key", "valid-key")
        .send()
        .await
        .assert_status(StatusCode::INTERNAL_SERVER_ERROR);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn api_key_openapi_security_scheme_is_generated() {
    let conf = test_conf()
        .auth(AuthConf::default().api_keys(ApiKeyConf::default().verifier(StaticApiKeyVerifier)));
    let bundle = bundles::bundle! {
        api_key_route,
    }
    .with_openapi(
        bundles::OpenApiConf::default()
            .title("Auth API")
            .spec("/openapi.json"),
    );
    let site = vyuh::Site::build(conf, bundle).await.unwrap();
    let client = TestSite::new(site.clone());

    let spec: serde_json::Value = client
        .get("/openapi.json")
        .send()
        .await
        .assert_ok()
        .json()
        .await;
    assert_eq!(
        spec["components"]["securitySchemes"]["apiKeyAuth"]["type"],
        "apiKey"
    );
    assert_eq!(
        spec["components"]["securitySchemes"]["apiKeyAuth"]["name"],
        "X-API-Key"
    );

    site.shutdown_and_wait().await;
}
