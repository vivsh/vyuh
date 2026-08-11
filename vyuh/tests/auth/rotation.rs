use super::*;

/// Verifies an old site secret remains verify-only after rotation.
#[tokio::test]
async fn site_secret_fallback_verifies_old_token() -> Result<(), AuthError> {
    let old = "old-auth-secret-minimum-32-characters";
    let current = "new-auth-secret-minimum-32-characters";
    let old_site = Site::build(config().secret_key(old), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = old_site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let token = login.credentials().access().to_owned();
    let new_conf = config().secret_key(current).secret_key_fallbacks([old]);
    let new_site = Site::build(new_conf, bundle()).await.map_err(auth_error)?;
    TestSite::new(new_site)
        .get("/me")
        .header("authorization", &format!("Bearer {token}"))
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies an RS256 provider accepts tokens signed by a named retired key.
#[tokio::test]
async fn jwt_rs256_rotation_accepts_retired_key() -> Result<(), AuthError> {
    let old_codec = Jwt::rs256(
        KeySource::inline(include_str!("../fixtures/jwt-old-private.pem")),
        KeySource::inline(include_str!("../fixtures/jwt-old-public.pem")),
    )
    .key_id("old");
    let old_auth =
        AuthConf::default().provider(ROTATING, TokenProvider::new(old_codec).without_refresh());
    let old_site = Site::build(config().auth(old_auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = old_site
        .auth()
        .using(ROTATING)
        .issue(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let old_token = login.credentials().access().to_owned();
    assert_retired_token_accepted(old_token).await
}

/// Builds the replacement key ring and verifies an old credential through a route.
async fn assert_retired_token_accepted(old_token: String) -> Result<(), AuthError> {
    let new_codec = rotating_verifier();
    let new_auth =
        AuthConf::default().provider(ROTATING, TokenProvider::new(new_codec).without_refresh());
    let new_site = Site::build(config().auth(new_auth), bundle())
        .await
        .map_err(auth_error)?;
    TestSite::new(new_site)
        .get("/me")
        .header("authorization", &format!("Bearer {old_token}"))
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies RS256 decoding rejects a valid signature carrying an unknown key ID.
#[tokio::test]
async fn jwt_rs256_rotation_rejects_unknown_key_id() -> Result<(), AuthError> {
    let unknown_codec = Jwt::rs256(
        KeySource::inline(include_str!("../fixtures/jwt-old-private.pem")),
        KeySource::inline(include_str!("../fixtures/jwt-old-public.pem")),
    )
    .key_id("unknown");
    let issuer_auth = AuthConf::default().provider(
        ROTATING,
        TokenProvider::new(unknown_codec).without_refresh(),
    );
    let issuer = Site::build(config().auth(issuer_auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = issuer
        .auth()
        .using(ROTATING)
        .issue(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    assert_unknown_token_rejected(login.credentials().access()).await
}

/// Verifies an otherwise valid token cannot bypass the configured key-ID set.
async fn assert_unknown_token_rejected(token: &str) -> Result<(), AuthError> {
    let verifier_auth = AuthConf::default().provider(
        ROTATING,
        TokenProvider::new(rotating_verifier()).without_refresh(),
    );
    let verifier = Site::build(config().auth(verifier_auth), bundle())
        .await
        .map_err(auth_error)?;
    TestSite::new(verifier)
        .get("/me")
        .header("authorization", &format!("Bearer {token}"))
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Builds the active and retired RS256 verification-key set used by rotation tests.
fn rotating_verifier() -> Jwt {
    Jwt::rs256(
        KeySource::inline(include_str!("../fixtures/jwt-new-private.pem")),
        KeySource::inline(include_str!("../fixtures/jwt-new-public.pem")),
    )
    .key_id("new")
    .verification_key(
        "old",
        KeySource::inline(include_str!("../fixtures/jwt-old-public.pem")),
    )
}
