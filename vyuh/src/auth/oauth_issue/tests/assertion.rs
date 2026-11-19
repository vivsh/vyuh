use jsonwebtoken::{EncodingKey, Header, encode};
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
};
use serde_json::json;

use super::*;

/// Wrong audiences stay rejected; trace output identifies the local token URL without echoing claims.
#[test]
fn audience_diagnostics() -> std::io::Result<()> {
    let issuer = "https://issuer.example";
    let token_url = format!("{issuer}/oauth/token");
    let private = "untrusted-value-not-for-logs";
    let log = tempfile::NamedTempFile::new()?;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("off,vyuh::oauth_diagnostics=debug")
        .with_writer(log.reopen()?)
        .finish();
    tracing::subscriber::with_default(subscriber, || -> Result<(), serde_json::Error> {
        for (aud, expected) in [
            (json!(issuer), None),
            (json!([issuer]), None),
            (
                json!(token_url),
                Some("assertion_audience_is_token_endpoint"),
            ),
            (
                json!([token_url]),
                Some("assertion_audience_is_token_endpoint"),
            ),
            (
                json!([issuer, token_url]),
                Some("assertion_audience_mismatch"),
            ),
            (json!(private), Some("assertion_audience_mismatch")),
        ] {
            let payload = json!({"iss": private, "sub": private, "jti": private, "exp": now()+60, "aud": aud});
            let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","kid":"test"}"#);
            let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload)?);
            // This test stops at pre-verification parsing, without performing a signed exchange.
            let parsed = Assertion::parse(&format!("{header}.{body}.AA"), issuer, None)
                .map(|_| ())
                .map_err(OAuthIssueError::code);
            assert_eq!(parsed, expected.map_or(Ok(()), |_| Err("invalid_client")));
            let claims: Claims = serde_json::from_value(payload)?;
            assert_eq!(claims.check(issuer, None, now()).err(), expected);
        }
        Ok(())
    })?;
    let output = std::fs::read_to_string(log.path())?;
    assert!(output.contains("assertion_audience_is_token_endpoint"));
    assert!(output.contains("assertion_audience_mismatch"));
    assert!(!output.contains(private) && !output.contains(issuer));
    Ok(())
}

/// Verifies an ES256 assertion using the same public JWK handling as signed clients.
#[test]
fn verifies_es256() -> Result<(), String> {
    let rng = SystemRandom::new();
    let algorithm = &ECDSA_P256_SHA256_FIXED_SIGNING;
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(algorithm, &rng).map_err(|_| "key creation failed")?;
    let key = EcdsaKeyPair::from_pkcs8(algorithm, pkcs8.as_ref(), &rng).map_err(|_| "bad key")?;
    let point = key.public_key().as_ref();
    let x = point.get(1..33).ok_or("missing public x coordinate")?;
    let y = point.get(33..65).ok_or("missing public y coordinate")?;
    let jwk = json!({"kty":"EC", "crv":"P-256", "kid":"signing-key",
        "x":URL_SAFE_NO_PAD.encode(x), "y":URL_SAFE_NO_PAD.encode(y), "key_ops":["verify"]});
    let issuer = "https://issuer.example.com";
    let client = "https://client.example.com/client.json";
    let input = json!({"iss":client, "sub":client, "aud":[issuer], "exp":now()+60, "jti":"p256"});
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some("signing-key".into());
    let raw = encode(&header, &input, &EncodingKey::from_ec_der(pkcs8.as_ref()))
        .map_err(|error| error.to_string())?;
    let assertion = Assertion::parse(&raw, issuer, None).map_err(|error| error.to_string())?;
    let key = decoding_key(&jwk, "ES256").map_err(|error| error.to_string())?;
    let verified = decode::<Claims>(&raw, &key, &assertion.validation())
        .map_err(|error| error.to_string())?
        .claims;
    assert!(verified.aud.is_only(issuer));
    assert_eq!(verified.jti, "p256");
    Ok(())
}
