use jsonwebtoken::{EncodingKey, Header, encode};
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
};
use serde_json::json;

use super::*;

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
    let assertion = Assertion::parse(&raw, issuer).map_err(|error| error.to_string())?;
    let key = decoding_key(&jwk, "ES256").map_err(|error| error.to_string())?;
    let verified = decode::<Claims>(&raw, &key, &assertion.validation(issuer))
        .map_err(|error| error.to_string())?
        .claims;
    assert!(verified.aud.is_only(issuer));
    assert_eq!(verified.jti, "p256");
    Ok(())
}
