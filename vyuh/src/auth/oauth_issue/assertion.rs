//! Signed client authentication and the issuer-wide assertion replay boundary.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use crate::{
    Site,
    auth::codecs::{reject_duplicate_claims, unique_json_fields},
};

use super::{
    IssuerRuntime, OAuthIssueError,
    config::bounded,
    protocol::store_failure,
    runtime::now,
    store::{self, AssertionRow},
};

pub(super) const TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
pub(super) const ALGORITHMS: [&str; 2] = ["RS256", "ES256"];
const MAX_ASSERTION: usize = 8192;
const WINDOW: i64 = 300;
const SKEW: i64 = 30;
const BAD: OAuthIssueError = OAuthIssueError::InvalidClient;

pub(super) struct Assertion<'a> {
    raw: &'a str,
    pub(super) client_id: String,
    algorithm: Algorithm,
    kid: String,
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: Audience,
    exp: i64,
    iat: Option<i64>,
    nbf: Option<i64>,
    jti: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    Single(String),
    List(Vec<String>),
}

impl Audience {
    fn is_only(&self, expected: &str) -> bool {
        match self {
            Self::Single(value) => value == expected,
            Self::List(values) => values == &[expected],
        }
    }
}

impl Claims {
    fn valid_time(&self, at: i64) -> bool {
        self.exp > at - SKEW
            && self.exp <= at + WINDOW + SKEW
            && self.iat.is_none_or(|iat| {
                iat >= at - WINDOW - SKEW
                    && iat <= at + SKEW
                    && iat < self.exp
                    && self.exp - iat <= WINDOW
            })
            && self
                .nbf
                .is_none_or(|nbf| nbf <= at + SKEW && nbf < self.exp)
    }
}

impl<'a> Assertion<'a> {
    /// Parses only bounded, unambiguous JWT input before any outbound request.
    pub(super) fn parse(raw: &'a str, issuer: &str) -> Result<Self, OAuthIssueError> {
        if raw.len() > MAX_ASSERTION {
            return Err(BAD);
        }
        reject_duplicate_claims(raw).map_err(|_| BAD)?;
        let (header, remainder) = raw.split_once('.').ok_or(BAD)?;
        let (claims, _) = remainder.split_once('.').ok_or(BAD)?;
        validate_header(header)?;
        let header = decode_header(raw).map_err(|_| BAD)?;
        let algorithm = header.alg;
        if !matches!(algorithm, Algorithm::RS256 | Algorithm::ES256) {
            return Err(BAD);
        }
        let kid = header.kid.filter(|value| bounded(value, 128)).ok_or(BAD)?;
        let bytes = URL_SAFE_NO_PAD.decode(claims).map_err(|_| BAD)?;
        let claims: Claims = serde_json::from_slice(&bytes).map_err(|_| BAD)?;
        if claims.iss != claims.sub
            || !claims.aud.is_only(issuer)
            || !claims.valid_time(now())
            || !bounded(&claims.jti, 128)
        {
            return Err(BAD);
        }
        Ok(Self {
            raw,
            client_id: claims.sub,
            algorithm,
            kid,
        })
    }

    /// Verifies against the current advertised public key and consumes the replay identity.
    pub(super) async fn authenticate(
        &self,
        site: &Site,
        issuer: &IssuerRuntime,
        keys: &[Value],
        registered_algorithm: Option<&str>,
    ) -> Result<(), OAuthIssueError> {
        let algorithm = match self.algorithm {
            Algorithm::RS256 => "RS256",
            Algorithm::ES256 => "ES256",
            _ => return Err(BAD),
        };
        if registered_algorithm.is_some_and(|value| value != algorithm) {
            return Err(BAD);
        }
        let jwk = keys
            .iter()
            .find(|key| key.get("kid").and_then(Value::as_str) == Some(&self.kid))
            .ok_or(BAD)?;
        let key = decoding_key(jwk, algorithm)?;
        let claims = decode::<Claims>(self.raw, &key, &self.validation(&issuer.issuer))
            .map_err(|_| BAD)?
            .claims;
        if !claims.aud.is_only(&issuer.issuer) || !claims.valid_time(now()) {
            return Err(BAD);
        }
        let row = AssertionRow {
            digest: replay_digest(&issuer.issuer, &self.client_id, &claims.jti),
            issuer: issuer.issuer.clone(),
            expires_at: claims.exp + 2 * SKEW,
        };
        if !store::claim_assertion(site.db(), &row)
            .await
            .map_err(store_failure("claim_assertion"))?
            || !claims.valid_time(now())
        {
            return Err(BAD);
        }
        Ok(())
    }

    fn validation(&self, issuer: &str) -> Validation {
        let mut validation = Validation::new(self.algorithm);
        validation.set_audience(&[issuer]);
        validation.set_issuer(&[&self.client_id]);
        validation.sub = Some(self.client_id.clone());
        validation.set_required_spec_claims(&["exp", "iss", "sub", "aud"]);
        validation.leeway = SKEW as u64;
        validation.validate_nbf = true;
        validation
    }
}

/// Rejects nested or alternative JWS processing, including embedded and remotely referenced keys.
fn validate_header(raw: &str) -> Result<(), OAuthIssueError> {
    let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| BAD)?;
    unique_json_fields(&bytes).map_err(|_| BAD)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| BAD)?;
    let fields = value.as_object().ok_or(BAD)?;
    if fields
        .keys()
        .any(|key| !matches!(key.as_str(), "alg" | "kid" | "typ"))
    {
        return Err(BAD);
    }
    if fields
        .get("typ")
        .is_some_and(|value| !matches!(value.as_str(), Some("client-authentication+jwt" | "JWT")))
    {
        return Err(BAD);
    }
    Ok(())
}

#[derive(Deserialize)]
struct Jwks<'a> {
    #[serde(borrow)]
    keys: Vec<&'a RawValue>,
}

/// Parses a bounded public key set, rejecting duplicate members and ambiguous key IDs.
pub(super) fn key_set(raw: &[u8]) -> Result<Vec<Value>, OAuthIssueError> {
    unique_json_fields(raw).map_err(|_| BAD)?;
    let jwks: Jwks<'_> = serde_json::from_slice(raw).map_err(|_| BAD)?;
    if !(1..=32).contains(&jwks.keys.len()) {
        return Err(BAD);
    }
    let mut keys: Vec<Value> = Vec::with_capacity(jwks.keys.len());
    for item in jwks.keys {
        unique_json_fields(item.get().as_bytes()).map_err(|_| BAD)?;
        let key: Value = serde_json::from_str(item.get()).map_err(|_| BAD)?;
        let fields = key.as_object().ok_or(BAD)?;
        if ["d", "p", "q", "dp", "dq", "qi", "oth", "k"]
            .iter()
            .any(|name| fields.contains_key(*name))
            || fields
                .get("kty")
                .and_then(Value::as_str)
                .is_none_or(|kty| kty == "oct")
        {
            return Err(BAD);
        }
        if fields.get("kid").is_some_and(|kid| {
            !kid.as_str().is_some_and(|value| bounded(value, 128))
                || keys.iter().any(|existing| existing.get("kid") == Some(kid))
        }) {
            return Err(BAD);
        }
        keys.push(key);
    }
    Ok(keys)
}

/// Selects a public signature key and enforces its declared use, algorithm, and strength.
pub(super) fn decoding_key(jwk: &Value, algorithm: &str) -> Result<DecodingKey, OAuthIssueError> {
    let field = |name| jwk.get(name).and_then(Value::as_str).ok_or(BAD);
    if jwk
        .get("use")
        .is_some_and(|value| value.as_str() != Some("sig"))
        || jwk.get("key_ops").is_some_and(|value| {
            !value.as_array().is_some_and(|ops| {
                ops.len() == 1 && ops.first().and_then(Value::as_str) == Some("verify")
            })
        })
        || jwk
            .get("alg")
            .is_some_and(|value| value.as_str() != Some(algorithm))
    {
        return Err(BAD);
    }
    match (algorithm, field("kty")?) {
        ("RS256", "RSA") => {
            let n = field("n")?;
            let e = field("e")?;
            let modulus = URL_SAFE_NO_PAD.decode(n).map_err(|_| BAD)?;
            let exponent = URL_SAFE_NO_PAD.decode(e).map_err(|_| BAD)?;
            if !(256..=512).contains(&modulus.len())
                || modulus
                    .first()
                    .is_none_or(|high| *high == 0 || (modulus.len() == 256 && *high < 128))
                || !(1..=4).contains(&exponent.len())
            {
                return Err(BAD);
            }
            DecodingKey::from_rsa_components(n, e).map_err(|_| BAD)
        }
        ("ES256", "EC") if field("crv")? == "P-256" => {
            let x = field("x")?;
            let y = field("y")?;
            if [x, y].into_iter().any(|coord| {
                !URL_SAFE_NO_PAD
                    .decode(coord)
                    .is_ok_and(|value| value.len() == 32)
            }) {
                return Err(BAD);
            }
            DecodingKey::from_ec_components(x, y).map_err(|_| BAD)
        }
        _ => Err(BAD),
    }
}

fn replay_digest(issuer: &str, client: &str, jti: &str) -> String {
    let mut digest = blake3::Hasher::new_derive_key("vyuh oauth-issue client assertion replay v1");
    for value in [issuer, client, jti] {
        digest.update(&(value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    digest.finalize().to_hex().to_string()
}

#[cfg(test)]
#[path = "tests/assertion.rs"]
mod tests;
