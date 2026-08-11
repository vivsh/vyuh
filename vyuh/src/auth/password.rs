use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::{
    digest, pbkdf2,
    rand::{SecureRandom, SystemRandom},
};
use thiserror::Error;

const DJANGO_5_2_PBKDF2_ITERATIONS: u32 = 1_000_000;
const MAX_PBKDF2_ITERATIONS: u32 = 10_000_000;
const UNUSABLE_PASSWORD_PREFIX: &str = "!";
const UNUSABLE_PASSWORD_SUFFIX_LEN: usize = 40;

/// Password hashing and encoded-hash validation failures.
#[derive(Debug, Error)]
pub enum PasswordError {
    /// The selected PBKDF2 algorithm is unsupported.
    #[error("unsupported password algorithm")]
    UnsupportedAlgorithm,
    /// The requested or stored PBKDF2 work factor is invalid.
    #[error("invalid password iterations")]
    InvalidIterations,
    /// The stored password hash has an invalid structure or encoding.
    #[error("invalid password hash")]
    InvalidHash,
    /// Cryptographically secure randomness was unavailable.
    #[error("password randomness unavailable")]
    Randomness,
    /// The asynchronous blocking worker could not complete.
    #[error("password worker failed")]
    Worker,
}

/// Creates a Django-compatible unusable password marker.
pub fn unusable_password() -> Result<String, PasswordError> {
    let mut bytes = [0_u8; UNUSABLE_PASSWORD_SUFFIX_LEN / 2];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| PasswordError::Randomness)?;
    Ok(format!("{UNUSABLE_PASSWORD_PREFIX}{}", hex(&bytes)))
}

/// Creates a Django-compatible PBKDF2 password hash without blocking an async worker.
pub async fn make_password(
    password: &str,
    salt: Option<&str>,
    algorithm: Option<&str>,
) -> Result<String, PasswordError> {
    make_password_with_iterations(password, salt, algorithm, DJANGO_5_2_PBKDF2_ITERATIONS).await
}

/// Creates a Django-compatible PBKDF2 hash with an application-selected work factor.
pub async fn make_password_with_iterations(
    password: &str,
    salt: Option<&str>,
    algorithm: Option<&str>,
    iterations: u32,
) -> Result<String, PasswordError> {
    let password = password.to_owned();
    let salt = salt.map(str::to_owned);
    let algorithm = algorithm.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        make_password_blocking(&password, salt.as_deref(), algorithm.as_deref(), iterations)
    })
    .await
    .map_err(|_| PasswordError::Worker)?
}

/// Result of password verification including whether the stored work factor is stale.
pub struct PasswordVerification {
    /// Whether the supplied password matched the stored hash.
    pub valid: bool,
    /// Whether a successful match should be rehashed at the requested work factor.
    pub needs_upgrade: bool,
}

/// Verifies a Django-compatible PBKDF2 password hash without blocking an async worker.
pub async fn check_password(password: &str, encoded: &str) -> Result<bool, PasswordError> {
    check_password_with_upgrade(password, encoded, DJANGO_5_2_PBKDF2_ITERATIONS)
        .await
        .map(|result| result.valid)
}

/// Verifies a stored hash and reports whether its iteration count should be upgraded.
pub async fn check_password_with_upgrade(
    password: &str,
    encoded: &str,
    preferred_iterations: u32,
) -> Result<PasswordVerification, PasswordError> {
    let password = password.to_owned();
    let encoded = encoded.to_owned();
    tokio::task::spawn_blocking(move || {
        check_password_blocking(&password, &encoded, preferred_iterations)
    })
    .await
    .map_err(|_| PasswordError::Worker)?
}

fn make_password_blocking(
    password: &str,
    salt: Option<&str>,
    algorithm: Option<&str>,
    iterations: u32,
) -> Result<String, PasswordError> {
    let name = algorithm.unwrap_or("pbkdf2_sha256");
    let salt = salt.map(str::to_owned).map_or_else(random_salt, Ok)?;
    if salt.is_empty() || salt.len() > 1024 {
        return Err(PasswordError::InvalidHash);
    }
    let iterations = validate_iterations(iterations)?;
    let (algorithm, length) = algorithm_details(name)?;
    let mut derived = vec![0; length];
    pbkdf2::derive(
        algorithm,
        iterations,
        salt.as_bytes(),
        password.as_bytes(),
        &mut derived,
    );
    Ok(format!(
        "{name}${}${salt}${}",
        iterations.get(),
        STANDARD.encode(derived)
    ))
}

fn check_password_blocking(
    password: &str,
    encoded: &str,
    preferred_iterations: u32,
) -> Result<PasswordVerification, PasswordError> {
    if encoded.starts_with(UNUSABLE_PASSWORD_PREFIX) {
        return Ok(PasswordVerification {
            valid: false,
            needs_upgrade: false,
        });
    }
    let (name, iterations, salt, hash) = password_parts(encoded)?;
    let stored = validate_iterations(iterations)?;
    let hash = STANDARD
        .decode(hash)
        .map_err(|_| PasswordError::InvalidHash)?;
    let (algorithm, length) = algorithm_details(name)?;
    if salt.is_empty() || salt.len() > 1024 || hash.len() != length {
        return Err(PasswordError::InvalidHash);
    }
    let valid = pbkdf2::verify(
        algorithm,
        stored,
        salt.as_bytes(),
        password.as_bytes(),
        &hash,
    )
    .is_ok();
    Ok(PasswordVerification {
        valid,
        needs_upgrade: valid && stored.get() < preferred_iterations,
    })
}

fn password_parts(encoded: &str) -> Result<(&str, u32, &str, &str), PasswordError> {
    let mut parts = encoded.split('$');
    let (Some(algorithm), Some(iterations), Some(salt), Some(hash), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(PasswordError::InvalidHash);
    };
    let iterations = iterations
        .parse::<u32>()
        .map_err(|_| PasswordError::InvalidIterations)?;
    Ok((algorithm, iterations, salt, hash))
}

fn validate_iterations(value: u32) -> Result<std::num::NonZeroU32, PasswordError> {
    if value > MAX_PBKDF2_ITERATIONS {
        return Err(PasswordError::InvalidIterations);
    }
    std::num::NonZeroU32::new(value).ok_or(PasswordError::InvalidIterations)
}

fn algorithm_details(name: &str) -> Result<(pbkdf2::Algorithm, usize), PasswordError> {
    match name {
        "pbkdf2_sha256" => Ok((pbkdf2::PBKDF2_HMAC_SHA256, digest::SHA256_OUTPUT_LEN)),
        "pbkdf2_sha1" => Ok((pbkdf2::PBKDF2_HMAC_SHA1, digest::SHA1_OUTPUT_LEN)),
        _ => Err(PasswordError::UnsupportedAlgorithm),
    }
}

fn random_salt() -> Result<String, PasswordError> {
    let mut bytes = [0_u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| PasswordError::Randomness)?;
    Ok(STANDARD.encode(bytes))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}
