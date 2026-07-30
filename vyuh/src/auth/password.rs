use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::{
    digest, pbkdf2,
    rand::{SecureRandom, SystemRandom},
};

use super::AuthError;

const DJANGO_5_2_PBKDF2_ITERATIONS: u32 = 1_000_000;
const UNUSABLE_PASSWORD_PREFIX: &str = "!";
const UNUSABLE_PASSWORD_SUFFIX_LEN: usize = 40;

/// Creates a Django-compatible unusable password marker.
pub fn unusable_password() -> Result<String, AuthError> {
    let mut bytes = [0_u8; UNUSABLE_PASSWORD_SUFFIX_LEN / 2];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| AuthError::Internal("randomness unavailable".into()))?;
    Ok(format!("{UNUSABLE_PASSWORD_PREFIX}{}", hex(&bytes)))
}

/// Creates a Django-compatible PBKDF2 password hash.
pub fn make_password(
    password: &str,
    salt: Option<&str>,
    algorithm: Option<&str>,
) -> Result<String, AuthError> {
    make_password_with_iterations(password, salt, algorithm, DJANGO_5_2_PBKDF2_ITERATIONS)
}

/// Creates a Django-compatible PBKDF2 hash with an application-selected work factor.
pub fn make_password_with_iterations(
    password: &str,
    salt: Option<&str>,
    algorithm: Option<&str>,
    iterations: u32,
) -> Result<String, AuthError> {
    let algorithm = algorithm.unwrap_or("pbkdf2_sha256");
    let salt = match salt {
        Some(value) => value.to_owned(),
        None => random_salt()?,
    };
    let iterations = std::num::NonZeroU32::new(iterations)
        .ok_or_else(|| AuthError::Internal("invalid PBKDF2 iterations".into()))?;
    let (algorithm_id, length) = match algorithm {
        "pbkdf2_sha256" => (pbkdf2::PBKDF2_HMAC_SHA256, digest::SHA256_OUTPUT_LEN),
        "pbkdf2_sha1" => (pbkdf2::PBKDF2_HMAC_SHA1, digest::SHA1_OUTPUT_LEN),
        _ => return Err(AuthError::Internal("unsupported password algorithm".into())),
    };
    let mut derived = vec![0; length];
    pbkdf2::derive(
        algorithm_id,
        iterations,
        salt.as_bytes(),
        password.as_bytes(),
        &mut derived,
    );
    Ok(format!(
        "{algorithm}${}${salt}${}",
        iterations.get(),
        STANDARD.encode(derived)
    ))
}

/// Result of password verification including whether the stored work factor is stale.
pub struct PasswordVerification {
    /// Whether the supplied password matched the stored hash.
    pub valid: bool,
    /// Whether a successful match should be rehashed at the requested work factor.
    pub needs_upgrade: bool,
}

/// Verifies a Django-compatible PBKDF2 password hash.
pub fn check_password(password: &str, encoded: &str) -> Result<bool, AuthError> {
    check_password_with_upgrade(password, encoded, DJANGO_5_2_PBKDF2_ITERATIONS)
        .map(|result| result.valid)
}

/// Verifies a stored hash and reports whether its iteration count should be upgraded.
pub fn check_password_with_upgrade(
    password: &str,
    encoded: &str,
    preferred_iterations: u32,
) -> Result<PasswordVerification, AuthError> {
    if encoded.starts_with(UNUSABLE_PASSWORD_PREFIX) {
        return Ok(PasswordVerification {
            valid: false,
            needs_upgrade: false,
        });
    }
    let mut parts = encoded.split('$');
    let (Some(algorithm), Some(iterations), Some(salt), Some(hash), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(AuthError::Internal("invalid password hash format".into()));
    };
    let stored_iterations = iterations
        .parse::<u32>()
        .ok()
        .and_then(std::num::NonZeroU32::new)
        .ok_or_else(|| AuthError::Internal("invalid password iterations".into()))?;
    let hash = STANDARD
        .decode(hash)
        .map_err(|_| AuthError::Internal("invalid password hash encoding".into()))?;
    let algorithm = match algorithm {
        "pbkdf2_sha256" => pbkdf2::PBKDF2_HMAC_SHA256,
        "pbkdf2_sha1" => pbkdf2::PBKDF2_HMAC_SHA1,
        _ => return Err(AuthError::Internal("unsupported password algorithm".into())),
    };
    let valid = pbkdf2::verify(
        algorithm,
        stored_iterations,
        salt.as_bytes(),
        password.as_bytes(),
        &hash,
    )
    .is_ok();
    Ok(PasswordVerification {
        valid,
        needs_upgrade: valid && stored_iterations.get() < preferred_iterations,
    })
}

fn random_salt() -> Result<String, AuthError> {
    let mut bytes = [0_u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| AuthError::Internal("randomness unavailable".into()))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies older Django iteration counts remain valid while requesting rehash.
    #[test]
    fn older_iteration_count_requests_upgrade() -> Result<(), AuthError> {
        let encoded = make_password_with_iterations(
            "correct horse",
            Some("testsalt"),
            Some("pbkdf2_sha256"),
            1_000,
        )?;
        let result = check_password_with_upgrade("correct horse", &encoded, 2_000)?;
        assert!(result.valid);
        assert!(result.needs_upgrade);
        Ok(())
    }
}
