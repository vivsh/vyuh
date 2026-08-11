//! Safe generation policies for passwordless one-time passwords.

use ring::rand::SecureRandom;

use crate::auth::AuthError;

pub(crate) const MAX_OTP_LENGTH: u8 = 16;
const MIN_NUMERIC_LENGTH: u8 = 6;
const MAX_NUMERIC_LENGTH: u8 = 10;
const MIN_BASE32_LENGTH: u8 = 5;

/// Configures framework-generated one-time passwords.
#[derive(Clone, Copy, Debug)]
pub enum OtpPolicy {
    /// Generates decimal codes with the requested number of digits.
    Numeric(u8),
    /// Generates unambiguous Crockford Base32 codes with the requested length.
    CrockfordBase32(u8),
}

impl OtpPolicy {
    /// Configures a decimal OTP.
    pub const fn numeric(length: u8) -> Self {
        Self::Numeric(length)
    }

    /// Configures an unambiguous upper-case Base32 OTP.
    pub const fn crockford_base32(length: u8) -> Self {
        Self::CrockfordBase32(length)
    }

    pub(crate) fn validate(self) -> Result<(), AuthError> {
        match self {
            Self::Numeric(length)
                if (MIN_NUMERIC_LENGTH..=MAX_NUMERIC_LENGTH).contains(&length) =>
            {
                Ok(())
            }
            Self::CrockfordBase32(length)
                if (MIN_BASE32_LENGTH..=MAX_OTP_LENGTH).contains(&length) =>
            {
                Ok(())
            }
            _ => Err(AuthError::InvalidProviderConfig(
                "invalid OTP policy".into(),
            )),
        }
    }

    fn length(self) -> usize {
        match self {
            Self::Numeric(length) | Self::CrockfordBase32(length) => usize::from(length),
        }
    }
}

pub(crate) fn random_code(policy: OtpPolicy) -> Result<String, AuthError> {
    let alphabet: &[u8] = match policy {
        OtpPolicy::Numeric(_) => b"0123456789",
        OtpPolicy::CrockfordBase32(_) => b"0123456789ABCDEFGHJKMNPQRSTVWXYZ",
    };
    random_from(alphabet, policy.length())
}

fn random_from(alphabet: &[u8], length: usize) -> Result<String, AuthError> {
    let width = u8::try_from(alphabet.len()).map_err(|_| AuthError::InvalidLoginState)?;
    if width == 0 {
        return Err(AuthError::InvalidLoginState);
    }
    let mut value = String::with_capacity(length);
    while value.len() < length {
        append_random(&mut value, alphabet, width, length)?;
    }
    Ok(value)
}

fn append_random(
    value: &mut String,
    alphabet: &[u8],
    width: u8,
    length: usize,
) -> Result<(), AuthError> {
    let threshold = u8::MAX - (u8::MAX % width);
    let mut bytes = [0_u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| AuthError::Internal("passwordless randomness failed".into()))?;
    for byte in bytes {
        if byte < threshold {
            append_byte(value, alphabet, width, length, byte)?;
            if value.len() == length {
                break;
            }
        }
    }
    Ok(())
}

fn append_byte(
    value: &mut String,
    alphabet: &[u8],
    width: u8,
    length: usize,
    byte: u8,
) -> Result<(), AuthError> {
    let index = usize::from(byte % width);
    let character = *alphabet.get(index).ok_or(AuthError::InvalidLoginState)?;
    value.push(char::from(character));
    if value.len() > length {
        return Err(AuthError::InvalidLoginState);
    }
    Ok(())
}
