use vyuh::auth::{PasswordError, check_password, make_password, unusable_password};

/// Verifies the default Django-compatible password policy round-trips.
#[tokio::test]
async fn round_trip_default() -> Result<(), PasswordError> {
    let pw = "s3cr3tP@ss";
    let encoded = make_password(pw, None, None).await?;
    let ok = check_password(pw, &encoded).await?;
    assert!(ok, "password should validate");
    Ok(())
}

/// Verifies a fixed salt and algorithm produce deterministic compatibility hashes.
#[tokio::test]
async fn same_salt_repeatable() -> Result<(), PasswordError> {
    let pw = "hello123";
    let salt = "fixed-salt-123";
    let a = make_password(pw, Some(salt), Some("pbkdf2_sha256")).await?;
    let b = make_password(pw, Some(salt), Some("pbkdf2_sha256")).await?;
    assert_eq!(a, b);
    assert!(check_password(pw, &a).await?);
    Ok(())
}

/// Verifies a non-matching password is rejected without an operational error.
#[tokio::test]
async fn wrong_password_fails() -> Result<(), PasswordError> {
    let pw = "one";
    let other = "two";
    let encoded = make_password(pw, None, None).await?;
    assert!(!check_password(other, &encoded).await?);
    Ok(())
}

/// Verifies Django-compatible unusable markers never authenticate.
#[tokio::test]
async fn unusable_password_fails_check() -> Result<(), PasswordError> {
    let unusable = unusable_password()?;
    assert!(unusable.starts_with("!"));
    assert!(!check_password("anything", &unusable).await?);
    Ok(())
}

/// Verifies malformed hashes and attacker-controlled work factors fail before PBKDF2 runs.
#[tokio::test]
async fn invalid_hashes_and_work_factors_are_rejected() {
    assert!(matches!(
        check_password("password", "pbkdf2_sha256$10000001$salt$AAAA").await,
        Err(PasswordError::InvalidIterations)
    ));
    assert!(matches!(
        check_password("password", "pbkdf2_sha256$1000$$AAAA").await,
        Err(PasswordError::InvalidHash)
    ));
}
