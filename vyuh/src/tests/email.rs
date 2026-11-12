use super::*;

/// Verifies enabled SMTP configuration requires a host and default sender.
#[test]
fn smtp_requires_connection_fields() {
    let conf = MailConf {
        enabled: true,
        ..MailConf::default()
    };
    let mut errors = Vec::new();
    conf.validate(&mut errors);
    assert_eq!(errors.len(), 2);
}

/// Verifies an existing SMTP URL still loads with no relay header mapping.
#[test]
fn smtp_url_without_mapping() -> Result<(), ConfError> {
    let conf = MailConf::from_url(
        "smtps://mailer:secret@mail.example.com?sender=noreply%40example.com&timeout_seconds=10",
    )?;
    assert!(conf.enabled);
    assert_eq!(conf.host, "mail.example.com");
    assert_eq!(conf.port, 465);
    assert_eq!(conf.username.as_deref(), Some("mailer"));
    assert_eq!(conf.password.as_deref(), Some("secret"));
    assert_eq!(conf.sender.as_deref(), Some("noreply@example.com"));
    assert_eq!(conf.timeout_seconds, 10);
    assert!(conf.idempotency_header.is_none());
    Ok(())
}

/// Verifies SMTP URL parsing decodes and retains the configured relay header.
#[test]
fn smtp_url_with_mapping() -> Result<(), ConfError> {
    let conf = MailConf::from_url(
        "smtps://mail.example.com?sender=noreply%40example.com&idempotency_header=X%2DRelay%2DKey",
    )?;
    assert_eq!(conf.idempotency_header.as_deref(), Some("X-Relay-Key"));
    Ok(())
}

/// Verifies empty, malformed, and reserved names fail parsing without exposing credentials.
#[test]
fn smtp_url_rejects_bad_headers() {
    let oversized = "X".repeat(77);
    for name in [
        "",
        "X-Key%0D%0A",
        "X+Key",
        "X%3AKey",
        "bCc",
        "CONTENT-Type",
        &oversized,
    ] {
        let url =
            format!("smtps://mailer:example-secret@mail.example.com?idempotency_header={name}");
        let error = MailConf::from_url(&url).err();
        assert!(
            matches!(&error, Some(ConfError::InvalidValue { field, .. }) if field == "mail.idempotency_header")
        );
        assert!(error.is_some_and(|error| !error.to_string().contains("example-secret")));
    }
}

/// Verifies `SiteConf::validate` also checks header names assigned in Rust or deserialized.
#[test]
fn site_rejects_bad_header() {
    let conf = crate::SiteConf::default().mail(MailConf {
        idempotency_header: Some("Message-ID".into()),
        ..MailConf::default()
    });
    assert!(
        matches!(conf.validate(), Err(ConfError::Many(errors)) if errors.iter().any(|error| {
            matches!(error, ConfError::InvalidValue { field, .. } if field == "mail.idempotency_header")
        }))
    );
}
