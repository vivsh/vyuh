use super::prepared::fallback;
use super::*;
use crate::utils::html::html_to_text;

/// Verifies HTML-only mail receives a non-empty generated text alternative.
#[test]
fn html_mail_generates_text() -> Result<(), EmailError> {
    let text = html_to_text("<h1>Hello</h1><p>Read <a href=\"https://example.com\">more</a>.</p>")
        .map_err(|_| EmailError::TextFallback)?;
    assert!(text.contains("Hello"));
    assert!(text.contains("https://example.com"));
    Ok(())
}

/// Verifies email cannot be built without an envelope recipient.
#[test]
fn mail_requires_recipient() {
    let error = Mail::new().subject("Hello").text("Body").build().err();
    assert!(matches!(error, Some(EmailError::MissingRecipient)));
}

/// Verifies malformed recipients fail before an SMTP connection is attempted.
#[test]
fn mail_rejects_invalid_recipient() {
    let error = Mail::new()
        .to("not an address")
        .subject("Hello")
        .text("Body")
        .build()
        .err();
    assert!(matches!(
        error,
        Some(EmailError::Invalid { field: "to", .. })
    ));
}

/// Verifies non-content HTML cannot create a blank text fallback silently.
#[test]
fn empty_html_fallback_fails() {
    let error = fallback(None, Some("<style>body { color: red; }</style>"), false).err();
    assert!(matches!(error, Some(EmailError::TextFallback)));
}

/// Verifies attachment names cannot introduce MIME header or path injection.
#[test]
fn attachment_rejects_unsafe_name() {
    let error = Attachment::bytes("report\r\nX-Test: injected", vec![1])
        .validate()
        .err();
    assert!(matches!(
        error,
        Some(EmailError::Invalid {
            field: "attachment.name",
            ..
        })
    ));
}

/// Verifies malformed submission keys fail at build time.
#[test]
fn rejects_invalid_keys() {
    for key in [
        "".into(),
        "receipt/one two".into(),
        "receipt\r\n".into(),
        "x".repeat(257),
    ] {
        let error = Mail::new()
            .to("ada@example.com")
            .subject("Receipt")
            .text("Body")
            .idempotency_key(key)
            .build()
            .err();
        assert!(matches!(
            error,
            Some(EmailError::Invalid {
                field: "idempotency_key",
                ..
            })
        ));
    }
}
