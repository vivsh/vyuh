use super::sanitize;

/// Credential formats, URL passwords, and terminal escapes cannot reach output.
#[test]
fn redacts_credentials() {
    for text in [
        "postgres://me:hunter2@localhost/db",
        "PASSWORD='hunter2' other=value",
        r#"{"client_secret": "hunter2", "reason": "denied"}"#,
        "Authorization: Bearer hunter2",
        "authorization=Basic hunter2; denied",
        "https://host/?access_token=hunter2&ok=yes",
        "-----BEGIN PRIVATE KEY-----\nhunter2\n-----END PRIVATE KEY-----",
    ] {
        let rendered = sanitize(text);
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("sanitizer unavailable"), "{rendered}");
    }
    assert_eq!(sanitize("\u{1b}[31mdenied\u{1b}[0m\r\u{202e}"), "denied");
    assert_eq!(sanitize("\u{1b}]8;;https://secret\u{7}link"), "link");
}

/// Redaction happens before UTF-8-safe truncation, including unterminated quotes.
#[test]
fn bounds_unicode_and_secrets() {
    let rendered = sanitize(&format!(
        "{} password='{}'",
        "é".repeat(2100),
        "s".repeat(6000)
    ));
    assert!(rendered.ends_with("[truncated]"));
    assert!(rendered.len() < 4200);
    assert!(!sanitize("password='unterminated secret").contains("unterminated"));
    assert_eq!(
        sanitize("missing file\npermission denied"),
        "missing file\npermission denied"
    );
}
