//! Bounded, failure-only sanitization; never changes the retained source error.

const TEXT_LIMIT: usize = 4096;

/// Recognizes secret-bearing configuration fields whose values must be omitted.
pub(super) fn sensitive(field: &str) -> bool {
    let field = field.to_ascii_lowercase();
    [
        "secret",
        "password",
        "token",
        "credential",
        "authorization",
        "api_key",
        "private_key",
    ]
    .iter()
    .any(|key| field.contains(key))
}

/// Redacts before truncating so a size boundary cannot expose half a credential.
pub(super) fn sanitize(value: &str) -> String {
    let mut value = value.to_owned();
    // Patterns are built only while presenting a failure, never in request paths.
    for (pattern, replacement) in [
        (
            r#"(?i)\bauthorization\b["']?\s*[:=][^\r\n;,}]+"#,
            "[authorization omitted]",
        ),
        (
            r"(?s)-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|$)",
            "[private key omitted]",
        ),
        (
            r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s/@]*@",
            "${1}[credentials omitted]@",
        ),
        (
            r#"(?i)\b(?:password|passwd|pwd|secret(?:_key)?|client_secret|api[_-]?key|access_token|refresh_token|token|authorization|credential)\b["']?\s*[:=]\s*(?:"(?:\\.|[^"\\])*"?|'(?:\\.|[^'\\])*'?|[^\s&,;\]}]+)"#,
            "[credential omitted]",
        ),
        (
            r"(?i)\b(?:Bearer|Basic)\s+[^\s,;]+",
            "[authorization omitted]",
        ),
        (r"\x1b\[[0-?]*[ -/]*[@-~]", ""),
        (r"(?s)\x1b\].*?(?:\x07|\x1b\\|$)", ""),
    ] {
        let Ok(pattern) = regex::Regex::new(pattern) else {
            return "[diagnostic text omitted: sanitizer unavailable]".into();
        };
        value = pattern.replace_all(&value, replacement).into_owned();
    }
    let value: String = value
        .chars()
        .filter(|c| {
            (!c.is_control() || *c == '\n')
                && !matches!(*c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
        .collect();
    truncate(value)
}

/// Bounds one rendered component without splitting a UTF-8 character.
fn truncate(mut value: String) -> String {
    if value.len() > TEXT_LIMIT {
        let mut end = TEXT_LIMIT;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
        value.push_str(" [truncated]");
    }
    value
}

#[cfg(test)]
#[path = "tests/sanitize.rs"]
mod tests;
