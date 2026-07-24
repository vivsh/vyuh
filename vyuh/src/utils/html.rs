//! HTML helpers for simple fallback rendering.

/// Escapes text for inclusion in a small HTML text or attribute context.
///
/// This is intended for framework fallback pages and plain snippets. Template
/// engines should continue to use their own escaping rules.
pub fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::escape;

    /// Verifies that text unsafe in HTML is escaped predictably.
    #[test]
    fn escape_html_text() {
        assert_eq!(
            escape("<a href='x'>&\""),
            "&lt;a href=&#39;x&#39;&gt;&amp;&quot;"
        );
    }
}
