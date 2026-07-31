//! HTML helpers for escaping and plain-text fallback rendering.

/// Errors raised while deriving plain text from HTML.
#[derive(Debug, thiserror::Error)]
pub enum HtmlTextError {
    /// The HTML renderer could not produce plain text.
    #[error("HTML-to-text conversion failed: {0}")]
    Render(#[from] html2text::Error),
    /// The input had no visible text after conversion.
    #[error("HTML contains no visible text")]
    Empty,
}

/// Converts HTML into normalized, readable plain text.
///
/// This parser-backed conversion preserves useful structural whitespace and
/// link destinations. It is suitable for email text alternatives and other
/// human-readable fallbacks; it is not an HTML sanitizer.
pub fn html_to_text(html: &str) -> Result<String, HtmlTextError> {
    let text = html2text::from_read(html.as_bytes(), 80)?;
    let text = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty())
        .then_some(text)
        .ok_or(HtmlTextError::Empty)
}

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
    use super::{escape, html_to_text};

    /// Verifies that text unsafe in HTML is escaped predictably.
    #[test]
    fn escape_html_text() {
        assert_eq!(
            escape("<a href='x'>&\""),
            "&lt;a href=&#39;x&#39;&gt;&amp;&quot;"
        );
    }

    /// Verifies HTML conversion preserves visible content and link destinations.
    #[test]
    fn converts_html_to_text() -> Result<(), super::HtmlTextError> {
        let text =
            html_to_text("<h1>Hello</h1><p>Read <a href=\"https://example.com\">more</a>.</p>")?;
        assert!(text.contains("Hello"));
        assert!(text.contains("https://example.com"));
        Ok(())
    }
}
