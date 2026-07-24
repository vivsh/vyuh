//! Text helpers for common web-application naming and URL tasks.

/// Converts free text into a lowercase ASCII slug.
///
/// Non-alphanumeric runs become single dashes. Empty or punctuation-only input
/// returns an empty string so callers can choose an application-specific
/// fallback.
pub fn slugify(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut dash = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

/// Appends a numeric suffix to a slug candidate.
///
/// Index zero returns the base unchanged. Higher indexes return `base-index`,
/// which is useful for deterministic unique slug generation.
pub fn numbered_slug(base: &str, index: usize) -> String {
    if index == 0 {
        base.to_string()
    } else {
        format!("{base}-{index}")
    }
}

#[cfg(test)]
mod tests {
    use super::{numbered_slug, slugify};

    /// Verifies that slug generation normalizes common title text.
    #[test]
    fn slugify_title_text() {
        assert_eq!(slugify("Hello, Rust World!"), "hello-rust-world");
    }

    /// Verifies that slug generation leaves fallback choice to callers.
    #[test]
    fn slugify_empty_text() {
        assert_eq!(slugify(" - "), "");
    }

    /// Verifies that numbered slugs keep the first candidate clean.
    #[test]
    fn numbered_slug_suffixes() {
        assert_eq!(numbered_slug("post", 0), "post");
        assert_eq!(numbered_slug("post", 2), "post-2");
    }
}
