pub(crate) struct GlobMatcher {
    regex: regex::Regex,
}

impl GlobMatcher {
    pub(crate) fn new(pattern: &str) -> Result<Self, ()> {
        let mut out = String::from("^");
        let mut chars = pattern.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '*' if chars.peek() == Some(&'*') => {
                    chars.next();
                    out.push_str(".*");
                }
                '*' => out.push_str("[^/]*"),
                '?' => out.push_str("[^/]"),
                _ => out.push_str(&regex::escape(&ch.to_string())),
            }
        }
        out.push('$');
        regex::Regex::new(&out)
            .map(|regex| Self { regex })
            .map_err(|_| ())
    }

    pub(crate) fn matches(&self, path: &str) -> bool {
        self.regex.is_match(path)
    }
}
