use std::{error::Error, fmt};

use super::sanitize::sanitize;

/// Actionable, sanitized presentation derived from a site-assembly failure.
///
/// Sources remain owned by the original error. Arbitrary application error
/// prose must not contain secrets; pattern-based sanitization cannot detect all
/// sensitive text. No backtrace or configuration dump is included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildDiagnostic {
    /// Concise explanation of what failed.
    pub summary: String,
    /// Relevant registration context and sanitized causes.
    pub details: Vec<String>,
    /// A concrete remedy, when one is known.
    pub hint: Option<String>,
}

impl BuildDiagnostic {
    pub(super) fn new(summary: impl AsRef<str>, hint: impl AsRef<str>) -> Self {
        Self {
            summary: sanitize(summary.as_ref()),
            details: Vec::new(),
            hint: Some(sanitize(hint.as_ref())),
        }
    }

    /// Appends bounded context while suppressing repeated source suffixes.
    pub(super) fn detail(mut self, detail: impl AsRef<str>) -> Self {
        let detail = sanitize(detail.as_ref());
        if !detail.is_empty()
            && detail != self.summary
            && !self
                .details
                .iter()
                .any(|existing| existing.ends_with(&detail))
        {
            self.details.push(detail);
        }
        self
    }

    /// Adds a bounded source chain, suppressing repeated transparent wrappers.
    pub(super) fn causes(mut self, error: &(dyn Error + 'static)) -> Self {
        let mut next = Some(error);
        for _ in 0..8 {
            let Some(error) = next else { return self };
            let (detail, source) = super::sources::cause(error);
            self = self.detail(detail);
            next = source;
        }
        if next.is_some() {
            self = self.detail("[cause chain truncated]");
        }
        self
    }
}

/// Writes the same projection for explicit printing and Result-returning main.
pub(crate) fn render(
    formatter: &mut fmt::Formatter<'_>,
    heading: &str,
    diagnostics: &[BuildDiagnostic],
) -> fmt::Result {
    writeln!(formatter, "{heading}:")?;
    for (index, diagnostic) in diagnostics.iter().enumerate() {
        write!(formatter, "\n{}. {}", index + 1, diagnostic.summary)?;
        for detail in &diagnostic.details {
            for line in detail.lines() {
                write!(formatter, "\n   {line}")?;
            }
        }
        if let Some(hint) = &diagnostic.hint {
            write!(formatter, "\n   hint: {hint}")?;
        }
        if index + 1 < diagnostics.len() {
            writeln!(formatter)?;
        }
    }
    Ok(())
}
