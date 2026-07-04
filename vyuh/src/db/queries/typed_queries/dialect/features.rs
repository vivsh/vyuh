//! Dialect feature flags.

/// Feature gates that differ between SQL dialects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::db::queries::typed_queries) enum DialectFeature {
    Returning,
    Ilike,
    Upsert,
    RecursiveCte,
}

impl DialectFeature {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Returning => "RETURNING",
            Self::Ilike => "ILIKE",
            Self::Upsert => "upsert",
            Self::RecursiveCte => "recursive CTEs",
        }
    }
}
