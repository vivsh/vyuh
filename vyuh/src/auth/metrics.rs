//! Bounded-cardinality authentication outcome metrics.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::atomic::{AtomicU64, Ordering},
};

use super::AuthError;

const OUTCOMES: [&str; 5] = ["accepted", "absent", "denied", "invalid", "error"];

/// Authentication counters indexed by startup-validated provider and method names.
pub(crate) struct AuthMetrics {
    providers: MetricSet,
    methods: MetricSet,
}

struct MetricSet {
    indexes: BTreeMap<String, usize>,
    counters: Vec<[AtomicU64; 5]>,
    unknown: [AtomicU64; 5],
}

impl AuthMetrics {
    pub(crate) fn new(
        providers: impl IntoIterator<Item = String>,
        methods: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            providers: MetricSet::new(providers),
            methods: MetricSet::new(methods),
        }
    }

    pub(crate) fn register_provider(&mut self, provider: &str) {
        self.providers.register(provider);
    }

    pub(crate) fn record<T>(&self, provider: &str, result: &Result<T, AuthError>) {
        self.providers.increment(provider, outcome(result));
    }

    pub(crate) fn record_login<T>(&self, method: &str, result: &Result<T, AuthError>) {
        self.methods.increment(method, outcome(result));
    }

    pub(crate) fn render(&self) -> String {
        let mut output = String::from("# TYPE vyuh_auth_attempts_total counter\n");
        self.providers
            .render(&mut output, "vyuh_auth_attempts_total", "provider");
        output.push_str("# TYPE vyuh_login_attempts_total counter\n");
        self.methods
            .render(&mut output, "vyuh_login_attempts_total", "method");
        output
    }
}

impl Clone for AuthMetrics {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            methods: self.methods.clone(),
        }
    }
}

impl MetricSet {
    fn new(names: impl IntoIterator<Item = String>) -> Self {
        let mut output = Self {
            indexes: BTreeMap::new(),
            counters: Vec::new(),
            unknown: std::array::from_fn(|_| AtomicU64::new(0)),
        };
        for name in names {
            output.register(&name);
        }
        output
    }

    fn register(&mut self, name: &str) {
        if self.indexes.contains_key(name) {
            return;
        }
        let index = self.counters.len();
        self.indexes.insert(name.to_owned(), index);
        self.counters
            .push(std::array::from_fn(|_| AtomicU64::new(0)));
    }

    fn increment(&self, name: &str, outcome: usize) {
        match self.indexes.get(name) {
            Some(index) => self.counters[*index][outcome].fetch_add(1, Ordering::Relaxed),
            None => self.unknown[outcome].fetch_add(1, Ordering::Relaxed),
        };
    }

    fn render(&self, output: &mut String, metric: &str, label: &str) {
        for (name, index) in &self.indexes {
            for (outcome, count) in OUTCOMES.iter().zip(&self.counters[*index]) {
                let value = count.load(Ordering::Relaxed);
                let _ = writeln!(
                    output,
                    "{metric}{{{label}=\"{name}\",outcome=\"{outcome}\"}} {value}"
                );
            }
        }
        for (outcome, count) in OUTCOMES.iter().zip(&self.unknown) {
            let value = count.load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "{metric}{{{label}=\"<unknown>\",outcome=\"{outcome}\"}} {value}"
            );
        }
    }
}

impl Clone for MetricSet {
    fn clone(&self) -> Self {
        let counters = self
            .counters
            .iter()
            .map(|values| {
                std::array::from_fn(|index| AtomicU64::new(values[index].load(Ordering::Relaxed)))
            })
            .collect();
        Self {
            indexes: self.indexes.clone(),
            counters,
            unknown: std::array::from_fn(|index| {
                AtomicU64::new(self.unknown[index].load(Ordering::Relaxed))
            }),
        }
    }
}

fn outcome<T>(result: &Result<T, AuthError>) -> usize {
    match result {
        Ok(_) => 0,
        Err(AuthError::NoCredential) => 1,
        Err(AuthError::AudienceMismatch | AuthError::Forbidden | AuthError::InvalidCsrfToken) => 2,
        Err(error) if operational_error(error) => 4,
        Err(_) => 3,
    }
}

fn operational_error(error: &AuthError) -> bool {
    matches!(
        error,
        AuthError::InvalidProviderConfig(_)
            | AuthError::ProviderNotFound(_)
            | AuthError::DuplicateProvider(_)
            | AuthError::AmbiguousProvider(_)
            | AuthError::ProviderUnavailable
            | AuthError::Internal(_)
            | AuthError::DeliveryFailed
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies unregistered selectors use one bounded label without exposing raw names.
    #[test]
    fn unknown_selectors_use_bounded_metric_bucket() {
        let metrics = AuthMetrics::new(["default".to_owned()], ["password".to_owned()]);
        metrics.record::<()>(
            "attacker-controlled-provider",
            &Err(AuthError::ProviderNotFound("ignored".into())),
        );
        metrics.record_login::<()>(
            "attacker-controlled-method",
            &Err(AuthError::LoginMethodNotFound("ignored".into())),
        );
        let rendered = metrics.render();
        assert!(rendered.contains("provider=\"<unknown>\""));
        assert!(rendered.contains("method=\"<unknown>\""));
        assert!(!rendered.contains("attacker-controlled"));
    }
}
