//! Bounded-cardinality cache operation metrics.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const OPERATIONS: [&str; 10] = [
    "get",
    "set",
    "add",
    "get_many",
    "set_many",
    "delete",
    "delete_many",
    "touch",
    "increment",
    "clear",
];

/// Fixed cache operation labels used by the site-owned metrics registry.
#[derive(Clone, Copy)]
pub(crate) enum CacheOperation {
    Get,
    Set,
    Add,
    GetMany,
    SetMany,
    Delete,
    DeleteMany,
    Touch,
    Increment,
    Clear,
}

impl CacheOperation {
    const fn index(self) -> usize {
        match self {
            Self::Get => 0,
            Self::Set => 1,
            Self::Add => 2,
            Self::GetMany => 3,
            Self::SetMany => 4,
            Self::Delete => 5,
            Self::DeleteMany => 6,
            Self::Touch => 7,
            Self::Increment => 8,
            Self::Clear => 9,
        }
    }
}

/// Metrics for fixed startup-validated provider names and operations.
pub(crate) struct CacheMetrics {
    indexes: BTreeMap<String, usize>,
    values: Vec<[OperationMetrics; OPERATIONS.len()]>,
    unknown: [OperationMetrics; OPERATIONS.len()],
}

struct OperationMetrics {
    successes: AtomicU64,
    failures: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    duration_nanos: AtomicU64,
}

impl CacheMetrics {
    pub(crate) fn new(names: impl IntoIterator<Item = String>) -> Self {
        let mut indexes = BTreeMap::new();
        let mut values = Vec::new();
        for name in names {
            if indexes.contains_key(&name) {
                continue;
            }
            indexes.insert(name, values.len());
            values.push(std::array::from_fn(|_| OperationMetrics::new()));
        }
        Self {
            indexes,
            values,
            unknown: std::array::from_fn(|_| OperationMetrics::new()),
        }
    }

    pub(crate) fn record(
        &self,
        name: &str,
        operation: CacheOperation,
        success: bool,
        hit: Option<bool>,
        duration: Duration,
    ) {
        let operation_index = operation.index();
        let values = self
            .indexes
            .get(name)
            .map(|provider_index| &self.values[*provider_index][operation_index])
            .unwrap_or(&self.unknown[operation_index]);
        values.record(success, hit, duration);
    }

    pub(crate) fn render(&self) -> String {
        let mut output = String::new();
        output.push_str("# TYPE vyuh_cache_operations_total counter\n");
        output.push_str("# TYPE vyuh_cache_hits_total counter\n");
        output.push_str("# TYPE vyuh_cache_misses_total counter\n");
        output.push_str("# TYPE vyuh_cache_operation_duration_seconds summary\n");
        for (name, index) in &self.indexes {
            self.render_provider(&mut output, name, &self.values[*index]);
        }
        self.render_provider(&mut output, "<unknown>", &self.unknown);
        output
    }

    fn render_provider(
        &self,
        output: &mut String,
        name: &str,
        values: &[OperationMetrics; OPERATIONS.len()],
    ) {
        for (operation, values) in OPERATIONS.iter().zip(values) {
            let success = values.successes.load(Ordering::Relaxed);
            let failure = values.failures.load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "vyuh_cache_operations_total{{cache=\"{name}\",operation=\"{operation}\",outcome=\"success\"}} {success}"
            );
            let _ = writeln!(
                output,
                "vyuh_cache_operations_total{{cache=\"{name}\",operation=\"{operation}\",outcome=\"error\"}} {failure}"
            );
            let hits = values.hits.load(Ordering::Relaxed);
            let misses = values.misses.load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "vyuh_cache_hits_total{{cache=\"{name}\",operation=\"{operation}\"}} {hits}"
            );
            let _ = writeln!(
                output,
                "vyuh_cache_misses_total{{cache=\"{name}\",operation=\"{operation}\"}} {misses}"
            );
            let seconds = values.duration_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
            let _ = writeln!(
                output,
                "vyuh_cache_operation_duration_seconds_sum{{cache=\"{name}\",operation=\"{operation}\"}} {seconds}"
            );
            let _ = writeln!(
                output,
                "vyuh_cache_operation_duration_seconds_count{{cache=\"{name}\",operation=\"{operation}\"}} {}",
                success + failure
            );
        }
    }
}

impl OperationMetrics {
    const fn new() -> Self {
        Self {
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            duration_nanos: AtomicU64::new(0),
        }
    }

    fn record(&self, success: bool, hit: Option<bool>, duration: Duration) {
        if success {
            self.successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        match hit {
            Some(true) => self.hits.fetch_add(1, Ordering::Relaxed),
            Some(false) => self.misses.fetch_add(1, Ordering::Relaxed),
            None => 0,
        };
        self.duration_nanos.fetch_add(
            duration.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}
