//! Shared fixed-point token-bucket arithmetic for task stores.

use std::time::Duration;

use chrono::{DateTime, Utc};

use super::{TaskError, TaskRate};

pub(crate) const TOKEN_SCALE: i64 = 1_000_000;

/// Monotonic token bucket owned by one local task runner.
#[derive(Debug)]
pub(crate) struct LocalRateBucket {
    rate: TaskRate,
    tokens_micros: i64,
    updated_at: tokio::time::Instant,
}

impl LocalRateBucket {
    /// Creates a full bucket for a validated local rate policy.
    pub(crate) fn new(rate: TaskRate, now: tokio::time::Instant) -> Self {
        Self {
            rate,
            tokens_micros: burst_tokens(rate),
            updated_at: now,
        }
    }

    /// Returns complete permits available at the supplied monotonic time.
    pub(crate) fn available(&mut self, now: tokio::time::Instant) -> usize {
        self.refill(now);
        usize::try_from(self.tokens_micros.max(0) / TOKEN_SCALE).unwrap_or(usize::MAX)
    }

    /// Consumes permits corresponding to rows actually claimed by the store.
    pub(crate) fn consume(&mut self, permits: usize, now: tokio::time::Instant) {
        self.refill(now);
        let permits = i64::try_from(permits).unwrap_or(i64::MAX);
        self.tokens_micros = self
            .tokens_micros
            .saturating_sub(permits.saturating_mul(TOKEN_SCALE))
            .max(0);
    }

    /// Returns the monotonic delay until another complete permit is available.
    pub(crate) fn next_permit(&mut self, now: tokio::time::Instant) -> Option<Duration> {
        self.refill(now);
        permit_delay(
            self.tokens_micros,
            self.rate,
            now.saturating_duration_since(self.updated_at),
        )
    }

    fn refill(&mut self, now: tokio::time::Instant) {
        let elapsed = now.saturating_duration_since(self.updated_at);
        let credited = credit(elapsed, self.rate);
        let burst = burst_tokens(self.rate);
        let replenished = self.tokens_micros.saturating_add(credited);
        if replenished >= burst {
            self.tokens_micros = burst;
            self.updated_at = now;
        } else if credited > 0 {
            self.tokens_micros = replenished;
            self.advance_clock(credited, now);
        }
    }

    fn advance_clock(&mut self, credited: i64, now: tokio::time::Instant) {
        let period = self.rate.period().as_micros();
        let denominator = u128::from(self.rate.permits()).saturating_mul(TOKEN_SCALE as u128);
        let consumed = u128::try_from(credited)
            .unwrap_or(u128::MAX)
            .saturating_mul(period)
            / denominator.max(1);
        let consumed = u64::try_from(consumed).unwrap_or(u64::MAX);
        self.updated_at = self
            .updated_at
            .checked_add(Duration::from_micros(consumed))
            .unwrap_or(now);
    }
}

/// Refills a fixed-point bucket without losing sub-token elapsed time.
pub(crate) fn refill(
    tokens_micros: &mut i64,
    updated_at: &mut DateTime<Utc>,
    rate: TaskRate,
    now: DateTime<Utc>,
) -> Result<(), TaskError> {
    let elapsed = (now - *updated_at)
        .num_microseconds()
        .unwrap_or_default()
        .max(0);
    let period = i128::try_from(rate.period().as_micros())
        .map_err(|_| TaskError::InvalidConfig("task rate period is too large".into()))?
        .max(1);
    let credited = i128::from(elapsed)
        .saturating_mul(i128::from(rate.permits()))
        .saturating_mul(i128::from(TOKEN_SCALE))
        / period;
    apply_credit(tokens_micros, updated_at, credited, period, rate, now)
}

/// Returns the delay until one complete permit is available.
pub(crate) fn next_permit(
    tokens_micros: i64,
    rate: TaskRate,
    refill_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<Duration> {
    let accrued = (now - refill_at).to_std().unwrap_or_default();
    permit_delay(tokens_micros, rate, accrued)
}

fn permit_delay(tokens_micros: i64, rate: TaskRate, accrued: Duration) -> Option<Duration> {
    if tokens_micros >= TOKEN_SCALE {
        return None;
    }
    let missing = i128::from(TOKEN_SCALE.saturating_sub(tokens_micros.max(0)));
    let period_nanos = i128::try_from(rate.period().as_nanos()).ok()?;
    let denominator = i128::from(rate.permits()).saturating_mul(i128::from(TOKEN_SCALE));
    let nanos = missing
        .saturating_mul(period_nanos)
        .saturating_add(denominator.saturating_sub(1))
        / denominator.max(1);
    let total = u64::try_from(nanos).ok().map(Duration::from_nanos)?;
    Some(total.saturating_sub(accrued))
}

fn credit(elapsed: Duration, rate: TaskRate) -> i64 {
    let period = rate.period().as_micros().max(1);
    let credited = elapsed
        .as_micros()
        .saturating_mul(u128::from(rate.permits()))
        .saturating_mul(TOKEN_SCALE as u128)
        / period;
    i64::try_from(credited).unwrap_or(i64::MAX)
}

fn burst_tokens(rate: TaskRate) -> i64 {
    i64::from(rate.burst_size()).saturating_mul(TOKEN_SCALE)
}

fn apply_credit(
    tokens_micros: &mut i64,
    updated_at: &mut DateTime<Utc>,
    credited: i128,
    period_micros: i128,
    rate: TaskRate,
    now: DateTime<Utc>,
) -> Result<(), TaskError> {
    let burst = i64::from(rate.burst_size()).saturating_mul(TOKEN_SCALE);
    let credited = i64::try_from(credited).unwrap_or(i64::MAX);
    let replenished = tokens_micros.saturating_add(credited);
    if replenished >= burst {
        *tokens_micros = burst;
        *updated_at = now;
    } else if credited > 0 {
        *tokens_micros = replenished;
        advance_clock(updated_at, credited, period_micros, rate)?;
    }
    Ok(())
}

fn advance_clock(
    updated_at: &mut DateTime<Utc>,
    credited: i64,
    period_micros: i128,
    rate: TaskRate,
) -> Result<(), TaskError> {
    let denominator = i128::from(rate.permits()).saturating_mul(i128::from(TOKEN_SCALE));
    let consumed = i128::from(credited).saturating_mul(period_micros) / denominator.max(1);
    let consumed = i64::try_from(consumed)
        .map_err(|_| TaskError::InvalidConfig("task rate interval is too large".into()))?;
    *updated_at = updated_at
        .checked_add_signed(chrono::Duration::microseconds(consumed))
        .ok_or_else(|| TaskError::InvalidConfig("task rate timestamp overflowed".into()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies the local bucket starts at its burst and replenishes against monotonic time.
    #[test]
    fn local_bucket_refills_without_store_time() {
        let now = tokio::time::Instant::now();
        let mut bucket = LocalRateBucket::new(TaskRate::per_second(2).burst(2), now);
        assert_eq!(bucket.available(now), 2);
        bucket.consume(2, now);
        assert_eq!(bucket.available(now), 0);
        assert_eq!(bucket.next_permit(now), Some(Duration::from_millis(500)));

        let later = now + Duration::from_millis(500);
        assert_eq!(bucket.available(later), 1);
        bucket.consume(1, later);
        assert_eq!(bucket.next_permit(later), Some(Duration::from_millis(500)));
    }

    /// Verifies consuming fewer claimed rows preserves unused local burst permits.
    #[test]
    fn local_bucket_consumes_actual_claims_only() {
        let now = tokio::time::Instant::now();
        let mut bucket = LocalRateBucket::new(TaskRate::per_minute(10).burst(5), now);
        bucket.consume(2, now);
        assert_eq!(bucket.available(now), 3);
        assert_eq!(bucket.next_permit(now), None);
    }
}
