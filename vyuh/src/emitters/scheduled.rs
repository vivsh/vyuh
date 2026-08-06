//! Per-site durable task scheduling for cron and periodic emitters.

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, TimeZone, Utc};

use super::{Emitter, EmitterError, EmitterSource, ScheduleStart};
use crate::{Site, callables::DataBox};

const INITIAL_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(60);

/// Starts one per-site worker for each validated task-targeted emitter.
pub(super) async fn start(
    site: &Site,
    sources: Vec<Arc<Emitter>>,
    permits: Arc<tokio::sync::Semaphore>,
) -> Result<(), EmitterError> {
    let names = schedule_names(&sources)?;
    let snapshot = site
        .task_schedule_snapshot(&names)
        .await
        .map_err(|error| EmitterError::OtherError(Box::new(error)))?;
    for source in sources {
        let cursor = cursor_for(&source, &snapshot.cursors)?;
        let work = ScheduleWork::new(site.clone(), source, cursor, snapshot.now, permits.clone());
        site.spawn(async move { work.run().await });
    }
    Ok(())
}

/// Resolves one source cursor without cloning the complete startup snapshot.
fn cursor_for(
    source: &Emitter,
    cursors: &std::collections::HashMap<String, DateTime<Utc>>,
) -> Result<Option<DateTime<Utc>>, EmitterError> {
    let name = source
        .schedule
        .as_ref()
        .ok_or_else(|| EmitterError::InvalidSchedule("missing task schedule metadata".into()))?;
    Ok(cursors.get(&name.name).copied())
}

/// Collects the stable cursor names before the store performs one batched lookup.
fn schedule_names(sources: &[Arc<Emitter>]) -> Result<Vec<String>, EmitterError> {
    sources
        .iter()
        .map(|source| {
            source
                .schedule
                .as_ref()
                .map(|schedule| schedule.name.clone())
                .ok_or_else(|| {
                    EmitterError::InvalidSchedule("missing task schedule metadata".into())
                })
        })
        .collect()
}

struct ScheduleWork {
    site: Site,
    source: Arc<Emitter>,
    cursor: Option<DateTime<Utc>>,
    startup_time: DateTime<Utc>,
    permits: Arc<tokio::sync::Semaphore>,
    iteration: usize,
    last_time: Option<tokio::time::Instant>,
}

impl ScheduleWork {
    /// Builds one isolated schedule worker from finalized emitter metadata.
    fn new(
        site: Site,
        source: Arc<Emitter>,
        cursor: Option<DateTime<Utc>>,
        startup_time: DateTime<Utc>,
        permits: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        Self {
            site,
            source,
            cursor,
            startup_time,
            permits,
            iteration: 0,
            last_time: None,
        }
    }

    /// Runs one durable schedule until its site begins shutdown.
    async fn run(mut self) {
        let mut next = match self.initial_deadline(self.startup_time) {
            Ok(value) => value,
            Err(error) => return log_schedule_error(&self.source, error),
        };
        let mut retry = INITIAL_RETRY;
        let mut pending = None;
        let shutdown = self.site.shutdown_notifier();
        loop {
            tokio::select! {
                _ = shutdown.notified() => break,
                _ = tokio::time::sleep(wait_duration(next)) => {
                    let result = self.fire(pending.take(), next).await;
                    match result {
                        Ok(FireResult::Submitted) => {
                            retry = INITIAL_RETRY;
                            next = match self.next_deadline(Utc::now()) {
                                Ok(value) => value,
                                Err(error) => return log_schedule_error(&self.source, error),
                            };
                        }
                        Ok(FireResult::Pending(value)) => {
                            pending = Some(value);
                            next = retry_deadline(retry);
                            retry = retry.saturating_mul(2).min(MAX_RETRY);
                        }
                        Err(error) => {
                            log_schedule_error(&self.source, error);
                            next = retry_deadline(retry);
                            retry = retry.saturating_mul(2).min(MAX_RETRY);
                        }
                    }
                }
            }
        }
    }

    /// Runs a producer once or retries an already-produced payload.
    async fn fire(
        &mut self,
        pending: Option<PendingSubmission>,
        occurrence: DateTime<Utc>,
    ) -> Result<FireResult, EmitterError> {
        let pending = match pending {
            Some(value) => value,
            None => self.produce(occurrence).await?,
        };
        match self
            .site
            .submit_scheduled_task(&pending.name, pending.occurrence, pending.payload.clone())
            .await
        {
            Ok(()) => Ok(FireResult::Submitted),
            Err(error) => Ok(FireResult::Pending(pending.with_error(error))),
        }
    }

    /// Produces deterministic task input while respecting the shared emitter limit.
    async fn produce(
        &mut self,
        occurrence: DateTime<Utc>,
    ) -> Result<PendingSubmission, EmitterError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EmitterError::InvalidSchedule("emitter worker limit closed".into()))?;
        let handler = self.handler()?.clone();
        let payload = handler
            .call(super::EmitterContext {
                site: self.site.clone(),
                payload: DataBox::new(String::new()),
                iter_count: self.iteration,
                last_time: self.last_time,
            })
            .await
            .map_err(|error| EmitterError::OtherError(Box::new(error)))?;
        drop(permit);
        self.iteration = self.iteration.wrapping_add(1);
        self.last_time = Some(tokio::time::Instant::now());
        Ok(PendingSubmission {
            name: self.schedule_name()?.into(),
            occurrence,
            payload,
        })
    }

    /// Selects the first conservative execution time from the durable cursor.
    fn initial_deadline(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, EmitterError> {
        match (self.cursor, self.start()?) {
            (None, ScheduleStart::Immediately) => Ok(now),
            (None, ScheduleStart::Next) => self.next_deadline(now),
            (Some(cursor), _) if self.next_deadline(cursor)? <= now => Ok(now),
            (Some(_), _) => self.next_deadline(now),
        }
    }

    /// Finds the next normal source occurrence strictly after the supplied time.
    fn next_deadline(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, EmitterError> {
        match &self.source.source {
            EmitterSource::Cron { schedule, .. } => {
                schedule.after(&after).next().ok_or_else(|| {
                    EmitterError::InvalidSchedule("cron expression has no next occurrence".into())
                })
            }
            EmitterSource::Periodic { interval, .. } => periodic_boundary(after, *interval),
            EmitterSource::PgNotify { .. } => Err(EmitterError::InvalidSchedule(
                "pgnotify cannot use a task executor".into(),
            )),
        }
    }

    /// Returns the selected task schedule name.
    fn schedule_name(&self) -> Result<&str, EmitterError> {
        self.source
            .schedule
            .as_ref()
            .map(|schedule| schedule.name.as_str())
            .ok_or_else(|| EmitterError::InvalidSchedule("missing task schedule metadata".into()))
    }

    /// Returns the selected task schedule activation policy.
    fn start(&self) -> Result<ScheduleStart, EmitterError> {
        self.source
            .schedule
            .as_ref()
            .map(|schedule| schedule.start)
            .ok_or_else(|| EmitterError::InvalidSchedule("missing task schedule metadata".into()))
    }

    /// Returns the configured producer callable for this timed emitter.
    fn handler(&self) -> Result<&super::EmitterHandler, EmitterError> {
        match &self.source.source {
            EmitterSource::Cron { handler, .. } | EmitterSource::Periodic { handler, .. } => {
                Ok(handler)
            }
            EmitterSource::PgNotify { .. } => Err(EmitterError::InvalidSchedule(
                "pgnotify cannot use a task executor".into(),
            )),
        }
    }
}

struct PendingSubmission {
    name: String,
    occurrence: DateTime<Utc>,
    payload: DataBox,
}

impl PendingSubmission {
    /// Keeps a successful producer result across transient task-store failures.
    fn with_error(self, error: crate::SiteError) -> Self {
        tracing::warn!(schedule = self.name, error = %error, "scheduled task submission will retry");
        self
    }
}

enum FireResult {
    Submitted,
    Pending(PendingSubmission),
}

/// Computes a UTC-epoch-aligned periodic boundary with millisecond precision.
pub(crate) fn periodic_boundary(
    after: DateTime<Utc>,
    interval: Duration,
) -> Result<DateTime<Utc>, EmitterError> {
    let milliseconds = i64::try_from(interval.as_millis()).map_err(|_| {
        EmitterError::InvalidSchedule("periodic interval exceeds the supported range".into())
    })?;
    if milliseconds <= 0 {
        return Err(EmitterError::InvalidSchedule(
            "task periodic intervals must be at least one millisecond".into(),
        ));
    }
    let next = after
        .timestamp_millis()
        .div_euclid(milliseconds)
        .checked_add(1)
        .and_then(|slot| slot.checked_mul(milliseconds))
        .ok_or_else(|| EmitterError::InvalidSchedule("periodic boundary overflowed".into()))?;
    Utc.timestamp_millis_opt(next)
        .single()
        .ok_or_else(|| EmitterError::InvalidSchedule("periodic boundary is invalid".into()))
}

/// Converts one wall-clock deadline into a bounded monotonic sleep duration.
fn wait_duration(deadline: DateTime<Utc>) -> Duration {
    deadline
        .signed_duration_since(Utc::now())
        .to_std()
        .unwrap_or_default()
}

/// Delays a retry without retaining a process-global timer.
fn retry_deadline(delay: Duration) -> DateTime<Utc> {
    chrono::Duration::from_std(delay)
        .ok()
        .and_then(|value| Utc::now().checked_add_signed(value))
        .unwrap_or_else(Utc::now)
}

/// Logs a safe durable schedule failure without exposing produced task input.
fn log_schedule_error(source: &Emitter, error: EmitterError) {
    let schedule = source
        .schedule
        .as_ref()
        .map(|schedule| schedule.name.as_str())
        .unwrap_or("unknown");
    tracing::error!(schedule, error = %error, "task-targeted emitter failed");
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    /// Verifies task periodic schedules align to UTC epoch boundaries.
    #[test]
    fn periodic_boundaries_are_epoch_aligned() -> Result<(), String> {
        let after = Utc
            .timestamp_millis_opt(1_500)
            .single()
            .ok_or("fixture timestamp is invalid")?;
        let next =
            periodic_boundary(after, Duration::from_secs(1)).map_err(|error| error.to_string())?;
        assert_eq!(next.timestamp_millis(), 2_000);
        Ok(())
    }
}
