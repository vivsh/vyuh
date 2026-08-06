use std::{fmt, time::Duration};

#[cfg(feature = "email")]
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[cfg(feature = "email")]
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[cfg(feature = "email")]
use tokio::sync::mpsc;
#[cfg(feature = "email")]
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
#[cfg(feature = "email")]
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use super::LoggingError;

const DEFAULT_DEBOUNCE_MILLIS: u64 = 300_000;
const MAX_DEBOUNCE_MILLIS: u64 = 86_400_000;
const DEFAULT_THROTTLE_PER_MINUTE: u32 = 12;
const DEFAULT_THROTTLE_PER_HOUR: u32 = 100;
const MAX_THROTTLES: usize = 8;
const MAX_THROTTLE_MILLIS: u64 = 86_400_000;
const MAX_THROTTLE_LIMIT: u32 = 100_000;
#[cfg(feature = "email")]
const REPORT_QUEUE_CAPACITY: usize = 1_024;
#[cfg(feature = "email")]
const MAX_ACTIVE_REPORTS: usize = 256;
#[cfg(feature = "email")]
const MAX_EVENT_BYTES: usize = 16 * 1024;
#[cfg(feature = "email")]
const MAX_MESSAGE_BYTES: usize = 8 * 1024;
#[cfg(feature = "email")]
const MAX_FIELD_BYTES: usize = 384;
#[cfg(feature = "email")]
const MAX_FIELDS: usize = 16;
#[cfg(feature = "email")]
const MAX_SPAN_BYTES: usize = 1_024;

/// Chooses how repeated administrator error reports are grouped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailDedupe {
    /// Groups errors emitted from the same tracing target and source callsite.
    #[default]
    Callsite,
    /// Also distinguishes events whose captured messages differ.
    Exact,
}

/// Chooses the follow-up email sent after a debounce window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailSummary {
    /// Suppresses repeats without sending a follow-up message.
    None,
    /// Sends the repeat count and first/last occurrence times.
    Count,
    /// Sends the count, times, and bounded first/last event samples.
    #[default]
    Samples,
}

/// Caps administrator-email delivery during a time window without delaying reports.
///
/// When the limit is exhausted, the corresponding email is discarded. It is not
/// queued for later delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailThrottle {
    limit: u32,
    window_millis: u64,
}

impl MailThrottle {
    /// Creates a lossy delivery throttle with the supplied message limit and window.
    pub fn new(limit: u32, window: Duration) -> Self {
        Self {
            limit,
            window_millis: duration_millis(window),
        }
    }

    /// Creates a throttle that permits at most `limit` emails per minute.
    pub fn per_minute(limit: u32) -> Self {
        Self::new(limit, Duration::from_secs(60))
    }

    /// Creates a throttle that permits at most `limit` emails per hour.
    pub fn per_hour(limit: u32) -> Self {
        Self::new(limit, Duration::from_secs(3_600))
    }

    #[cfg(feature = "email")]
    fn window(&self) -> Duration {
        Duration::from_millis(self.window_millis)
    }

    fn validate(&self) -> Result<(), LoggingError> {
        if self.limit == 0 || self.limit > MAX_THROTTLE_LIMIT {
            return Err(LoggingError::MailAdminsThrottle {
                limit: self.limit,
                millis: self.window_millis,
            });
        }
        if self.window_millis == 0 || self.window_millis > MAX_THROTTLE_MILLIS {
            return Err(LoggingError::MailAdminsThrottle {
                limit: self.limit,
                millis: self.window_millis,
            });
        }
        Ok(())
    }
}

/// Configures a bounded, debounced administrator-email logging sink.
#[derive(Clone, Serialize, Deserialize)]
pub struct MailAdmins {
    recipients: Vec<String>,
    #[serde(default = "default_debounce_millis")]
    debounce_millis: u64,
    #[serde(default)]
    dedupe: MailDedupe,
    #[serde(default)]
    summary: MailSummary,
    #[serde(default = "default_throttles")]
    throttles: Vec<MailThrottle>,
}

impl fmt::Debug for MailAdmins {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailAdmins")
            .field(
                "recipients",
                &format_args!("<{} configured>", self.recipients.len()),
            )
            .field("debounce_millis", &self.debounce_millis)
            .field("dedupe", &self.dedupe)
            .field("summary", &self.summary)
            .field("throttles", &self.throttles)
            .finish()
    }
}

impl MailAdmins {
    /// Creates an administrator sink using callsite grouping, sampled summaries, and a five-minute debounce.
    pub fn new<I, S>(recipients: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            recipients: recipients.into_iter().map(Into::into).collect(),
            debounce_millis: DEFAULT_DEBOUNCE_MILLIS,
            dedupe: MailDedupe::default(),
            summary: MailSummary::default(),
            throttles: default_throttles(),
        }
    }

    /// Sets the minimum interval between the first error and its repeat summary.
    pub fn debounce(mut self, duration: Duration) -> Self {
        self.debounce_millis = duration_millis(duration);
        self
    }

    /// Sets how repeated error events are grouped during the debounce interval.
    pub fn dedupe(mut self, dedupe: MailDedupe) -> Self {
        self.dedupe = dedupe;
        self
    }

    /// Sets the follow-up report produced for repeated errors.
    pub fn summary(mut self, summary: MailSummary) -> Self {
        self.summary = summary;
        self
    }

    /// Adds a lossy delivery throttle. Every configured throttle must allow an email.
    pub fn throttle(mut self, throttle: MailThrottle) -> Self {
        self.throttles.push(throttle);
        self
    }

    /// Removes the default delivery throttles before adding application-specific limits.
    pub fn unthrottled(mut self) -> Self {
        self.throttles.clear();
        self
    }

    #[cfg(feature = "email")]
    pub(crate) fn recipients(&self) -> &[String] {
        &self.recipients
    }

    #[cfg(feature = "email")]
    pub(crate) fn debounce_duration(&self) -> Duration {
        Duration::from_millis(self.debounce_millis)
    }

    #[cfg(feature = "email")]
    pub(crate) fn dedupe_mode(&self) -> MailDedupe {
        self.dedupe
    }

    #[cfg(feature = "email")]
    pub(crate) fn summary_mode(&self) -> MailSummary {
        self.summary
    }

    #[cfg(feature = "email")]
    fn throttles(&self) -> &[MailThrottle] {
        &self.throttles
    }

    pub(crate) fn validate(&self) -> Result<(), LoggingError> {
        if self.recipients.is_empty() {
            return Err(LoggingError::MailAdminsRecipients);
        }
        if self
            .recipients
            .iter()
            .any(|recipient| recipient.trim().is_empty())
        {
            return Err(LoggingError::MailAdminsRecipientEmpty);
        }
        if self.debounce_millis == 0 || self.debounce_millis > MAX_DEBOUNCE_MILLIS {
            return Err(LoggingError::MailAdminsDebounce {
                millis: self.debounce_millis,
            });
        }
        if self.throttles.len() > MAX_THROTTLES {
            return Err(LoggingError::MailAdminsThrottleCount {
                count: self.throttles.len(),
            });
        }
        for (index, throttle) in self.throttles.iter().enumerate() {
            throttle.validate()?;
            if self.throttles[..index]
                .iter()
                .any(|existing| existing.window_millis == throttle.window_millis)
            {
                return Err(LoggingError::MailAdminsDuplicateThrottle {
                    millis: throttle.window_millis,
                });
            }
        }
        Ok(())
    }
}

/// Captures error events without blocking the tracing caller.
#[cfg(feature = "email")]
pub(crate) struct MailAdminsLayer {
    sender: mpsc::Sender<MailEvent>,
    dropped: Arc<AtomicU64>,
}

#[cfg(feature = "email")]
impl<S> Layer<S> for MailAdminsLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        if *event.metadata().level() != Level::ERROR {
            return;
        }
        let event = MailEvent::capture(event, context);
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Holds the receiver half of one site-owned mail-admin sink.
#[cfg(feature = "email")]
pub(crate) struct MailAdminsRuntime {
    name: String,
    conf: MailAdmins,
    receiver: parking_lot::Mutex<Option<mpsc::Receiver<MailEvent>>>,
    dropped: Arc<AtomicU64>,
}

#[cfg(feature = "email")]
impl MailAdminsRuntime {
    pub(crate) fn new(name: &str, conf: MailAdmins) -> (MailAdminsLayer, Self) {
        let (sender, receiver) = mpsc::channel(REPORT_QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let layer = MailAdminsLayer {
            sender,
            dropped: dropped.clone(),
        };
        let runtime = Self {
            name: name.to_string(),
            conf,
            receiver: parking_lot::Mutex::new(Some(receiver)),
            dropped,
        };
        (layer, runtime)
    }

    pub(crate) fn start(
        &self,
        reporter: crate::email::MailReporter,
        shutdown: crate::notifiers::CancellationNotifier,
        joinset: &mut tokio::task::JoinSet<()>,
    ) {
        let receiver = self.receiver.lock().take();
        let Some(receiver) = receiver else {
            return;
        };
        let name = self.name.clone();
        let conf = self.conf.clone();
        let dropped = self.dropped.clone();
        joinset.spawn(async move {
            run(receiver, name, conf, dropped, reporter, shutdown).await;
        });
    }
}

/// Builds one tracing layer and its site-owned reporting runtime.
#[cfg(feature = "email")]
pub(crate) fn layer(name: &str, conf: MailAdmins) -> (MailAdminsLayer, MailAdminsRuntime) {
    MailAdminsRuntime::new(name, conf)
}

#[cfg(feature = "email")]
#[derive(Clone)]
struct MailEvent {
    at: DateTime<Utc>,
    target: String,
    name: String,
    file: Option<String>,
    line: Option<u32>,
    message: String,
    fields: String,
    spans: String,
}

#[cfg(feature = "email")]
impl MailEvent {
    fn capture<S>(event: &Event<'_>, context: Context<'_, S>) -> Self
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        let metadata = event.metadata();
        let mut fields = Fields::default();
        event.record(&mut fields);
        let spans = context
            .event_scope(event)
            .map(|scope| {
                scope
                    .from_root()
                    .map(|span| span.metadata().name())
                    .collect::<Vec<_>>()
                    .join(" > ")
            })
            .unwrap_or_default();
        Self {
            at: Utc::now(),
            target: metadata.target().to_string(),
            name: metadata.name().to_string(),
            file: metadata.file().map(ToString::to_string),
            line: metadata.line(),
            message: fields.message.unwrap_or_default(),
            fields: fields.values.join(", "),
            spans: bounded_to(spans, MAX_SPAN_BYTES),
        }
    }

    fn sample(&self) -> String {
        let mut output = format!(
            "time: {}\ntarget: {}\nmessage: {}",
            self.at.to_rfc3339(),
            self.target,
            self.message
        );
        if !self.fields.is_empty() {
            output.push_str(&format!("\nfields: {}", self.fields));
        }
        if !self.spans.is_empty() {
            output.push_str(&format!("\nspans: {}", self.spans));
        }
        if let (Some(file), Some(line)) = (&self.file, self.line) {
            output.push_str(&format!("\nsource: {file}:{line}"));
        }
        bounded(output)
    }
}

#[cfg(feature = "email")]
#[derive(Default)]
struct Fields {
    message: Option<String>,
    values: Vec<String>,
}

#[cfg(feature = "email")]
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let value = bounded_to(format!("{value:?}"), MAX_FIELD_BYTES);
        if field.name() == "message" {
            self.message = Some(bounded_to(value, MAX_MESSAGE_BYTES));
            return;
        }
        let entry = bounded_to(format!("{}={value}", field.name()), MAX_FIELD_BYTES);
        if self.values.len() < MAX_FIELDS {
            self.values.push(entry);
        }
    }
}

#[cfg(feature = "email")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReportKey(String);

#[cfg(feature = "email")]
impl Hash for ReportKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[cfg(feature = "email")]
impl ReportKey {
    fn new(event: &MailEvent, dedupe: MailDedupe) -> Self {
        let mut value = format!(
            "{}:{}:{}:{}",
            event.target,
            event.name,
            event.file.as_deref().unwrap_or_default(),
            event.line.unwrap_or_default()
        );
        if dedupe == MailDedupe::Exact {
            value.push(':');
            value.push_str(&event.message);
        }
        Self(value)
    }
}

#[cfg(feature = "email")]
struct PendingReport {
    repeats: u64,
    first_at: DateTime<Utc>,
    last_at: DateTime<Utc>,
    deadline: tokio::time::Instant,
    samples: Option<(MailEvent, MailEvent)>,
}

#[cfg(feature = "email")]
impl PendingReport {
    fn new(event: &MailEvent, conf: &MailAdmins) -> Self {
        let samples =
            (conf.summary_mode() == MailSummary::Samples).then(|| (event.clone(), event.clone()));
        Self {
            repeats: 0,
            first_at: event.at,
            last_at: event.at,
            deadline: tokio::time::Instant::now() + conf.debounce_duration(),
            samples,
        }
    }

    fn record(&mut self, event: &MailEvent) {
        self.repeats = self.repeats.saturating_add(1);
        self.last_at = event.at;
        if let Some((_, last)) = &mut self.samples {
            *last = event.clone();
        }
    }
}

#[cfg(feature = "email")]
async fn run(
    mut receiver: mpsc::Receiver<MailEvent>,
    name: String,
    conf: MailAdmins,
    dropped: Arc<AtomicU64>,
    reporter: crate::email::MailReporter,
    shutdown: crate::notifiers::CancellationNotifier,
) {
    let mut reports = HashMap::new();
    let mut throttles = DeliveryThrottles::new(conf.throttles());
    loop {
        let deadline = next_deadline(&reports);
        let event = receive(&mut receiver, deadline, shutdown.clone()).await;
        match event {
            Receive::Event(event) => {
                record(event, &mut reports, &mut throttles, &name, &conf, &reporter).await
            }
            Receive::Due => {
                flush(&mut reports, &mut throttles, &name, &conf, &reporter, false).await
            }
            Receive::Shutdown | Receive::Closed => {
                flush(&mut reports, &mut throttles, &name, &conf, &reporter, true).await;
                return;
            }
        }
        let dropped = dropped.swap(0, Ordering::Relaxed);
        if dropped > 0 {
            tracing::warn!(
                target: "vyuh::logging::mail_admins",
                sink = %name,
                dropped,
                "mail-admins log queue was full"
            );
        }
    }
}

#[cfg(feature = "email")]
async fn record(
    event: MailEvent,
    reports: &mut HashMap<ReportKey, PendingReport>,
    throttles: &mut DeliveryThrottles,
    name: &str,
    conf: &MailAdmins,
    reporter: &crate::email::MailReporter,
) {
    let key = ReportKey::new(&event, conf.dedupe_mode());
    if let Some(report) = reports.get_mut(&key) {
        report.record(&event);
        return;
    }
    if reports.len() >= MAX_ACTIVE_REPORTS {
        tracing::warn!(
            target: "vyuh::logging::mail_admins",
            sink = %name,
            "mail-admins active report limit reached"
        );
        return;
    }
    reports.insert(key, PendingReport::new(&event, conf));
    send_initial(reporter, conf, throttles, name, &event).await;
}

#[cfg(feature = "email")]
async fn flush(
    reports: &mut HashMap<ReportKey, PendingReport>,
    throttles: &mut DeliveryThrottles,
    name: &str,
    conf: &MailAdmins,
    reporter: &crate::email::MailReporter,
    shutdown: bool,
) {
    let now = tokio::time::Instant::now();
    let due = reports
        .iter()
        .filter(|(_, report)| shutdown || report.deadline <= now)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in due {
        let Some(report) = reports.remove(&key) else {
            continue;
        };
        if report.repeats > 0 && conf.summary_mode() != MailSummary::None {
            send_summary(reporter, conf, throttles, name, report).await;
        }
    }
}

#[cfg(feature = "email")]
async fn send_initial(
    reporter: &crate::email::MailReporter,
    conf: &MailAdmins,
    throttles: &mut DeliveryThrottles,
    name: &str,
    event: &MailEvent,
) {
    if !throttles.allow(name) {
        return;
    }
    let subject = format!("[Vyuh] ERROR {}", event.target);
    if let Err(error) = reporter
        .send(conf.recipients(), &subject, event.sample())
        .await
    {
        tracing::warn!(
            target: "vyuh::logging::mail_admins",
            sink = %name,
            error = %error,
            "mail-admins initial report delivery failed"
        );
    }
}

#[cfg(feature = "email")]
async fn send_summary(
    reporter: &crate::email::MailReporter,
    conf: &MailAdmins,
    throttles: &mut DeliveryThrottles,
    name: &str,
    report: PendingReport,
) {
    if !throttles.allow(name) {
        return;
    }
    let subject = format!("[Vyuh] ERROR summary ({})", report.repeats);
    let mut body = format!(
        "{} matching error(s) occurred after the initial alert.\nfirst: {}\nlast: {}",
        report.repeats,
        report.first_at.to_rfc3339(),
        report.last_at.to_rfc3339()
    );
    if let Some((first, last)) = report.samples {
        body.push_str(&format!(
            "\n\nfirst sample:\n{}\n\nlast sample:\n{}",
            first.sample(),
            last.sample()
        ));
    }
    if let Err(error) = reporter
        .send(conf.recipients(), &subject, bounded(body))
        .await
    {
        tracing::warn!(
            target: "vyuh::logging::mail_admins",
            sink = %name,
            error = %error,
            "mail-admins summary delivery failed"
        );
    }
}

#[cfg(feature = "email")]
struct DeliveryThrottles {
    windows: Vec<ThrottleWindow>,
    suppressed: u64,
}

#[cfg(feature = "email")]
impl DeliveryThrottles {
    fn new(throttles: &[MailThrottle]) -> Self {
        Self {
            windows: throttles.iter().copied().map(ThrottleWindow::new).collect(),
            suppressed: 0,
        }
    }

    fn allow(&mut self, name: &str) -> bool {
        let now = tokio::time::Instant::now();
        for window in &mut self.windows {
            window.refresh(now);
        }
        if self.windows.iter().any(ThrottleWindow::exhausted) {
            self.suppressed = self.suppressed.saturating_add(1);
            return false;
        }
        for window in &mut self.windows {
            window.consume();
        }
        if self.suppressed > 0 {
            tracing::warn!(
                target: "vyuh::logging::mail_admins",
                sink = %name,
                suppressed = self.suppressed,
                "mail-admins delivery throttle dropped reports"
            );
            self.suppressed = 0;
        }
        true
    }
}

#[cfg(feature = "email")]
struct ThrottleWindow {
    throttle: MailThrottle,
    opened_at: tokio::time::Instant,
    delivered: u32,
}

#[cfg(feature = "email")]
impl ThrottleWindow {
    fn new(throttle: MailThrottle) -> Self {
        Self {
            throttle,
            opened_at: tokio::time::Instant::now(),
            delivered: 0,
        }
    }

    fn refresh(&mut self, now: tokio::time::Instant) {
        if now.duration_since(self.opened_at) >= self.throttle.window() {
            self.opened_at = now;
            self.delivered = 0;
        }
    }

    fn exhausted(&self) -> bool {
        self.delivered >= self.throttle.limit
    }

    fn consume(&mut self) {
        self.delivered = self.delivered.saturating_add(1);
    }
}

#[cfg(feature = "email")]
enum Receive {
    Event(MailEvent),
    Due,
    Shutdown,
    Closed,
}

#[cfg(feature = "email")]
async fn receive(
    receiver: &mut mpsc::Receiver<MailEvent>,
    deadline: Option<tokio::time::Instant>,
    shutdown: crate::notifiers::CancellationNotifier,
) -> Receive {
    match deadline {
        Some(deadline) => {
            tokio::select! {
                event = receiver.recv() => event.map(Receive::Event).unwrap_or(Receive::Closed),
                _ = tokio::time::sleep_until(deadline) => Receive::Due,
                _ = shutdown.notified() => Receive::Shutdown,
            }
        }
        None => {
            tokio::select! {
                event = receiver.recv() => event.map(Receive::Event).unwrap_or(Receive::Closed),
                _ = shutdown.notified() => Receive::Shutdown,
            }
        }
    }
}

#[cfg(feature = "email")]
fn next_deadline(reports: &HashMap<ReportKey, PendingReport>) -> Option<tokio::time::Instant> {
    reports.values().map(|report| report.deadline).min()
}

fn default_debounce_millis() -> u64 {
    DEFAULT_DEBOUNCE_MILLIS
}

fn default_throttles() -> Vec<MailThrottle> {
    vec![
        MailThrottle::per_minute(DEFAULT_THROTTLE_PER_MINUTE),
        MailThrottle::per_hour(DEFAULT_THROTTLE_PER_HOUR),
    ]
}

fn duration_millis(duration: Duration) -> u64 {
    match u64::try_from(duration.as_millis()) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[cfg(feature = "email")]
fn bounded(value: String) -> String {
    bounded_to(value, MAX_EVENT_BYTES)
}

#[cfg(feature = "email")]
fn bounded_to(mut value: String, limit: usize) -> String {
    if value.len() <= limit {
        return value;
    }
    let mut end = limit.saturating_sub(3);
    while end > 0 && !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value.truncate(end);
    value.push_str("...");
    value
}

#[cfg(test)]
#[path = "mail_admins_tests.rs"]
mod tests;
