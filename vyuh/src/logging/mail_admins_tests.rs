use super::*;

/// Verifies the compact constructor selects the operationally safe report policy.
#[test]
fn mail_admins_defaults_are_safe() {
    let admins = MailAdmins::new(["ops@example.com"]);
    assert_eq!(admins.debounce_millis, DEFAULT_DEBOUNCE_MILLIS);
    assert_eq!(admins.dedupe, MailDedupe::Callsite);
    assert_eq!(admins.summary, MailSummary::Samples);
    assert_eq!(admins.throttles, default_throttles());
}

/// Verifies empty recipients and invalid debounce windows fail site configuration validation.
#[test]
fn mail_admins_validation_rejects_invalid_values() {
    assert!(MailAdmins::new(Vec::<String>::new()).validate().is_err());
    assert!(
        MailAdmins::new(["ops@example.com"])
            .debounce(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        MailAdmins::new(["ops@example.com"])
            .unthrottled()
            .throttle(MailThrottle::new(0, Duration::from_secs(1)))
            .validate()
            .is_err()
    );
    assert!(
        MailAdmins::new(["ops@example.com"])
            .unthrottled()
            .throttle(MailThrottle::per_minute(1))
            .throttle(MailThrottle::per_minute(2))
            .validate()
            .is_err()
    );
}

#[cfg(feature = "email")]
/// Verifies callsite grouping ignores dynamic messages while exact grouping preserves them.
#[test]
fn dedupe_policy_controls_error_grouping() {
    let first = event("first");
    let second = event("second");
    assert_eq!(
        ReportKey::new(&first, MailDedupe::Callsite),
        ReportKey::new(&second, MailDedupe::Callsite)
    );
    assert_ne!(
        ReportKey::new(&first, MailDedupe::Exact),
        ReportKey::new(&second, MailDedupe::Exact)
    );
}

#[cfg(feature = "email")]
/// Verifies every configured throttle must permit an outgoing report.
#[test]
fn delivery_throttles_compose_as_one_limit() {
    let rules = [
        MailThrottle::new(1, Duration::from_secs(60)),
        MailThrottle::new(2, Duration::from_secs(60 * 60)),
    ];
    let mut throttles = DeliveryThrottles::new(&rules);
    assert!(throttles.allow("ADMINS"));
    assert!(!throttles.allow("ADMINS"));
}

#[cfg(all(feature = "email", feature = "test-support"))]
/// Verifies the first event is delivered immediately and repeats receive a sampled summary.
#[tokio::test]
async fn reports_immediately_then_summarizes() -> Result<(), crate::email::EmailError> {
    let conf = MailAdmins::new(["ops@example.com"]).debounce(Duration::from_millis(5));
    let reporter = test_reporter()?;
    let outbox = reporter.outbox();
    let (sender, receiver) = mpsc::channel(8);
    let shutdown = crate::notifiers::CancellationNotifier::new();
    let task = tokio::spawn(run(
        receiver,
        "ADMINS".into(),
        conf,
        Arc::new(AtomicU64::new(0)),
        reporter,
        shutdown.clone(),
    ));
    assert!(sender.send(event("first")).await.is_ok());
    wait_for_messages(&outbox, 1).await;
    assert!(sender.send(event("second")).await.is_ok());
    wait_for_messages(&outbox, 2).await;
    let messages = outbox.messages();
    let summary = messages
        .get(1)
        .map(|message| message.source())
        .unwrap_or("");
    assert!(summary.contains("matching error(s)"));
    assert!(summary.contains("first sample"));
    assert!(summary.contains("last sample"));
    shutdown.notify_waiters();
    assert!(task.await.is_ok());
    Ok(())
}

#[cfg(all(feature = "email", feature = "test-support"))]
/// Verifies silent and count-only summaries omit event samples as configured.
#[tokio::test]
async fn summary_policy_controls_follow_up_body() -> Result<(), crate::email::EmailError> {
    let count = run_summary(MailSummary::Count).await?;
    let count_source = count.get(1).map(|message| message.source()).unwrap_or("");
    assert!(count_source.contains("matching error(s)"));
    assert!(!count_source.contains("first sample"));
    let none = run_summary(MailSummary::None).await?;
    assert_eq!(none.len(), 1);
    Ok(())
}

#[cfg(all(feature = "email", feature = "test-support"))]
/// Verifies exhausted delivery throttles discard summary mail instead of delaying it.
#[tokio::test]
async fn delivery_throttle_discards_excess_mail() -> Result<(), crate::email::EmailError> {
    let conf = MailAdmins::new(["ops@example.com"])
        .unthrottled()
        .throttle(MailThrottle::new(1, Duration::from_secs(60)))
        .debounce(Duration::from_millis(5));
    let reporter = test_reporter()?;
    let outbox = reporter.outbox();
    let (sender, receiver) = mpsc::channel(8);
    let shutdown = crate::notifiers::CancellationNotifier::new();
    let task = tokio::spawn(run(
        receiver,
        "ADMINS".into(),
        conf,
        Arc::new(AtomicU64::new(0)),
        reporter,
        shutdown.clone(),
    ));
    assert!(sender.send(event("first")).await.is_ok());
    wait_for_messages(&outbox, 1).await;
    assert!(sender.send(event("second")).await.is_ok());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(outbox.messages().len(), 1);
    shutdown.notify_waiters();
    assert!(task.await.is_ok());
    Ok(())
}

#[cfg(feature = "email")]
fn event(message: &str) -> MailEvent {
    MailEvent {
        at: Utc::now(),
        target: "app::worker".into(),
        name: "task_error".into(),
        file: Some("src/worker.rs".into()),
        line: Some(42),
        message: message.into(),
        fields: String::new(),
        spans: String::new(),
    }
}

#[cfg(all(feature = "email", feature = "test-support"))]
fn test_reporter() -> Result<crate::email::MailReporter, crate::email::EmailError> {
    let conf = crate::email::MailConf {
        enabled: true,
        host: "mail.example.com".into(),
        sender: Some("Vyuh <noreply@example.com>".into()),
        ..crate::email::MailConf::default()
    };
    let delivery = crate::email::delivery(&conf)?;
    Ok(crate::email::reporter(&conf, delivery))
}

#[cfg(all(feature = "email", feature = "test-support"))]
/// Delivers one repeated pair through a selected summary policy.
async fn run_summary(
    summary: MailSummary,
) -> Result<Vec<crate::email::OutboxMessage>, crate::email::EmailError> {
    let conf = MailAdmins::new(["ops@example.com"])
        .debounce(Duration::from_millis(5))
        .summary(summary);
    let reporter = test_reporter()?;
    let outbox = reporter.outbox();
    let (sender, receiver) = mpsc::channel(8);
    let shutdown = crate::notifiers::CancellationNotifier::new();
    let task = tokio::spawn(run(
        receiver,
        "ADMINS".into(),
        conf,
        Arc::new(AtomicU64::new(0)),
        reporter,
        shutdown.clone(),
    ));
    assert!(sender.send(event("first")).await.is_ok());
    wait_for_messages(&outbox, 1).await;
    assert!(sender.send(event("second")).await.is_ok());
    if summary == MailSummary::None {
        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown.notify_waiters();
        let _ = task.await;
        return Ok(outbox.messages());
    }
    wait_for_messages(&outbox, 2).await;
    shutdown.notify_waiters();
    let _ = task.await;
    Ok(outbox.messages())
}

#[cfg(all(feature = "email", feature = "test-support"))]
/// Waits for the bounded reporter worker to append the expected test messages.
async fn wait_for_messages(outbox: &crate::email::MailOutbox, expected: usize) {
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        while outbox.messages().len() < expected {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(result.is_ok());
}
