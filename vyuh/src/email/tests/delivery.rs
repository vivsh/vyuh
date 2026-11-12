use super::*;
use crate::{
    Site, SiteConf, SiteError, bundles,
    logging::{LogRule, LogSink, LoggingConf, MailAdmins},
};

#[derive(Clone)]
struct Observe(tokio::sync::mpsc::UnboundedSender<PreparedEmail>);

impl MailDelivery for Observe {
    fn send(
        &self,
        message: PreparedEmail,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, DeliveryError>> + Send + '_>> {
        Box::pin(async move {
            let delivery = receipt(&message, DeliveryOutcome::Accepted);
            self.0
                .send(message)
                .map_err(|_| permanent(std::io::Error::other("test receiver dropped")))?;
            Ok(delivery)
        })
    }
}

/// Verifies admin-mail validation and reporting work with the selected custom backend.
#[tokio::test]
async fn admin_uses_custom_delivery() -> Result<(), SiteError> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conf = SiteConf::default()
        .log_init(true)
        .mail(MailConf {
            sender: Some("alerts@example.com".into()),
            ..MailConf::default()
        })
        .logging(LoggingConf {
            env_prefix: Some("VYUH_TEST_CUSTOM_MAIL_LOG".into()),
            rules: vec![LogRule {
                name: "MailDeliveryTest".into(),
                sink: LogSink::mail_admins(MailAdmins::new(["ops@example.com"])),
                default_filter: "off".into(),
            }],
        });
    let observer = Observe(tx);
    let delivery = select_delivery(&conf.mail, Some(Arc::new(observer.clone())), false)
        .map_err(std::io::Error::other)?;
    let reporter = MailReporter::new(conf.mail.sender.clone(), delivery);
    let _site = Site::builder(conf)
        .mail_delivery(observer)
        .build(bundles::bundle([]))
        .await?;
    reporter
        .send(
            &["ops@example.com".into()],
            "Alert",
            "test admin report".into(),
        )
        .await
        .map_err(std::io::Error::other)?;
    let message = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::other("expected an admin report"))?;
    assert_eq!(message.from(), "alerts@example.com");
    assert_eq!(message.to(), ["ops@example.com"]);
    assert!(
        message
            .text()
            .is_some_and(|text| text.contains("test admin report"))
    );
    Ok(())
}
