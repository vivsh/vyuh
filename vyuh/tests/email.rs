#![cfg(all(feature = "email", feature = "test-support"))]

use vyuh::{
    Site, SiteConf, bundles,
    email::{DeliveryOutcome, Mail},
    routes::StatusCode,
    testing::TestSite,
};

#[bundles::route(path = "/send-email", method = "POST")]
async fn send_email(site: Site) -> StatusCode {
    let email = match Mail::new()
        .from("Vyuh <noreply@example.com>")
        .to("Ada <ada@example.com>")
        .subject("Welcome")
        .html("<h1>Welcome</h1><p>Thanks for joining.</p>")
        .idempotency_key("welcome/ada")
        .build()
    {
        Ok(email) => email,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    match site.mail().send(email).await {
        Ok(delivery) if delivery.outcome == DeliveryOutcome::Captured => StatusCode::NO_CONTENT,
        Ok(_) => StatusCode::INTERNAL_SERVER_ERROR,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Verifies a normal endpoint delivery is captured by the automatic test outbox.
#[tokio::test]
async fn endpoint_mail_is_captured() -> Result<(), String> {
    let site = Site::builder(SiteConf::default().log_init(false))
        .console_mail_fallback()
        .mail_delivery(NeverSend)
        .build(bundles::bundle! { send_email })
        .await
        .map_err(|error| error.to_string())?;
    let client = TestSite::new(site.clone());

    client
        .post("/send-email")
        .send()
        .await
        .assert_status(StatusCode::NO_CONTENT);

    let message = site
        .mail_outbox()
        .single()
        .ok_or_else(|| "expected one captured email".to_string())?;
    assert!(message.source().contains("Subject: Welcome"));
    assert!(message.source().contains("Welcome"));
    assert!(message.source().contains("text/plain"));
    assert!(message.source().contains("text/html"));
    assert!(!message.message_id().is_empty());
    assert!(message.source().contains(message.message_id()));
    assert_eq!(message.idempotency_key(), Some("welcome/ada"));
    assert!(!message.source().contains("welcome/ada"));
    Ok(())
}

/// Verifies separate sites own separate outboxes and handles observe the same site's clears.
#[tokio::test]
async fn site_outboxes_are_isolated() -> Result<(), vyuh::SiteError> {
    let conf = SiteConf::default().log_init(false);
    let first = Site::build(conf.clone(), bundles::bundle! { send_email }).await?;
    let second = Site::build(conf, bundles::bundle! { send_email }).await?;
    let first_outbox = first.mail_outbox();
    let second_outbox = second.mail_outbox();

    TestSite::new(first.clone())
        .post("/send-email")
        .send()
        .await
        .assert_status(StatusCode::NO_CONTENT);
    assert_eq!(first_outbox.messages().len(), 1);
    assert!(second_outbox.messages().is_empty());

    TestSite::new(second.clone())
        .post("/send-email")
        .send()
        .await
        .assert_status(StatusCode::NO_CONTENT);
    assert_eq!(first_outbox.messages().len(), 1);
    assert_eq!(second_outbox.messages().len(), 1);

    first.mail_outbox().clear();
    assert!(first_outbox.messages().is_empty());
    assert_eq!(second.mail_outbox().messages().len(), 1);
    Ok(())
}

struct NeverSend;

impl vyuh::email::MailDelivery for NeverSend {
    fn send(
        &self,
        _: vyuh::email::PreparedEmail,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<vyuh::email::Delivery, vyuh::email::DeliveryError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Err(vyuh::email::DeliveryError::IdempotencyUnsupported) })
    }
}
