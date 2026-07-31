#![cfg(all(feature = "email", feature = "test-support"))]

use vyuh::{Site, SiteConf, bundles, email::Mail, routes::StatusCode, testing::TestSite};

#[bundles::route(path = "/send-email", method = "POST")]
async fn send_email(site: Site) -> StatusCode {
    let email = match Mail::new()
        .from("Vyuh <noreply@example.com>")
        .to("Ada <ada@example.com>")
        .subject("Welcome")
        .html("<h1>Welcome</h1><p>Thanks for joining.</p>")
        .build()
    {
        Ok(email) => email,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    match site.mail().send(email).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Verifies a normal endpoint delivery is captured by the automatic test outbox.
#[tokio::test]
async fn endpoint_mail_is_captured() -> Result<(), String> {
    let site = Site::build(
        SiteConf::default().log_init(false),
        bundles::bundle! { send_email },
    )
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
    Ok(())
}
