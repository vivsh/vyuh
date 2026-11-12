#![cfg(all(feature = "email", not(feature = "test-support")))]

use std::{future::Future, io, pin::Pin, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use vyuh::{
    Site, SiteConf, bundles,
    email::{
        Attachment, Delivery, DeliveryError, DeliveryOutcome, Email, EmailError, Mail, MailConf,
        MailDelivery, MailTls, PreparedEmail, SmtpDelivery,
    },
};

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Site(#[from] vyuh::SiteError),
    #[error(transparent)]
    Email(#[from] EmailError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
}

struct JsonBackend;

impl MailDelivery for JsonBackend {
    fn send(
        &self,
        message: PreparedEmail,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, DeliveryError>> + Send + '_>> {
        Box::pin(async move {
            let request = serde_json::json!({
                "from": message.from(), "to": message.to(), "bcc": message.bcc(),
                "text": message.text(), "html": message.html().map(str::trim_end),
                "attachments": message.attachments().iter().map(|item| serde_json::json!({
                    "filename": item.name(), "type": item.mime_type(), "bytes": item.data(),
                })).collect::<Vec<_>>(),
            });
            assert_eq!(message.idempotency_key(), Some("receipt/123"));
            assert_eq!(
                request,
                serde_json::json!({
                    "from": "orders@example.com", "to": ["buyer@example.com"],
                    "bcc": ["copy@example.com"], "text": "Receipt for Ada.",
                "html": "<p>Receipt for Ada.</p>",
                    "attachments": [{ "filename": "receipt.txt", "type": "text/plain", "bytes": [65, 66] }],
                })
            );
            Ok(Delivery {
                message_id: message.message_id().to_owned(),
                outcome: DeliveryOutcome::Accepted,
                provider_submission_id: Some("provider-123".into()),
            })
        })
    }
}

fn conf() -> SiteConf {
    SiteConf::default().log_init(false).mail(MailConf {
        sender: Some("orders@example.com".into()),
        ..MailConf::default()
    })
}

/// Verifies a registered JSON backend receives rendered structured fields and an opaque key.
#[tokio::test]
async fn custom_json_delivery() -> Result<(), TestError> {
    let assets = vyuh::embed::embed_assets!("tests/fixtures/email", force = true);
    let site = Site::builder(conf())
        .console_mail_fallback()
        .mail_delivery(JsonBackend)
        .build(bundles::bundle([bundles::asset_dir(assets)]))
        .await?;
    let email = Mail::new()
        .to("buyer@example.com")
        .bcc("copy@example.com")
        .subject("Receipt")
        .html_template("receipt.html", &serde_json::json!({"name": "Ada"}))?
        .attachment(Attachment::bytes("receipt.txt", b"AB".to_vec()).content_type("text/plain"))
        .idempotency_key("receipt/123")
        .build()?;
    let delivery = site.mail().send(email).await?;
    assert_eq!(delivery.outcome, DeliveryOutcome::Accepted);
    assert_eq!(
        delivery.provider_submission_id.as_deref(),
        Some("provider-123")
    );
    assert!(!delivery.message_id.is_empty());
    Ok(())
}

fn smtp_conf(port: u16) -> MailConf {
    MailConf {
        host: "127.0.0.1".into(),
        port,
        tls: MailTls::None,
        timeout_seconds: 2,
        ..MailConf::default()
    }
}

fn keyed_mail() -> Mail {
    Mail::new()
        .to("buyer@example.com")
        .bcc("copy@example.com")
        .subject("Receipt")
        .text("Your receipt.")
        .idempotency_key("receipt/123")
}

/// Verifies the SMTP URL mapping, envelope-only Bcc, and actual MIME identifier.
#[tokio::test]
async fn smtp_url_maps_key() -> Result<(), TestError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let mail = MailConf::from_url(&format!(
        "smtp://127.0.0.1:{port}?tls=none&sender=orders%40example.com&idempotency_header=Resend-Idempotency-Key"
    ))
    .map_err(vyuh::SiteError::from)?;
    let site = Site::build(conf().mail(mail), bundles::bundle([])).await?;
    let server = tokio::spawn(smtp_fixture(listener, Some("250 queued\r\n")));
    let delivery = site.mail().send(keyed_mail().build()?).await?;
    let (envelope, source) = tokio::time::timeout(Duration::from_secs(3), server).await???;
    assert!(
        envelope
            .iter()
            .any(|line| line.contains("<copy@example.com>"))
    );
    assert!(!source.contains("copy@example.com"));
    assert!(source.contains("Resend-Idempotency-Key: receipt/123\r\n"));
    assert!(source.contains(&format!("Message-ID: {}", delivery.message_id)));
    assert_eq!(delivery.outcome, DeliveryOutcome::Accepted);
    Ok(())
}

/// Verifies generic SMTP rejects keyed mail and mapped SMTP rejects duplicate headers locally.
#[tokio::test]
async fn smtp_rejects_invalid_keys() -> Result<(), TestError> {
    let conf_smtp = smtp_conf(1);
    let site = Site::builder(conf())
        .mail_delivery(SmtpDelivery::new(&conf_smtp)?)
        .build(bundles::bundle([]))
        .await?;
    assert!(matches!(
        site.mail().send(keyed_mail().build()?).await,
        Err(EmailError::Delivery(DeliveryError::IdempotencyUnsupported))
    ));
    let mapped = SmtpDelivery::with_idempotency_header(&conf_smtp, "Resend-Idempotency-Key")?;
    let site = Site::builder(conf())
        .mail_delivery(mapped)
        .build(bundles::bundle([]))
        .await?;
    let email = keyed_mail()
        .header("resend-idempotency-key", "other")
        .build()?;
    assert!(matches!(
        site.mail().send(email).await,
        Err(EmailError::Delivery(
            DeliveryError::IdempotencyHeaderConflict
        ))
    ));
    assert!(SmtpDelivery::with_idempotency_header(&conf_smtp, "Bcc").is_err());
    assert!(SmtpDelivery::with_idempotency_header(&conf_smtp, "X-Key\r\n").is_err());
    let email = keyed_mail().idempotency_key("receipt/é").build()?;
    assert!(matches!(
        site.mail().send(email).await,
        Err(EmailError::Delivery(DeliveryError::InvalidIdempotencyKey(
            _
        )))
    ));
    Ok(())
}

/// Verifies relay replies and lost acceptance map to distinct structured failures.
#[tokio::test]
async fn smtp_classifies_failures() -> Result<(), TestError> {
    for reply in [Some("451 try later\r\n"), Some("550 rejected\r\n"), None] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let mail = MailConf::from_url(&format!(
            "smtp://127.0.0.1:{port}?tls=none&sender=orders%40example.com&timeout_seconds=2"
        ))
        .map_err(vyuh::SiteError::from)?;
        let site = Site::builder(conf().mail(mail))
            .console_mail_fallback()
            .build(bundles::bundle([]))
            .await?;
        let server = tokio::spawn(smtp_fixture(listener, reply));
        let result = site.mail().send(plain_mail()?).await;
        tokio::time::timeout(Duration::from_secs(3), server).await???;
        assert!(matches!(
            (reply, result),
            (
                Some("451 try later\r\n"),
                Err(EmailError::Delivery(DeliveryError::Temporary(_)))
            ) | (
                Some("550 rejected\r\n"),
                Err(EmailError::Delivery(DeliveryError::Permanent(_)))
            ) | (
                None,
                Err(EmailError::Delivery(DeliveryError::UnknownAcceptance(_)))
            )
        ));
    }
    Ok(())
}

fn plain_mail() -> Result<Email, EmailError> {
    Mail::new()
        .to("buyer@example.com")
        .subject("Receipt")
        .text("Your receipt.")
        .build()
}

/// Verifies opt-in console previews and the disabled default.
#[tokio::test]
async fn development_console_fallback() -> Result<(), TestError> {
    let disabled = Site::build(SiteConf::default().log_init(false), bundles::bundle([])).await?;
    assert!(matches!(
        disabled.mail().send(plain_mail()?).await,
        Err(EmailError::Disabled)
    ));
    let console = Site::builder(conf())
        .console_mail_fallback()
        .build(bundles::bundle([]))
        .await?;
    assert_eq!(
        console.mail().send(plain_mail()?).await?.outcome,
        DeliveryOutcome::Console
    );
    let invalid = conf().mail(MailConf {
        enabled: true,
        ..MailConf::default()
    });
    assert!(
        Site::builder(invalid)
            .console_mail_fallback()
            .build(bundles::bundle([]))
            .await
            .is_err()
    );
    let production =
        SiteConf::production().secret_key("production-test-secret-long-enough-for-validation");
    let result = Site::builder(production)
        .console_mail_fallback()
        .build(bundles::bundle([]))
        .await;
    assert!(result.err().is_some_and(|error| {
        error
            .to_string()
            .contains("console mail is only available in development")
    }));
    Ok(())
}

/// Accepts one SMTP transaction and records its envelope and MIME source.
async fn smtp_fixture(
    listener: TcpListener,
    reply: Option<&str>,
) -> io::Result<(Vec<String>, String)> {
    let (stream, _) = listener.accept().await?;
    let mut stream = BufReader::new(stream);
    stream
        .get_mut()
        .write_all(b"220 localhost test SMTP\r\n")
        .await?;
    let mut envelope = Vec::new();
    loop {
        let line = read_line(&mut stream).await?;
        let response: &[u8] = if line.starts_with("EHLO ") {
            b"250 localhost\r\n"
        } else if line == "DATA\r\n" {
            break;
        } else {
            envelope.push(line);
            b"250 OK\r\n"
        };
        stream.get_mut().write_all(response).await?;
    }
    stream.get_mut().write_all(b"354 continue\r\n").await?;
    let mut source = String::new();
    loop {
        let line = read_line(&mut stream).await?;
        if line == ".\r\n" {
            break;
        }
        source.push_str(&line);
    }
    if let Some(reply) = reply {
        stream.get_mut().write_all(reply.as_bytes()).await?;
    }
    Ok((envelope, source))
}

async fn read_line(stream: &mut BufReader<TcpStream>) -> io::Result<String> {
    let mut line = String::new();
    if stream.read_line(&mut line).await? == 0 {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
    }
    Ok(line)
}
