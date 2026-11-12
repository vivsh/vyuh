use std::{error::Error, future::Future, pin::Pin, sync::Arc, time::Duration};

use lettre::{
    AsyncSmtpTransport, AsyncTransport, Tokio1Executor,
    transport::smtp::authentication::Credentials,
};
use tokio::io::AsyncWriteExt;

use super::{EmailError, Mail, MailConf, MailTls, PreparedEmail, validate_header};

/// Immediate result of submitting an email to the selected backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// Vyuh's message identifier, also used in MIME when generated locally.
    pub message_id: String,
    /// What the backend did with the message.
    pub outcome: DeliveryOutcome,
    /// An optional provider-assigned submission identifier.
    pub provider_submission_id: Option<String>,
}

/// What a successful submission did with the prepared email.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryOutcome {
    /// The relay or provider accepted responsibility for the submission.
    Accepted,
    /// The development backend wrote a preview to stdout.
    Console,
    /// The test outbox recorded the message.
    Captured,
}

/// A failure while submitting a prepared email.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeliveryError {
    /// The site has no active mail backend.
    #[error("email delivery is disabled")]
    Disabled,
    /// The backend cannot honor a supplied submission key.
    #[error("the mail backend does not support idempotency")]
    IdempotencyUnsupported,
    /// The supplied key cannot be represented by the backend's protocol.
    #[error("invalid idempotency key for this mail backend: {0}")]
    InvalidIdempotencyKey(&'static str),
    /// The configured relay header conflicts with a custom message header.
    #[error("the SMTP idempotency header conflicts with a message header")]
    IdempotencyHeaderConflict,
    /// The provider explicitly rejected this attempt with a temporary failure.
    #[error("temporary mail delivery failure")]
    Temporary(#[source] Box<dyn Error + Send + Sync>),
    /// The message could not be submitted or was permanently rejected.
    #[error("permanent mail delivery failure")]
    Permanent(#[source] Box<dyn Error + Send + Sync>),
    /// The sender could not determine whether the provider accepted the message.
    #[error("mail acceptance is unknown")]
    UnknownAcceptance(#[source] Box<dyn Error + Send + Sync>),
}

/// Site-owned asynchronous backend for prepared email submissions.
pub trait MailDelivery: Send + Sync {
    /// Submits one prepared message, honoring its key or returning an error before delivery.
    fn send(
        &self,
        message: PreparedEmail,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, DeliveryError>> + Send + '_>>;

    #[doc(hidden)]
    fn is_enabled(&self) -> bool {
        true
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    fn outbox(&self) -> Option<MailOutbox> {
        None
    }
}

pub(crate) type SharedMailDelivery = Arc<dyn MailDelivery>;

/// Pooled SMTP delivery with an optional relay-specific idempotency mapping.
pub struct SmtpDelivery {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    idempotency_header: Option<String>,
}

impl SmtpDelivery {
    /// Builds a pooled SMTP backend. Keyed submissions require an explicit relay mapping.
    pub fn new(conf: &MailConf) -> Result<Self, EmailError> {
        Ok(Self {
            transport: transport(conf)?,
            idempotency_header: None,
        })
    }

    /// Builds a pooled backend for a relay documented to honor the specified header.
    pub fn with_idempotency_header(
        conf: &MailConf,
        name: impl Into<String>,
    ) -> Result<Self, EmailError> {
        let name = name.into();
        validate_header(&name, "")?;
        Ok(Self {
            transport: transport(conf)?,
            idempotency_header: Some(name),
        })
    }

    fn map_key(&self, message: &mut PreparedEmail) -> Result<(), DeliveryError> {
        let Some(key) = message.idempotency_key.take() else {
            return Ok(());
        };
        let name = self
            .idempotency_header
            .as_ref()
            .ok_or(DeliveryError::IdempotencyUnsupported)?;
        if !key.is_ascii() {
            return Err(DeliveryError::InvalidIdempotencyKey(
                "SMTP relay headers require an ASCII key",
            ));
        }
        if message
            .headers()
            .iter()
            .any(|(header, _)| header.eq_ignore_ascii_case(name))
        {
            return Err(DeliveryError::IdempotencyHeaderConflict);
        }
        message.add_header(name.clone(), key);
        Ok(())
    }
}

impl MailDelivery for SmtpDelivery {
    fn send(
        &self,
        mut message: PreparedEmail,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, DeliveryError>> + Send + '_>> {
        Box::pin(async move {
            self.map_key(&mut message)?;
            let delivery = receipt(&message, DeliveryOutcome::Accepted);
            let message = message.into_message().map_err(permanent)?;
            self.transport.send(message).await.map_err(smtp_error)?;
            Ok(delivery)
        })
    }
}

fn permanent(error: impl Error + Send + Sync + 'static) -> DeliveryError {
    DeliveryError::Permanent(Box::new(error))
}

fn smtp_error(error: lettre::transport::smtp::Error) -> DeliveryError {
    if error.is_transient() {
        DeliveryError::Temporary(Box::new(error))
    } else if error.is_permanent() || error.is_tls() {
        permanent(error)
    } else {
        // Lettre does not expose the SMTP phase of a lost acknowledgement.
        DeliveryError::UnknownAcceptance(Box::new(error))
    }
}

/// Validates transport settings and builds the pool without opening a connection.
fn transport(conf: &MailConf) -> Result<AsyncSmtpTransport<Tokio1Executor>, EmailError> {
    conf.validate_transport()?;
    let builder = match conf.tls {
        MailTls::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&conf.host),
        MailTls::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(&conf.host),
        MailTls::None => Ok(AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(
            &conf.host,
        )),
    }
    .map_err(|error| EmailError::Invalid {
        field: "mail.host",
        reason: error.to_string(),
    })?;
    let builder = builder
        .port(conf.port)
        .timeout(Some(Duration::from_secs(conf.timeout_seconds)));
    Ok(match (&conf.username, &conf.password) {
        (Some(username), Some(password)) => {
            builder.credentials(Credentials::new(username.clone(), password.clone()))
        }
        _ => builder,
    }
    .build())
}

struct DisabledDelivery;

impl MailDelivery for DisabledDelivery {
    fn is_enabled(&self) -> bool {
        false
    }
    fn send(
        &self,
        _: PreparedEmail,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, DeliveryError>> + Send + '_>> {
        Box::pin(async { Err(DeliveryError::Disabled) })
    }
}

struct ConsoleDelivery;

impl MailDelivery for ConsoleDelivery {
    fn send(
        &self,
        message: PreparedEmail,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, DeliveryError>> + Send + '_>> {
        Box::pin(async move {
            let mut stdout = tokio::io::stdout();
            stdout
                .write_all(preview(&message).as_bytes())
                .await
                .map_err(permanent)?;
            stdout.flush().await.map_err(permanent)?;
            Ok(receipt(&message, DeliveryOutcome::Console))
        })
    }
}

/// Formats a development preview without attachment bytes or submission metadata.
fn preview(message: &PreparedEmail) -> String {
    let mut output = format!(
        "\n--- Vyuh email preview ---\nFrom: {}\nTo: {}\nCc: {}\nBcc: {}\nSubject: {}\n",
        message.from(),
        message.to().join(", "),
        message.cc().join(", "),
        message.bcc().join(", "),
        message.subject()
    );
    if let Some(reply_to) = message.reply_to() {
        output.push_str(&format!("Reply-To: {reply_to}\n"));
    }
    for (kind, body) in [("Text", message.text()), ("HTML", message.html())] {
        if let Some(body) = body {
            output.push_str(&format!("\n{kind}:\n{body}\n"));
        }
    }
    for attachment in message.attachments() {
        output.push_str(&format!(
            "Attachment: {} ({} bytes)\n",
            attachment.name(),
            attachment.data().len()
        ));
    }
    output
}

fn receipt(message: &PreparedEmail, outcome: DeliveryOutcome) -> Delivery {
    Delivery {
        message_id: message.message_id().into(),
        outcome,
        provider_submission_id: None,
    }
}

pub(crate) fn select_delivery(
    conf: &MailConf,
    custom: Option<SharedMailDelivery>,
    console: bool,
) -> Result<SharedMailDelivery, EmailError> {
    if let Some(outbox) = test_delivery() {
        return Ok(outbox);
    }
    if let Some(custom) = custom {
        return Ok(custom);
    }
    if conf.enabled {
        return Ok(Arc::new(SmtpDelivery::new(conf)?));
    }
    if console {
        return Ok(Arc::new(ConsoleDelivery));
    }
    Ok(Arc::new(DisabledDelivery))
}

#[cfg(not(feature = "test-support"))]
fn test_delivery() -> Option<SharedMailDelivery> {
    None
}

#[cfg(feature = "test-support")]
fn test_delivery() -> Option<SharedMailDelivery> {
    Some(Arc::new(OutboxDelivery(MailOutbox::new())))
}

/// Framework reporter for subsystems without a site handle.
#[derive(Clone)]
pub(crate) struct MailReporter {
    sender: Option<String>,
    delivery: SharedMailDelivery,
}

impl MailReporter {
    pub(crate) fn new(sender: Option<String>, delivery: SharedMailDelivery) -> Self {
        Self { sender, delivery }
    }

    pub(crate) async fn send(
        &self,
        recipients: &[String],
        subject: &str,
        text: String,
    ) -> Result<Delivery, EmailError> {
        let mail = recipients.iter().fold(
            Mail::new().subject(subject).text(text),
            |mail, recipient| mail.to(recipient),
        );
        let message = mail.build()?.prepare(None, self.sender.as_deref())?;
        Ok(self.delivery.send(message).await?)
    }

    #[cfg(all(feature = "test-support", test))]
    pub(crate) fn outbox(&self) -> MailOutbox {
        self.delivery.outbox().unwrap_or_default()
    }
}

#[cfg(feature = "test-support")]
#[path = "outbox.rs"]
mod outbox;
#[cfg(feature = "test-support")]
use outbox::OutboxDelivery;

#[cfg(all(test, not(feature = "test-support")))]
#[path = "tests/delivery.rs"]
mod tests;
#[cfg(feature = "test-support")]
pub use outbox::{MailOutbox, OutboxMessage};
