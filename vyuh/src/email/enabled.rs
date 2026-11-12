use std::path::Path;

use lettre::message::{Attachment as LettreAttachment, Mailbox, SinglePart, header::HeaderName};
use serde::Serialize;

use super::{MailConf, MailTls};
#[path = "delivery.rs"]
mod delivery;
use crate::{
    SavedFile, Site,
    templates::{TemplateEngine, TemplateError},
};
pub use delivery::{Delivery, DeliveryError, DeliveryOutcome, MailDelivery, SmtpDelivery};
#[cfg(feature = "test-support")]
pub use delivery::{MailOutbox, OutboxMessage};
pub(crate) use delivery::{MailReporter, SharedMailDelivery, select_delivery};

/// Errors raised while building, rendering, or delivering an email.
#[derive(Debug, thiserror::Error)]
pub enum EmailError {
    #[error("email delivery is disabled")]
    Disabled,
    #[error("invalid {field}: {reason}")]
    Invalid { field: &'static str, reason: String },
    #[error("email requires a sender")]
    MissingSender,
    #[error("email requires at least one recipient")]
    MissingRecipient,
    #[error("email requires a subject")]
    MissingSubject,
    #[error("email requires text or HTML content")]
    MissingBody,
    #[error("could not derive a usable text alternative from HTML")]
    TextFallback,
    #[error("template error: {0}")]
    Template(#[from] TemplateError),
    #[error("email templates require a site template engine")]
    TemplateEngineUnavailable,
    #[error("attachment I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("MIME message error: {0}")]
    Mime(#[from] lettre::error::Error),
    #[error("SMTP transport error: {0}")]
    Smtp(#[from] lettre::transport::smtp::Error),
    /// The selected backend could not complete submission.
    #[error(transparent)]
    Delivery(DeliveryError),
}

impl From<DeliveryError> for EmailError {
    fn from(error: DeliveryError) -> Self {
        match error {
            DeliveryError::Disabled => Self::Disabled,
            error => Self::Delivery(error),
        }
    }
}

/// A binary email attachment with a validated display name and MIME type.
#[derive(Debug, Clone)]
pub struct Attachment {
    name: String,
    bytes: Vec<u8>,
    content_type: String,
}

impl Attachment {
    /// Returns the filename presented to the recipient.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the attachment's declared MIME type.
    pub fn mime_type(&self) -> &str {
        &self.content_type
    }

    /// Borrows the original attachment bytes.
    pub fn data(&self) -> &[u8] {
        &self.bytes
    }

    /// Creates an attachment from bytes with an octet-stream MIME type.
    pub fn bytes(name: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            bytes: bytes.into(),
            content_type: "application/octet-stream".into(),
        }
    }

    /// Reads an attachment from a runtime file without blocking an async caller.
    pub async fn file(path: impl AsRef<Path>) -> Result<Self, EmailError> {
        let path = path.as_ref();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| EmailError::Invalid {
                field: "attachment.name",
                reason: "path must end with a UTF-8 file name".into(),
            })?;
        let bytes = tokio::fs::read(path).await?;
        Ok(Self::bytes(name, bytes).content_type(guess_type(name)))
    }

    /// Reads an attachment from Vyuh's runtime file-storage boundary.
    pub async fn saved_file(file: &SavedFile) -> Result<Self, EmailError> {
        let bytes = tokio::fs::read(&file.path).await?;
        let name = file.name.as_str();
        Ok(Self::bytes(name, bytes).content_type(guess_type(name)))
    }

    /// Sets the attachment MIME type.
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    fn validate(&self) -> Result<lettre::message::header::ContentType, EmailError> {
        validate_name(&self.name, "attachment.name")?;
        let content_type = lettre::message::header::ContentType::parse(&self.content_type)
            .map_err(|error| EmailError::Invalid {
                field: "attachment.content_type",
                reason: error.to_string(),
            })?;
        Ok(content_type)
    }

    fn into_part(self) -> Result<SinglePart, EmailError> {
        let content_type = self.validate()?;
        Ok(LettreAttachment::new(self.name).body(self.bytes, content_type))
    }
}

/// Builder for a complete outgoing email.
#[derive(Debug, Default)]
pub struct Mail {
    from: Option<String>,
    to: Vec<String>,
    cc: Vec<String>,
    bcc: Vec<String>,
    reply_to: Option<String>,
    subject: Option<String>,
    text: Option<Body>,
    html: Option<Body>,
    html_only: bool,
    headers: Vec<(String, String)>,
    attachments: Vec<Attachment>,
    idempotency_key: Option<String>,
}

#[derive(Debug)]
enum Body {
    Value(String),
    Template {
        name: String,
        context: serde_json::Value,
    },
}

impl Mail {
    /// Starts a mail builder with no implicit recipients or body.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the sender, overriding the configured default sender.
    pub fn from(mut self, address: impl Into<String>) -> Self {
        self.from = Some(address.into());
        self
    }
    /// Adds a primary recipient.
    pub fn to(mut self, address: impl Into<String>) -> Self {
        self.to.push(address.into());
        self
    }
    /// Adds a carbon-copy recipient.
    pub fn cc(mut self, address: impl Into<String>) -> Self {
        self.cc.push(address.into());
        self
    }
    /// Adds a blind-carbon-copy recipient.
    pub fn bcc(mut self, address: impl Into<String>) -> Self {
        self.bcc.push(address.into());
        self
    }
    /// Sets the reply-to mailbox.
    pub fn reply_to(mut self, address: impl Into<String>) -> Self {
        self.reply_to = Some(address.into());
        self
    }
    /// Sets the message subject.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }
    /// Sets an explicit plain-text body.
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(Body::Value(text.into()));
        self
    }
    /// Sets an HTML body and derives text unless an explicit text body is supplied.
    pub fn html(mut self, html: impl Into<String>) -> Self {
        self.html = Some(Body::Value(html.into()));
        self
    }
    /// Renders a plain-text bundle template when the mail is sent.
    pub fn text_template<T: Serialize>(
        mut self,
        name: impl Into<String>,
        context: &T,
    ) -> Result<Self, EmailError> {
        self.text = Some(template(name, context)?);
        Ok(self)
    }
    /// Renders an HTML bundle template when the mail is sent.
    pub fn html_template<T: Serialize>(
        mut self,
        name: impl Into<String>,
        context: &T,
    ) -> Result<Self, EmailError> {
        self.html = Some(template(name, context)?);
        Ok(self)
    }
    /// Delivers HTML without generating a text alternative.
    pub fn html_only(mut self) -> Self {
        self.html_only = true;
        self
    }
    /// Adds a validated custom message header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
    /// Adds an attachment to the message.
    pub fn attachment(mut self, attachment: Attachment) -> Self {
        self.attachments.push(attachment);
        self
    }

    /// Sets the provider submission key for this logical send and its retries.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Validates the builder's local invariants and produces a sendable email.
    pub fn build(self) -> Result<Email, EmailError> {
        if self.to.is_empty() && self.cc.is_empty() && self.bcc.is_empty() {
            return Err(EmailError::MissingRecipient);
        }
        if self.subject.as_deref().is_none_or(str::is_empty) {
            return Err(EmailError::MissingSubject);
        }
        if self.text.is_none() && self.html.is_none() {
            return Err(EmailError::MissingBody);
        }
        self.validate_metadata()?;
        validate_addresses(&self.to, "to")?;
        validate_addresses(&self.cc, "cc")?;
        validate_addresses(&self.bcc, "bcc")?;
        if let Some(reply_to) = &self.reply_to {
            parse_mailbox(reply_to, "reply_to")?;
        }
        if let Some(from) = &self.from {
            parse_mailbox(from, "from")?;
        }
        Ok(Email { mail: self })
    }

    fn validate_metadata(&self) -> Result<(), EmailError> {
        if let Some(key) = &self.idempotency_key
            && (key.is_empty()
                || key.len() > 256
                || key.chars().any(|c| c.is_whitespace() || c.is_control()))
        {
            return Err(EmailError::Invalid {
                field: "idempotency_key",
                reason: "must be 1–256 bytes without whitespace or control characters".into(),
            });
        }
        for (name, value) in &self.headers {
            validate_header(name, value)?;
        }
        for attachment in &self.attachments {
            attachment.validate()?;
        }
        Ok(())
    }
}

/// A validated email whose templates and default sender resolve at submission.
#[derive(Debug)]
pub struct Email {
    mail: Mail,
}

/// Validates an administrator recipient during site construction.
pub(crate) fn validate_recipient(value: &str) -> Result<(), String> {
    value
        .parse::<Mailbox>()
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// A site-bound mail facade.
#[derive(Clone)]
pub struct Mailer {
    site: Site,
    runtime: SharedMailDelivery,
}

impl Mailer {
    pub(crate) fn new(site: Site, runtime: SharedMailDelivery) -> Self {
        Self { site, runtime }
    }

    /// Prepares an email and submits it through the site's selected backend.
    pub async fn send(&self, email: Email) -> Result<Delivery, EmailError> {
        if !self.runtime.is_enabled() {
            return Err(EmailError::Disabled);
        }
        let conf = &self.site.conf().mail;
        let message = email.prepare(Some(self.site.template_engine()), conf.sender.as_deref())?;
        Ok(self.runtime.send(message).await?)
    }
}

#[path = "prepared.rs"]
mod prepared;
pub use prepared::PreparedEmail;

fn template<T: Serialize>(name: impl Into<String>, context: &T) -> Result<Body, EmailError> {
    Ok(Body::Template {
        name: name.into(),
        context: serde_json::to_value(context).map_err(|error| EmailError::Invalid {
            field: "template.context",
            reason: error.to_string(),
        })?,
    })
}

fn render(
    templates: Option<&TemplateEngine>,
    body: Option<Body>,
) -> Result<Option<String>, EmailError> {
    match body {
        Some(Body::Value(value)) => Ok(Some(value)),
        Some(Body::Template { name, context }) => {
            let templates = templates.ok_or(EmailError::TemplateEngineUnavailable)?;
            Ok(Some(templates.render(&name, &context)?))
        }
        None => Ok(None),
    }
}

fn mailbox(value: Option<&str>, field: &'static str) -> Result<Mailbox, EmailError> {
    parse_mailbox(value.ok_or(EmailError::MissingSender)?, field)
}

fn parse_mailbox(value: &str, field: &'static str) -> Result<Mailbox, EmailError> {
    value
        .parse()
        .map_err(|error: lettre::address::AddressError| EmailError::Invalid {
            field,
            reason: error.to_string(),
        })
}

fn validate_addresses(addresses: &[String], field: &'static str) -> Result<(), EmailError> {
    for address in addresses {
        parse_mailbox(address, field)?;
    }
    Ok(())
}

fn validate_name(value: &str, field: &'static str) -> Result<(), EmailError> {
    if value.is_empty() || value.contains(['\r', '\n', '/', '\\']) {
        return Err(EmailError::Invalid {
            field,
            reason: "must be a single file name".into(),
        });
    }
    Ok(())
}

fn validate_header(name: &str, value: &str) -> Result<(), EmailError> {
    let _ = header_name(name)?;
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("content-")
        || matches!(
            lower.as_str(),
            "from"
                | "sender"
                | "to"
                | "cc"
                | "bcc"
                | "reply-to"
                | "return-path"
                | "subject"
                | "date"
                | "message-id"
                | "mime-version"
        )
    {
        return Err(EmailError::Invalid {
            field: "header.name",
            reason: "reserved for the email envelope or MIME builder".into(),
        });
    }
    if value.contains(['\r', '\n']) {
        return Err(EmailError::Invalid {
            field: "header.value",
            reason: "must not contain line breaks".into(),
        });
    }
    Ok(())
}

fn header_name(value: &str) -> Result<HeaderName, EmailError> {
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_graphic() && byte != b':')
    {
        return Err(EmailError::Invalid {
            field: "header.name",
            reason: "must contain only visible ASCII characters other than ':'".into(),
        });
    }
    HeaderName::new_from_ascii(value.into()).map_err(|error| EmailError::Invalid {
        field: "header.name",
        reason: error.to_string(),
    })
}

fn guess_type(name: &str) -> String {
    mime_guess::from_path(name)
        .first_or_octet_stream()
        .essence_str()
        .into()
}

#[cfg(test)]
#[path = "tests/enabled.rs"]
mod tests;
