//! Outbound SMTP email built on an internal Lettre transport.

use serde::{Deserialize, Serialize};

use crate::conf::ConfError;

/// SMTP transport security policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailTls {
    /// Upgrade an SMTP connection with STARTTLS.
    #[default]
    StartTls,
    /// Connect through TLS from the start of the connection.
    Tls,
    /// Connect without transport encryption.
    None,
}

/// Deployment configuration for Vyuh's optional SMTP mail facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailConf {
    /// Enables the SMTP transport for this site.
    #[serde(default)]
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    #[serde(skip_serializing)]
    pub password: Option<String>,
    pub sender: Option<String>,
    #[serde(default)]
    pub tls: MailTls,
    pub timeout_seconds: u64,
}

impl Default for MailConf {
    fn default() -> Self {
        Self {
            enabled: false,
            host: String::new(),
            port: 587,
            username: None,
            password: None,
            sender: None,
            tls: MailTls::StartTls,
            timeout_seconds: 30,
        }
    }
}

impl MailConf {
    /// Parses an SMTP URL into enabled mail configuration.
    ///
    /// `smtp://` defaults to STARTTLS and port 587; `smtps://` defaults to
    /// TLS and port 465. The URL user-info supplies SMTP credentials. Optional
    /// `sender`, `tls`, and `timeout_seconds` query parameters refine it.
    pub fn from_url(value: &str) -> Result<Self, ConfError> {
        let url = url::Url::parse(value)
            .map_err(|error| ConfError::Other(format!("invalid SMTP URL: {error}")))?;
        let tls = match url.scheme() {
            "smtp" => MailTls::StartTls,
            "smtps" => MailTls::Tls,
            scheme => {
                return Err(ConfError::Other(format!(
                    "unsupported SMTP URL scheme '{scheme}'; expected smtp or smtps"
                )));
            }
        };
        let host = url.host_str().ok_or_else(|| {
            ConfError::Other(
                "SMTP URL must include a host, for example smtp://mail.example.com".into(),
            )
        })?;
        let mut conf = Self {
            enabled: true,
            host: host.into(),
            port: url.port().unwrap_or(match tls {
                MailTls::Tls => 465,
                _ => 587,
            }),
            username: (!url.username().is_empty()).then(|| url.username().into()),
            password: url.password().map(Into::into),
            sender: None,
            tls,
            timeout_seconds: 30,
        };
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "sender" => conf.sender = Some(value.into_owned()),
                "tls" => conf.tls = parse_tls(&value)?,
                "timeout_seconds" => {
                    conf.timeout_seconds = value.parse::<u64>().map_err(|_| {
                        ConfError::Other(format!(
                            "SMTP URL timeout_seconds must be a u64, got: {value}"
                        ))
                    })?;
                }
                unknown => {
                    return Err(ConfError::Other(format!(
                        "unsupported SMTP URL option '{unknown}'"
                    )));
                }
            }
        }
        Ok(conf)
    }

    /// Adds configuration errors when an enabled SMTP transport cannot connect safely.
    pub(crate) fn validate(&self, errors: &mut Vec<ConfError>) {
        if !self.enabled {
            return;
        }
        required(&self.host, "mail.host", errors);
        required_option(&self.sender, "mail.sender", errors);
        if self.port == 0 {
            errors.push(invalid("mail.port", "must be non-zero", "1-65535"));
        }
        if self.timeout_seconds == 0 {
            errors.push(invalid(
                "mail.timeout_seconds",
                "must be non-zero",
                "positive seconds",
            ));
        }
        if self.username.is_some() != self.password.is_some() {
            errors.push(invalid(
                "mail.credentials",
                "username and password must be supplied together",
                "both username and password",
            ));
        }
        #[cfg(feature = "email")]
        if let Some(sender) = &self.sender {
            if sender.parse::<lettre::message::Mailbox>().is_err() {
                errors.push(invalid(
                    "mail.sender",
                    "must be a valid mailbox",
                    "name <address@example.com>",
                ));
            }
        }
    }
}

fn required(value: &str, field: &str, errors: &mut Vec<ConfError>) {
    if value.trim().is_empty() {
        errors.push(ConfError::RequiredField {
            field: field.into(),
            reason: "cannot be empty when mail is enabled".into(),
        });
    }
}

fn required_option(value: &Option<String>, field: &str, errors: &mut Vec<ConfError>) {
    if value.as_deref().is_none_or(|value| value.trim().is_empty()) {
        errors.push(ConfError::RequiredField {
            field: field.into(),
            reason: "cannot be empty when mail is enabled".into(),
        });
    }
}

fn invalid(field: &str, reason: &str, expected: &str) -> ConfError {
    ConfError::InvalidValue {
        field: field.into(),
        reason: reason.into(),
        expected: Some(expected.into()),
    }
}

fn parse_tls(value: &str) -> Result<MailTls, ConfError> {
    match value {
        "start_tls" => Ok(MailTls::StartTls),
        "tls" => Ok(MailTls::Tls),
        "none" => Ok(MailTls::None),
        _ => Err(ConfError::Other(
            "SMTP URL tls must be 'start_tls', 'tls', or 'none'".into(),
        )),
    }
}

#[cfg(feature = "email")]
mod enabled {
    use std::{path::Path, time::Duration};

    use lettre::{
        AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
        message::{
            Attachment as LettreAttachment, Mailbox, MultiPart, SinglePart,
            header::{HeaderName, HeaderValue},
        },
        transport::smtp::authentication::Credentials,
    };
    use serde::Serialize;

    use super::{MailConf, MailTls};
    use crate::{SavedFile, Site, templates::TemplateError};

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
        #[error("attachment I/O error: {0}")]
        Io(#[from] std::io::Error),
        #[error("MIME message error: {0}")]
        Mime(#[from] lettre::error::Error),
        #[error("SMTP transport error: {0}")]
        Smtp(#[from] lettre::transport::smtp::Error),
    }

    /// A binary email attachment with a validated display name and MIME type.
    #[derive(Debug, Clone)]
    pub struct Attachment {
        name: String,
        bytes: Vec<u8>,
        content_type: String,
    }

    impl Attachment {
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

        fn part(&self) -> Result<SinglePart, EmailError> {
            validate_name(&self.name, "attachment.name")?;
            let content_type = lettre::message::header::ContentType::parse(&self.content_type)
                .map_err(|error| EmailError::Invalid {
                    field: "attachment.content_type",
                    reason: error.to_string(),
                })?;
            Ok(LettreAttachment::new(self.name.clone()).body(self.bytes.clone(), content_type))
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
            for (name, value) in &self.headers {
                validate_header(name, value)?;
            }
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
    }

    /// A validated email ready for an SMTP delivery attempt.
    #[derive(Debug)]
    pub struct Email {
        mail: Mail,
    }

    /// Result metadata from an accepted SMTP delivery.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Delivery;

    /// A site-bound SMTP mail facade.
    #[derive(Clone)]
    pub struct Mailer {
        site: Site,
    }

    impl Mailer {
        pub(crate) fn new(site: Site) -> Self {
            Self { site }
        }

        /// Builds MIME content and sends one email through the configured SMTP relay.
        pub async fn send(&self, email: Email) -> Result<Delivery, EmailError> {
            let conf = &self.site.conf().mail;
            if !conf.enabled {
                return Err(EmailError::Disabled);
            }
            let message = email.into_message(&self.site, conf)?;
            transport(conf)?.send(message).await?;
            Ok(Delivery)
        }
    }

    impl Email {
        fn into_message(self, site: &Site, conf: &MailConf) -> Result<Message, EmailError> {
            let Mail {
                from,
                to,
                cc,
                bcc,
                reply_to,
                subject,
                text,
                html,
                html_only,
                headers,
                attachments,
            } = self.mail;
            let mut builder = Message::builder()
                .from(mailbox(from.as_deref().or(conf.sender.as_deref()), "from")?)
                .subject(subject.unwrap_or_default());
            for address in &to {
                builder = builder.to(mailbox(Some(address), "to")?);
            }
            for address in &cc {
                builder = builder.cc(mailbox(Some(address), "cc")?);
            }
            for address in &bcc {
                builder = builder.bcc(mailbox(Some(address), "bcc")?);
            }
            if let Some(reply_to) = &reply_to {
                builder = builder.reply_to(mailbox(Some(reply_to), "reply_to")?);
            }
            for (name, value) in &headers {
                builder = builder.raw_header(HeaderValue::new(header_name(name)?, value.clone()));
            }
            let body = body(render(site, text)?, render(site, html)?, html_only)?;
            finish_message(builder, body, attachments)
        }
    }

    fn template<T: Serialize>(name: impl Into<String>, context: &T) -> Result<Body, EmailError> {
        Ok(Body::Template {
            name: name.into(),
            context: serde_json::to_value(context).map_err(|error| EmailError::Invalid {
                field: "template.context",
                reason: error.to_string(),
            })?,
        })
    }

    fn render(site: &Site, body: Option<Body>) -> Result<Option<String>, EmailError> {
        match body {
            Some(Body::Value(value)) => Ok(Some(value)),
            Some(Body::Template { name, context }) => {
                Ok(Some(site.templates().render(&name, &context)?))
            }
            None => Ok(None),
        }
    }

    fn body(
        text: Option<String>,
        html: Option<String>,
        html_only: bool,
    ) -> Result<MimeBody, EmailError> {
        match (text, html) {
            (Some(text), Some(html)) => Ok(MimeBody::Multi(MultiPart::alternative_plain_html(
                text, html,
            ))),
            (None, Some(html)) if html_only => Ok(MimeBody::Single(SinglePart::html(html))),
            (None, Some(html)) => Ok(MimeBody::Multi(MultiPart::alternative_plain_html(
                html_to_text(&html)?,
                html,
            ))),
            (Some(text), None) => Ok(MimeBody::Single(SinglePart::plain(text))),
            (None, None) => Err(EmailError::MissingBody),
        }
    }

    enum MimeBody {
        Single(SinglePart),
        Multi(MultiPart),
    }

    fn finish_message(
        builder: lettre::message::MessageBuilder,
        body: MimeBody,
        attachments: Vec<Attachment>,
    ) -> Result<Message, EmailError> {
        if attachments.is_empty() {
            return match body {
                MimeBody::Single(part) => builder.singlepart(part).map_err(EmailError::from),
                MimeBody::Multi(parts) => builder.multipart(parts).map_err(EmailError::from),
            };
        }
        let mixed = match body {
            MimeBody::Single(part) => MultiPart::mixed().singlepart(part),
            MimeBody::Multi(parts) => MultiPart::mixed().multipart(parts),
        };
        let mixed = attachments
            .into_iter()
            .try_fold(mixed, |parts, attachment| {
                Ok::<_, EmailError>(parts.singlepart(attachment.part()?))
            })?;
        builder.multipart(mixed).map_err(EmailError::from)
    }

    fn html_to_text(html: &str) -> Result<String, EmailError> {
        let text =
            html2text::from_read(html.as_bytes(), 80).map_err(|_| EmailError::TextFallback)?;
        let text = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        (!text.is_empty())
            .then_some(text)
            .ok_or(EmailError::TextFallback)
    }

    fn transport(conf: &MailConf) -> Result<AsyncSmtpTransport<Tokio1Executor>, EmailError> {
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
        if value.contains(['\r', '\n']) {
            return Err(EmailError::Invalid {
                field: "header.value",
                reason: "must not contain line breaks".into(),
            });
        }
        Ok(())
    }

    fn header_name(value: &str) -> Result<HeaderName, EmailError> {
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
    mod tests {
        use super::*;

        /// Verifies HTML-only mail receives a non-empty generated text alternative.
        #[test]
        fn html_mail_generates_text() -> Result<(), EmailError> {
            let text = html_to_text(
                "<h1>Hello</h1><p>Read <a href=\"https://example.com\">more</a>.</p>",
            )?;
            assert!(text.contains("Hello"));
            assert!(text.contains("https://example.com"));
            Ok(())
        }

        /// Verifies email cannot be built without an envelope recipient.
        #[test]
        fn mail_requires_recipient() {
            let error = Mail::new().subject("Hello").text("Body").build().err();
            assert!(matches!(error, Some(EmailError::MissingRecipient)));
        }

        /// Verifies malformed recipients fail before an SMTP connection is attempted.
        #[test]
        fn mail_rejects_invalid_recipient() {
            let error = Mail::new()
                .to("not an address")
                .subject("Hello")
                .text("Body")
                .build()
                .err();
            assert!(matches!(
                error,
                Some(EmailError::Invalid { field: "to", .. })
            ));
        }

        /// Verifies non-content HTML cannot create a blank text fallback silently.
        #[test]
        fn empty_html_fallback_fails() {
            let error = html_to_text("<style>body { color: red; }</style>").err();
            assert!(matches!(error, Some(EmailError::TextFallback)));
        }

        /// Verifies attachment names cannot introduce MIME header or path injection.
        #[test]
        fn attachment_rejects_unsafe_name() {
            let error = Attachment::bytes("report\r\nX-Test: injected", vec![1])
                .part()
                .err();
            assert!(matches!(
                error,
                Some(EmailError::Invalid {
                    field: "attachment.name",
                    ..
                })
            ));
        }
    }
}

#[cfg(feature = "email")]
pub use enabled::{Attachment, Delivery, Email, EmailError, Mail, Mailer};

#[cfg(test)]
mod conf_tests {
    use super::*;

    /// Verifies enabled SMTP configuration requires a host and default sender.
    #[test]
    fn enabled_mail_requires_connection_fields() {
        let conf = MailConf {
            enabled: true,
            ..MailConf::default()
        };
        let mut errors = Vec::new();
        conf.validate(&mut errors);
        assert_eq!(errors.len(), 2);
    }

    /// Verifies SMTP URLs supply transport credentials and URL options.
    #[test]
    fn smtp_url_builds_mail_configuration() -> Result<(), ConfError> {
        let conf = MailConf::from_url(
            "smtps://mailer:secret@mail.example.com?sender=noreply%40example.com&timeout_seconds=10",
        )?;
        assert!(conf.enabled);
        assert_eq!(conf.host, "mail.example.com");
        assert_eq!(conf.port, 465);
        assert_eq!(conf.username.as_deref(), Some("mailer"));
        assert_eq!(conf.password.as_deref(), Some("secret"));
        assert_eq!(conf.sender.as_deref(), Some("noreply@example.com"));
        assert_eq!(conf.timeout_seconds, 10);
        Ok(())
    }
}
