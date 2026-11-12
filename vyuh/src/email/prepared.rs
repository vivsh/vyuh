use lettre::{
    Message,
    message::{MultiPart, SinglePart, header::HeaderValue},
};

use super::{Attachment, Email, EmailError, Mail, TemplateEngine, header_name, mailbox, render};
use crate::utils::html::html_to_text;

/// Validated, rendered email and submission metadata passed to a delivery backend.
pub struct PreparedEmail {
    from: String,
    to: Vec<String>,
    cc: Vec<String>,
    bcc: Vec<String>,
    reply_to: Option<String>,
    subject: String,
    text: Option<String>,
    html: Option<String>,
    headers: Vec<(String, String)>,
    attachments: Vec<Attachment>,
    message_id: String,
    pub(super) idempotency_key: Option<String>,
}

impl PreparedEmail {
    /// Returns the resolved sender mailbox.
    pub fn from(&self) -> &str {
        &self.from
    }
    /// Returns primary recipients.
    pub fn to(&self) -> &[String] {
        &self.to
    }
    /// Returns carbon-copy recipients.
    pub fn cc(&self) -> &[String] {
        &self.cc
    }
    /// Returns blind-copy recipients for envelope routing.
    pub fn bcc(&self) -> &[String] {
        &self.bcc
    }
    /// Returns the reply-to mailbox, if supplied.
    pub fn reply_to(&self) -> Option<&str> {
        self.reply_to.as_deref()
    }
    /// Returns the message subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }
    /// Returns the rendered or generated text alternative.
    pub fn text(&self) -> Option<&str> {
        self.text.as_deref()
    }
    /// Returns the rendered HTML alternative.
    pub fn html(&self) -> Option<&str> {
        self.html.as_deref()
    }
    /// Returns custom message headers in insertion order.
    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }
    /// Borrows the message attachments.
    pub fn attachments(&self) -> &[Attachment] {
        &self.attachments
    }
    /// Returns Vyuh's message identifier for this submission attempt.
    pub fn message_id(&self) -> &str {
        &self.message_id
    }
    /// Returns the opaque submission key, when requested by the application.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// Serializes the email as MIME without publishing Bcc or the submission key.
    /// Raw-message adapters should read the envelope recipients before consuming it.
    pub fn into_mime(self) -> Result<Vec<u8>, EmailError> {
        Ok(self.into_message()?.formatted())
    }

    pub(super) fn add_header(&mut self, name: String, value: String) {
        self.headers.push((name, value));
    }

    /// Builds the MIME message and its separate SMTP envelope.
    pub(super) fn into_message(self) -> Result<Message, EmailError> {
        let mut builder = Message::builder()
            .from(mailbox(Some(&self.from), "from")?)
            .subject(self.subject)
            .message_id(Some(self.message_id));
        for address in &self.to {
            builder = builder.to(mailbox(Some(address), "to")?);
        }
        for address in &self.cc {
            builder = builder.cc(mailbox(Some(address), "cc")?);
        }
        for address in &self.bcc {
            builder = builder.bcc(mailbox(Some(address), "bcc")?);
        }
        if let Some(reply_to) = &self.reply_to {
            builder = builder.reply_to(mailbox(Some(reply_to), "reply_to")?);
        }
        for (name, value) in self.headers {
            builder = builder.raw_header(HeaderValue::new(header_name(&name)?, value));
        }
        finish_message(builder, self.text, self.html, self.attachments)
    }
}

impl Email {
    /// Resolves site-dependent fields once before invoking a backend.
    pub(super) fn prepare(
        self,
        templates: Option<&TemplateEngine>,
        default_sender: Option<&str>,
    ) -> Result<PreparedEmail, EmailError> {
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
            idempotency_key,
        } = self.mail;
        let from = from.or_else(|| default_sender.map(str::to_owned));
        let sender = mailbox(from.as_deref(), "from")?;
        let message_id = format!("<{}@{}>", uuid::Uuid::new_v4(), sender.email.domain());
        let html = render(templates, html)?;
        let text = fallback(render(templates, text)?, html.as_deref(), html_only)?;
        Ok(PreparedEmail {
            from: from.ok_or(EmailError::MissingSender)?,
            to,
            cc,
            bcc,
            reply_to,
            subject: subject.ok_or(EmailError::MissingSubject)?,
            text,
            html,
            headers,
            attachments,
            message_id,
            idempotency_key,
        })
    }
}

pub(super) fn fallback(
    text: Option<String>,
    html: Option<&str>,
    html_only: bool,
) -> Result<Option<String>, EmailError> {
    match (text, html, html_only) {
        (None, Some(html), false) => html_to_text(html)
            .map(Some)
            .map_err(|_| EmailError::TextFallback),
        (None, None, _) => Err(EmailError::MissingBody),
        (text, _, _) => Ok(text),
    }
}

/// Builds plain, alternative, or mixed MIME from the rendered bodies.
fn finish_message(
    builder: lettre::message::MessageBuilder,
    text: Option<String>,
    html: Option<String>,
    attachments: Vec<Attachment>,
) -> Result<Message, EmailError> {
    let single = match (text, html) {
        (Some(text), Some(html)) => {
            let alternatives = MultiPart::alternative_plain_html(text, html);
            if attachments.is_empty() {
                return Ok(builder.multipart(alternatives)?);
            }
            return mixed(
                builder,
                MultiPart::mixed().multipart(alternatives),
                attachments,
            );
        }
        (Some(text), None) => SinglePart::plain(text),
        (None, Some(html)) => SinglePart::html(html),
        (None, None) => return Err(EmailError::MissingBody),
    };
    if attachments.is_empty() {
        return Ok(builder.singlepart(single)?);
    }
    mixed(builder, MultiPart::mixed().singlepart(single), attachments)
}

fn mixed(
    builder: lettre::message::MessageBuilder,
    body: MultiPart,
    attachments: Vec<Attachment>,
) -> Result<Message, EmailError> {
    let body = attachments
        .into_iter()
        .try_fold(body, |parts, attachment| {
            Ok::<_, EmailError>(parts.singlepart(attachment.into_part()?))
        })?;
    Ok(builder.multipart(body)?)
}
