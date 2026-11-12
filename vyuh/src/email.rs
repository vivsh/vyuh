//! Outbound email preparation and site-selected delivery.

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
    /// Relay-specific header used for a message's idempotency key, when supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_header: Option<String>,
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
            idempotency_header: None,
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
    /// `sender`, `tls`, `timeout_seconds`, and `idempotency_header` query parameters refine it.
    /// An invalid or reserved idempotency header fails parsing.
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
            tls,
            ..Self::default()
        };
        conf.apply_url_options(&url)?;
        Ok(conf)
    }

    /// Applies supported SMTP URL options and validates any relay header name.
    fn apply_url_options(&mut self, url: &url::Url) -> Result<(), ConfError> {
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "sender" => self.sender = Some(value.into_owned()),
                "tls" => self.tls = parse_tls(&value)?,
                "idempotency_header" => {
                    validate_idempotency_header(&value)?;
                    self.idempotency_header = Some(value.into_owned());
                }
                "timeout_seconds" => {
                    self.timeout_seconds = value.parse::<u64>().map_err(|_| {
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
        Ok(())
    }

    /// Validates the relay header and checks connection settings when SMTP is enabled.
    pub(crate) fn validate(&self, errors: &mut Vec<ConfError>) {
        if let Some(name) = &self.idempotency_header
            && let Err(error) = validate_idempotency_header(name)
        {
            errors.push(error);
        }
        if !self.enabled {
            return;
        }
        required_option(&self.sender, "mail.sender", errors);
        self.transport_errors(errors);
        self.sender_errors(errors);
    }

    #[cfg(feature = "email")]
    fn validate_transport(&self) -> Result<(), enabled::EmailError> {
        let mut errors = Vec::new();
        self.transport_errors(&mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(enabled::EmailError::Invalid {
                field: "mail.smtp",
                reason: ConfError::Many(errors).to_string(),
            })
        }
    }

    /// Validates connection options independently of site sender defaults.
    fn transport_errors(&self, errors: &mut Vec<ConfError>) {
        required(&self.host, "mail.host", errors);
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
    }

    pub(crate) fn sender_errors(&self, errors: &mut Vec<ConfError>) {
        #[cfg(not(feature = "email"))]
        let _ = errors;
        #[cfg(feature = "email")]
        if let Some(sender) = &self.sender
            && sender.parse::<lettre::message::Mailbox>().is_err()
        {
            errors.push(invalid(
                "mail.sender",
                "must be a valid mailbox",
                "name <address@example.com>",
            ));
        }
    }
}

fn validate_idempotency_header(name: &str) -> Result<(), ConfError> {
    match header_name_issue(name) {
        Some(reason) => Err(invalid(
            "mail.idempotency_header",
            reason,
            "an unreserved SMTP header name",
        )),
        None => Ok(()),
    }
}

/// Checks the header syntax accepted by the MIME builder and excludes its reserved fields.
fn header_name_issue(name: &str) -> Option<&'static str> {
    if name.is_empty() || name.len() > 76 {
        return Some("must be between 1 and 76 ASCII characters");
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_graphic() && byte != b':')
    {
        return Some("must contain only visible ASCII characters other than ':'");
    }
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
        return Some("reserved for the email envelope or MIME builder");
    }
    None
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
mod enabled;

#[cfg(feature = "email")]
pub use enabled::{
    Attachment, Delivery, DeliveryError, DeliveryOutcome, Email, EmailError, Mail, MailDelivery,
    Mailer, PreparedEmail, SmtpDelivery,
};
#[cfg(all(feature = "email", feature = "test-support"))]
pub use enabled::{MailOutbox, OutboxMessage};
#[cfg(feature = "email")]
pub(crate) use enabled::{MailReporter, SharedMailDelivery, select_delivery, validate_recipient};

#[cfg(test)]
#[path = "tests/email.rs"]
mod tests;
