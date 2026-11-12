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
