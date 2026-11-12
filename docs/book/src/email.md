# Email

Vyuh's optional `email` feature sends transactional email through SMTP or a
custom backend. Both receive the same validated, rendered message. SMTP uses
Lettre internally.

```toml
[dependencies]
vyuh = { version = "0.2", features = ["email"] }
```

## SMTP configuration

Use `MailConf::from_url` directly or set `SMTP_URL`. An SMTP URL configures the
host, port, username, password, and transport security in one value:

```dotenv
SMTP_URL=smtps://mailer:secret@mail.example.com?sender=noreply%40example.com
```

`smtp://` uses STARTTLS and port `587` by default. `smtps://` uses TLS from the
start and port `465`. URL user-info provides the SMTP username and password.
The optional query parameters are `sender`, `tls` (`start_tls`, `tls`, or
`none`), `timeout_seconds`, and `idempotency_header`.

The sender can also be configured explicitly when it should not be kept with
the SMTP URL:

```rust
use vyuh::email::MailConf;

let mut mail = MailConf::from_url("smtp://mailer:secret@mail.example.com")?;
mail.sender = Some("noreply@example.com".into());
let conf = vyuh::SiteConf::from_env()?.mail(mail);
```

`Site::build` rejects an enabled mail configuration without a host, sender, or
complete credentials. Individual `MAIL_*` variables remain available for
deployment systems that do not permit a URL secret: `MAIL_ENABLED`,
`MAIL_HOST`, `MAIL_PORT`, `MAIL_USERNAME`, `MAIL_PASSWORD`, `MAIL_SENDER`,
`MAIL_TLS`, and `MAIL_TIMEOUT_SECONDS`.

## Sending mail

Build an email, then send it through the site-owned mailer:

```rust
use vyuh::email::Mail;

let email = Mail::new()
    .to("Ada <ada@example.com>")
    .subject("Welcome")
    .html("<h1>Welcome</h1><p>Thanks for joining.</p>")
    .build()?;

site.mail().send(email).await?;
```

An HTML-only message receives a deterministic generated plain-text alternative
through `vyuh::utils::html::html_to_text`, which applications can also use for
their own non-email fallbacks.
An explicit `.text(...)` takes precedence; use `.html_only()` only for the rare
case where an HTML-only message is required.

## Templates and attachments

Mail templates use the same bundle-owned MiniJinja templates as pages. Rendering
happens immediately before delivery, so a missing template or render failure is
returned as `EmailError`.

```rust
let attachment = vyuh::email::Attachment::saved_file(&report).await?;
let email = Mail::new()
    .to(user.email())
    .subject("Your report")
    .html_template("mail/report.html", &context)?
    .attachment(attachment)
    .build()?;

site.mail().send(email).await?;
```

Use `Attachment::bytes` for generated content and `Attachment::file` for a
runtime path. Attachments are nested correctly with the text/HTML body.

## Custom delivery

Implement `vyuh::email::MailDelivery` and register one backend at site startup.
For a JSON provider, use `PreparedEmail` accessors for addresses, rendered text
and HTML, custom headers, and attachment bytes. A raw-message provider can
read the envelope recipients and then call `PreparedEmail::into_mime()`.
Bcc recipients are available separately from the serialized headers.

```rust
let site = Site::builder(conf)
    .mail_delivery(provider_backend)
    .build(bundle)
    .await?;
```

Set `conf.mail.sender` for a default sender or use `.from(...)` on each mail.
Admin mail shares the same backend and requires the default sender.
`Delivery` reports the message ID, the outcome (accepted, printed, or captured),
and an optional provider submission ID. Acceptance is acknowledgement by the
relay or provider. Delivery failures surface as `EmailError::Delivery`, with
separate cases for temporary failure, permanent rejection, and unknown acceptance.

## Idempotent submissions

Use `.idempotency_key(...)` for a logical send. A backend receives the key as
submission metadata. A JSON adapter can forward it as an HTTP header or API
field. Plain SMTP rejects a keyed message before connecting. When your SMTP
relay documents a deduplication header, configure its exact name explicitly.

Set `idempotency_header` in `SMTP_URL` and load configuration as usual with
`SiteConf::from_env()`. The URL sets the header name. Each message supplies
its own key.

```dotenv
SMTP_URL="smtps://mailer:secret@mail.example.com?sender=noreply%40example.com&idempotency_header=Resend-Idempotency-Key"
```

```rust
use vyuh::email::Mail;

let email = Mail::new()
    .to("customer@example.com")
    .subject("Your receipt")
    .text("Thanks for your order.")
    .idempotency_key("receipt/order-123")
    .build()?;
let delivery = site.mail().send(email).await?;
```

In Rust, set `MailConf::idempotency_header` or pass an explicit header to
`SmtpDelivery::with_idempotency_header(...)`. Invalid and reserved header names
fail configuration validation before SMTP connects.

Keys are 1–256 bytes without whitespace or control characters. The SMTP
mapping requires ASCII so the relay receives the key without MIME encoding.
A keyed SMTP submission fails if a custom header already uses the mapped name.
Use the same key and provider payload on retry, within the relay's documented
deduplication window. Give an intentional resend a new key. `send` awaits one
submission attempt. For durable retries, persist the key and stable payload in
an application task.

## Development and tests

`Site::builder(conf).console_mail_fallback()` opts into stdout previews in
development when neither a custom backend nor SMTP is configured. Previews can
contain sensitive message content. Production rejects the fallback setting.
Without a backend or fallback, `send` returns `EmailError::Disabled`.

With `email` and `test-support`, sites automatically capture mail in memory,
ahead of every configured backend. Inspect it through `site.mail_outbox()`.
See [Site testing](site.md#testing) for an example.
