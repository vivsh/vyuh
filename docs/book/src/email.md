# Email

Vyuh's optional `email` feature sends transactional email through SMTP. It uses
Lettre internally, but application code works only with Vyuh mail types.

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
`none`), and `timeout_seconds`.

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
