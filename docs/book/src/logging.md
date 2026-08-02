# Logging

Vyuh logging is built on Rust's `tracing` ecosystem. A site initializes tracing
when it is built, keeps file writer guards alive for the site lifetime, and lets
applications route logs to stdout, stderr, or rotating files.

Logging is configuration-driven. Application code uses ordinary `tracing`
macros such as `tracing::info!`, while `SiteConf.logging` decides which sinks
receive those events and which filters are active.

## Overview

The main public pieces are:

- `LoggingConf` for the full logging setup.
- `LogRule` for one sink plus one default filter.
- `LogSink` for stdout, stderr, or file output.
- `Rotation` for file sink rotation.
- `LoggingError` for validation and initialization failures.

`SiteConf::default()` enables a pretty stdout rule in debug builds and no rules
in release builds. Its debug filter is `debug,sqlx=error`, so Vyuh diagnostics
and SQLx failures remain visible without SQLx query noise. Release applications
should configure logging explicitly.

## Configuration

Configure logging on `SiteConf`:

```rust
use vyuh::prelude::*;
use vyuh::logging::{LogRule, LogSink, LoggingConf, Rotation};

let conf = SiteConf::default().logging(LoggingConf {
    env_prefix: Some("APP_LOG".into()),
    rules: vec![
        LogRule {
            name: "APP".into(),
            sink: LogSink::Stdout { pretty: true },
            default_filter: "info,vyuh=warn".into(),
        },
        LogRule {
            name: "AUDIT".into(),
            sink: LogSink::File {
                dir: "logs".into(),
                rotation: Rotation::Daily,
            },
            default_filter: "warn".into(),
        },
    ],
});
```

Each rule creates one tracing layer. A rule can be disabled by resolving to
`off`, `0`, `false`, or `no`.

## Environment Overrides

The environment prefix defaults to `RUST_LOG`. For each rule, Vyuh resolves the
filter in this order:

1. `<PREFIX>_<UPPERCASE_RULE_NAME>`
2. `<PREFIX>`
3. `LogRule::default_filter`

For example, with `env_prefix: Some("APP_LOG")` and a rule named `Audit`, Vyuh
checks:

```text
APP_LOG_AUDIT
APP_LOG
```

Rule names may use mixed case, but environment variable names use uppercase
rule names. Filter values use normal `tracing_subscriber::EnvFilter` syntax:

```sh
APP_LOG=info
APP_LOG_AUDIT=vyuh::auth=debug,sqlx=warn
APP_LOG_AUDIT=off
```

Rule-specific overrides are useful when one sink should be verbose and another
should stay quiet.

## Sinks

Stdout and stderr support two formats:

- `pretty: true` - human-readable development output with ANSI colors.
- `pretty: false` - JSON output.

File sinks always write JSON and include target, span data, file/line metadata,
thread metadata, and RFC3339 UTC timestamps. Relative file sink directories are
resolved under `SiteConf.project_dir`; absolute directories are used as-is.

File sinks use non-blocking writers. Vyuh stores the writer guards inside the
built `Site`, so logs continue flushing for the site lifetime.

## Rotation

File sinks support:

- `Rotation::Daily`
- `Rotation::Hourly`
- `Rotation::Minutely`

The rule name is used as the file prefix. Rule names must be unique because
they are used for both environment variables and file prefixes.

## Validation

Logging configuration is validated during site build:

- Rule names must start with an ASCII letter, then contain only letters, digits,
  or underscores.
- Rule names must be 48 characters or fewer.
- Rule names must be unique.
- `env_prefix`, when set, must be uppercase letters, digits, or underscores and
  must start with an uppercase letter.
- Filters must parse as valid tracing filter directives unless they disable the
  rule with `off`, `0`, `false`, or `no`.

Invalid logging configuration returns `SiteError::LoggingError`.

## Tests

Tests often disable site logging to avoid global tracing subscriber conflicts
and noisy output:

```rust
let conf = vyuh::SiteConf::default().log_init(false).logging(
    vyuh::logging::LoggingConf {
        env_prefix: None,
        rules: vec![],
    },
);
```

`tracing` has one global subscriber per process. If another test or application
has already initialized tracing, site logging initialization can fail with
`LoggingError::SubscriberInit`. For integration tests, prefer one shared logging
initialization strategy or disable site logging.

## Example

The snippets in this chapter show stdout logging, rotating file logging, and
environment override names.

## Failure Modes

- Invalid rule names return `LoggingError::InvalidRuleName`.
- Invalid environment prefixes return `LoggingError::InvalidEnvPrefix`.
- Duplicate rule names return `LoggingError::DuplicateRuleName`.
- Invalid filter syntax returns `LoggingError::FilterParse`.
- File sink directory creation errors return `LoggingError::DirCreation`.
- A second global tracing initialization returns `LoggingError::SubscriberInit`.

## Current Limitations

- Logging is initialized once per process through tracing's global subscriber.
- Vyuh does not currently expose runtime log-level reconfiguration.
- File logging is JSON-only.
