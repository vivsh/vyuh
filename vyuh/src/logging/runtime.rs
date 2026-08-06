use std::{
    fs,
    path::{Path, PathBuf},
};

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(feature = "email")]
use super::mail_admins;
use super::{LogSink, LoggingConf, LoggingError, Rotation};

/// Keeps configured logging writers and reporting workers alive for a built site.
pub struct LoggingGuard {
    _file_guards: Vec<WorkerGuard>,
    #[cfg(feature = "email")]
    mail_admins: Vec<mail_admins::MailAdminsRuntime>,
    #[cfg(feature = "email")]
    reporter: Option<crate::email::MailReporter>,
}

impl LoggingGuard {
    pub(crate) fn noop() -> Self {
        Self {
            _file_guards: Vec::new(),
            #[cfg(feature = "email")]
            mail_admins: Vec::new(),
            #[cfg(feature = "email")]
            reporter: None,
        }
    }

    /// Starts each configured mail-admin worker under the site's shutdown lifecycle.
    pub(crate) fn start_mail_admins(
        &self,
        shutdown: crate::notifiers::CancellationNotifier,
        joinset: &mut tokio::task::JoinSet<()>,
    ) {
        #[cfg(feature = "email")]
        if let Some(reporter) = &self.reporter {
            for runtime in &self.mail_admins {
                runtime.start(reporter.clone(), shutdown.clone(), joinset);
            }
        }
        #[cfg(not(feature = "email"))]
        let _ = (shutdown, joinset);
    }
}

/// Resolves a file sink directory relative to the configured project root.
pub(crate) fn resolve_log_dir(project_dir: &Path, dir: &str) -> PathBuf {
    let path = Path::new(dir);
    if path.is_relative() {
        project_dir.join(path)
    } else {
        path.to_path_buf()
    }
}

/// Installs the configured process-wide tracing layers and retains their resources.
pub(crate) fn init_tracing(
    project_dir: &Path,
    conf: &LoggingConf,
    #[cfg(feature = "email")] reporter: Option<crate::email::MailReporter>,
) -> Result<LoggingGuard, LoggingError> {
    if conf.rules.is_empty() {
        return Ok(empty_guard(
            #[cfg(feature = "email")]
            reporter,
        ));
    }
    conf.validate()?;
    let env_prefix = conf.resolved_env_prefix();
    let global_filter = std::env::var(env_prefix).ok();
    let mut guards = Vec::new();
    #[cfg(feature = "email")]
    let mut mail_admins = Vec::new();
    type BoxedLayer = Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>;
    let mut layers: Vec<BoxedLayer> = Vec::new();

    for rule in &conf.rules {
        let Some(filter) = rule.build_filter(env_prefix, global_filter.as_ref())? else {
            continue;
        };
        install_rule(
            project_dir,
            rule,
            filter,
            &mut guards,
            &mut layers,
            #[cfg(feature = "email")]
            &mut mail_admins,
            #[cfg(feature = "email")]
            reporter.as_ref(),
        )?;
    }

    if layers.is_empty() {
        return Ok(LoggingGuard {
            _file_guards: Vec::new(),
            #[cfg(feature = "email")]
            mail_admins,
            #[cfg(feature = "email")]
            reporter,
        });
    }
    tracing_subscriber::registry().with(layers).try_init()?;
    Ok(LoggingGuard {
        _file_guards: guards,
        #[cfg(feature = "email")]
        mail_admins,
        #[cfg(feature = "email")]
        reporter,
    })
}

fn empty_guard(
    #[cfg(feature = "email")] reporter: Option<crate::email::MailReporter>,
) -> LoggingGuard {
    LoggingGuard {
        _file_guards: Vec::new(),
        #[cfg(feature = "email")]
        mail_admins: Vec::new(),
        #[cfg(feature = "email")]
        reporter,
    }
}

fn install_rule(
    project_dir: &Path,
    rule: &super::LogRule,
    filter: tracing_subscriber::EnvFilter,
    guards: &mut Vec<WorkerGuard>,
    layers: &mut Vec<Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>>,
    #[cfg(feature = "email")] mail_admins: &mut Vec<mail_admins::MailAdminsRuntime>,
    #[cfg(feature = "email")] reporter: Option<&crate::email::MailReporter>,
) -> Result<(), LoggingError> {
    match &rule.sink {
        LogSink::File { dir, rotation } => install_file(
            project_dir,
            dir,
            &rule.name,
            *rotation,
            filter,
            guards,
            layers,
        ),
        LogSink::Stdout { pretty } => {
            install_stdout(*pretty, filter, layers);
            Ok(())
        }
        LogSink::Stderr { pretty } => {
            install_stderr(*pretty, filter, layers);
            Ok(())
        }
        LogSink::MailAdmins(admins) => install_mail_admins(
            &rule.name,
            admins,
            filter,
            layers,
            #[cfg(feature = "email")]
            mail_admins,
            #[cfg(feature = "email")]
            reporter,
        ),
    }
}

fn install_file(
    project_dir: &Path,
    dir: &str,
    name: &str,
    rotation: Rotation,
    filter: tracing_subscriber::EnvFilter,
    guards: &mut Vec<WorkerGuard>,
    layers: &mut Vec<Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>>,
) -> Result<(), LoggingError> {
    let log_dir = resolve_log_dir(project_dir, dir);
    fs::create_dir_all(&log_dir)?;
    let appender = match rotation {
        Rotation::Daily => tracing_appender::rolling::daily(&log_dir, name),
        Rotation::Hourly => tracing_appender::rolling::hourly(&log_dir, name),
        Rotation::Minutely => tracing_appender::rolling::minutely(&log_dir, name),
    };
    let (writer, guard) = tracing_appender::non_blocking(appender);
    guards.push(guard);
    let layer = fmt::layer()
        .json()
        .with_writer(writer)
        .with_current_span(true)
        .with_span_list(true)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_timer(fmt::time::UtcTime::rfc_3339());
    layers.push(layer.with_filter(filter).boxed());
    Ok(())
}

fn install_stdout(
    pretty: bool,
    filter: tracing_subscriber::EnvFilter,
    layers: &mut Vec<Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>>,
) {
    if pretty {
        layers.push(
            fmt::layer()
                .pretty()
                .with_ansi(true)
                .with_writer(std::io::stdout)
                .with_filter(filter)
                .boxed(),
        );
    } else {
        layers.push(
            fmt::layer()
                .json()
                .with_writer(std::io::stdout)
                .with_current_span(true)
                .with_span_list(true)
                .with_target(true)
                .with_timer(fmt::time::UtcTime::rfc_3339())
                .with_filter(filter)
                .boxed(),
        );
    }
}

fn install_stderr(
    pretty: bool,
    filter: tracing_subscriber::EnvFilter,
    layers: &mut Vec<Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>>,
) {
    if pretty {
        layers.push(
            fmt::layer()
                .pretty()
                .with_ansi(true)
                .with_writer(std::io::stderr)
                .with_filter(filter)
                .boxed(),
        );
    } else {
        layers.push(
            fmt::layer()
                .json()
                .with_writer(std::io::stderr)
                .with_current_span(true)
                .with_span_list(true)
                .with_target(true)
                .with_timer(fmt::time::UtcTime::rfc_3339())
                .with_filter(filter)
                .boxed(),
        );
    }
}

fn install_mail_admins(
    name: &str,
    admins: &super::MailAdmins,
    filter: tracing_subscriber::EnvFilter,
    layers: &mut Vec<Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>>,
    #[cfg(feature = "email")] mail_admins: &mut Vec<mail_admins::MailAdminsRuntime>,
    #[cfg(feature = "email")] reporter: Option<&crate::email::MailReporter>,
) -> Result<(), LoggingError> {
    #[cfg(feature = "email")]
    {
        if reporter.is_none() {
            return Err(LoggingError::MailAdminsMailDisabled);
        }
        let (layer, runtime) = mail_admins::layer(name, admins.clone());
        layers.push(layer.with_filter(filter).boxed());
        mail_admins.push(runtime);
        Ok(())
    }
    #[cfg(not(feature = "email"))]
    {
        let _ = (name, admins, filter, layers);
        Err(LoggingError::MailAdminsFeature)
    }
}
