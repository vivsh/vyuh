use crate::{SiteError, notifiers::CancellationNotifier};
use notify::{RecursiveMode, Watcher};
use std::{future::Future, path::PathBuf, pin::Pin, time::Duration};
use tokio::signal;

pub async fn watch_file(path: PathBuf) -> Result<(), SiteError> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<notify::Result<notify::Event>>(1024);
    let mut watcher = notify::recommended_watcher(move |res| {
        tx.try_send(res).ok();
    })
    .map_err(|e| SiteError::FileWatchError(format!("Failed to create watcher: {}", e)))?;
    watcher
        .watch(path.as_path(), RecursiveMode::NonRecursive)
        .map_err(|e| SiteError::FileWatchError(format!("Failed to watch file: {}", e)))?;
    rx.recv().await;
    Ok(())
}

async fn reload_signal(touch_reload: Option<String>) -> Result<(), SiteError> {
    if let Some(path) = touch_reload {
        let path = PathBuf::from(path);
        if path.exists() {
            return watch_file(path).await;
        }
    }
    futures::future::pending().await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownSource {
    Programmatic,
    CtrlC,
    Terminate,
    TouchReload,
}

#[derive(Clone)]
pub(crate) struct ShutdownController {
    touch_reload: Option<String>,
    graceful: CancellationNotifier,
    forced: CancellationNotifier,
    done: CancellationNotifier,
    grace_period: Duration,
}

impl ShutdownController {
    pub(crate) fn new(
        touch_reload: Option<String>,
        graceful: CancellationNotifier,
        grace_period: Duration,
    ) -> Self {
        Self {
            touch_reload,
            graceful,
            forced: CancellationNotifier::new(),
            done: CancellationNotifier::new(),
            grace_period,
        }
    }

    pub(crate) fn force_notifier(&self) -> CancellationNotifier {
        self.forced.child()
    }

    pub(crate) fn complete(&self) {
        self.done.notify_waiters();
    }

    pub(crate) async fn graceful(self) {
        let source = self.first_source().await;
        print_start(source);
        tracing::info!("Shutdown requested from {:?}", source);
        self.graceful.notify_waiters();
        self.spawn_force_monitor();
    }

    async fn first_source(&self) -> ShutdownSource {
        tokio::select! {
            _ = self.graceful.notified() => ShutdownSource::Programmatic,
            _ = interrupt_signal() => ShutdownSource::CtrlC,
            _ = terminate_signal() => ShutdownSource::Terminate,
            touch = reload_signal(self.touch_reload.clone()) => touch_source(touch),
        }
    }

    fn spawn_force_monitor(&self) {
        let done = self.done.clone();
        let forced = self.forced.clone();
        let grace_period = self.grace_period;
        tokio::spawn(async move {
            wait_for_force(done, forced, grace_period, Box::pin(interrupt_signal())).await;
        });
    }
}

async fn interrupt_signal() {
    if let Err(err) = signal::ctrl_c().await {
        tracing::warn!("Failed to install Ctrl+C handler: {}", err);
        futures::future::pending::<()>().await;
    }
}

#[cfg(unix)]
async fn terminate_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => {
            sigterm.recv().await;
        }
        Err(err) => {
            tracing::warn!("Failed to install SIGTERM handler: {}", err);
            futures::future::pending::<()>().await;
        }
    }
}

#[cfg(not(unix))]
async fn terminate_signal() {
    futures::future::pending::<()>().await;
}

fn touch_source(result: Result<(), SiteError>) -> ShutdownSource {
    if let Err(err) = result {
        tracing::error!("Error during file watch: {}", err);
    } else {
        tracing::info!("File change detected, reloading...");
    }
    ShutdownSource::TouchReload
}

fn print_start(source: ShutdownSource) {
    match source {
        ShutdownSource::CtrlC => {
            eprintln!("Ctrl+C received. Gracefully shutting down; press Ctrl+C again to force.");
        }
        ShutdownSource::Terminate => {
            eprintln!("Shutdown signal received. Gracefully shutting down.");
        }
        ShutdownSource::TouchReload => {
            eprintln!("Reload requested. Gracefully shutting down.");
        }
        ShutdownSource::Programmatic => {}
    }
}

fn timeout_message(grace_period: Duration) -> String {
    format!(
        "Graceful shutdown timed out after {}ms. Forcing shutdown now.",
        grace_period.as_millis()
    )
}

async fn wait_for_force(
    done: CancellationNotifier,
    forced: CancellationNotifier,
    grace_period: Duration,
    interrupt: Pin<Box<dyn Future<Output = ()> + Send>>,
) {
    tokio::select! {
        _ = done.notified() => {},
        _ = interrupt => {
            eprintln!("Forcing shutdown now.");
            forced.notify_waiters();
        },
        _ = tokio::time::sleep(grace_period) => {
            eprintln!("{}", timeout_message(grace_period));
            forced.notify_waiters();
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn force_monitor_times_out() {
        let done = CancellationNotifier::new();
        let forced = CancellationNotifier::new();
        wait_for_force(
            done,
            forced.clone(),
            Duration::from_millis(1),
            Box::pin(futures::future::pending()),
        )
        .await;
        assert!(forced.is_notified());
    }

    #[tokio::test]
    async fn force_monitor_honors_done() {
        let done = CancellationNotifier::new();
        let forced = CancellationNotifier::new();
        done.notify_waiters();
        wait_for_force(
            done,
            forced.clone(),
            Duration::from_millis(1),
            Box::pin(futures::future::pending()),
        )
        .await;
        assert!(!forced.is_notified());
    }

    #[tokio::test]
    async fn force_monitor_honors_interrupt() {
        let done = CancellationNotifier::new();
        let forced = CancellationNotifier::new();
        wait_for_force(
            done,
            forced.clone(),
            Duration::from_secs(60),
            Box::pin(async {}),
        )
        .await;
        assert!(forced.is_notified());
    }

    #[tokio::test]
    async fn programmatic_shutdown_is_graceful_source() {
        let graceful = CancellationNotifier::new();
        let controller = ShutdownController::new(None, graceful.clone(), Duration::from_secs(1));
        graceful.notify_waiters();
        let source = tokio::time::timeout(Duration::from_secs(1), controller.first_source()).await;
        assert!(matches!(source, Ok(ShutdownSource::Programmatic)));
    }

    #[test]
    fn timeout_message_includes_duration() {
        let message = timeout_message(Duration::from_millis(10_000));
        assert!(message.contains("10000ms"));
    }
}
