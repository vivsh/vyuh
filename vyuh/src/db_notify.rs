#![allow(async_fn_in_trait)]

//! Vyuh-owned PostgreSQL LISTEN/NOTIFY helpers.
//!
//! The database pool comes from the shared DB toolkit. Notification wiring
//! stays in Vyuh because it depends on framework shutdown coordination.

use tokio::sync::mpsc;

use crate::db::{DbError, DbPool};
use crate::notifiers::CancellationNotifier;

#[cfg(feature = "postgres")]
use tokio::sync::mpsc::error::TrySendError;

/// A PostgreSQL notification received by a Vyuh site.
#[derive(Debug, Clone)]
pub struct Notify {
    /// Notification channel name.
    pub channel: String,
    /// Notification payload.
    pub payload: String,
}

/// PostgreSQL notification helpers for Vyuh database pools.
pub trait PgNotifyDbExt {
    /// Send a PostgreSQL notification.
    async fn send_pgnotify(&self, channel: &str, payload: &str) -> Result<(), DbError>;

    /// Consume PostgreSQL notifications until shutdown is requested.
    async fn consume_notify(
        &self,
        topics: &[String],
        capacity: usize,
        reconnect_initial_ms: u64,
        reconnect_max_ms: u64,
        shutdown: CancellationNotifier,
    ) -> Result<mpsc::Receiver<Notify>, DbError>;
}

#[cfg(feature = "postgres")]
impl PgNotifyDbExt for DbPool {
    async fn send_pgnotify(&self, channel: &str, payload: &str) -> Result<(), DbError> {
        crate::db::sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(self.as_sqlx())
            .await?;
        Ok(())
    }

    async fn consume_notify(
        &self,
        topics: &[String],
        capacity: usize,
        reconnect_initial_ms: u64,
        reconnect_max_ms: u64,
        shutdown: CancellationNotifier,
    ) -> Result<mpsc::Receiver<Notify>, DbError> {
        let (sender, receiver) = mpsc::channel::<Notify>(capacity);
        let reconnect_initial = reconnect_duration(reconnect_initial_ms, 1);
        let reconnect_max = reconnect_duration(reconnect_max_ms, reconnect_initial_ms.max(1));
        spawn_notify_listener(
            self.as_sqlx().clone(),
            topics.to_vec(),
            sender,
            reconnect_initial,
            reconnect_max,
            shutdown,
        );
        Ok(receiver)
    }
}

#[cfg(not(feature = "postgres"))]
impl PgNotifyDbExt for DbPool {
    async fn send_pgnotify(&self, _channel: &str, _payload: &str) -> Result<(), DbError> {
        Err(DbError::Capability {
            operation: "send_pgnotify",
            capability: "PgNotify (Postgres only)",
        })
    }

    async fn consume_notify(
        &self,
        _topics: &[String],
        _capacity: usize,
        _reconnect_initial_ms: u64,
        _reconnect_max_ms: u64,
        _shutdown: CancellationNotifier,
    ) -> Result<mpsc::Receiver<Notify>, DbError> {
        Err(DbError::Capability {
            operation: "consume_notify",
            capability: "LISTEN/NOTIFY (Postgres only)",
        })
    }
}

#[cfg(feature = "postgres")]
fn reconnect_duration(value_ms: u64, min_ms: u64) -> std::time::Duration {
    std::time::Duration::from_millis(value_ms.max(min_ms))
}

#[cfg(feature = "postgres")]
fn spawn_notify_listener(
    pool: crate::db::Pool,
    topics: Vec<String>,
    sender: mpsc::Sender<Notify>,
    reconnect_initial: std::time::Duration,
    reconnect_max: std::time::Duration,
    shutdown: CancellationNotifier,
) {
    tokio::spawn(async move {
        run_notify_listener(
            pool,
            topics,
            None,
            move |notification| notification.is_none_or(|value| send_notification(&sender, value)),
            reconnect_initial,
            reconnect_max,
            shutdown,
        )
        .await;
    });
}

#[cfg(feature = "postgres")]
/// Shares subscription, bounded reconnects and shutdown handling between notification consumers.
async fn run_notify_listener<F>(
    pool: crate::db::Pool,
    topics: Vec<String>,
    mut listener: Option<crate::db::sqlx::postgres::PgListener>,
    mut receive: F,
    reconnect_initial: std::time::Duration,
    reconnect_max: std::time::Duration,
    shutdown: CancellationNotifier,
) where
    F: FnMut(Option<Notify>) -> bool + Send,
{
    let mut backoff = reconnect_initial;
    loop {
        let connected = match listener.take() {
            Some(listener) => Ok(listener),
            None => connect_listener(&pool, &topics).await,
        };
        let outcome = match connected {
            Ok(mut listener) => {
                if !receive(None) {
                    break;
                }
                backoff = reconnect_initial;
                receive_notifications(&mut listener, &mut receive, &shutdown).await
            }
            Err(error) => {
                tracing::warn!("notification listener connection failed: {error}");
                NotifyLoop::Disconnected
            }
        };
        match outcome {
            NotifyLoop::Shutdown | NotifyLoop::Closed => break,
            NotifyLoop::Disconnected => {}
        }
        if !sleep_reconnect_backoff(&shutdown, backoff).await {
            break;
        }
        backoff = next_reconnect_backoff(backoff, reconnect_max);
    }
    tracing::info!("notification listener ended");
}

#[cfg(feature = "postgres")]
enum NotifyLoop {
    Closed,
    Disconnected,
    Shutdown,
}

#[cfg(feature = "postgres")]
/// Establishes every subscription before exposing the session to its consumer.
async fn connect_listener(
    pool: &crate::db::Pool,
    topics: &[String],
) -> Result<crate::db::sqlx::postgres::PgListener, crate::db::sqlx::Error> {
    let mut listener = crate::db::sqlx::postgres::PgListener::connect_with(pool).await?;
    for topic in topics {
        listener.listen(topic).await?;
    }
    Ok(listener)
}

#[cfg(feature = "postgres")]
/// Reports disconnects to the reconnect loop so task consumers can rescan afterward.
async fn receive_notifications<F>(
    listener: &mut crate::db::sqlx::postgres::PgListener,
    receive: &mut F,
    shutdown: &CancellationNotifier,
) -> NotifyLoop
where
    F: FnMut(Option<Notify>) -> bool + Send,
{
    loop {
        tokio::select! {
            _ = shutdown.notified() => return NotifyLoop::Shutdown,
            notif = listener.try_recv() => match notif {
                Ok(Some(notification)) => {
                    if !receive(Some(Notify { channel: notification.channel().into(), payload: notification.payload().into() })) {
                        return NotifyLoop::Closed;
                    }
                }
                Ok(None) => return NotifyLoop::Disconnected,
                Err(err) => {
                    tracing::warn!("notification listener receive failed: {}", err);
                    return NotifyLoop::Disconnected;
                }
            },
        }
    }
}

#[cfg(feature = "postgres")]
fn send_notification(sender: &mpsc::Sender<Notify>, notify: Notify) -> bool {
    match sender.try_send(notify) {
        Ok(()) => true,
        Err(TrySendError::Full(notify)) => {
            tracing::warn!(
                channel = notify.channel.as_str(),
                "notification dropped because internal notification queue is full"
            );
            true
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

/// Starts an owned listener only after subscriptions succeed, using a separate session pool.
/// `None` tells the consumer to rescan after initial subscription or reconnection.
#[cfg(feature = "postgres")]
pub(crate) async fn listen_owned<F>(
    pool: &crate::db::Pool,
    topics: Vec<String>,
    receive: F,
    shutdown: CancellationNotifier,
) -> Result<tokio::task::JoinHandle<()>, DbError>
where
    F: FnMut(Option<Notify>) -> bool + Send + 'static,
{
    let pool = crate::db::sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy_with((*pool.connect_options()).clone());
    let listener = connect_listener(&pool, &topics).await?;
    Ok(tokio::spawn(run_notify_listener(
        pool,
        topics,
        Some(listener),
        receive,
        std::time::Duration::from_millis(100),
        std::time::Duration::from_secs(5),
        shutdown,
    )))
}

#[cfg(feature = "postgres")]
fn next_reconnect_backoff(
    current: std::time::Duration,
    max: std::time::Duration,
) -> std::time::Duration {
    let doubled = current.saturating_mul(2).min(max);
    let jitter_window = (doubled.as_millis() / 4).max(1) as u64;
    let jitter = rand::random::<u64>() % jitter_window;
    doubled
        .saturating_add(std::time::Duration::from_millis(jitter))
        .min(max)
}

#[cfg(feature = "postgres")]
async fn sleep_reconnect_backoff(
    shutdown: &CancellationNotifier,
    backoff: std::time::Duration,
) -> bool {
    tokio::select! {
        _ = shutdown.notified() => false,
        _ = tokio::time::sleep(backoff) => true,
    }
}
