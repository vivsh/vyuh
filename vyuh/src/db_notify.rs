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
            sender,
            reconnect_initial,
            reconnect_max,
            shutdown,
        )
        .await;
    });
}

#[cfg(feature = "postgres")]
async fn run_notify_listener(
    pool: crate::db::Pool,
    topics: Vec<String>,
    sender: mpsc::Sender<Notify>,
    reconnect_initial: std::time::Duration,
    reconnect_max: std::time::Duration,
    shutdown: CancellationNotifier,
) {
    let mut backoff = reconnect_initial;
    loop {
        match listen_until_disconnect(&pool, &topics, &sender, &shutdown).await {
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
async fn listen_until_disconnect(
    pool: &crate::db::Pool,
    topics: &[String],
    sender: &mpsc::Sender<Notify>,
    shutdown: &CancellationNotifier,
) -> NotifyLoop {
    let mut listener = match crate::db::sqlx::postgres::PgListener::connect_with(pool).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::warn!("notification listener connect failed: {}", err);
            return NotifyLoop::Disconnected;
        }
    };
    if !listen_topics(&mut listener, topics).await {
        return NotifyLoop::Disconnected;
    }
    receive_notifications(&mut listener, sender, shutdown).await
}

#[cfg(feature = "postgres")]
async fn listen_topics(
    listener: &mut crate::db::sqlx::postgres::PgListener,
    topics: &[String],
) -> bool {
    for topic in topics {
        if let Err(err) = listener.listen(topic).await {
            tracing::warn!(
                channel = topic.as_str(),
                "notification listener LISTEN failed: {}",
                err
            );
            return false;
        }
    }
    true
}

#[cfg(feature = "postgres")]
async fn receive_notifications(
    listener: &mut crate::db::sqlx::postgres::PgListener,
    sender: &mpsc::Sender<Notify>,
    shutdown: &CancellationNotifier,
) -> NotifyLoop {
    loop {
        tokio::select! {
            _ = shutdown.notified() => return NotifyLoop::Shutdown,
            notif = listener.recv() => match notif {
                Ok(notification) => {
                    if !send_notification(sender, notification) {
                        return NotifyLoop::Closed;
                    }
                }
                Err(err) => {
                    tracing::warn!("notification listener receive failed: {}", err);
                    return NotifyLoop::Disconnected;
                }
            },
        }
    }
}

#[cfg(feature = "postgres")]
fn send_notification(
    sender: &mpsc::Sender<Notify>,
    notification: crate::db::sqlx::postgres::PgNotification,
) -> bool {
    let notify = Notify {
        channel: notification.channel().into(),
        payload: notification.payload().into(),
    };
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
