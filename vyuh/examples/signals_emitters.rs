//! End-to-end signals and emitters demo.

use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct UserEvent {
    user_id: i64,
    action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct PeriodicHeartbeat {
    tick: usize,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct CronHeartbeat {
    tick: usize,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct DirectHeartbeat {
    tick: usize,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct DatabaseNotice {
    payload: String,
    seen: usize,
}

#[bundles::signal]
async fn audit_user_event(Data(event): Data<UserEvent>) {
    println!(
        "audit signal: user={} action={}",
        event.user_id, event.action
    );
}

#[bundles::signal]
async fn metrics_user_event(Data(event): Data<UserEvent>) {
    println!("metrics signal: counted {}", event.action);
}

#[bundles::signal]
async fn handle_periodic(Data(event): Data<PeriodicHeartbeat>) {
    println!(
        "periodic emitter -> signal: {} #{}",
        event.source, event.tick
    );
}

#[bundles::signal]
async fn handle_cron(Data(event): Data<CronHeartbeat>) {
    println!("cron emitter -> signal: {} #{}", event.source, event.tick);
}

#[bundles::signal]
async fn handle_direct(Data(event): Data<DirectHeartbeat>) {
    println!("direct emitter -> signal: {} #{}", event.source, event.tick);
}

#[bundles::signal]
async fn handle_database_notice(Data(event): Data<DatabaseNotice>) {
    println!(
        "pgnotify emitter -> signal: payload='{}' seen={}",
        event.payload, event.seen
    );
}

// Macro registration is the common static path for application code.
#[bundles::periodic(millis = 500)]
async fn periodic_heartbeat(
    vyuh::emitters::IterCount(tick): vyuh::emitters::IterCount,
) -> Data<PeriodicHeartbeat> {
    Data::new(PeriodicHeartbeat {
        tick,
        source: "macro-periodic".to_string(),
    })
}

// Cron uses the same target path as periodic emitters: handler -> Data<T> -> signal.
#[bundles::cron(expr = "0/1 * * * * * *")]
async fn cron_heartbeat(
    vyuh::emitters::IterCount(tick): vyuh::emitters::IterCount,
) -> Data<CronHeartbeat> {
    Data::new(CronHeartbeat {
        tick,
        source: "macro-cron".to_string(),
    })
}

// PgNotify handlers receive the raw notification payload as Data<String>.
#[cfg(feature = "postgres")]
#[bundles::pgnotify(
    channel = "vyuh_demo_events",
    debounce = "trailing",
    debounce_millis = 100
)]
async fn database_notice(
    vyuh::emitters::IterCount(seen): vyuh::emitters::IterCount,
    Data(payload): Data<String>,
) -> Data<DatabaseNotice> {
    Data::new(DatabaseNotice {
        payload: payload.as_ref().clone(),
        seen,
    })
}

async fn direct_heartbeat(
    vyuh::emitters::IterCount(tick): vyuh::emitters::IterCount,
) -> Data<DirectHeartbeat> {
    Data::new(DirectHeartbeat {
        tick,
        source: "direct-periodic".to_string(),
    })
}

fn emitter_bundle() -> vyuh::bundles::Bundle {
    let direct = bundles::periodic::<DirectHeartbeat, _, _>(
        direct_heartbeat,
        bundles::PeriodicConf::new(tokio::time::Duration::from_millis(700)),
    );

    bundles::bundle([direct])
}

#[cfg(feature = "postgres")]
fn pgnotify_bundle() -> vyuh::bundles::Bundle {
    bundles::bundle! {
        database_notice,
    }
}

#[cfg(not(feature = "postgres"))]
fn pgnotify_bundle() -> vyuh::bundles::Bundle {
    bundles::bundle([])
}

#[cfg(feature = "postgres")]
async fn send_pg_notifications(site: &Site) -> Result<(), SiteError> {
    site.db()
        .send_pgnotify("vyuh_demo_events", "first database payload")
        .await?;
    site.db()
        .send_pgnotify("vyuh_demo_events", "second database payload")
        .await?;
    Ok(())
}

async fn run_demo(site: &Site) -> Result<(), SiteError> {
    site.signals().emit(UserEvent {
        user_id: 42,
        action: "registered".to_string(),
    })?;

    #[cfg(feature = "postgres")]
    send_pg_notifications(site).await?;

    // Give the runtime long enough to run the immediate signal, delayed signal,
    // first cron tick, periodic ticks, and optional PgNotify debounce window.
    tokio::time::sleep(tokio::time::Duration::from_millis(1_600)).await;
    site.shutdown_and_wait().await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), SiteError> {
    let bundle = bundles::bundle! {
        audit_user_event,
        metrics_user_event,
        handle_periodic,
        handle_cron,
        handle_direct,
        handle_database_notice,
        periodic_heartbeat,
        cron_heartbeat,
    }
    .merge(emitter_bundle());

    let bundle = bundle.merge(pgnotify_bundle());

    let site = Site::build(SiteConf::default(), bundle).await?;
    run_demo(&site).await
}
