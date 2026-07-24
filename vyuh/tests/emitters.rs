#![cfg(feature = "postgres")]

use sqlx::PgPool;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Duration;
use vyuh::Data;
use vyuh::db::PgNotifyDbExt;
use vyuh::emitters::{self, *};

async fn create_site(pool: PgPool) -> vyuh::Site {
    let conf = vyuh::SiteConf {
        log_init: false,
        logging: vyuh::logging::LoggingConf {
            env_prefix: None,
            rules: vec![],
        },
        ..vyuh::SiteConf::from_env().unwrap()
    };
    let parts: Vec<vyuh::bundles::BundlePart> = vec![];
    let bundle = vyuh::bundles::bundle(parts);
    vyuh::Site::test(conf, bundle, pool)
        .await
        .expect("Failed to create test site")
}

#[sqlx::test]
async fn test_periodic(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct Sample;

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    async fn handler(cnt: Arc<AtomicUsize>) -> Data<Sample> {
        cnt.fetch_add(1, Ordering::SeqCst);
        Data::new(Sample)
    }

    let site = create_site(pool).await;
    let emitter = emitters::periodic(
        move |emitters::IterCount(_it): emitters::IterCount| handler(counter_clone.clone()),
        emitters::PeriodicConf {
            interval: Duration::from_millis(100),
            target: emitters::EmitTarget::Signal,
        },
    )?;

    let mut registry = EmitterRegistry::new();
    registry.register(emitter)?;

    let task_site = site.clone();
    let engine = registry.create_engine();
    let run_handle = tokio::spawn(async move { engine.run(task_site).await });

    // Wait for periodic fires (3+ expected in 350ms with 100ms intervals)
    tokio::time::sleep(Duration::from_millis(350)).await;

    let fired_count = counter.load(Ordering::SeqCst);
    assert!(
        fired_count >= 3,
        "Expected at least 3 periodic fires, got {}",
        fired_count
    );

    site.shutdown_and_wait().await;
    let _ = tokio::time::timeout(Duration::from_millis(100), run_handle).await;
    Ok(())
}

#[sqlx::test]
async fn test_pgnotify_trailing_debounce_uses_last_payload(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct NotifyData {
        raw: String,
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let payloads = Arc::new(Mutex::new(Vec::new()));
    let counter_clone = counter.clone();
    let payloads_clone = payloads.clone();

    let site = create_site(pool.clone()).await;
    let emitter = emitters::pgnotify(
        move |payload: Data<String>| {
            let cnt = counter_clone.clone();
            let seen = payloads_clone.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                seen.lock().unwrap().push(payload.to_string());
                Data::new(NotifyData {
                    raw: payload.to_string(),
                })
            }
        },
        emitters::PgNotifyConf {
            channel: "test_trailing_debounce".to_string(),
            target: emitters::EmitTarget::Signal,
            debounce: Some(emitters::DebounceConf {
                window: Duration::from_millis(100),
                mode: emitters::DebounceMode::Trailing,
            }),
        },
    )?;

    let mut registry = EmitterRegistry::new();
    registry.register(emitter)?;

    let task_site = site.clone();
    let engine = registry.create_engine();
    let run_handle = tokio::spawn(async move { engine.run(task_site).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    site.db()
        .send_pgnotify("test_trailing_debounce", "first")
        .await?;
    site.db()
        .send_pgnotify("test_trailing_debounce", "middle")
        .await?;
    site.db()
        .send_pgnotify("test_trailing_debounce", "last")
        .await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(payloads.lock().unwrap().as_slice(), ["last"]);

    site.shutdown_and_wait().await;
    let _ = tokio::time::timeout(Duration::from_millis(100), run_handle).await;
    Ok(())
}

#[sqlx::test]
async fn test_pgnotify_leading_trailing_debounce_emits_first_and_last(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct NotifyData {
        raw: String,
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let payloads = Arc::new(Mutex::new(Vec::new()));
    let counter_clone = counter.clone();
    let payloads_clone = payloads.clone();

    let site = create_site(pool.clone()).await;
    let emitter = emitters::pgnotify(
        move |payload: Data<String>| {
            let cnt = counter_clone.clone();
            let seen = payloads_clone.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                seen.lock().unwrap().push(payload.to_string());
                Data::new(NotifyData {
                    raw: payload.to_string(),
                })
            }
        },
        emitters::PgNotifyConf {
            channel: "test_leading_trailing_debounce".to_string(),
            target: emitters::EmitTarget::Signal,
            debounce: Some(emitters::DebounceConf {
                window: Duration::from_millis(100),
                mode: emitters::DebounceMode::LeadingAndTrailing,
            }),
        },
    )?;

    let mut registry = EmitterRegistry::new();
    registry.register(emitter)?;

    let task_site = site.clone();
    let engine = registry.create_engine();
    let run_handle = tokio::spawn(async move { engine.run(task_site).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    site.db()
        .send_pgnotify("test_leading_trailing_debounce", "first")
        .await?;
    site.db()
        .send_pgnotify("test_leading_trailing_debounce", "middle")
        .await?;
    site.db()
        .send_pgnotify("test_leading_trailing_debounce", "last")
        .await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(payloads.lock().unwrap().as_slice(), ["first", "last"]);

    site.shutdown_and_wait().await;
    let _ = tokio::time::timeout(Duration::from_millis(100), run_handle).await;
    Ok(())
}

#[sqlx::test]
async fn test_pgnotify_slow_handler_does_not_block_other_notifications(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct SlowData;

    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct FastData;

    let slow_counter = Arc::new(AtomicUsize::new(0));
    let fast_counter = Arc::new(AtomicUsize::new(0));
    let slow_counter_clone = slow_counter.clone();
    let fast_counter_clone = fast_counter.clone();

    let site = create_site(pool.clone()).await;
    let slow = emitters::pgnotify(
        move |_payload: Data<String>| {
            let cnt = slow_counter_clone.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                cnt.fetch_add(1, Ordering::SeqCst);
                Data::new(SlowData)
            }
        },
        emitters::PgNotifyConf {
            channel: "test_slow_pgnotify".to_string(),
            target: emitters::EmitTarget::Signal,
            debounce: None,
        },
    )?;
    let fast = emitters::pgnotify(
        move |_payload: Data<String>| {
            let cnt = fast_counter_clone.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Data::new(FastData)
            }
        },
        emitters::PgNotifyConf {
            channel: "test_fast_pgnotify".to_string(),
            target: emitters::EmitTarget::Signal,
            debounce: None,
        },
    )?;

    let mut registry = EmitterRegistry::new();
    registry.register(slow)?;
    registry.register(fast)?;

    let task_site = site.clone();
    let engine = registry.create_engine();
    let run_handle = tokio::spawn(async move { engine.run(task_site).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    site.db()
        .send_pgnotify("test_slow_pgnotify", "slow")
        .await?;
    tokio::time::sleep(Duration::from_millis(25)).await;
    site.db()
        .send_pgnotify("test_fast_pgnotify", "fast")
        .await?;

    assert!(
        wait_for_count(&fast_counter, 1, Duration::from_millis(150)).await,
        "fast pgnotify handler was blocked by slow handler"
    );
    assert_eq!(slow_counter.load(Ordering::SeqCst), 0);
    assert!(wait_for_count(&slow_counter, 1, Duration::from_millis(400)).await);

    site.shutdown_and_wait().await;
    let _ = tokio::time::timeout(Duration::from_millis(100), run_handle).await;
    Ok(())
}

#[sqlx::test]
async fn test_cron(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct CronData;

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    async fn handler(cnt: Arc<AtomicUsize>) -> Data<CronData> {
        cnt.fetch_add(1, Ordering::SeqCst);
        Data::new(CronData)
    }

    let site = create_site(pool).await;
    let emitter = emitters::cron(
        move || handler(counter_clone.clone()),
        emitters::CronConf {
            expr: "* * * * * *".into(), // Every second
            target: emitters::EmitTarget::Signal,
        },
    )?;

    let mut registry = EmitterRegistry::new();
    registry.register(emitter)?;

    let task_site = site.clone();
    let engine = registry.create_engine();
    let run_handle = tokio::spawn(async move { engine.run(task_site).await });

    // Wait for cron fires (2+ expected in 2.5 seconds)
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let fired_count = counter.load(Ordering::SeqCst);
    assert!(
        fired_count >= 2,
        "Expected at least 2 cron fires, got {}",
        fired_count
    );

    site.shutdown_and_wait().await;
    let _ = tokio::time::timeout(Duration::from_millis(100), run_handle).await;
    Ok(())
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout {
        if counter.load(Ordering::SeqCst) >= expected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    counter.load(Ordering::SeqCst) >= expected
}

#[sqlx::test]
async fn test_pgnotify_debounce_still_postpones_periodic_fallback(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct NotifyData;

    let periodic_counter = Arc::new(AtomicUsize::new(0));
    let pgnotify_counter = Arc::new(AtomicUsize::new(0));
    let periodic_counter_clone = periodic_counter.clone();
    let pgnotify_counter_clone = pgnotify_counter.clone();

    let site = create_site(pool.clone()).await;
    let periodic = emitters::periodic(
        move || {
            let cnt = periodic_counter_clone.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Data::new(NotifyData)
            }
        },
        emitters::PeriodicConf {
            interval: Duration::from_millis(200),
            target: emitters::EmitTarget::Signal,
        },
    )?;
    let pgnotify = emitters::pgnotify(
        move |_payload: Data<String>| {
            let cnt = pgnotify_counter_clone.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Data::new(NotifyData)
            }
        },
        emitters::PgNotifyConf {
            channel: "test_debounce_periodic_fallback".to_string(),
            target: emitters::EmitTarget::Signal,
            debounce: Some(emitters::DebounceConf {
                window: Duration::from_millis(100),
                mode: emitters::DebounceMode::Trailing,
            }),
        },
    )?;

    let mut registry = EmitterRegistry::new();
    registry.register(periodic)?;
    registry.register(pgnotify)?;

    let task_site = site.clone();
    let engine = registry.create_engine();
    let run_handle = tokio::spawn(async move { engine.run(task_site).await });

    assert!(wait_for_count(&periodic_counter, 1, Duration::from_millis(250)).await);
    let initial_periodic_count = periodic_counter.load(Ordering::SeqCst);

    site.db()
        .send_pgnotify("test_debounce_periodic_fallback", "first")
        .await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    site.db()
        .send_pgnotify("test_debounce_periodic_fallback", "middle")
        .await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    site.db()
        .send_pgnotify("test_debounce_periodic_fallback", "last")
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        periodic_counter.load(Ordering::SeqCst),
        initial_periodic_count,
        "periodic fallback fired during an active pgnotify burst"
    );
    assert!(wait_for_count(&pgnotify_counter, 1, Duration::from_millis(250)).await);
    assert!(
        wait_for_count(
            &periodic_counter,
            initial_periodic_count + 1,
            Duration::from_millis(350),
        )
        .await
    );

    site.shutdown_and_wait().await;
    let _ = tokio::time::timeout(Duration::from_millis(100), run_handle).await;
    Ok(())
}

#[sqlx::test]
async fn test_pgnotify(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    struct NotifyData;

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let site = create_site(pool.clone()).await;
    let emitter = emitters::pgnotify(
        move |_s: Data<String>| {
            let cnt = counter_clone.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Data::new(NotifyData)
            }
        },
        emitters::PgNotifyConf {
            channel: "test_channel".to_string(),
            target: emitters::EmitTarget::Signal,
            debounce: None,
        },
    )?;

    let mut registry = EmitterRegistry::new();
    registry.register(emitter)?;

    let task_site = site.clone();
    let engine = registry.create_engine();
    let run_handle = tokio::spawn(async move { engine.run(task_site).await });

    // Wait for notifications to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    for _ in 0..3 {
        site.db().send_pgnotify("test_channel", "").await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let fired_count = counter.load(Ordering::SeqCst);
    assert!(
        fired_count >= 3,
        "Expected at least 3 pgnotify fires, got {}",
        fired_count
    );

    site.shutdown_and_wait().await;
    let _ = tokio::time::timeout(Duration::from_millis(100), run_handle).await;
    Ok(())
}
