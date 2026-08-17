use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use super::*;
use crate::notifiers::CancellationNotifier;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct TestNotice {
    user_id: i64,
    text: String,
}

/// Verifies direct channels require an explicit logical identity before opening.
#[tokio::test]
async fn direct_channel_requires_key() -> Result<(), Box<dyn std::error::Error>> {
    let channels = Channels::new(SubscriptionRuntime::default());
    let stream = channels.user(UserKey::new("42")?).deliver::<TestNotice>();
    assert!(matches!(
        channels.open_stream(stream, None).await,
        Err(ChannelError::MissingChannelKey)
    ));
    Ok(())
}

/// Verifies two logical keys for one user retain policies and replay independently.
#[tokio::test]
async fn direct_keys_are_policy_and_replay_isolated() -> Result<(), Box<dyn std::error::Error>> {
    let channels = Channels::new(SubscriptionRuntime::default());
    let user = UserKey::new("42")?;
    let first = ChannelKey::new("first")?;
    let second = ChannelKey::new("second")?;
    let mut first_open = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(first)
                .deliver_if::<TestNotice, _>(|event| event.user_id == 42),
            None,
        )
        .await?;
    let mut second_open = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(second.clone())
                .deliver_if::<TestNotice, _>(|event| event.user_id == 7),
            None,
        )
        .await?;
    publish(&channels, 42, "first").await?;
    assert_eq!(next_text(&mut first_open).await?, "first");
    assert!(no_event(&mut second_open).await);
    publish(&channels, 7, "second").await?;
    assert_eq!(next_text(&mut second_open).await?, "second");

    let replay = channels
        .open_stream(
            channels
                .user(user)
                .channel(second)
                .deliver_if::<TestNotice, _>(|event| event.user_id == 7),
            None,
        )
        .await?;
    assert_eq!(replay.replay.len(), 1);
    assert_eq!(replay.replay[0].data["text"], serde_json::json!("second"));
    Ok(())
}

/// Verifies replacing one logical channel policy leaves a sibling channel unchanged.
#[tokio::test]
async fn policy_replacement_is_limited_to_one_key() -> Result<(), Box<dyn std::error::Error>> {
    let channels = Channels::new(SubscriptionRuntime::default());
    let user = UserKey::new("42")?;
    let first = ChannelKey::new("first")?;
    let second = ChannelKey::new("second")?;
    let _first = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(first.clone())
                .deliver::<TestNotice>(),
            None,
        )
        .await?;
    let mut second_open = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(second)
                .deliver::<TestNotice>(),
            None,
        )
        .await?;
    let _replacement = channels
        .open_stream(
            channels
                .user(user)
                .channel(first)
                .deliver_if::<TestNotice, _>(|event| event.user_id == 7),
            None,
        )
        .await?;
    publish(&channels, 42, "sibling").await?;
    assert_eq!(next_text(&mut second_open).await?, "sibling");
    Ok(())
}

/// Verifies dropping one receiver removes only that physical session.
#[tokio::test]
async fn receiver_drop_removes_only_its_session() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = SubscriptionRuntime::default();
    let channels = Channels::new(runtime.clone());
    let user = UserKey::new("42")?;
    let key = ChannelKey::new("events")?;
    let first = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(key.clone())
                .deliver::<TestNotice>(),
            None,
        )
        .await?;
    let mut second = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(key.clone())
                .deliver::<TestNotice>(),
            None,
        )
        .await?;
    assert_eq!(runtime.session_count(&user, &key), 2);
    drop(first);
    assert_eq!(runtime.session_count(&user, &key), 1);
    publish(&channels, 42, "remaining").await?;
    assert_eq!(next_text(&mut second).await?, "remaining");
    Ok(())
}

/// Verifies a completed long-poll drops its physical session immediately.
#[tokio::test]
async fn completed_long_poll_removes_its_session() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = SubscriptionRuntime::default();
    let channels = Channels::new(runtime.clone());
    let user = UserKey::new("42")?;
    let key = ChannelKey::new("events")?;
    let open = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(key.clone())
                .deliver::<TestNotice>(),
            None,
        )
        .await?;
    assert_eq!(runtime.session_count(&user, &key), 1);
    let _events =
        ChannelLongPoll::wait(open.receiver, Duration::ZERO, CancellationNotifier::new()).await;
    assert_eq!(runtime.session_count(&user, &key), 0);
    Ok(())
}

/// Verifies a response dropped before SSE streaming releases its session lease.
#[tokio::test]
async fn dropped_sse_response_removes_its_session() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = SubscriptionRuntime::default();
    let channels = Channels::new(runtime.clone());
    let user = UserKey::new("42")?;
    let key = ChannelKey::new("events")?;
    let open = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(key.clone())
                .deliver::<TestNotice>(),
            None,
        )
        .await?;
    let response = open.into_sse(CancellationNotifier::new());
    assert_eq!(runtime.session_count(&user, &key), 1);
    drop(response);
    assert_eq!(runtime.session_count(&user, &key), 0);
    Ok(())
}

/// Verifies same logical names never share state across authenticated users.
#[tokio::test]
async fn same_key_is_isolated_between_users() -> Result<(), Box<dyn std::error::Error>> {
    let channels = Channels::new(SubscriptionRuntime::default());
    let key = ChannelKey::new("events")?;
    let mut first = channels
        .open_stream(
            channels
                .user(UserKey::new("one")?)
                .channel(key.clone())
                .deliver_if::<TestNotice, _>(|event| event.user_id == 1),
            None,
        )
        .await?;
    let mut second = channels
        .open_stream(
            channels
                .user(UserKey::new("two")?)
                .channel(key)
                .deliver_if::<TestNotice, _>(|event| event.user_id == 2),
            None,
        )
        .await?;
    publish(&channels, 1, "one").await?;
    assert_eq!(next_text(&mut first).await?, "one");
    assert!(no_event(&mut second).await);
    Ok(())
}

/// Verifies channel keys and the per-user logical-channel bound fail predictably.
#[tokio::test]
async fn keys_and_channel_limit_are_validated() -> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        ChannelKey::new(""),
        Err(ChannelError::InvalidKey(_))
    ));
    let runtime = SubscriptionRuntime::new(
        ChannelConf {
            max_channels_per_user: 1,
            ..ChannelConf::default()
        },
        BTreeMap::new(),
    );
    let channels = Channels::new(runtime);
    let user = UserKey::new("42")?;
    let _first = channels
        .open_stream(
            channels
                .user(user.clone())
                .channel(ChannelKey::new("one")?)
                .deliver::<TestNotice>(),
            None,
        )
        .await?;
    let second = channels
        .open_stream(
            channels
                .user(user)
                .channel(ChannelKey::new("two")?)
                .deliver::<TestNotice>(),
            None,
        )
        .await;
    assert!(matches!(
        second,
        Err(ChannelError::ChannelLimitExceeded { max: 1 })
    ));
    Ok(())
}

/// Verifies debounce is scoped by user, logical channel, and signal type.
#[tokio::test]
async fn debounce_is_isolated_by_logical_channel() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = SubscriptionRuntime::default();
    let channels = Channels::new(runtime.clone());
    let shutdown = CancellationNotifier::new();
    let scheduler = tokio::spawn({
        let shutdown = shutdown.child();
        async move { runtime.run_debounce(shutdown).await }
    });
    let mut first = channels
        .open_stream(
            channels
                .user(UserKey::new("42")?)
                .channel(ChannelKey::new("one")?)
                .deliver_spec(debounced()),
            None,
        )
        .await?;
    let mut second = channels
        .open_stream(
            channels
                .user(UserKey::new("42")?)
                .channel(ChannelKey::new("two")?)
                .deliver_if::<TestNotice, _>(|event| event.user_id == 7),
            None,
        )
        .await?;
    publish(&channels, 42, "first").await?;
    publish(&channels, 42, "latest").await?;
    assert_eq!(next_text(&mut first).await?, "latest");
    assert!(no_event(&mut second).await);
    shutdown.notify_waiters();
    scheduler.await?;
    Ok(())
}

/// Verifies final Beacon keys are derived only for explicitly registered Beacon operations.
#[test]
fn beacon_keys_require_explicit_operation_markers() {
    let mut beacon = crate::callables::Operation::from_api_doc("live", "/api/live");
    beacon.kind = crate::callables::OperationKind::Route;
    beacon.methods = crate::routes::Methods::GET;
    let mut regular = crate::callables::Operation::from_api_doc("status", "/api/status");
    regular.kind = crate::callables::OperationKind::Route;
    regular.methods = crate::routes::Methods::GET;
    let beacon_id = beacon.id;
    let regular_id = regular.id;
    let operations = BTreeMap::from([(beacon_id, beacon), (regular_id, regular)]);
    let keys = beacon_keys(&operations, &BTreeSet::from([beacon_id]));
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys.get(&beacon_id),
        Some(&ChannelKey::beacon("live", "/api/live"))
    );
    assert!(!keys.contains_key(&regular_id));
}

/// Publishes one signal payload used by the local runtime tests.
async fn publish(channels: &Channels, user_id: i64, text: &str) -> Result<(), ChannelError> {
    channels
        .publish_signal(&TestNotice {
            user_id,
            text: text.to_string(),
        })
        .await
}

/// Receives an event and returns its text field.
async fn next_text(open: &mut OpenStream) -> Result<String, Box<dyn std::error::Error>> {
    let event = tokio::time::timeout(Duration::from_millis(100), open.receiver.recv())
        .await?
        .ok_or("channel closed before event arrived")?;
    event.data["text"]
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| "event text is missing".into())
}

/// Checks that a receiver does not yield an event within the short test window.
async fn no_event(open: &mut OpenStream) -> bool {
    tokio::time::timeout(Duration::from_millis(25), open.receiver.recv())
        .await
        .is_err()
}

/// Builds a trailing debounced direct-channel rule for the test signal.
fn debounced() -> types::DeliverySpec {
    let mut rule = delivery::<TestNotice, _>(|_| true);
    rule.debounce = Some(Duration::from_millis(10));
    rule
}
