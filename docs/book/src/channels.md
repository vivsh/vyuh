# Channels

Vyuh channels deliver signal payloads to clients over WebSocket, SSE, or long
polling. Use them when browser or machine clients need live updates from the
same typed events that already drive in-process signal handlers.

Channels are best-effort refresh notifications, not durable work queues. Use
[Tasks](tasks.md) for durable background work and [Signals](signals.md) for
in-process handler fanout. Prefer event payloads with an entity ID and version
hint; clients should refetch authoritative state after receiving an event.

## Mental Model

| Need | Use |
| --- | --- |
| Client-facing live signal delivery | `channels` |
| In-process application event fanout | `signals` |
| Scheduled or external event sources | `emitters` |
| Durable retryable work | `tasks` |
| Site-lifetime state and workers | `services` |

Applications emit typed events with `site.signals().emit(T)`. Channel
subscribers declare which signal payload types a user should receive.

## Subscribing

Routes extract `Subscriber` and `Channels`. `Subscriber` negotiates WebSocket,
SSE, or long polling from the request; application handlers do not need Axum
upgrade extractors.

```rust
use schemars::JsonSchema;
use vyuh::auth::AuthUser;
use vyuh::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct TaskUpdated {
    task_id: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct NotificationCreated {
    user_key: String,
    message: String,
}

async fn subscribe(
    user: AuthUser,
    sub: Subscriber,
    channels: Channels,
) -> Result<ChannelResponse, Error> {
    let stream = channels
        .user(UserKey::new(user.subject())?)
        .channel(ChannelKey::new("events")?)
        .deliver::<TaskUpdated>()
        .deliver_if::<NotificationCreated>(move |msg| msg.user_key == user.subject());

    sub.attach(stream).allow(WS | SSE | POLL).await
}
```

Because this route extracts `AuthUser`, register it in a bundle with an
application `Audience`; see [Authentication](auth.md#audiences).

If `allow(...)` is omitted, all transports are allowed:

```rust
sub.attach(stream).await
```

## Beacon Endpoints

Use `Beacon` when a live endpoint is entirely a policy over typed signals. A
Beacon is an authenticated `GET` route: it inherits its bundle audience, tags,
prefix, and slash policy, then negotiates WebSocket, SSE, or polling exactly as
`Subscriber` does. Signals remain the only publish path.

```rust
use std::time::Duration;
use schemars::JsonSchema;
use vyuh::auth::Audience;
use vyuh::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct NoteChanged {
    owner: String,
    note_id: i64,
}

#[bundles::beacon(path = "/live", modes = [ws, sse, poll])]
fn live() -> Beacon {
    Beacon::builder()
        .rule::<NoteChanged>(["notes:read"])
        .debounce::<NoteChanged>(Duration::from_millis(150))
        .build()
}

let bundle = bundles::bundle! { live }
    .with_conf(bundles::conf().audience(Audience::new("notes")));
```

`rule::<T>(scopes)` requires every listed scope. `rule_with::<T>(scopes,
predicate)` adds a typed `Fn(&AuthUser, &T) -> bool` check; it runs before
serialization. A user without an eligible rule receives `403`.

`debounce::<T>(duration)` is trailing-edge and keeps the latest accepted `T`
after each quiet window. Its pending state and local replay queue are shared by
sessions for the same user and Beacon endpoint, never with another endpoint or
with direct `Channels::user(...)` subscriptions.

`#[bundles::beacon]` is sugar over:

```rust
bundles::beacon(
    live(),
    BeaconConf::new("live", "/live").modes(WS | SSE | POLL),
)
```

Use the direct constructor for generated or conditional Beacon registration.

## Publishing

There is no channel-specific publish API for normal application events. Emit a
signal:

```rust
site.signals().emit(TaskUpdated { task_id: 42 })?;
```

The emitted payload is delivered to registered signal handlers and to channel
subscribers whose user stream accepts that payload type.

## Delivery Rules

Direct `Channels::user(...)` delivery rules are scoped by `(UserKey, ChannelKey)`:

- `ChannelKey` is an application-owned stable logical name, not a connection or cursor id.
- `deliver::<T>()` sends every emitted `T` to that logical channel.
- `deliver_if::<T>(predicate)` sends only payloads accepted by the predicate.
- Multiple connections for the same user and key share delivery rules and replay.
- Re-registering one `(UserKey, ChannelKey)` replaces only that key's older rules.
- Omitting `.channel(...)` fails when the stream is attached; there is no implicit default channel.
- Predicates run on the server before the message is sent or retained.

Beacon derives this logical key from its finalized route name, path, and GET
method. Two Beacon routes therefore neither replace each other’s policy nor
share retained replay events. Physical sessions are internal and close
automatically when their response or receiver is dropped.

Direct-channel authorization belongs in the route before attaching the stream.
Beacon authenticates its route through the bundle audience and applies each
rule's declared scopes and optional predicate before delivery. Do not rely on
client-side filtering for private data.

## Transport Negotiation

`Subscriber` chooses a transport from the request:

- WebSocket when upgrade headers are present, or `?transport=ws`.
- SSE when `Accept: text/event-stream`, or `?transport=sse`.
- Poll when `?transport=poll`, or as the fallback.

Use `allow(WS | SSE)`, `allow(SSE)`, or another bitmask to restrict a route. A
request for a disallowed transport returns a stable bad-request error.

## JavaScript Clients

All transports deliver the same event envelope:

```json
{
  "id": 123,
  "type": "TaskUpdated",
  "data": { "task_id": 42 },
  "created_at": 1710000000
}
```

Use the returned cursor or last event id when reconnecting or polling.

### Polling

```javascript
let cursor = null;

async function pollChannels() {
  const url = new URL("/events", window.location.origin);
  url.searchParams.set("transport", "poll");
  if (cursor !== null) {
    url.searchParams.set("cursor", cursor);
  }

  const response = await fetch(url, {
    headers: { Accept: "application/json" },
    credentials: "include",
  });
  if (!response.ok) {
    throw new Error(`channel poll failed: ${response.status}`);
  }

  const body = await response.json();
  cursor = body.cursor ?? cursor;

  for (const event of body.events) {
    handleChannelEvent(event);
  }
}

async function pollLoop() {
  for (;;) {
    try {
      await pollChannels();
    } catch (error) {
      console.error(error);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

pollLoop();
```

### SSE

```javascript
let lastEventId = null;

function connectSse() {
  const url = new URL("/events", window.location.origin);
  url.searchParams.set("transport", "sse");
  if (lastEventId !== null) {
    url.searchParams.set("after", lastEventId);
  }

  const events = new EventSource(url, { withCredentials: true });

  events.onmessage = (message) => {
    const event = JSON.parse(message.data);
    lastEventId = event.id;
    handleChannelEvent(event);
  };

  events.addEventListener("TaskUpdated", (message) => {
    const event = JSON.parse(message.data);
    lastEventId = event.id;
    handleTaskUpdated(event.data);
  });

  events.onerror = () => {
    events.close();
    setTimeout(connectSse, 1000);
  };
}

connectSse();
```

### WebSocket

```javascript
let cursor = null;
let socket = null;

function connectWebSocket() {
  const url = new URL("/events", window.location.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("transport", "ws");
  if (cursor !== null) {
    url.searchParams.set("cursor", cursor);
  }

  socket = new WebSocket(url);

  socket.onmessage = (message) => {
    const event = JSON.parse(message.data);
    cursor = event.id;
    handleChannelEvent(event);
  };

  socket.onclose = () => {
    socket = null;
    setTimeout(connectWebSocket, 1000);
  };

  socket.onerror = () => {
    socket.close();
  };
}

connectWebSocket();
```

```javascript
function handleChannelEvent(event) {
  switch (event.type) {
    case "TaskUpdated":
      handleTaskUpdated(event.data);
      break;
    default:
      console.debug("unhandled channel event", event);
  }
}

function handleTaskUpdated(task) {
  console.log("task updated", task.task_id);
}
```

## Replay And Backpressure

Channels provide live delivery with bounded replay. `ChannelCursor` is opaque;
clients should pass it back unchanged as `after` or `cursor`.

The local subscription runtime keeps recent accepted events in memory. This is
a short, process-local reconnect convenience, not a delivery guarantee. It is
not durable and does not deliver across multiple server processes.

Subscribers have bounded queues. Slow clients are disconnected, so signal
emission does not wait indefinitely on client consumption.

## Configuration

```rust
use vyuh::prelude::*;
use vyuh::channels::ChannelConf;

let conf = SiteConf::default().channels(ChannelConf {
    retention_events: 20_000,
    subscriber_queue: 512,
    max_channels_per_user: 128,
    ..ChannelConf::default()
});
```

Important limits include `retention_events`, `max_message_bytes`,
`replay_limit`, `subscriber_queue`, `max_channels_per_user`, and
`long_poll_timeout_ms`.

## Future Shared Fanout

Vyuh has no shared channel backend or configuration in this release. Its
internal fanout boundary is intentionally ephemeral: a future Redis adapter
will use Pub/Sub, not Streams. An incoming event will be decoded and evaluated
against subscriptions currently attached to that node, then use that node's
ordinary debounce, retention, and transport delivery.

That mode will not provide cross-node replay, global debounce, exactly-once
delivery, or durable notification. A client connected to more than one node can
receive more than one refresh hint. Shared mode will require an explicit
application namespace at site construction.

## Failure Modes

- invalid cursor, user key, or channel key: `400`
- missing logical channel key or exceeding the per-user channel limit: `400`
- disallowed transport: `400`
- oversized messages: `400`
- unavailable local scheduler: `503`
- serialization or transport failure: application error

## Current Limitations

- Local replay is process-local and bounded; it is never cross-node replay.
- Channels provide best-effort refresh hints, not durable delivery.
- Direct-channel authorization is application-owned and belongs in route
  handlers; Beacon applies its declared scopes and predicates after route
  authentication.
- Direct-channel predicate rules are registered by active subscriptions, not
  persistent configuration.
