# Channels

Vyuh channels deliver signal payloads to clients over WebSocket, SSE, or long
polling. Use them when browser or machine clients need live updates from the
same typed events that already drive in-process signal handlers.

Channels are not durable work queues. Use [Tasks](tasks.md) for durable
background work and [Signals](signals.md) for in-process handler fanout.

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
        .user(UserKey::new(user.key.clone())?)
        .deliver::<TaskUpdated>()
        .deliver_if::<NotificationCreated>(move |msg| msg.user_key == user.key);

    sub.attach(stream).allow(WS | SSE | POLL).await
}
```

If `allow(...)` is omitted, all transports are allowed:

```rust
sub.attach(stream).await
```

## Publishing

There is no channel-specific publish API for normal application events. Emit a
signal:

```rust
site.signals().emit(TaskUpdated { task_id: 42 })?;
```

The emitted payload is delivered to registered signal handlers and to channel
subscribers whose user stream accepts that payload type.

## Delivery Rules

Delivery rules are user-scoped:

- `deliver::<T>()` sends every emitted `T` to that user stream.
- `deliver_if::<T>(predicate)` sends only payloads accepted by the predicate.
- Multiple client connections for the same user share delivery rules.
- Re-registering a `UserKey` replaces that user's older delivery rules.
- Predicates run on the server before the message is sent or retained.

Authorization belongs in the route before attaching the stream. Do not rely on
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

The local backend keeps recent events in memory. It is fast and single-process.
It is not durable and does not deliver across multiple server processes.

Subscribers have bounded queues. Slow clients are disconnected, so signal
emission does not wait indefinitely on client consumption.

## Configuration

```rust
use vyuh::prelude::*;
use vyuh::channels::ChannelConf;

let conf = SiteConf::default().channels(ChannelConf {
    retention_events: 20_000,
    subscriber_queue: 512,
    ..ChannelConf::default()
});
```

Important limits include `retention_events`, `max_message_bytes`,
`replay_limit`, `subscriber_queue`, and `long_poll_timeout_ms`.

## Custom Backends

`LocalChannelBackend` is the default implementation. The public backend trait is
still shaped around bounded replay, opaque cursors, non-blocking publish, and
explicit validation so Redis-like backends can later provide cross-process
delivery and replay storage.

Predicate closures are process-local. External stores should retain accepted
messages and cursors, not predicate code.

## Failure Modes

- invalid cursor or user key: `400`
- disallowed transport: `400`
- oversized messages: `413`
- unavailable backend: `503`
- serialization or transport failure: application error

## Current Limitations

- `LocalChannelBackend` is process-local and in-memory.
- Channels provide bounded replay, not durable delivery.
- Authorization is application-owned and belongs in route handlers.
- Predicate rules are registered by active subscriptions, not persistent config.
