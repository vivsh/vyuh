# Signals

Signals are typed, in-process notifications for decoupling application code.
They are fire-and-forget: Vyuh does not guarantee delivery, ordering,
durability, retries, or handler completion. Use tasks when work must be durable
or observable as a unit of background execution.

Use signals for lightweight local fanout after application events. Channels can
also consume emitted signal payloads for client-facing live delivery over
WebSocket, SSE, or long polling. Do not use signals for durable queues,
scheduled polling, or work that must be retried.

## Overview

A signal is dispatched by Rust data type. Every handler registered for that data
type is eligible to run when the signal is emitted.

Signals are useful for lightweight local fan-out:

- update an in-memory projection after a route succeeds,
- notify several local handlers after an emitter produces data,
- split non-critical side effects out of request handlers.

Signals are not a queue. If the process exits, pending emitted signals can be
lost.

## Macro Sugar And Direct API

`#[bundles::signal]` is sugar over direct bundle registration with
`bundles::signal(handler, SignalConf::default())`.

Use the macro for ordinary handlers:

```rust
use vyuh::prelude::*;

#[bundles::signal]
async fn index_note_change(Data(event): Data<NoteChanged>) -> Result<(), vyuh::Error> {
    println!("note {} changed", event.id);
    Ok(())
}

let bundle = bundles::bundle! {
    index_note_change,
};
```

The equivalent direct registration is:

```rust
let bundle = bundles::bundle([bundles::signal::<NoteChanged, _, _>(
    index_note_change,
    signals::SignalConf::default(),
)]);
```

The macro does not add a unique runtime capability. Prefer the direct API for
generated, conditional, or table-driven registration.

## Data

Signal data types must implement Vyuh's data bounds: they are serializable,
deserializable, schema-capable, sendable, syncable, and `'static`.

```rust
use schemars::JsonSchema;

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
struct NoteChanged {
    id: i64,
}
```

Handlers extract signal data with `Data<T>`. The data extractor must be the last
handler argument.

```rust
use vyuh::prelude::*;

#[bundles::signal]
async fn audit_note_change(site: Site, Data(event): Data<NoteChanged>) -> Result<(), vyuh::Error> {
    tracing::info!("note {} changed in {:?}", event.id, site.project_dir());
    Ok(())
}
```

`Site` and other site-derived extractors can appear before `Data<T>`.
Handler logic should return `vyuh::Error` when it can fail. Vyuh logs handler
errors and continues dispatching later handlers.

## Emitting Signals

Application code emits signals through the site-scoped signal client:

```rust
site.signals().emit(NoteChanged { id: 42 })?;
```

`emit` queues dispatch on the site runtime and returns. It is fire-and-forget:
emitting a payload with no handlers or channel subscribers is still `Ok(())`.
Handler errors are logged and are not returned to the emitter.

## Delayed Signals

`SignalClient` intentionally has no delayed emit API. Use emitters for scheduled
event production, and use tasks when delayed work must be durable, observable,
or retryable.

## Bundles

Signal handlers are registered as `BundlePart` values. Macro signal handlers
and direct `bundles::signal(...)` registration produce the same kind of bundle
part.

When bundles are merged, handlers for the same data type are appended. A single
emitted value can therefore fan out to multiple handlers.

See [Bundles](bundles.md) for `BundlePart`, `bundle!`, cross-module bundle
organization, validation, composition behavior, and the general patch API.

## Emitters

Emitters can produce typed data and target signals. Cron, periodic, and
notification-driven emitters use the same signal dispatch path. Signals remain
fire-and-forget even when the source is an emitter.

## Examples

Run the signal and emitter example:

```sh
cargo run -p vyuh --example signals_emitters
```

`signals_emitters` demonstrates macro signal handlers, direct signal emission,
multiple handlers for a payload, and emitters feeding the same signal path.

## Failure Modes

- Handler failure: the failure is logged and dispatch continues to later
  handlers.
- Process shutdown: pending emitted signals can be cancelled or lost.
- Data type mismatch: usually indicates manually constructed data was
  dispatched with the wrong type.

## Best Practices

- Keep signal handlers small and non-critical.
- Use stable data structs instead of primitive values for public subsystem
  boundaries.
- Use tasks for durable work, retries, delayed persistence, or continuation state.
- Return `vyuh::Error` for application failures inside handlers; keep
  `SignalError` for signal machinery.
- Treat scheduling as a convenience timer, not as a queue.
- Prefer direct registration when signal handlers are generated or conditional.

## Current Limitations

- Signals are in-process only.
- Dispatch is type-based, not name-based or topic-based.
- There is no delivery acknowledgement.
- There is no ordering guarantee across handlers or submissions.
- Debounce is not part of the v0 signal surface.
