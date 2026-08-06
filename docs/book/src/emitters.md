# Emitters

Emitters are typed in-process event sources. They run on the site runtime,
produce `Data<T>` values, and dispatch that data to another subsystem.
The default target is signals. Cron and periodic emitters can instead submit
their produced data through the existing durable task runtime.

Emitters start only when Vyuh serves the site. Commands may inspect and use the
same site, but they never start cron, periodic, or PgNotify sources implicitly.

Signal emitters are not durable queues. Missed cron or periodic ticks are not
replayed, Postgres notifications are not persisted by Vyuh, and handler
failures are logged rather than retried. Task-targeted cron and periodic
emitters use one durable schedule cursor and submit a task atomically; they
perform one coalesced recovery submission after a restart.

Use emitters to turn schedules or external notifications into typed `Data<T>`.
Do not use emitters as durable schedulers, queues, or client-facing pub/sub.

Emitter handler execution is isolated from the engine loop and bounded by
`EmitterConf::max_in_flight_handlers`. When that limit is reached, new emitter
handler invocations are skipped and logged instead of blocking timers,
debounce deadlines, or PgNotify receives.

## Overview

Vyuh has three public emitter sources:

- `cron`: produce data from a cron schedule.
- `periodic`: produce data at a fixed interval.
- `pgnotify`: produce data from a Postgres `LISTEN`/`NOTIFY` channel.

Emitter handlers return `Data<T>`. With the default signal target, the
emitted data type `T` is offered to registered signal handlers and channel
subscribers. A value with no consumers is ignored.
Handlers that can fail should return `Result<Data<T>, vyuh::Error>`.

## Macro Sugar And Direct API

Emitter macros are sugar over direct bundle registration APIs:

- `#[bundles::cron]` maps to `bundles::cron(handler, CronConf)`.
- `#[bundles::periodic]` maps to `bundles::periodic(handler, PeriodicConf)`.
- `#[bundles::pgnotify]` maps to `bundles::pgnotify(handler, PgNotifyConf)`.

Use the macro for ordinary static emitters:

```rust
#[bundles::periodic(secs = 30)]
async fn publish_heartbeat(IterCount(count): IterCount) -> Data<Heartbeat> {
    Data::new(Heartbeat { count })
}
```

The equivalent direct registration is:

```rust
let part = bundles::periodic::<Heartbeat, _, _>(
    publish_heartbeat,
    emitters::PeriodicConf::new(std::time::Duration::from_secs(30)),
);
```

The macro path does not add a unique runtime capability. Prefer direct
registration when emitters are generated, conditional, or table-driven.

## Handler Signatures

Emitter handlers can extract `Site`, `IterCount`, and `IterInstant` before
returning `Data<T>`.

```rust
#[bundles::periodic(secs = 60)]
async fn publish_minute(site: Site, IterCount(count): IterCount) -> Result<Data<MinuteTick>, vyuh::Error> {
    Ok(Data::new(MinuteTick {
        count,
        project: site.project_dir().display().to_string(),
    }))
}
```

`IterCount` is the number of times that emitter work item has fired. It starts
at `0`. `IterInstant` is the previous fire time, or `None` for the first run.

## Cron

Cron emitters use the `cron` crate schedule syntax. Macro cron expressions are
parsed at compile time.

```rust
#[bundles::cron(expr = "0 0 0 * * *")]
async fn publish_daily() -> Data<DailyTick> {
    Data::new(DailyTick)
}
```

Direct registration uses `CronConf`:

```rust
let part = bundles::cron::<DailyTick, _, _>(
    publish_daily,
    emitters::CronConf::new("0 0 0 * * *"),
);
```

Cron emitters run in-process. If the site is stopped during a scheduled time,
Vyuh does not replay that tick when the site starts again.

## Periodic

Periodic emitters run on a fixed in-process interval. The macro accepts `secs`,
`millis`, or both.

```rust
#[bundles::periodic(secs = 1, millis = 500)]
async fn publish_queue_tick() -> Data<QueueTick> {
    Data::new(QueueTick)
}
```

Direct registration uses `PeriodicConf`:

```rust
let part = bundles::periodic::<QueueTick, _, _>(
    publish_queue_tick,
    emitters::PeriodicConf::new(std::time::Duration::from_millis(1500)),
);
```

Periodic emitters are timers, not queues. Slow handlers and process shutdown can
delay or lose ticks.

## Durable Task Execution

Cron and periodic producers can submit their `Data<T>` to a registered task
whose input type is `T`. The producer must only construct deterministic input:
the durable task handler owns every side effect.

```rust
#[bundles::task]
async fn rebuild(Data(input): Data<RebuildIndex>) -> Result<(), Error> {
    rebuild_search_index(input).await
}

#[bundles::cron(
    expr = "0 0 2 * * *",
    executor = "task",
    schedule = "nightly-index-v1"
)]
async fn nightly_index() -> Data<RebuildIndex> {
    Data::new(RebuildIndex)
}
```

The direct form is identical in capability:

```rust
let part = bundles::periodic::<RebuildIndex, _, _>(
    refresh_index,
    emitters::PeriodicConf::new(std::time::Duration::from_secs(300))
        .executor(emitters::EmitterExecutor::Task)
        .on_start(emitters::ScheduleStart::Immediately),
);
```

Task schedules wait for the next normal occurrence by default. Use
`ScheduleStart::Immediately` only when a bootstrap submission is required.
Periodic task schedules align to UTC epoch boundaries. A task schedule stores a
single cursor in `vyuh_schedules`; replicas may produce the same input, but only
one transaction advances the cursor and accepts the task. On restart, Vyuh
coalesces missed source occurrences into one catch-up task instead of replaying
every missed slot. A produced payload survives transient store failures in the
local runtime and is retried with bounded backoff.

## PgNotify

PgNotify emitters listen to a Postgres channel and receive the raw notification
data as `Data<String>`.

```rust
#[bundles::pgnotify(channel = "notes_changed")]
async fn publish_note_notification(payload: Data<String>) -> Data<NoteNotification> {
    Data::new(NoteNotification {
        raw: payload.to_string(),
    })
}
```

Direct registration uses `PgNotifyConf`:

```rust
let part = bundles::pgnotify::<NoteNotification, _, _>(
    publish_note_notification,
    emitters::PgNotifyConf {
        channel: "notes_changed".to_string(),
        debounce: None,
    },
);
```

PgNotify is Postgres-only. MySQL and SQLite builds can use cron and periodic
emitters, but `pgnotify` requires Postgres `LISTEN`/`NOTIFY`.

### PgNotify Debounce

PgNotify emitters can debounce bursty notifications before running the handler:

```rust
#[bundles::pgnotify(
    channel = "notes_changed",
    debounce_millis = 250,
    debounce = "leading_trailing"
)]
async fn publish_note_notification(payload: Data<String>) -> Data<NoteNotification> {
    Data::new(NoteNotification {
        raw: payload.to_string(),
    })
}
```

Supported modes are:

| Mode | Behavior |
| --- | --- |
| `leading` | run immediately for the first notification and suppress the rest of the window |
| `trailing` | run once after a quiet window with the last payload |
| `leading_trailing` | run immediately, then run once more with the last payload only when more notifications arrived |

If `debounce_millis` or `debounce_secs` is set without `debounce`, the mode
defaults to `trailing`. Debounce is scoped to one PgNotify emitter
registration, not shared globally by channel name.

When a PgNotify emitter produces the same `Data<T>` as a cron or periodic
emitter, every raw notification still postpones that timer fallback. This means
periodic or cron fallback runs when no notifications arrive, but is pushed back
while notifications are active, even if debounce suppresses immediate handler
execution.

Pending trailing emissions are not flushed on shutdown.

PgNotify listeners reconnect automatically with bounded backoff and re-listen
to configured channels. PgNotify is still best-effort: notifications can be
missed during database disconnects or dropped when the internal notification
queue is full. Use periodic or cron fallback when missed notifications require
reconciliation.

Emitter runtime limits can be configured on `SiteConf`:

```rust
use vyuh::prelude::*;
use vyuh::emitters::EmitterConf;

let conf = SiteConf::default().emitters(EmitterConf {
    notify_channel_capacity: 2048,
    max_in_flight_handlers: 128,
    pgnotify_reconnect_initial_ms: 250,
    pgnotify_reconnect_max_ms: 30_000,
});
```

## Bundles

Emitters are registered as `BundlePart` values. Macro emitters and direct
`bundles::cron`, `bundles::periodic`, or `bundles::pgnotify` registration
produce the same kind of bundle part.

Signal emitter registrations are unique by emitted data type and emitter source kind.
Registering two periodic emitters for the same data type, for example,
is rejected during bundle validation. Task-targeted schedules are instead unique
by their stable schedule name, so multiple schedules may submit the same task
input type.

See [Bundles](bundles.md) for `BundlePart`, `bundle!`, cross-module bundle
organization, validation, composition behavior, and the general patch API.

## Examples

Run the signal and emitter example:

```sh
cargo run -p vyuh --example signals_emitters
```

`signals_emitters` demonstrates cron, periodic, direct API, and
Postgres-gated PgNotify emitter registration in one runnable example. To include
the PgNotify path, run it with Postgres enabled:

```sh
cargo run -p vyuh --features postgres --example signals_emitters
```

## Failure Modes

- Invalid cron expression: macro registration fails at compile time; direct
  registration records a bundle error.
- Invalid periodic interval attributes: the macro requires `secs`, `millis`, or
  both.
- Duplicate emitter: the same data type and source kind is already
  registered.
- PgNotify setup failure: startup fails if Postgres notification listening
  cannot be established.
- Handler failure: the error is logged and the emitter continues running.
- Signal target without a consumer: the emitted value is ignored.

## Best Practices

- Return stable data structs and handle the real work in signal handlers.
- Keep emitter handlers small; use them to produce events, not durable work.
- Return `vyuh::Error` for application failures; keep `EmitterError` for
  emitter registration and source machinery.
- Use direct registration for generated or conditional emitter lists.
- Keep pgnotify data parsing explicit and small.
- Use tasks for durable continuations, retries, persistence, or job observability.

## Current Limitations

- Emitters are in-process only.
- Cron and periodic ticks are not persisted or replayed.
- PgNotify is Postgres-only.
- The public v0 target is signals.
