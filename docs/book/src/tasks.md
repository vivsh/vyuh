# Tasks

Vyuh tasks are durable, typed background handlers for work that must survive
process restarts, worker crashes, retries, timed delays, and delayed external
decisions. Use them for emails, imports, report generation, webhook retries,
approvals, chunked processing, polling loops, and long-running business work
where fire-and-forget signals are not enough.

Tasks are part of the same runtime model as routes, commands, signals, emitters,
and services. They are registered through bundles, submitted by input type, and
inspected through the task store and console APIs.

Use tasks for work that needs persistence, retry, sleep, leases, or external
resume. Do not use tasks for in-process fanout, site-lifetime loops, or
interactive CLI tools.

## When To Use Tasks

Use tasks when work needs one or more of these properties:

- Durability across process restarts.
- Retry after transient failures.
- Delayed execution.
- Continuation over multiple attempts.
- Waiting for an external decision before continuing.
- Controlled background concurrency.

Use [signals](signals.md) for in-process notifications. Use
[emitters](emitters.md) to produce scheduled or external events. Use
[services](services.md) for site-lifetime clients, caches, and workers.

## Mental Model

A task is one durable handler backed by one task record. It may run, save state,
sleep, suspend, resume, retry, and eventually complete or fail.

Each task record stores:

- `input`: immutable submitted data.
- `state`: private continuation state saved by the handler.
- `resume_input`: optional input supplied when a suspended task is resumed.

Each wake runs the handler with the latest durable snapshot:

```text
input + state + resume_input -> handler -> () | TaskState
```

Tasks are value-less: use `()` or `Result<(), Error>` for completed work, and
`TaskState` or `Result<TaskState, Error>` for explicit lifecycle control.
Task persistence is framework-owned; applications compose work through
`site.tasks()` rather than implementing a scheduler store.

Persist durable artifacts in application records or object storage. Submit
follow-on work explicitly from domain state, signals, or another task submission,
using idempotency and an outbox where retries cross external boundaries.

Execution is at least once. Submission idempotency prevents duplicate durable
intents; it cannot make an external email, payment, or HTTP request exactly
once. Use a transactional outbox or a domain-owned idempotency key around those
effects.

Vyuh tasks are durable continuations for a single unit of work. They do not
provide a workflow DAG engine, child task orchestration, joins, branches, or
dependency graphs.

## Registration

The task macro is sugar over direct bundle registration. It does not unlock
capabilities that direct registration cannot express.

Macro registration:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct SendEmailJob {
    to: String,
    subject: String,
}

#[bundles::task(name = "send_email")]
async fn send_email(input: Data<SendEmailJob>) {
    println!("sending email to {}", input.to);
}

let bundle = bundles::bundle! {
    send_email,
};
```

Equivalent direct registration:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;
use vyuh::bundles;
use vyuh::bundles::IntoBundle;
use vyuh::tasks::TaskHandlerConf;

let bundle = bundles::task(
    send_email,
    TaskHandlerConf::new("send_email"),
)
.into_bundle();
```

Task names are used for registration, storage, diagnostics, logs, and console
inspection. Submission is typed: `site.tasks().submit(...)` finds the registered
handler by the submitted data type. Vyuh enforces one handler per task input
type.

Task handlers may extract their canonical operation identity before `Data<T>`:

```rust
#[bundles::task]
async fn send_email(
    operation_id: OperationId,
    site: Site,
    Data(job): Data<SendEmail>,
) {
    let _metadata = site.operations().find(operation_id);
    // process job
}
```

The ID identifies the currently registered task operation and is not persisted
as part of the task record.

## Handler Shapes

Fire-and-forget handlers can return nothing:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[bundles::task]
async fn send_email(input: Data<SendEmailJob>) {
    println!("sending email to {}", input.to);
}
```

Fallible fire-and-forget handlers can return `Result<(), Error>`:

```rust
use vyuh::prelude::*;

#[bundles::task]
async fn process_data(input: Data<ProcessingJob>) -> Result<(), Error> {
    println!("processing {}", input.data);
    Ok(())
}
```

Handlers that need explicit continuation control should return
`Result<TaskState, Error>`:

```rust
use std::time::Duration;
use vyuh::prelude::*;

#[bundles::task]
async fn poll_status(input: Data<PollJob>) -> Result<TaskState, Error> {
    if is_ready(input.id).await? {
        return Ok(TaskState::complete());
    }

    Ok(TaskState::sleep(
        format!("waiting for {}", input.id),
        Duration::from_secs(30),
    )?)
}
```

## Input, State, And Resume Data

`Data<T>` is the immutable submitted input. It stays the same for the lifetime
of the task.

`Continuation<S, R>` is an optional handler argument for tasks that save state,
sleep, suspend, or resume. Initial execution has neither value, sleeping work
has state only, and resumed work has state plus resume input. Its accessors
borrow values, so continuation types do not need `Clone`.

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ApprovalRequest {
    document_id: i64,
    title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ApprovalDecision {
    Approved { approver: String },
    Rejected { approver: String, reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingApproval {
    document_id: i64,
    title: String,
}

#[bundles::task(name = "approve_document")]
async fn approve_document(
    continuation: Continuation<PendingApproval, ApprovalDecision>,
    input: Data<ApprovalRequest>,
) -> Result<TaskState, Error> {
    if let Some(decision) = continuation.resume() {
        apply_decision(&input, decision).await?;
        return Ok(TaskState::complete());
    }

    let state = PendingApproval {
        document_id: input.document_id,
        title: input.title.clone(),
    };

    Ok(TaskState::suspend(state)?)
}
```

`TaskState` has no result type. The state supplied to `suspend` or `sleep` is
the only serializable value it carries.

## Complete, Suspend, Sleep, Retry, And Fail

Use `TaskState` constructors for explicit outcomes:

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::TaskState;

let done = TaskState::complete();
let suspended = TaskState::suspend(state)?;
let sleeping = TaskState::sleep(state, Duration::from_secs(30))?;
let retry = TaskState::retry("try again using the lane's backoff policy");
let failed = TaskState::fail("permanent failure");
```

An `Err(vyuh::Error)` from a handler is committed as `Task handler failed`.
Vyuh writes the native causal chain only to structured logs with task, operation,
lane, and attempt context; it never retains that chain in task history or the
console. Messages passed explicitly to `TaskState::retry` and `TaskState::fail`
are application-owned durable summaries and must not contain secrets.
Retry is never inferred from `ErrorKind`; return `TaskState::retry(...)` when
the task should be tried again later. Handlers cannot choose retry timing or
attempt limits; the selected task lane owns both.

## Suspend And Resume

Suspension is the lifecycle state for tasks that cannot continue until something else happens:
approval, payment confirmation, a webhook, a file upload, or another application
event.

When a task suspends, it stores private `state`. The task becomes durable and
inactive. It does not consume a worker slot or keep a Rust future alive.

Resume targets a specific task ID:

```rust
let receipt = site.tasks().submit(ApprovalRequest {
    document_id: 101,
    title: "Budget".into(),
}).await?;

let resumed = site
    .tasks()
    .resume(receipt.id(), ApprovalDecision::Approved {
        approver: "carol".into(),
    })
    .await?;
```

`resume` stores the serialized resume input, moves the suspended task back to
pending, notifies the local worker, and returns `true` when it changed the task.
It returns `false` when the ID is absent or no longer suspended.

There are no retained topic events in the current task model. If an application
needs to resume multiple tasks for one external event, it should keep its own
mapping from event keys to task IDs and call `resume` for each task.

## Sleep And Continuation

Sleep is for timed continuation. The handler saves state, chooses a delay, and
Vyuh wakes the task after that delay:

```rust
TaskState::sleep(state, Duration::from_secs(30))?
```

Use sleep for polling external systems, chunked imports, slow retries with
progress, and staged work where the next step is time-based rather than
event-based.

Sleep is durable. If the process exits while a task is sleeping, the task
remains pending with a future `ready_at` time and can be claimed after that
time when workers are running again.

## Submit Tasks

Submit by registered data type:

```rust
let receipt = site.tasks().submit(SendEmailJob {
    to: "user@example.com".into(),
    subject: "Welcome".into(),
}).await?;
```

The receipt is `Queued`, `Existing`, or `Ignored` and always exposes the new or
existing task ID through `.id()`.

Use `submit_many` to enqueue inputs in one store transaction. Use
`submit_many_with` for a shared lane, delay, and idempotency rule:

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::{TaskLane, TaskOptions};

const EMAIL: TaskLane = TaskLane::new("email");

let receipts = site.tasks()
    .submit_many_with(
        jobs,
        TaskOptions::new()
            .lane(EMAIL)
            .delay(Duration::from_secs(300))
            .idempotency_key(|job: &SendEmailJob| format!("welcome:{}", job.to))
            .ignore_conflicts(),
    )
    .await?;
```

Submission is immediate: the transaction commits before the terminal returns,
then the local worker is notified. There is no ingress buffer. A bounded bulk
submission is atomic and its receipts preserve input order; a non-ignored
conflict or store failure rolls back the whole batch. An empty batch succeeds
with no receipts.

All option builders are infallible. Invalid state serialization, lane names,
keys, or durations are reported only by the terminal submission call.

Idempotency keys are scoped by registered task handler. Vyuh fingerprints the
canonical input together with execution-affecting options. Repeating the same
intent returns `Existing`; reusing the key for a different intent rejects the
whole batch. `.ignore_conflicts()` keeps non-conflicting entries and returns
`Ignored` for conflicting entries instead.

The site-wide retention policy defaults to `TaskIdempotency::active_only()`.
Use `TaskIdempotency::retain_for(Duration::from_secs(30 * 24 * 60 * 60))` when a completed key must
remain unavailable for an archive window. The window begins when the task
reaches a terminal state.

Initial delayed execution is `TaskOptions::delay`, timed continuation is
`TaskState::sleep`, and recurring creation belongs in emitters.

## Lanes, Throughput, And Rate Limits

Named lanes isolate slow work without introducing priority. Each lane owns a
bounded local queue and per-worker concurrency quota. One dispatcher rotates
the lanes fairly, while one global concurrency limit and one common outcome
buffer bound the whole runner.

Queue prefetch uses a fixed hysteresis rule: Vyuh refills a lane only when its
queued work is below half of that lane's concurrency quota, then claims up to
the lane's normal free capacity. The paced store tick still runs for commits,
lease renewal, and other lanes; a well-buffered lane is simply skipped rather
than queried again.

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::{TaskConf, TaskLane, TaskLaneConf, TaskIdempotency, TaskRate,
    TaskRetry, DEFAULT_TASK_LANE};

const EMAIL: TaskLane = TaskLane::new("email");
const EXPORTS: TaskLane = TaskLane::new("exports");

let tasks = TaskConf::default()
    .concurrency(10)
    .batch_size(100)
    .poll_interval(Duration::from_secs(1))
    .fallback_poll_interval(Duration::from_secs(300))
    .lease_duration(Duration::from_secs(300))
    .idempotency(TaskIdempotency::retain_for(Duration::from_secs(30 * 24 * 60 * 60)))
    .lanes([
        TaskLaneConf::new(DEFAULT_TASK_LANE, 6),
        TaskLaneConf::new(EMAIL, 2)
            .retry(
                TaskRetry::exponential(5, Duration::from_secs(10))
                    .max_delay(Duration::from_secs(300)),
            )
            .rate_limit(TaskRate::per_second(10).burst(5))
            .global_rate_limit(TaskRate::per_minute(60).burst(10)),
        TaskLaneConf::new(EXPORTS, 2),
    ]);
let conf = SiteConf::default().tasks(tasks);
```

A site may configure at most 32 lanes. Names are stable lowercase descriptors,
quotas must be positive, and their sum cannot exceed global concurrency.
Each lane also owns its retry limit and exponential backoff. The default is
five total handler attempts, beginning at one second and capped at five
minutes. A retry after attempt `n` waits `initial_delay * 2^(n - 1)`, bounded
by `max_delay`. This policy cannot be overridden by a submission or handler.
`rate_limit` is an inexpensive in-memory token bucket owned by the local site
runner. `global_rate_limit` coordinates starts across workers sharing a durable
task store. Configure either one independently, or configure both when each
start must satisfy local smoothing and a shared external quota. Global permits
are reserved with claimed rows in the same transaction and in batches, so a
high-throughput lane does not require one rate-state write per task. The memory
store coordinates a global limit only among runners sharing that in-process
store. Restarting a runner restores its local burst, and adding processes
multiplies a local-only limit.

Removing a configured lane never silently moves its work. Non-terminal orphaned
tasks prevent worker startup. After running work drains, explicitly call
`site.tasks().reassign_lane(OLD, NEW)` before deploying the configuration that
removes the old lane.

## Adaptive Polling And Leases

`poll_interval` is the short backlog interval. A lane whose candidate query
fills its requested batch is revisited after this interval when it has capacity.
`fallback_poll_interval` is the maximum idle recheck interval. Future
`ready_at`, lease-expiry, and rate-token deadlines wake the runner at their
store-relative database time when they are earlier. Deadlines are tracked per
lane, so activity in one lane does not force an idle or rate-limited lane to
query early.

Local submission, resume, handler completion, and outcome commit mark the local
runner for its next eligible tick; they never create an early background store
query. Vyuh intentionally adds no distributed notification channel; work
submitted by another process may wait until the fallback poll.
Choose a smaller fallback or an external deployment wake mechanism when that
latency is unacceptable.

Running leases are renewed in bounded batches before one third of the lease
remains. A worker that loses ownership cancels its local handler and cannot
commit its outcome. A crashed worker leaves its lease to expire; reclaiming the
task consumes another attempt and another rate permit.

When observability is enabled, Vyuh exports bounded-label task counters for
submission receipts and conflicts, claims and reclaimed leases, handler starts
and lifecycle outcomes, lease renewals and ownership loss, and store failures.
Queue, handler, and outcome-commit durations are exported without dynamic
application-data labels. Handler and lane labels come only from the immutable
site registries.

## Runtime Health

Task runtime health is updated from initialization and existing scheduler-store
ticks; readiness checks never issue an additional task-store query. The default
requires successful task initialization and then tolerates transient tick
failures:

```rust
use vyuh::tasks::{TaskConf, TaskReadiness};

let tasks = TaskConf::default()
    .readiness(TaskReadiness::startup_only());
```

For task-critical sites, make readiness fail after a consecutive failure
threshold and recover after the next successful tick:

```rust
let tasks = TaskConf::default()
    .readiness(TaskReadiness::after_failures(3));
```

`TaskReadiness::disabled()` keeps task health visible in metrics and the
console but excludes it from `/readyz`. Metrics expose the current readiness
gauge, consecutive store failures, and the last successful scheduler tick.
The console exposes only safe state and failure-class diagnostics.

## Stores

With a database backend feature enabled, Vyuh stores tasks durably:

- `postgres`: `vyuh_tasks`
- `mysql`: `vyuh_tasks`
- `sqlite`: `vyuh_tasks`

Durable stores use four framework-owned tables: task lifecycle records,
idempotency ownership, per-lane rate buckets, and the store-wide scheduling
policy fingerprint. The fingerprint prevents workers with incompatible lane,
retry, rate, or idempotency policies from sharing one store.

Persistent task tables are migration-owned. Apply the application's Mool/Gaman
migrations before starting task workers; `Site::build` never creates or alters
task tables. This makes schema changes reviewable and prevents a replica from
changing production DDL during startup.

The value-less task revision removes the historical `output` and `result`
columns. Deploy it by stopping old workers, generating and applying the normal
application migration, deploying the new binary, then starting workers again.
Pending, sleeping, and suspended work keeps its input and continuation state;
only historical task result values are discarded.

Claims, commits, runner queues, and persistence records remain internal. The
ordinary task API exposes only typed submission, resumption, reassignment, and
read-only inspection.

Task workers start only in the serving runtime. Commands can submit durable
tasks, but they do not claim or execute them themselves.

With no backend feature enabled, Vyuh uses `MemoryTaskStore`. This is good for
quick starts, local experiments, docs, and tests that do not need durability. It
is not a production durable queue. A production site with registered tasks and
no durable backend is rejected during site construction. Database rate limits
configured with `global_rate_limit` are store-wide; the memory store coordinates
them only within its process. `rate_limit` always remains local to one site
runner regardless of backend.

Use Postgres for production multi-worker deployments by default. SQLite is for
embedded, local, and single-process durable execution. MySQL is compile
supported but experimental until its migration and concurrent-claimer evidence
matches the Postgres and SQLite release gates.

## Examples

The canonical runnable task example is:

```sh
cargo run -p vyuh --example tasks
```

It covers:

- Fire-and-forget task handlers.
- Fallible task handlers.
- Direct registration without the task macro.
- Suspend/resume with `Continuation<S, R>` and `TaskState`.

## Failure Modes

- Unregistered task data types return `TaskError::TaskNotFound`.
- Handler `Err(vyuh::Error)` values are committed as failed task outcomes.
- Stale workers cannot overwrite tasks they no longer own.
- A crashed worker's running task is reclaimed only after its lease expires;
  the replacement invocation consumes another attempt and may repeat effects.
- A malformed historical running row without a lease deadline is marked failed
  during task-runtime initialization rather than being retried unsafely.
- Retried tasks become failed when their lane's maximum attempt count is reached.
- Conflicting idempotency intents reject the submission batch unless conflict
  ignoring was explicitly selected.
- Unknown or orphaned lanes fail explicitly and never fall back to `default`.
- `resume` returns `false` when the task ID does not identify a suspended task.

## Current Limitations

- No exactly-once guarantee.
- No retained topic events.
- No durable per-attempt audit history.
- No multi-task workflow orchestration, child tasks, joins, branches,
  dependency graphs, or workflow execution engine.
- `MemoryTaskStore` is not durable and is not for production task queues.
- SQLite is intended for embedded, local, and single-process task execution.
