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
- `output`: optional intermediate output saved while suspended.
- `result`: final output after completion.

Each wake runs the handler with the latest durable snapshot:

```text
input + state + resume_input -> handler -> Data<T> | TaskState<T> | ()
```

Return `Data<T>` or `Result<Data<T>, Error>` when a task completes with a typed
result and does not need continuation control. Return `TaskState<T>` when the
handler must suspend, sleep, retry, fail explicitly, or complete from a
continuation. `TaskOutcome` still exists for low-level store implementors.

Vyuh tasks are durable continuations for a single unit of work. They do not
provide a workflow DAG engine, child task orchestration, joins, branches, or
dependency graphs.

## Registration

The task macro is sugar over direct bundle registration. It does not unlock
capabilities that direct registration cannot express.

Macro registration:

```rust
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

## Handler Shapes

Fire-and-forget handlers can return nothing:

```rust
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

Handlers that complete with a typed result can return `Data<T>`:

```rust
use vyuh::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ReportJob {
    account_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct Report {
    title: String,
}

#[bundles::task]
async fn build_report(input: Data<ReportJob>) -> Data<Report> {
    Data::new(Report {
        title: format!("report for {}", input.account_id),
    })
}
```

The fallible form is `Result<Data<T>, Error>`. On success, `T` is serialized
into the task record `result`. On error, the task is committed as failed.

Handlers that need explicit continuation control should return
`Result<TaskState<T>, Error>`:

```rust
use std::time::Duration;
use vyuh::prelude::*;

#[bundles::task]
async fn poll_status(input: Data<PollJob>) -> Result<TaskState<String>, Error> {
    if is_ready(input.id).await? {
        return Ok(TaskState::complete("ready".to_string())?);
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

`Suspension<T>` is an optional handler argument for tasks that can suspend and
later resume. `suspension.get()` returns `None` on the first run and
`Some(T)` on a resumed run.

```rust
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
    suspension: Suspension<ApprovalDecision>,
    input: Data<ApprovalRequest>,
) -> Result<TaskState<ApprovalDecision>, Error> {
    if let Some(decision) = suspension.get() {
        return Ok(TaskState::complete(decision)?);
    }

    let state = PendingApproval {
        document_id: input.document_id,
        title: input.title.clone(),
    };

    Ok(TaskState::suspend(
        ApprovalDecision::Approved {
            approver: "(pending)".to_string(),
        },
        state,
    )?)
}
```

The generic type on `TaskState<T>` is the output/result type. The state passed
to `TaskState::suspend` or `TaskState::sleep` may be a different serializable
type.

## Complete, Suspend, Sleep, Retry, And Fail

Use `TaskState` constructors for explicit outcomes:

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::TaskState;

let done = TaskState::complete("ok".to_string())?;
let suspended = TaskState::suspend("waiting".to_string(), state)?;
let sleeping = TaskState::<String>::sleep(state, Duration::from_secs(30))?;
let retry = TaskState::<String>::retry(Some(Duration::from_secs(60)), "try later");
let failed = TaskState::<String>::fail("permanent failure");
```

An `Err(vyuh::Error)` from a handler is committed as a failed task outcome.
Retry is never inferred from `ErrorKind`; return `TaskState::retry(...)` when
the task should be tried again later.

## Suspend And Resume

Suspension is for tasks that cannot continue until something else happens:
approval, payment confirmation, a webhook, a file upload, or another application
event.

When a task suspends, it stores private `state` and optional externally visible
`output`. The task becomes durable and inactive. It does not consume a worker
slot or keep a Rust future alive.

Resume targets a specific task ID:

```rust
let task_id = site.tasks().submit(ApprovalRequest {
    document_id: 101,
    title: "Budget".into(),
}).await?;

let resumed = site
    .tasks()
    .resume(task_id, ApprovalDecision::Approved {
        approver: "carol".into(),
    })
    .await?;
```

`resume` stores the serialized resume input, moves the suspended task back to
pending, notifies workers, and returns the number of affected records. It
returns `0` when the task ID does not identify a currently suspended task.

There are no retained topic events in the current task model. If an application
needs to resume multiple tasks for one external event, it should keep its own
mapping from event keys to task IDs and call `resume` for each task.

## Sleep And Continuation

Sleep is for timed continuation. The handler saves state, chooses a delay, and
Vyuh wakes the task after that delay:

```rust
TaskState::<String>::sleep(state, Duration::from_secs(30))?
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
site.tasks().submit(SendEmailJob {
    to: "user@example.com".into(),
    subject: "Welcome".into(),
}).await?;
```

Use `submit_with` when the caller needs priority, an initial delay, identity,
retry policy, lease duration, max attempts, or initial state:

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::TaskOptions;

site.tasks()
    .submit_with(
        SendEmailJob {
            to: "user@example.com".into(),
            subject: "Welcome".into(),
        },
        TaskOptions {
            priority: 10,
            initial_delay: Some(Duration::from_secs(300)),
            retry_delay: Some(Duration::from_secs(60)),
            lease_duration: Some(Duration::from_secs(900)),
            max_attempts: Some(5),
            identity: Some("welcome:user@example.com".into()),
            ..TaskOptions::default()
        },
    )
    .await?;
```

`TaskOptions::identity` is an optional duplicate key. When set, Vyuh allows only
one active task with that identity. Active means `pending`, `running`, or
`suspended`; terminal `succeeded` and `failed` tasks release the identity.

`TaskOptions::priority` defaults to `0`. Higher values are claimed first. For
tasks with the same priority, Vyuh orders by eligibility time and creation time.

Initial delayed execution is `TaskOptions::initial_delay`, timed continuation is
`TaskState::sleep`, and recurring creation belongs in emitters.

## Concurrency And Leases

`TaskConf.concurrency` is the maximum number of tasks a runner executes in
parallel. `TaskConf.batch_size` controls how many tasks a runner claims at a
time. `TaskConf.lease_duration_ms` controls the default lease duration for
running tasks. Within each claim batch, eligible tasks are ordered by priority
first.

```rust
use vyuh::prelude::*;
use vyuh::tasks::TaskConf;

let conf = SiteConf::default().tasks(TaskConf {
    concurrency: 4,
    batch_size: 100,
    lease_duration_ms: 300_000,
    ..TaskConf::default()
});
```

Lease reclaim is conservative: an expired running task is eligible to be claimed
again and may run another time. Use `TaskOptions::lease_duration` for task
instances that are expected to run longer than the default lease. A longer lease
reduces premature duplicate execution for slow work, but it also delays reclaim
when the worker has actually crashed.

## Stores

With a database backend feature enabled, Vyuh stores tasks durably:

- `postgres`: `vyuh.tasks`
- `mysql`: `vyuh_tasks`
- `sqlite`: `vyuh_tasks`

All durable stores keep the lifecycle in one row and index the hot paths for
claiming pending work, resuming suspended tasks, enforcing active identity
uniqueness, and reclaiming expired running leases.

With no backend feature enabled, Vyuh uses `MemoryTaskStore`. This is good for
quick starts, local experiments, docs, and tests that do not need durability. It
is not a production durable queue.

Use Postgres for production multi-worker deployments by default. Use MySQL when
the rest of the application is already on MySQL and the deployment uses an
InnoDB-compatible server with row-locking support. Use SQLite when the app is
embedded, local, single-process, or needs a durable queue without a separate
database service.

## Examples

The canonical runnable task example is:

```sh
cargo run -p vyuh --features sqlite --example tasks
```

It covers:

- Fire-and-forget task handlers.
- Fallible task handlers.
- Direct registration without the task macro.
- Suspend/resume with `Suspension<T>` and `TaskState<T>`.

## Failure Modes

- Unregistered task data types return `TaskError::TaskNotFound`.
- Handler `Err(vyuh::Error)` values are committed as failed task outcomes.
- Stale workers cannot overwrite tasks they no longer own.
- Retried tasks become failed when `max_attempts` is reached.
- Active task identities cannot be duplicated until the active task reaches a
  terminal state.
- `resume` returns `0` when the task ID does not identify a suspended task.

## Current Limitations

- No exactly-once guarantee.
- No retained topic events.
- No durable per-attempt audit history.
- No multi-task workflow orchestration, child tasks, joins, branches,
  dependency graphs, or workflow execution engine.
- `MemoryTaskStore` is not durable and is not for production task queues.
- SQLite is intended for embedded, local, and single-process task execution.
