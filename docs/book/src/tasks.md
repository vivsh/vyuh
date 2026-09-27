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
[emitters](emitters.md) to produce scheduled or external events; cron and
periodic emitters can atomically enqueue a registered task when scheduled
creation must survive replica races and restarts. Use
[services](services.md) for site-lifetime clients, caches, and workers.

## Mental Model

A task is one durable handler backed by one task record. It may run, save state,
sleep, suspend, resume, retry, and eventually complete or fail.

Each task record stores:

- `input`: immutable submitted data.
- `state`: private continuation state saved by the handler.
- `resume_input`: optional input supplied when a suspended task is resumed.
- `parent_id` and `root_id`: nullable lineage assigned to spawned children.
- `kind`: `work` or `flow`, inferred from the registered handler.

Ordinary submission leaves lineage unset. Spawn derives lineage in the accepting
store transaction. Kind is a registration capability boundary, with no public setter or separate
scheduling/recovery policy.

Each wake runs the handler with the latest durable snapshot:

```text
input + state + resume_input -> handler -> Output | TaskState<Output> | FlowState<Output>
```

Work can return a serializable value directly, or `TaskState<T>` for completion
or suspension. Use `Result<_, TaskError>` for explicit failure/retry decisions.
Flow returns unit or `FlowState<T>`, optionally in `Result<_, FlowError>`.
Infrastructure APIs return `TaskRuntimeError`; these are not handler decisions.
Task persistence is framework-owned; applications compose work through
`site.tasks()` rather than implementing a scheduler store.

Persist durable artifacts in application records or object storage. Submit
follow-on work explicitly from domain state, signals, or another task submission,
using idempotency and an outbox where retries cross external boundaries.

Execution is at least once. Submission idempotency prevents duplicate durable
intents; it cannot make an external email, payment, or HTTP request exactly
once. Use a transactional outbox or a domain-owned idempotency key around those
effects.

Vyuh tasks are durable continuations, not a workflow DAG interpreter.
Sequential, nested, and homogeneous parallel child orchestration use explicit
Flow state transitions; `all` waits for every member rather than selecting a winner.

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
use vyuh::tasks::TaskDefinition;

let bundle = bundles::bundle([bundles::task(
    send_email,
    TaskDefinition::new("send_email"),
)]);
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

Fallible fire-and-forget handlers can return `Result<(), TaskError>`:

```rust
use vyuh::prelude::*;

#[bundles::task]
async fn process_data(input: Data<ProcessingJob>) -> Result<(), TaskError> {
    println!("processing {}", input.data);
    Ok(())
}
```

Direct-output Work handlers declare their persisted successful type:

```rust,ignore
#[bundles::task]
async fn calculate(input: Data<Input>) -> Output { calculate_output(&input) }

#[bundles::task]
async fn charge(input: Data<Charge>) -> Result<Receipt, TaskError> {
    Ok(payments.charge(&input).await?)
}
```

Output needs `Serialize + 'static`, not `Clone`, `Sync`, `Deserialize` or a schema.
Only `Result<_, TaskError>` represents Work error decisions. A serializable
`Result<T, E>` with another error type is an ordinary persisted value.
Use explicit `TaskError::retry` for retries; automatic `?` conversions are terminal.

## Work And Flow

Work handlers are asynchronous and can complete, suspend, fail, or explicitly retry.
Flow handlers are synchronous and can complete, fail, suspend, sleep, spawn, or
join a homogeneous group with `all`.
Both use the same statuses, lanes, concurrency, rates, lane locks, leases,
attempt counters, cancellation, and atomic outcome flushes. Neither executes
a child or resumed parent recursively; those run after a later claim.

Use `#[bundles::flow]` or the equivalent
`bundles::flow(handler, TaskDefinition::new("handler"))`. Both accept the same
name, lane, and idempotency definition as Work. Registration uses Rust trait
bounds, not recognition of argument type spelling. Submission remains
`tasks.submit(input).await?` for either kind.

For associated functions, use direct registration, for example
`bundles::flow(Checkout::advance, TaskDefinition::new("checkout"))`.

Flow accepts `Continuation<S, R>` and a final `Data<T>`. It does not expose
site/services, task identity, or operation identity. Work can also extract
`Continuation<S, R>` and suspend, but cannot sleep or spawn. There is no Flow
batch registration.

A synchronous signature is not a purity sandbox. Flow code must be short,
deterministic, bounded, and non-blocking; put I/O and expensive computation in
Work children. Globals and captured effects cannot be prevented by Rust's type
system. A non-yielding function cannot be preempted mid-call.

Flow cannot request retry, but an uncommitted execution is recovered with exactly
the same lease, retry-budget, and backoff rules as Work. A returned error or panic
is terminal for both. Shared lanes also mean shared contention; configure a
separate lane for isolation when needed.

## Input, State, And Resume Data

`Data<T>` is the immutable submitted input. It stays the same for the lifetime
of the task.

`Continuation<S, R>` is an optional Work or Flow handler argument for tasks that save state,
sleep, suspend, or resume. Initial execution has neither value, sleeping work
has state only, and resumed work has state plus a `Result<R, TaskFailure>`. Its accessors
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

#[bundles::flow(name = "approve_document")]
fn approve_document(
    continuation: Continuation<PendingApproval, ApprovalDecision>,
    input: Data<ApprovalRequest>,
) -> Result<FlowState<ApprovalDecision>, FlowError> {
    if let Some(Ok(decision)) = continuation.resume() {
        return Ok(FlowState::complete(decision.clone()));
    }
    if let Some(Err(failure)) = continuation.resume() {
        return Err(FlowError::fail(failure.message()));
    }

    let state = PendingApproval {
        document_id: input.document_id,
        title: input.title.clone(),
    };

    Ok(FlowState::suspend(state)?)
}
```

`R` is the successful resume value type. A failure carries a safe message and an
optional originating task ID. Initial execution has no result; `Ok(())` is a
successful unit result, not an absent input. Retrying a step preserves its resume
result so the next attempt sees the same input.

## Spawning And Waiting For A Child

Return a spawn outcome directly from the parent handler; no `Site` argument is needed:

```rust,ignore
#[bundles::flow]
fn build_report(
    continuation: Continuation<ReportCheckpoint, ReportData>,
    input: Data<ReportRequest>,
) -> Result<FlowState<ReportData>, FlowError> {
    match continuation.resume() {
        Some(Ok(data)) => Ok(FlowState::complete(data.clone())),
        Some(Err(failure)) => Err(FlowError::fail(failure.message())),
        None => Ok(FlowState::spawn(
            FetchReport { source: input.source.clone() },
            ReportCheckpoint { report_id: input.report_id },
        )?),
    }
}

#[bundles::task]
async fn fetch_report(input: Data<FetchReport>) -> Result<ReportData, TaskError> {
    Ok(fetch_data(&input.source).await?)
}
```

The equivalent parent registration is `bundles::flow(handler,
TaskDefinition::new("handler_name"))`. Spawning adds no macro syntax.

`FlowState::spawn(input, state)` and `FlowState::spawn_with(input, state, options)`
construct an outcome, not a submission. They serialize the checkpoint and reject
invalid options immediately. After the handler returns, the runtime resolves the
child through the executing site's registry, serializes its input, and validates
configured payload limits. Preparation errors fail the parent without creating a
child. Batch handlers cannot spawn children.

The accepted outcome atomically checkpoints and suspends the parent and inserts
its child. The child inherits the parent's root ID and records the parent's ID.
Its own registered lane, retries, and submission delay still apply.
`ignore_conflicts` is invalid for spawning. A child key already owned by another
task fails this spawn; it never adopts that task. Discarding an outcome creates no
child. The site task facade has no public spawn operation.

The child's successful output becomes `Ok(value)` in the parent continuation;
plain `()`/`complete(())` becomes `Ok(())`. Terminal failure, including exhausted
retries or leases, becomes `Err(TaskFailure)` with the child's ID. A retry,
sleep, or suspension is not terminal and does not resume the parent. The child's
result is retained on its task row and copied to the parent atomically.
Applications own any archive that must outlive task cleanup.

Child insertion and terminal-child parent resumption commit in the same
transaction as the originating outcome. New children and resumed parents are
eligible only through a subsequent poll, never synchronous invocation or that
turn's claim selection. Another process may claim them after transaction commit.

One outstanding child per parent supports sequential and nested workflows.
**Do not externally resume a parent waiting for its child.** This is a caller
contract, not a runtime waiting-on guard. Missing or no-longer-suspended parents
do not receive a child's result. Value-only batch handlers cannot spawn children.

## Parallel Children With `all`

`FlowState::all(children, checkpoint)` atomically checkpoints a Flow and creates
a group of children of one registered input type. It never submits work while
constructing the outcome. Use the same `#[bundles::flow]` registration, or the
equivalent `bundles::flow(handler, TaskDefinition::new("collect"))`:

```rust,ignore
#[bundles::flow]
fn collect(
    continuation: Continuation<(), Vec<Result<ItemOutput, TaskFailure>>>,
    input: Data<CollectInput>,
) -> Result<FlowState<Vec<ItemOutput>>, FlowError> {
    if let (_, Some(results)) = continuation.into_parts() {
        let values = results?.into_iter().collect::<Result<Vec<_>, _>>()?;
        return Ok(FlowState::complete(values));
    }
    Ok(FlowState::all(input.items.clone(), ())?)
}
```

Results are in **input order**, not completion order. The outer result describes
whether the join could be assembled; each inner result describes one child.
Failures and cancellations count as completed members. Retrying, sleeping, and
suspended children do not. No sibling is cancelled and no failure resumes the
parent early. An empty group resumes on a later poll with `Ok([])`.

Set `TaskConf::max_all_children(n)` to limit fan-out (default 256; valid 1–10,000).
This is separate from concurrency and claim/commit `batch_size`. The full combined
resume JSON envelope must fit **32,768 bytes**, even when individual child results
fit. Overflow or missing/malformed member results become an outer failure for
the parent; children keep their own results. Prefer application-owned artifact
references for large outputs. Decoding into `ItemOutput` still happens through
the normal continuation extractor.

The store decrements a durable counter when children finish. It collects results
only when the satisfied parent is actually claimed, persists `resume_input`,
and clears the wait atomically. Crash recovery then reuses that persisted input.
Groups use normal lanes, rates, scheduling, leases, retries, and cancellation.
Cancellation of a parent does not cancel its children.

Do not externally resume a parent awaiting `all`. Duplicate/conflicting child
idempotency keys reject the entire group without adopting existing tasks. Children
use registered policies and default scheduling; per-child options and heterogeneous
groups are not supported. Work and batch handlers cannot return `all`.

Completed tasks remain stored; this feature adds no retention policy. Any future
deletion feature must preserve results still needed by active joins. Upgrade using
the coordinated task-protocol templates before starting new workers or writers.

## Retained Results

`TaskState::complete(value)` and `FlowState::complete(value)` retain typed values
until the handler returns. The framework serializes a successful value and stores it as
`{"Ok": value}` on the completed task. Ordinary unit-return handlers retain
`{"Ok": null}`. Retrying and terminal failures retain `{"Err": TaskFailure}`;
the task status distinguishes a retry from terminal failure. Successful suspend,
sleep, spawn, and all checkpoints clear the previous result. Claims and lease
renewals preserve it. This is the latest result, not an attempt history.

Inspect results with `task.last_result::<Output>()?`, which returns
`Option<Result<Output, TaskFailure>>`, or borrow their JSON using
`task.last_result_json()`. A decoding error is an inspection error, not a task
failure. Old rows without retained outputs have no result, even if succeeded.
Results disappear when their task records are deleted.

Both retained results and `resume_input` have a fixed **32 KiB (32,768 bytes)**
limit, including the entire JSON envelope and escaping. The input/checkpoint
payload setting does not change this limit. Serialization failures and oversized
successful outputs terminally fail the affected task; they are never truncated.
External resume rejects oversized envelopes with `TaskRuntimeError::ResultTooLarge`.
Oversized external resume
requests leave the task unchanged. Error diagnostics may be shortened to fit.
Store large artifacts elsewhere and return their identifiers.

The console exposes results in task details, not routine list responses. Task
search matches names, lanes, and idempotency keys, not result payloads. Treat
results as application data and return only values appropriate for task viewers.

## Local Handler Batching

A handler can consume matching tasks already present in its process-local lane
queue in one call by accepting `Data<Batch<T>>`:

```rust
use vyuh::prelude::*;

#[bundles::task_batch]
async fn index_documents(Data(documents): Data<Batch<IndexDocument>>) -> Result<(), TaskError> {
    search.index_all(documents.as_ref()).await?;
    Ok(())
}
```

The distinct macro selects the reduced value-only batch contract without inspecting
or resolving the handler's argument syntax. The equivalent direct registration is
`bundles::task_batch(index_documents, TaskDefinition::new("index_documents"))`.
The submitted type remains `T`: every call to `submit(T)` creates its own
durable task row, attempt count, lease, retry policy, and terminal outcome.

Batching never waits for more rows and adds no store query. At dispatch, Vyuh
groups all currently eligible queued rows for the same registered handler while
leaving other task names in their existing relative order. One batch call uses
one local handler-concurrency slot; rate permits and durable lifecycle remain
per task.

Return `()` for uniform unit completion, `TaskState<T>` for uniform output, or
`Result<_, TaskError>` for uniform failure/retry. Ordered outcomes use
`Batch<TaskState<T>>` or `Batch<Result<TaskState<T>, TaskError>>`, with exactly one
item per valid input. An outer `Result<_, TaskError>` remains supported.
Uniform output is serialized once; ordered serialization failures affect only
that item. Cardinality mismatch fails the valid invocation. Bare `Batch<T>` is
not a shorthand for per-item outputs. Invalid historical inputs fail individually.
Batch handlers do not expose `TaskId`, `Continuation`, state, or resume input.
A returned suspension terminally fails that item; sleep and spawn remain unavailable.

Local batching and `TaskLaneLock` are orthogonal. An ordinary lane can batch;
a locked lane may also batch the matching task names inside a claimed cohort.
The lock controls when a cohort reaches the local queue, not the handler type.

## Complete, Suspend, Sleep, Retry, And Fail

Use `TaskState` for Work outcomes and `FlowState` for Flow outcomes:

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::TaskState;

let done = TaskState::complete(());
let suspended = TaskState::<()>::suspend(state)?;
let sleeping = FlowState::<()>::sleep(state, Duration::from_secs(30))?;
let retry = TaskError::retry("try again using the lane's backoff policy");
let failed = TaskError::fail("permanent failure");
```

Use `?` to convert framework errors into the handler's `TaskError` or `FlowError`.
These conversions log the underlying error and produce a safe terminal diagnostic;
no raw framework/database error is copied into persisted results. A runtime error
becomes `Task operation failed`; a framework error becomes `Task handler failed`. Messages passed explicitly to `TaskError::retry` and `TaskError::fail`
are application-owned durable summaries and must not contain secrets.
Retry is never inferred from `ErrorKind`; return `TaskError::retry(...)` when
the task should be tried again later. Handlers cannot choose retry timing or
attempt limits; the selected task lane owns both.

## Suspend And Resume

External responses must be durably retried when `resume` returns `false` because
suspension has not committed yet. Vyuh does not buffer early resume responses.

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

`resume` stores the input inside `{"Ok": value}`, moves the suspended task back to
pending, notifies the local worker, and returns `true` when it changed the task.
It returns `false` when the ID is absent or no longer suspended.

Use `resume_failed(id, TaskFailure::new(None, "safe explanation"))` to deliver an
external failure instead. These remain independent operations; child completion
does not call either method. Neither method may be used to interrupt a child wait.

There are no retained topic events in the current task model. If an application
needs to resume multiple tasks for one external event, it should keep its own
mapping from event keys to task IDs and call `resume` for each task.

## Cancellation

```rust
let requested = site.tasks().cancel(receipt.id()).await?;
```

`cancel` records durable intent, not immediate completion. It returns `true` for
a newly accepted request and `false` for missing, terminal, or already-cancelled
tasks. Store errors propagate. Inspect intent with `TaskInfo::cancelled()`;
status remains authoritative about whether finalization has happened.

Suspended tasks become pending and due now. Future pending timers advance to
now; already-due timers keep their ordering. Running tasks retain their status
and lease until a normal store turn accepts cancellation. `ready_at` is the
store-owned processing eligibility timer, not an audit of the original schedule.

Normal polling finalizes cancellation as `Failed`, storing
`Err(TaskFailure)` with the task ID and message `Task cancelled`. No handler is
invoked when cancellation is observed during candidate selection. Running-task
renewals and outcome commits also check durable intent: once accepted, a later
success, retry, checkpoint, or spawn cannot override cancellation. Cancelled
tasks cannot be resumed.

A cancelled child delivers the same failure to its suspended parent atomically;
the parent continues on a subsequent poll and can handle the failure normally.
Cancelling a parent does not cancel its children, and their later results cannot
revive it. Checkpoints remain intact when cancellation is requested.

Cancellation follows normal polling, concurrency, rate, batch-threshold, and lane
hook gates. It can remain pending while those gates block discovery. Cancellation
protects the durable terminal result; it does not guarantee that external execution
stops. This holds for both individual and batch tasks, even on one worker.

When renewal finalizes cancellation of a member of a shared batch invocation,
the invocation continues. Remaining members retain their leases and commit normally;
cancelled members stop receiving renewals and their returned outcomes are discarded.
Even if every member is cancelled, the shared invocation retains its execution slot
until it finishes. The lane stays busy until execution and outstanding commits drain,
then its normal `idle_after` debounce begins. A hung shared handler can therefore
delay lane idle and graceful shutdown. A cancelled child's parent may already have
received its failure while that external operation is still running.

Cancellation of an invocation containing only one task still aborts its future as
best-effort cleanup. Genuine task or lane ownership loss still invalidates an entire
invocation. Crash recovery can replay work, but cancellation of a batch member no
longer deliberately forces unaffected members through lease expiry and replay.

Task snapshots held by runners and handlers remain immutable. All authoritative
mutations, including cancellation resolution, happen inside the store. Existing
deployments must apply the coordinated cancellation migrations with workers and
writers stopped; see the task protocol migration instructions.

## Sleep And Continuation

Sleep is for timed continuation. The handler saves state, chooses a delay, and
Vyuh wakes the task after that delay:

```rust
FlowState::sleep(state, Duration::from_secs(30))?
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
`submit_many_with` for a shared initial delay and conflict policy:

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::TaskOptions;

let receipts = site.tasks()
    .submit_many_with(
        jobs,
        TaskOptions::new()
            .delay(Duration::from_secs(300))
            .ignore_conflicts(),
    )
    .await?;
```

Submission is immediate: the transaction commits before the terminal returns,
then the local worker is notified. There is no ingress buffer. A bounded bulk
submission is atomic and its receipts preserve input order; a non-ignored
conflict or store failure rolls back the whole batch. An empty batch succeeds
with no receipts.

All option builders are infallible. Invalid state serialization, generated
keys, or durations are reported only by the terminal submission call.

Idempotency belongs to the static task definition, not a particular submission:

```rust
fn email_key(job: &SendEmailJob) -> String {
    format!("welcome:{}", job.to)
}

let definition = TaskDefinition::new("send_email")
    .lane(EMAIL)
    .idempotency(TaskIdempotency::new("email-v1", email_key));
```

Vyuh fingerprints the canonical input with the definition's stable key-rule
revision. Repeating the same intent returns `Existing`; reusing a key for a
different intent rejects the batch. `.ignore_conflicts()` keeps non-conflicting
entries and returns `Ignored` for conflicts. Retention is lane policy: without
`.idempotency_retention(...)`, a terminal task releases its key; a retained key
remains unavailable from terminal completion until the configured duration.

Initial delayed execution is `TaskOptions::delay`, timed continuation is
`FlowState::sleep`, and recurring creation belongs in emitters. A
task-targeted cron or periodic emitter stores one durable `vyuh_schedules`
cursor and coalesces missed occurrences after restart; it does not turn tasks
into a general workflow or recurring-service abstraction.

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
use vyuh::tasks::{TaskConf, TaskLane, TaskLaneConf, TaskRate,
    TaskRetry, DEFAULT_TASK_LANE};

const EMAIL: TaskLane = TaskLane::new("email");
const EXPORTS: TaskLane = TaskLane::new("exports");

let tasks = TaskConf::default()
    .concurrency(10)
    .batch_size(100)
    .poll_interval(Duration::from_secs(1))
    .fallback_poll_interval(Duration::from_secs(300))
    .lease_duration(Duration::from_secs(300))
    .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 6))
    .lane(TaskLaneConf::new(EMAIL, 2)
            .retry(
                TaskRetry::exponential(5, Duration::from_secs(10))
                    .max_delay(Duration::from_secs(300)),
            )
            .rate_limit(TaskRate::per_second(10).burst(5))
            .global_rate_limit(TaskRate::per_minute(60).burst(10))
            .idempotency_retention(Duration::from_secs(30 * 24 * 60 * 60)))
    .lane(TaskLaneConf::new(EXPORTS, 2));
let conf = SiteConf::default().tasks(tasks);
```

A site may configure at most 32 lanes. Names are stable lowercase descriptors,
quotas must be positive, and their sum cannot exceed global concurrency.
Each lane also owns its retry limit and exponential backoff. The default is
five handler attempts per continuation step, beginning at one second and capped at five
minutes. A retry after attempt `n` waits `initial_delay * 2^(n - 1)`, bounded
by `max_delay`. This policy cannot be overridden by a submission or handler.
`attempts()` reports lifetime invocations; `step_attempts()` reports the current
step's retry budget. A committed `suspend`, `sleep`, or `spawn` resets the step
counter. Retries and lease reclaims do not reset it.
`rate_limit` is an inexpensive in-memory token bucket owned by the local site
runner. `global_rate_limit` coordinates starts across workers sharing a durable
task store. Configure either one independently, or configure both when each
start must satisfy local smoothing and a shared external quota. Global permits
are reserved with claimed rows in the same transaction and in batches, so a
high-throughput lane does not require one rate-state write per task. The memory
store coordinates a global limit only among runners sharing that in-process
store. Restarting a runner restores its local burst, and adding processes
multiplies a local-only limit.

## Durable Lane Ownership

A lane can opt into cluster-safe ownership when external capacity has a real
idle cost, such as a GPU process or compute server:

```rust
use std::time::Duration;
use vyuh::{Error, Service};
use vyuh::tasks::{TaskLaneContext, TaskLaneLock};

async fn stop_gpu(
    gpu: Service<GpuManager>,
    lane: TaskLaneContext,
) -> Result<(), Error> {
    gpu.stop(lane.lane()).await
}

async fn start_gpu(
    gpu: Service<GpuManager>,
    lane: TaskLaneContext,
) -> Result<(), Error> {
    gpu.ensure_running(lane.lane()).await
}

let gpu = TaskLaneConf::new(GPU, 8).lock(
    TaskLaneLock::new(32)
        .deadline(Duration::from_secs(2))
        .idle_after(Duration::from_secs(30))
        .on_idle(stop_gpu)
        .on_busy(start_gpu),
);
```

`TaskLaneLock::new(32)` is a scheduling threshold, not a batch-handler API.
Candidates remain pending until 32 ready rows have accumulated or the oldest
ready row reaches the optional `deadline`. Vyuh then claims a bounded cohort
and invokes ordinary handlers per task or explicitly batch-registered handlers
over matching rows already in that local cohort. Task durability, retry, rate,
and owner fencing remain per row.

One worker owns the lane through accumulation, execution, and commit. Ownership
uses a renewable store-time lease and an opaque token. A replacement can take
over after expiry, while the token prevents a stale worker from renewing tasks,
committing outcomes, applying hook results, or releasing the replacement's
lease. Handler and hook execution never holds a database transaction.

`idle_after` is continuous-empty debounce. Scheduled future work does not keep
the lane busy; its readiness time becomes a wake deadline. Ready work resets the
debounce. After the lane drains, `on_idle` runs before ownership is released.
When work later becomes ready, `on_busy` must succeed before any task in that
lane can be claimed.

Lifecycle hooks are extracted Vyuh callables, not durable tasks. They run in
independently spawned futures, do not consume `TaskConf::concurrency` or lane
task concurrency, and do not block polling, commits, or other lanes. Configure
both hooks or neither. Prefer function items as shown above and obtain mutable
configuration through site services; captured process-local closures are not a
cluster-stable configuration identity. Hooks must remain asynchronous and must
delegate blocking integration work with `spawn_blocking` or to an external
service.

Hook calls are at least once and must reconcile the desired external state.
An idle-hook failure releases ownership and fails open, but another idle attempt
is suppressed until real work runs and drains. A busy-hook failure releases
ownership, leaves work pending, and fails closed until a later normal poll
retries it. Panics become bounded failures. A hung hook holds only its lane in a
transition while the owner lease continues renewing; shutdown or ownership loss
aborts the local future.

Only locked lanes use the lane-lock table and bounded owner coordination query.
Ordinary lanes retain their existing claim query, and task submission never
reads or writes lane ownership state.

Lane lease renewal is part of the runner's central paced store turn. The runner
wakes that turn at the earliest normal poll, useful-work, fallback, task-lease,
or half-lane-lease deadline; ownership does not create a separate per-lane
poller or renewal query stream.

Reusable bundles can contribute a complete default for a named lane without
changing `bundle!` syntax:

```rust
let bundle = bundles::bundle! { send_email }
    .with_conf(bundles::conf().task_lane(TaskLaneConf::new(EMAIL, 2)));
```

The application remains authoritative: `TaskConf::lane(...)` replaces that
contributed definition by name. A task that declares an unavailable lane uses
`default` with a startup warning by default. Use
`TaskConf::missing_lane(TaskLanePolicy::RequireConfigured)` to make that a
site-construction error instead:

```rust
use vyuh::tasks::TaskLanePolicy;

let tasks = TaskConf::default()
    .missing_lane(TaskLanePolicy::RequireConfigured);
```

Bundle defaults cannot configure `default`, and duplicate contributed lane
names are rejected.

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

Local submission, cancellation, resume, handler completion, and outcome commit mark the local
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
Batch handlers additionally expose invocation and item counters by registered
handler name.
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

Durable stores use framework-owned tables for task lifecycle records,
idempotency ownership, per-lane rate buckets, opt-in lane-owner leases, durable
schedule cursors, and the store-wide scheduling policy fingerprint. The
fingerprint prevents workers with incompatible lane, retry, rate, ownership,
hook, or idempotency policies from sharing one store. The lane-owner table has
one primary-key row per configured locked lane and no foreign keys or secondary
indexes.

Persistent task tables are migration-owned. Apply the application's Mool/Gaman
migrations before starting task workers; `Site::build` never creates or alters
task tables. This makes schema changes reviewable and prevents a replica from
changing production DDL during startup.

The resume-result protocol is a coordinated breaking upgrade. Stop old workers
and writers, integrate the backend-specific templates from
`vyuh/migrations/task_protocol/` into the application's Gaman migration history,
and apply them through its ledger before starting new workers. Keep runtime
policy unchanged during this upgrade. Schema creation alone is insufficient:
every legacy non-null resume value must be wrapped once as `Ok(value)` and retry
counters must be backfilled. Never infer migration state from a JSON object's
shape. New workers reject an unmarked legacy policy. Existing running leases
remain recoverable; global rate buckets are preserved. There is still no
separate task output/result archive.

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
- Local `Data<Batch<T>>` handler registration and ordered outcomes.
- Synchronous Flow suspend/resume with `Continuation<S, R>` and `FlowState`.

## Failure Modes

- Unregistered task data types return `TaskRuntimeError::TaskNotFound`.
- Handler `Err(vyuh::Error)` values are committed as failed task outcomes.
- Stale workers cannot overwrite tasks they no longer own.
- Stale lane owners cannot renew tasks, commit outcomes, or apply lifecycle
  results after another owner takes over.
- Lane lifecycle hooks may repeat after lease recovery and therefore must be
  idempotent reconcilers rather than one-shot external effects.
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
- No declarative workflow interpreter, parallel child joins, or dependency
  graphs. Flow handlers can implement sequential and nested child orchestration.
- `MemoryTaskStore` is not durable and is not for production task queues.
- SQLite is intended for embedded, local, and single-process task execution.
