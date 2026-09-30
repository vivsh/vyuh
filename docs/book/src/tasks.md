# Tasks

Vyuh tasks combine typed background work and durable workflow orchestration in
one runtime. A task has one of two kinds: **Work** handlers are asynchronous and perform effects; **Flow**
definitions are immutable and advance synchronously to the next durable transition. Both return
typed results and use the same task records, lanes, leases, recovery, and
cancellation mechanisms.

Use them for emails, imports, report generation, webhook retries, approvals,
sequential or nested child workflows, and parallel work joined with `all`.
Database-backed stores preserve committed progress across process restarts;
the in-memory store is for development and tests.

Tasks are part of the same runtime model as routes, commands, signals, emitters,
and services. They are registered through bundles, submitted by input type, and
inspected through the task store and console APIs.

Start with [registration](#registration) and [Work and Flow](#work-and-flow).
For orchestration, see [spawn](#spawning-and-waiting-for-a-child),
[`all`](#parallel-children-with-all), and [external resume](#suspend-and-resume).
[Handler batching](#local-handler-batching) and [lane ownership](#durable-lane-ownership)
are independent execution controls, not requirements for workflows.

## When To Use Tasks

Use tasks when work needs one or more of these properties:

- Durability across process restarts.
- Retry after transient failures.
- Delayed execution.
- Continuation over multiple attempts.
- Waiting for an external decision before continuing.
- Waiting for one child or all children, with typed success/failure results.
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
- `last_result`: the latest JSON success or failure envelope, not an attempt log.
- `status` and `cancelled`: lifecycle status and durable cancellation intent.
- `attempts` and `step_attempts`: lifetime and current-step invocation counts.
- `parent_id` and `root_id`: nullable lineage assigned to spawned children.
- `kind`: `work` or `flow`, inferred from the registered handler.

Ordinary submission leaves lineage unset. `spawn` and `all` derive lineage in the
accepting store transaction. Kind is a registration capability boundary, with no
public setter or separate scheduling/recovery policy.

Each wake runs the handler with the latest durable snapshot:

```text
input + state + resume_input -> handler -> Output | WorkState<Output> | FlowState<Output>
```

Work can return a serializable value directly, or `WorkState<T>` for completion
or suspension. Use `Result<_, WorkError>` for explicit failure/retry decisions.
Flow factories build once per site; each invocation calls the definition's
`advance` method, returning `Result<FlowState<T>, FlowError>`.
Infrastructure APIs return `TaskRuntimeError`; these are not handler decisions.
Task persistence is framework-owned; applications compose work through
`site.tasks()` rather than implementing a scheduler store.

Use Flow outcomes for dependent work: `spawn` waits for one child and `all` waits
for a homogeneous group. Use ordinary submission for independent work. Keep
large artifacts in application records or object storage and return references.

Execution is at least once. Submission idempotency prevents duplicate durable
intents; it cannot make an external email, payment, or HTTP request exactly
once. Use a transactional outbox or a domain-owned idempotency key around those
effects.

Manual workflows use explicit Flow state transitions; `all` waits for every member
rather than selecting a winner. The optional `pravah` feature adds declarative
graphs through the same durable continuation boundary, not a second task scheduler.

The durable execution boundary is always:

```text
claim -> immutable handler snapshot -> outcome -> atomic store commit -> later claim
```

Returning a value or constructing an outcome is not a commit. Only the store
mutates authoritative task state. New children and resumed parents are finalized
after that turn's claim selection; they never execute synchronously from the
parent or child handler. Another worker may claim the committed work afterward;
there is no cluster-wide poll counter. Crashes before commit can replay a step.

## Registration

The `work`, `work_batch`, and `flow` macros are sugar over their corresponding
direct bundle registration functions. They do not unlock
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

#[bundles::work(name = "send_email")]
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

let bundle = bundles::bundle([bundles::work(
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
#[bundles::work]
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

Unit-completion Work handlers can return nothing:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[bundles::work]
async fn send_email(input: Data<SendEmailJob>) {
    println!("sending email to {}", input.to);
}
```

Fallible unit-completion handlers can return `Result<(), WorkError>`:

```rust
use vyuh::prelude::*;

#[bundles::work]
async fn process_data(input: Data<ProcessingJob>) -> Result<(), WorkError> {
    println!("processing {}", input.data);
    Ok(())
}
```

Direct-output Work handlers declare their persisted successful type:

```rust,ignore
#[bundles::work]
async fn calculate(input: Data<Input>) -> Output { calculate_output(&input) }

#[bundles::work]
async fn charge(input: Data<Charge>) -> Result<Receipt, WorkError> {
    Ok(payments.charge(&input).await?)
}
```

Output needs `Serialize + 'static`, not `Clone`, `Sync`, `Deserialize` or a schema.
Only `Result<_, WorkError>` represents Work error decisions. A serializable
`Result<T, E>` with another error type is an ordinary persisted value.
Use explicit `WorkError::retry` for retries; automatic `?` conversions are terminal.

## Work And Flow

Work handlers are asynchronous and can complete, suspend, fail, or explicitly retry.
Flow handlers are synchronous and can complete, fail, suspend, sleep, spawn, or
join a homogeneous group with `all`.
Both use the same statuses, lanes, concurrency, rates, lane locks, leases,
attempt counters, cancellation, and atomic outcome flushes. Neither executes
a child or resumed parent recursively; those run after a later claim.

| Capability | Work | Flow | Work batch |
|---|---|---|---|
| Registration | `work` | `flow` | `work_batch` |
| Function | `async fn` | one-time synchronous factory; synchronous `Flow::advance` | `async fn` |
| Success | Direct `T` or `WorkState<T>` | `FlowState<T>` | Unit, uniform state, or ordered states |
| Handler error | `WorkError` (`Retry` or `Fail`) | `FlowError` (terminal) | `WorkError`, uniform or per item |
| External suspension | Yes, with `Continuation<S, R>` | Yes, with `Continuation<S, R>` | No |
| Sleep, spawn, `all` | No | Yes | No |
| Site/service extraction | Yes | No | Yes |

Use `Result<Success, HandlerError>` for fallible forms. `WorkState<T>` and
`FlowState<T>` default `T` to `()`. Their `complete(value)` accepts exactly `T`
and returns the state directly, without `?`; the framework validates serialization
and result size after the handler returns. `suspend`, `sleep`, `spawn`, and `all`
can fail during request preparation and therefore still use `?`.

Use `#[bundles::flow]` or the equivalent
`bundles::flow(factory, FlowConf::new("handler"))`. Both accept the same
name, lane, and idempotency definition as Work. Registration uses Rust trait
bounds, not recognition of argument type spelling. Submission remains
`tasks.submit(input).await?` for either kind.

For associated functions, use direct registration, for example
`bundles::flow(Checkout::build, FlowConf::new("checkout"))`.

Factories return an immutable implementation of `Flow`, optionally inside
`Result<_, FlowError>`. A manual factory takes no arguments or a build-time
`PartialSite`. It never receives submitted input or continuation. `PartialSite`
only exposes the configured database handle: no runtime services or task facade.
Factories and effects-policy construction must not perform blocking I/O.

`Flow::advance` receives read-only `TaskId`, `Data<Self::Input>`, and
`Continuation<Self::Checkpoint, Self::Resume>`. It does not receive site/services
or operation identity. Work can also extract
`Continuation<S, R>` and suspend, but cannot sleep or spawn. There is no Flow
batch registration.

A synchronous signature is not a purity sandbox. Flow code must be short,
deterministic, bounded, and non-blocking; put I/O and expensive computation in
Work children. Globals and captured effects cannot be prevented by Rust's type
system. A non-yielding function cannot be preempted mid-call.

Flow cannot request retry, but an uncommitted execution is recovered with exactly
the same lease, retry-budget, and backoff rules as Work. A returned error or panic
is terminal, except an explicit Work `WorkError::Retry`. Shared lanes also mean
shared contention; configure a separate lane for isolation when needed.

## Input, State, And Resume Data

`Data<T>` is the immutable submitted input. It stays the same for the lifetime
of the task.

`Continuation<S, R>` is a Work extractor and an argument to `Flow::advance` for tasks that save state,
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
fn approve_document() -> ApprovalFlow { ApprovalFlow }

struct ApprovalFlow;
impl Flow for ApprovalFlow {
    type Input = ApprovalRequest;
    type Output = ApprovalDecision;
    type Checkpoint = PendingApproval;
    type Resume = ApprovalDecision;

fn advance(&self, _: TaskId,
    input: Data<ApprovalRequest>,
    continuation: Continuation<PendingApproval, ApprovalDecision>,
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
fn build_report() -> ReportFlow { ReportFlow }

struct ReportFlow;
impl Flow for ReportFlow {
    type Input = ReportRequest;
    type Output = ReportData;
    type Checkpoint = ReportCheckpoint;
    type Resume = ReportData;

fn advance(&self, _: TaskId,
    input: Data<ReportRequest>,
    continuation: Continuation<ReportCheckpoint, ReportData>,
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
}

#[bundles::work]
async fn fetch_report(input: Data<FetchReport>) -> Result<ReportData, WorkError> {
    Ok(fetch_data(&input.source).await?)
}
```

The equivalent parent registration is `bundles::flow(build_report,
FlowConf::new("build_report"))`. Spawning adds no macro syntax.

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

Single-child `spawn` supports sequential and nested workflows; use `all` below
when a parent needs to wait for several children at once.
**Do not externally resume a parent waiting for its child.** This is a caller
contract, not a runtime waiting-on guard. Missing or no-longer-suspended parents
do not receive a child's result. Value-only batch handlers cannot spawn children.

## Parallel Children With `all`

`FlowState::all(children, checkpoint)` atomically checkpoints a Flow and creates
a group of children of one registered input type. It never submits work while
constructing the outcome. Use the same `#[bundles::flow]` registration, or the
equivalent direct registration shown after this example:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
struct Item { value: u32 }

#[derive(Serialize, Deserialize, JsonSchema)]
struct Collection { items: Vec<Item> }

#[bundles::work]
async fn process_item(input: Data<Item>) -> u32 {
    input.value.saturating_mul(2)
}

#[bundles::flow]
fn collect() -> CollectFlow { CollectFlow }

struct CollectFlow;
impl Flow for CollectFlow {
    type Input = Collection;
    type Output = Vec<u32>;
    type Checkpoint = ();
    type Resume = Vec<Result<u32, TaskFailure>>;

fn advance(&self, _: TaskId,
    input: Data<Collection>,
    continuation: Continuation<(), Vec<Result<u32, TaskFailure>>>,
) -> Result<FlowState<Vec<u32>>, FlowError> {
    if let (_, Some(results)) = continuation.into_parts() {
        let values = results?.into_iter().collect::<Result<Vec<_>, _>>()?;
        return Ok(FlowState::complete(values));
    }
    Ok(FlowState::all(input.items.clone(), ())?)
}
}

let bundle = bundles::bundle! { process_item, collect };
```

The equivalent direct bundle uses `bundles::work(process_item,
TaskDefinition::new("process_item"))` and `bundles::flow(collect,
FlowConf::new("collect"))`; import `TaskDefinition` from `vyuh::tasks`.
Submit `Collection { items: vec![Item { value: 2 }, Item { value: 5 }] }`
through `site.tasks().submit(...)`. Its successful result is `[4, 10]`.
This handler chooses to fail the Flow if any child failed; it could instead
inspect every inner `Result` and retain partial successes.

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
references for large outputs. Decoding into the requested child output type still happens through
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

## Optional Pravah Graphs

Enable `vyuh`'s non-default `pravah` feature and import the matching API from
`vyuh::pravah`. It does not enable Pravah's testing or MCP features. Manual Flow
definitions do not require Pravah.

The runnable `pravah_tasks` example demonstrates explicit Work routing with a
stub provider: `cargo run -p vyuh --example pravah_tasks --features pravah`.

```rust,ignore
use vyuh::{bundles, pravah, PartialSite};
use vyuh::tasks::{FlowConf, FlowError, PravahEffects, WorkRequest};

#[bundles::flow(effects = AppEffects)]
fn checkout(root: pravah::Flow<Checkout>) -> pravah::Flow<Confirmation> {
    build_checkout(root)
}

// Equivalent direct registration:
bundles::flow(checkout, FlowConf::new("checkout")
    .effects::<AppEffects>().step_limit(256).revision("1"));
```

Factories receive symbolic roots, optionally preceded by `PartialSite`, never
submitted input. The framework compiles once; factories may also return an
already compiled definition. Each invocation creates an isolated temporary VM
and restores its snapshot from the ordinary task continuation. Factory or policy
errors/panics prevent site construction. A pure graph can omit effects; ordinary
external suspension also works without a policy.

Opaque returns must expose the matching preparation bound: `impl IntoFlow<Input>`
without effects, or `impl IntoFlow<Input, AppEffects>` with that policy. A manual
factory may return `impl Flow<Input = Input>`.

A reusable policy **routes requests, not results**:

```rust,ignore
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct FetchJob {
    #[schemars(with = "String")]
    id: uuid::Uuid,
    request: pravah::FetchRequest,
}

struct AppEffects;
impl PravahEffects for AppEffects {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }

    fn fetch(&self, request: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Ok(WorkRequest::new(FetchJob {
            id: request.id(), request: request.request().clone(),
        }))
    }
}
```

The policy is constructed once per selected type per site and shared across
definitions. `suspend(&Suspension)` may return `Some(WorkRequest)` or `None` for
external waiting; its default is `None`. `WorkRequest::options(...)` validates
scheduling options and rejects conflict adoption. Targets must be registered
Work, never Flow. Construction and routing cannot submit work: the snapshot and
child are committed through the existing atomic spawn path.

Register a Work handler accepting `Data<FetchJob>` and returning `FetchResponse`.
That handler performs the effect using normal services. It may use an explicitly
configured `FetchExecutor`; agent/tool requests must use the appropriate handler
registry and services. Vyuh installs no universal executor. Do not use
`Data<pravah::Fetch>` directly: it lacks the schema bound required by `DataValue`.

Successful Fetch results are decoded directly as `FetchResponse`. Terminal Work
failures become Fetch errors with code `vyuh_task_failure`, the safe diagnostic,
and optional originating task ID in details. Suspension Work must return its
declared resume type; failed suspension Work or failed external resume terminates
the parent. There are no result-conversion callbacks. Only Vyuh's outer persisted
`Result` is unwrapped: a successfully returned domain `Result` remains data.
Malformed or schema-incompatible values fail the Flow explicitly.

Fetch/suspension boundaries checkpoint immediately. Pure instructions run locally
up to `step_limit` (default 256, valid 1–10,000); exhaustion checkpoints through
zero-delay sleep and resumes on a later poll. A boundary emitted by the final
instruction is handled before yielding. The limit cannot preempt one blocking
instruction. Graph parallelism is not automatically translated to task `all`.

Snapshots obey the existing checkpoint limit; final results and resume envelopes
still have the fixed 32,768-byte limit. Snapshots and Fetch payloads can contain
credentials or sensitive data: apply the same storage/access controls as inputs.

Graph fingerprint, policy type, instruction limit and application revision enter
deployment compatibility. Increment `.revision(...)` when captured configuration
or Rust callbacks change replay semantics; graph fingerprints cannot detect every
such change. Stop incompatible workers and writers and apply the appended
`0012_flow_factories_protocol` template. It changes protocol markers, not task
columns. Retained manual checkpoints must match the new implementation; they are
never reinterpreted as Pravah snapshots. No runtime checkpoint/schema repair or
mixed-version operation is supported.

## Retained Results

`WorkState::complete(value)` and `FlowState::complete(value)` retain typed values
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

Fetch the read-only `TaskInfo` through `site.tasks().get(receipt.id()).await?`.
`None` from `get` means the task is absent; `None` from `last_result` means it
has no recorded result. Inspect `status()` as well: a pending retry may still
carry its last failure. `list(TaskFilter::new().idempotency_key(key))` provides
bounded lookup by idempotency key; submission receipts are identities, not
futures awaiting handler output.

Both retained results and `resume_input` have a fixed **32 KiB (32,768 bytes)**
limit, including the entire JSON envelope and escaping. The input/checkpoint
payload setting does not change this limit. Serialization failures and oversized
successful outputs terminally fail the affected task; they are never truncated.
External resume rejects oversized envelopes with `TaskRuntimeError::ResultTooLarge`
and leaves the task unchanged. Error diagnostics may be shortened to fit.
Store large artifacts elsewhere and return their identifiers.

The console exposes results in task details, not routine list responses. Task
search matches names, lanes, and idempotency keys, not result payloads. Treat
results as application data and return only values appropriate for task viewers.

## Local Handler Batching

A handler can consume matching tasks already present in its process-local lane
queue in one call by accepting `Data<Batch<T>>`:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Serialize, Deserialize, JsonSchema)]
struct NormalizeText { text: String }

#[bundles::work_batch]
async fn normalize_texts(Data(items): Data<Batch<NormalizeText>>) -> Batch<WorkState<String>> {
    items.iter()
        .map(|item| WorkState::complete(item.text.trim().to_owned()))
        .collect()
}

let bundle = bundles::bundle! { normalize_texts };
```

The distinct macro selects the reduced value-only batch contract without inspecting
or resolving the handler's argument syntax. The equivalent direct registration is
`bundles::work_batch(normalize_texts, TaskDefinition::new("normalize_texts"))`.
The submitted type remains `T`: every call to `submit(T)` creates its own
durable task row, attempt count, lease, retry policy, and terminal outcome.

Batching never waits for more rows and adds no store query. At dispatch, Vyuh
groups all currently eligible queued rows for the same registered handler while
leaving other task names in their existing relative order. One batch call uses
one local handler-concurrency slot; rate permits and durable lifecycle remain
per task.

Return `()` for uniform unit completion, `WorkState<T>` for uniform output, or
`Result<_, WorkError>` for uniform failure/retry. Ordered outcomes use
`Batch<WorkState<T>>` or `Batch<Result<WorkState<T>, WorkError>>`, with exactly one
item per valid input. An outer `Result<_, WorkError>` remains supported.
Uniform output is serialized once; ordered serialization failures affect only
that item. Cardinality mismatch fails the valid invocation. Bare `Batch<T>` is
not a shorthand for per-item outputs. Invalid historical inputs fail individually.
Batch handlers do not expose `TaskId`, `Continuation`, state, or resume input.
A returned suspension terminally fails that item; sleep and spawn remain unavailable.

Local batching and `TaskLaneLock` are orthogonal. An ordinary lane can batch;
a locked lane may also batch the matching task names inside a claimed cohort.
The lock controls when a cohort reaches the local queue, not the handler type.

## Complete, Suspend, Sleep, Retry, And Fail

Use `WorkState` for Work outcomes and `FlowState` for Flow outcomes:

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::WorkState;

let done = WorkState::complete(());
let suspended = WorkState::<()>::suspend(state)?;
let sleeping = FlowState::<()>::sleep(state, Duration::from_secs(30))?;
let retry = WorkError::retry("try again using the lane's backoff policy");
let failed = WorkError::fail("permanent failure");
```

Use `?` to convert framework errors into the handler's `WorkError` or `FlowError`.
These conversions log the underlying error and produce a safe terminal diagnostic;
no raw framework/database error is copied into persisted results. A runtime error
becomes `Task operation failed`; a framework error becomes `Task handler failed`. Messages passed explicitly to `WorkError::retry` and `WorkError::fail`
are application-owned durable summaries and must not contain secrets.
Retry is never inferred from `ErrorKind`; return `WorkError::retry(...)` when
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
It returns `false` when the ID is absent, cancelled, or no longer suspended.

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

A cancelled single child delivers its failure to its suspended `spawn` parent
atomically; the parent continues on a subsequent poll. Within `all`, cancellation
counts as one terminal child, but the parent still waits for every other member
before receiving the ordered results.
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

Sleep is for timed Flow continuation. The handler saves state, chooses a delay, and
Vyuh wakes the task after that delay:

```rust
FlowState::sleep(state, Duration::from_secs(30))?
```

Use sleep between Work children for polling an external system, processing
another chunk, or staged work where the next step is time-based rather than
event-based. Keep I/O in Work handlers; Flow only chooses the next transition.
For transient failures within one Work step, use `WorkError::retry` and the
lane's backoff policy instead of treating sleep as a Work retry API.

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

Option builders are infallible. Invalid input serialization, generated keys,
or durations are reported by the terminal submission call.

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
cursor and coalesces missed occurrences after restart. Recurring creation belongs
to that emitter; orchestration within each occurrence belongs to Flow.

## Lanes, Throughput, And Rate Limits

Named lanes isolate slow work without introducing priority. Each lane owns a
bounded local queue and per-worker concurrency quota. One dispatcher rotates
the lanes fairly, while one global concurrency limit and one common outcome
buffer bound the whole runner.

These controls have different jobs:

| Control | What it bounds or groups |
|---|---|
| `TaskConf::concurrency(n)` | Running handler invocations across the local runner |
| `TaskLaneConf::new(lane, n)` | Per-runner lane handler concurrency and refill capacity |
| `TaskConf::batch_size(n)` | Store claim/commit batch bound, not handler batch size |
| `#[bundles::work_batch]` | Matching, eligible inputs already queued locally; never waits |
| `TaskLaneLock::new(n).deadline(duration)` | Owner-held accumulation threshold before claiming a cohort |
| `TaskConf::max_all_children(n)` | Maximum fan-out per `all`, not execution concurrency |

Increasing the store batch size does not override concurrency, rates, poll gates,
or the availability of ready work. Ordinary lanes keep independent input queues;
all lanes share the outcome buffer. Fan-out flushes additionally bound expanded
child writes without splitting an `all` group across transactions.

Queue prefetch uses a fixed hysteresis rule: Vyuh refills a lane only when its
queued work is below half of that lane's concurrency quota, then claims up to
the lane's normal free capacity. Refill capacity subtracts running invocations,
queued tasks, and completed-but-uncommitted task outcomes from the lane quota.
This applies to every outcome, not just fan-out. Batch completion counts every
item until its outcome is acknowledged, although execution uses one handler slot.
The paced store tick still runs for commits, lease renewal, and other lanes; a
full lane is simply skipped rather than queried again. Claims are calculated
before commit acknowledgement, so a fully occupied fast-handler lane may alternate
commit-only and refill polls. Already queued work continues executing normally.

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
step's retry budget. A committed `suspend`, `sleep`, `spawn`, or `all` resets the step
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

Database work is batched without changing polling or lease timing. Ordinary
lanes never consult lane-lock storage. Optional rates, lane ownership, recovery,
and workflow transitions can require additional bounded statements in the same
transaction. PostgreSQL and SQLite combine ordinary claim evidence. PostgreSQL
also shares ordinary outcome/renewal observations; SQLite retains separate reads.
Mixed turns still apply outcomes and renewals before selecting more work. Batched
database access does not imply a fixed one- or two-query budget for every turn.
Recovery eligibility is checked from current database evidence, not a cache or
a slower recovery schedule. Expired idempotency reservations are physically cleaned up
periodically; this does not extend their logical retention window or prevent
reuse of an expired key.

Persistent task tables are migration-owned. Apply the application's Mool/Gaman
migrations before starting task workers; `Site::build` never creates or alters
task tables. This makes schema changes reviewable and prevents a replica from
changing production DDL during startup.

### Upgrading Existing Task Stores

These APIs and persisted protocols require a coordinated upgrade. Stop all old
workers and writers, back up the database, and integrate the backend-specific
[task-protocol templates](https://github.com/vivsh/vyuh/tree/main/vyuh/migrations/task_protocol)
into the application's Gaman migration history. Apply them through its ledger
before starting new workers. See [task schema migrations](migrations.md#durable-task-schema-changes)
for deployment order and preflight requirements. Mixed-version operation and
startup schema repair are unsupported.

For applications using older task APIs:

| Earlier API | Current API |
|---|---|
| `complete()` / fallible `complete_with(value)` | Infallible `WorkState::complete(())` / `WorkState::complete(value)`; declare `WorkState<T>` |
| `WorkState::retry` / `WorkState::fail` | Return `Err(WorkError::retry(...))` / `Err(WorkError::fail(...))` |
| Work sleep or child spawn | Synchronous `flow` registration with `FlowState<T>` and `FlowError` |
| Infrastructure `WorkError` | `TaskRuntimeError`; `WorkError` now describes Work decisions |
| Raw resume value `R` | `Result<R, TaskFailure>` inside the continuation |
| `last_error()` inspection | `last_result::<T>()?` and `last_result_json()` |

Continuable Work remains supported for external suspension. Preserve registered
names and JSON types where possible; reclassify existing rows only for handlers
actually converted to Flow. New successful outputs are retained, but migrations
cannot reconstruct historical successful values. Never infer whether resume
data was migrated by inspecting its JSON shape or reapply ledger-tracked wrapping.

### Runtime And Backend Selection

Claims, commits, runner queues, and persistence records remain internal. The
ordinary task API exposes typed submission, cancellation, resumption, reassignment, and
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
embedded, local, and single-process durable execution. MySQL/MariaDB implement
the same task-store contract and have backend-specific migration templates, but
remain experimental; validate migration and concurrent-worker behavior against
your application's workload before production use.

## Examples

The canonical runnable task example is:

```sh
cargo run -p vyuh --example tasks
```

It covers:

- Unit and typed-output Work handlers.
- Fallible task handlers, lane retry/rate configuration, and bulk submission.
- Atomic single-child spawning and result delivery to a Flow.
- Synchronous Flow suspend/resume with `Continuation<S, R>` and `FlowState`.

This chapter also shows equivalent direct registration, ordered batch outcomes,
and a complete `all` handler pair above.

## Failure Modes

- Unregistered task data types return `TaskRuntimeError::TaskNotFound`.
- Work `WorkError::Fail`, Flow `FlowError`, and panics become terminal failures.
  Work `WorkError::Retry` requests lane-controlled retry. Using `?` on a
  `vyuh::Error` or `TaskRuntimeError` converts it to the handler's terminal error;
  `Result<_, vyuh::Error>` is not the task-handler decision contract.
- Stale workers cannot overwrite tasks they no longer own.
- Stale lane owners cannot renew tasks, commit outcomes, or apply lifecycle
  results after another owner takes over.
- Lane lifecycle hooks may repeat after lease recovery and therefore must be
  idempotent reconcilers rather than one-shot external effects.
- A crashed worker's running task is reclaimed only after its lease expires;
  the replacement invocation consumes another attempt and may repeat effects.
- A malformed historical running row without a lease deadline is marked failed
  during task-runtime initialization rather than being retried unsafely.
- Retried tasks become failed when their lane's per-step attempt budget is exhausted;
  lifetime invocation count is not that budget.
- Conflicting idempotency intents reject the submission batch unless conflict
  ignoring was explicitly selected.
- An unavailable declared lane falls back to `default` with a warning unless
  `RequireConfigured` is selected. Persisted nonterminal work in a removed lane
  blocks startup until it is explicitly reassigned.
- `resume` returns `false` for absent, non-suspended, or cancelled tasks. Responses
  arriving before suspension commits must be retried by the application.

## Current Limitations

- No exactly-once guarantee.
- No retained topic events.
- No durable per-attempt audit history.
- Manual Flow supports sequential, nested, and homogeneous parallel `all` joins.
  Optional Pravah graphs use the same runtime; graph parallelism is not automatically
  converted into durable `all` groups.
- No first-completion `select`, heterogeneous `all`, or per-child `all` options.
- Child input types are registered, but their output types are not associated at
  compile time; a mismatched continuation result type fails decoding at runtime.
- Result and resume envelopes are bounded to 32 KiB, including all joined results.
- External resume cannot safely interrupt a parent waiting for its children.
- `MemoryTaskStore` is not durable and is not for production task queues.
- SQLite is intended for embedded, local, and single-process task execution.
