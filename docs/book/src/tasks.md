# Tasks

> Breaking upgrade: existing lease-owned stores must stop all workers and writers
> and apply the destructive task-reset and node-schema migrations before restart.
> Old tasks and their results are deleted. See [upgrading](#upgrading-existing-task-stores).

Vyuh tasks combine typed background work and durable workflow orchestration in
one runtime. A task has one of two kinds: **Work** handlers are asynchronous and perform effects; **Flow**
definitions are immutable and advance synchronously to the next durable transition. Both return
typed results and use the same task records, lanes, ownership, recovery, and
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

## Runner deployment mode

```rust
use std::time::Duration;
use vyuh::tasks::{TaskConf, TaskMode};

// Default: multiple runners may share the store.
let clustered = TaskConf::default()
    .mode(TaskMode::Clustered)
    .heartbeat_interval(Duration::from_secs(12));

// Deployment MUST prevent overlapping runners, including during restarts.
let standalone = TaskConf::default().mode(TaskMode::Standalone);
```

All runners register their fresh UUID before claiming. Clustered runners
periodically update their node row with database time, even while idle. The interval is independent of task
polling and must be between one millisecond and seven days. Heartbeats use the
existing database concurrency budget, not handler concurrency. No node query is
added to submission, claims, or flushes. Standalone performs one node-registration
insert at startup and no periodic heartbeat or retirement queries. In both modes,
six heartbeat intervals also set the database-contact watchdog described below.
Standalone worker startup
returns all retained running tasks to pending with one database update, after
ordinary worker setup and before registration and claims. The previous runner must have stopped.
Clustered startup never resets another incarnation's claims.

Each runner gets a fresh UUID in both modes. A shared store with multiple
runners is clustered even if each runner serves different lanes. Mode changes
require stopped runners and a coordinated deployment; Vyuh does not enforce
standalone exclusivity or detect mixed modes.

Each successful clustered heartbeat performs two separately committed statements:
refresh its own node, then delete stale peers. The self-update releases its lock
before peer cleanup begins. A clustered runner deletes peers older than **six
heartbeat intervals** (72 seconds by default). All clustered runners must use the
same interval. The task ownership FK clears deleted nodes' `node_id` values;
ordinary claims reclaim eligible ownerless Running tasks without a recovery scan.
Counters, checkpoints, resume input and cancellation intent are preserved.
Task and lane fencing still reject stale outputs; external effects can repeat.

A heartbeat matching no node aborts task/hook futures and **exits that runner**,
without flushing, giving back claims, or registering again. Other application
services are not terminated. Already-issued SQL may still finish server-side.
Transient database errors recover at normal poll boundaries; permanent errors
stop the runner. Heartbeats
continue during graceful draining; node rows remain after shutdown until a peer
retires them. Standalone does no periodic cleanup. Stalled DB calls, FK cleanup,
polling and lane gates can delay recovery beyond the cutoff. Existing lane-lock
coordination remains independent. Per-query timeouts remain deferred; the
database-contact watchdog can still exit a runner with a stalled operation.

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
- `status` and `cancelled`: authoritative lifecycle status and a persisted cancellation marker.
- `attempts` and `step_attempts`: lifetime and current-step invocation counts.
- `parent_id` and `root_id`: nullable lineage assigned to spawned children.
- `kind`: `work` or `flow`, inferred from the registered handler.

Ordinary submission leaves lineage unset. `spawn` and `all` derive lineage in the
accepting store transaction. Kind is a registration capability boundary, with no
public setter or separate scheduling/recovery policy.

Each accepted claim supplies an immutable snapshot to the handler:

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
store claim/insertion -> lane queue -> handler -> shared output buffer -> atomic store commit
```

Returning a value or constructing an outcome is not a commit. Only the store
mutates authoritative task state. New children and resumed parents become eligible
when the output transaction commits. Immediately ready submissions/children and
matching cached scalar Flow resumptions can enter a local lane queue after commit
acknowledgement without another claim. Other work uses independently poll-paced
claims. Nothing executes synchronously from a parent or child handler. Another worker may claim ownerless committed work afterward;
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
Both use the same statuses, lanes, lane locks, node ownership, attempt counters,
cancellation, and atomic outcome flushes. Work concurrency and rate limits do not
apply to synchronous Flow advancement. Neither executes a child or resumed parent
recursively; committed input delivery or an ordinary claim schedules its invocation.

### Task placement and Flow caching

Ordinary lanes default to `sticky(true)`: prefer local child delivery and cached
Flow resumption when capacity is available. Stickiness never pins waiting work
to a node. Only Running tasks have execution owners; retries, sleeps, suspended
checkpoints and overflow children are ownerless.

Immediately ready children may be persisted as Running and delivered locally
after commit acknowledgement. Otherwise, any eligible node may claim them.
Work still obeys its concurrency and rate limits. Locked lanes retain their
existing exclusive lane-owner gates and never bypass them through local delivery.

```rust
TaskLaneConf::new(CHECKOUT, 8)
    .sticky(true)
    .flow_cache_bytes(8 * 1024 * 1024);
```

Use `.sticky(false)` to disable local child/cache preference. Zero cache bytes
disables Flow caching without disabling local child delivery. The lane-local byte
LRU contains immutable snapshots only; eviction neither loses work nor requires
a database update. Cache presence is not execution ownership. A cached scalar
resume must match the authoritative parent's suspended checkpoint and attempt;
the store establishes fresh Running ownership in the child-completion transaction.
A missing/stale cache or exhausted delivery capacity leaves an ownerless Pending parent.

External resumes, scheduled sleeps, `all` result assembly and coordinated lanes
retain normal claiming. Claims select ownerless Pending tasks and abandoned
ownerless Running tasks. Outcome fencing still reads authoritative store state;
caching is not a promise of completely read-free workflows. Flow advancement
remains bounded by queue/output pressure and must be short and non-blocking.
Node retirement clears abandoned Running ownership through the existing FK.
Upgrades from durable affinity require stopped workers/writers and the
`0017_local_sticky` migration; no checkpoints, results or attempts are rewritten.

PostgreSQL runners subscribe to advisory generic and node-specific wakeups using
Vyuh's LISTEN/NOTIFY transport. Committed pending submissions, actual parent
readiness, external resumption and stale-node retirement wake normal polling.
Ownerless pending work uses generic hints; directly admitted Running work needs
no readiness notification. Notifications contain only a lane name (or a broad
rescan hint), never task data.
They are emitted inside the existing write statement and delivered after commit;
they neither bypass readiness/rate/lane gates nor acknowledge task execution.
Intermediate `all` completions do not wake the parent.

Each PostgreSQL runner needs one additional dedicated, session-capable database
connection; producer-only sites need none. The configured connection endpoint must
support session-level LISTEN; a transaction-only pooling endpoint is insufficient.
Reconnection triggers a rescan, and fallback polling remains the
correctness path when hints are missed. Other backends retain polling and benefit
from sticky child placement without PostgreSQL dependencies. Stickiness reduces
cross-node handoffs but can concentrate work; use non-sticky child lanes when
cross-node distribution is more important than locality.

Immediate submission reserves bounded local delivery before committing ownership.
Full/closed channels, producer-only sites and pre-start task handles use Pending
insertion. A caller dropped during commit, or a lost submission acknowledgement,
may leave undelivered owned work for the existing node/startup recovery path.

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
Factories and dispatcher construction must not perform blocking I/O.

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
the same node-ownership, retry-budget, and backoff rules as Work. A returned error or panic
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
retries or abandoned executions, becomes `Err(TaskFailure)` with the child's ID. A retry,
sleep, or suspension is not terminal and does not resume the parent. The child's
result is retained on its task row and copied to the parent atomically.
Applications own any archive that must outlive task cleanup.

Child insertion and terminal-child parent resumption commit in the same
transaction as the originating outcome. New children and resumed parents are
eligible through an independently poll-paced claim after commit, never synchronous
invocation. An already-eligible concurrent claim may observe the committed work;
claims and flushes do not share a scheduler transaction or a wave barrier.

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
This is separate from concurrency, lane claim sizes, and `commit_batch_size`. The full combined
resume JSON envelope must fit **32,768 bytes**, even when individual child results
fit. Overflow or missing/malformed member results become an outer failure for
the parent; children keep their own results. Prefer application-owned artifact
references for large outputs. Decoding into the requested child output type still happens through
the normal continuation extractor.

The store groups accepted terminal child completions by parent and bulk-updates
their durable counters. Completion does not read join membership or sibling
results. It collects results only when the satisfied parent is actually claimed,
persists `resume_input`,
and clears the wait atomically. Crash recovery then reuses that persisted input.
Groups use normal lanes, scheduling, node ownership, retries, and cancellation;
Work children obey Work rates and concurrency, while Flow advancement is exempt.
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
use vyuh::tasks::{FlowConf, FlowError, PravahDispatcher, WorkRequest};

#[bundles::flow(dispatch = AppDispatcher)]
fn checkout(root: pravah::Flow<Checkout>) -> pravah::Flow<Confirmation> {
    build_checkout(root)
}

// Equivalent direct registration:
bundles::flow(checkout, FlowConf::new("checkout")
    .dispatch::<AppDispatcher>().step_limit(256));
```

Factories receive symbolic roots, optionally preceded by `PartialSite`, never
submitted input. The framework compiles once; factories may also return an
already compiled definition. Each invocation creates an isolated temporary VM
and restores its snapshot from the ordinary task continuation. Factory or dispatcher
errors/panics prevent site construction. A pure graph can omit dispatch; ordinary
external suspension also works without a dispatcher.

Opaque returns must expose the matching preparation bound: `impl IntoFlow<Input>`
without dispatch, or `impl IntoFlow<Input, AppDispatcher>` with that dispatcher. A manual
factory may return `impl Flow<Input = Input>`.

A reusable dispatcher **routes requests, not results**:

```rust,ignore
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct AgentJob {
    #[schemars(with = "serde_json::Value")]
    request: pravah::AgentRequest,
}

struct AppDispatcher;
impl PravahDispatcher for AppDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }

    fn agent(&self, request: &pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Ok(WorkRequest::new(AgentJob {
            request: request.clone(),
        }))
    }
}
```

The dispatcher is constructed once per selected type per site and shared across
definitions. `suspend(&Suspension)` may return `Some(WorkRequest)` or `None` for
external waiting; its default is `None`. `WorkRequest::options(...)` validates
scheduling options and rejects conflict adoption. Targets must be registered
Work, never Flow. Construction and routing cannot submit work: the snapshot and
child are committed through the existing atomic spawn path.

Missing dispatch for a statically declared Agent fails site construction, including
nested graphs. A dynamically encountered Agent without dispatch returns
`FlowError::MissingDispatcher`.

Register a Work handler accepting `Data<AgentJob>` and returning the **entire
`AgentResponse`** from an explicitly configured `AgentExecutor`. Its context and
handler registry must match the graph. When building that registry separately,
use the same symbolic root as the factory: `checkout(pravah::Flow::root()).finish::<Checkout>()`.
`pravah::compile(checkout)` chooses a different root name and can produce different
handler keys. Build registries once, not per task. Vyuh installs no universal executor.
The schema annotation above permits a typed request wrapper without weakening
`DataValue`; protocol validation still occurs in Pravah.

An Agent response containing an inner `AgentError` is still a successful Work
output: return it unchanged, including history/compaction acknowledgements from
stages that succeeded before the error. Do not flatten it into `WorkError`, which
would discard that evidence. A terminal Work failure instead becomes an Agent
response with code `vyuh_task_failure`, the safe diagnostic, and optional originating
task ID in details. Suspension Work must return its
declared resume type; failed suspension Work or failed external resume terminates
the parent. There are no result-conversion callbacks. Only Vyuh's outer persisted
`Result` is unwrapped: a successfully returned domain `Result` remains data.
Malformed or schema-incompatible values fail the Flow explicitly.

Use typed suspensions for ordinary application/network work, rather than encoding
it as an Agent request. Agent configuration, controller and tool callbacks execute
in Work through the selected executor, not during synchronous Flow advancement.

`PravahDispatcher::history_policy()` defaults to `HistoryPolicy::default()` (all
flags disabled). Override it to opt into `persist`, `load`, or `compact`, and provide
the required store/compactor on the Work executor. Vyuh applies the policy only
when starting an execution; restored snapshots retain their saved policy. History
I/O belongs to Work. Missing required services fail explicitly; Vyuh creates no
history manager or archive. Large loaded history and acknowledgements must still
fit the task result/resume limit.

Agent/suspension boundaries checkpoint immediately. Pure instructions run locally
up to `step_limit` (default 256, valid 1–10,000); exhaustion checkpoints through
zero-delay sleep and resumes on a later poll. A boundary emitted by the final
instruction is handled before yielding. The limit cannot preempt one blocking
instruction. Graph parallelism is not automatically translated to task `all`.

Snapshots obey the existing checkpoint limit; final results and resume envelopes
still have the fixed 32,768-byte limit. Snapshots and Agent payloads can contain
credentials or sensitive data: apply the same storage/access controls as inputs.

The runtime does not compute or verify a deployment fingerprint; unused Flow revision and compatibility APIs have been removed.
Keep worker configurations consistent and stop incompatible workers and writers
before changing persisted checkpoint representations. Retained manual checkpoints
must match the new implementation; they are never reinterpreted as Pravah snapshots.
Pravah's own snapshot checks remain. Version-based task admission is not implemented.

The integration uses the published Pravah 0.4.24 crate. Earlier Fetch-based snapshots
are incompatible with its Agent protocol. Drain old executions before upgrading;
there is no automatic checkpoint conversion, task deletion, or task-table migration.

## Retained Results

`WorkState::complete(value)` and `FlowState::complete(value)` retain typed values
until the handler returns. The framework serializes a successful value and stores it as
`{"Ok": value}` on the completed task. Ordinary unit-return handlers retain
`{"Ok": null}`. Retrying and terminal failures retain `{"Err": TaskFailure}`;
the task status distinguishes a retry from terminal failure. Successful suspend,
sleep, spawn, and all checkpoints clear the previous result. Claims preserve it. This is the latest result, not an attempt history.

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
durable task row, attempt count, execution owner, retry policy, and terminal outcome.

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
site.tasks().cancel(receipt.id()).await?;
```

`cancel` returns `Result<(), TaskRuntimeError>` after the store transaction has
acknowledged terminal cancellation. Pending, suspended, or running tasks become
`Failed`, retain `cancelled = true`, and store `Err(TaskFailure)` with the task ID
and message `Task cancelled`. Missing and already-terminal tasks are idempotent
no-ops. `TaskInfo::cancelled()` distinguishes a cancelled failure from other failures.

Cancellation uses the same atomic commit machinery as handler outputs; there is
no intermediate intent update or later cancellation flush. It does not wait for
lane polling, rates, or lifecycle hooks. Once cancellation commits, later handler
outputs are stale and cannot complete, retry, checkpoint, or spawn from that task.
If terminal completion commits first, cancellation does nothing. Cancelled tasks
cannot be resumed.

Store errors propagate. An acknowledgement error can occur after the database
committed; retrying cancellation is safe and idempotent. Successful cancellation
does **not** acknowledge that an already-running handler has stopped.

A cancelled single child delivers its failure to its suspended `spawn` parent
atomically; the parent continues through committed input delivery or a later claim. Within `all`, cancellation
counts as one terminal child, but the parent still waits for every other member
before receiving the ordered results.
Cancelling a parent does not cancel its children, and their later results cannot
revive it. Normal terminal cleanup applies to the cancelled task.

After acknowledgement, the submitting dispatcher sends a best-effort local
notification. A runner removes matching items still in its lane queues without
creating another output or changing uncommitted accounting. Other queued items
keep their order; no execution permit is needed for a skipped item. There is no
retained cancellation cache or per-task lookup on the execution hot path.

**Cancellation never interrupts a running handler**, single or batch. Its later
output is rejected as stale. Batch siblings complete normally;
cancelling one member does not deliberately replay the others. Execution permits
remain held until completion collection, just as for ordinary work. A hung
handler can delay lane idle and graceful shutdown, but not durable cancellation
or parent delivery. A resumed parent may therefore run before its cancelled
child's external effects finish. Ownership loss and runner termination retain
their separate abort behavior.

Notifications are local and bounded, not cluster broadcasts. A task on another
runner, a claim response still in flight, or notification lag can therefore
allow already-claimed work to execute. Its output cannot overwrite the terminal
row. Cancellation cannot undo external effects. Parent execution remains subject
to ordinary polling, rates, concurrency, batch thresholds, and lane hooks; there
is no cancellation scan or watcher.

Task snapshots held by runners and handlers remain immutable. All authoritative
mutations, including cancellation resolution, happen inside the store. Existing
deployments must follow the [coordinated task-store upgrade](#upgrading-existing-task-stores)
with workers and writers stopped.

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

Use `submit_many` to enqueue up to 10,000 inputs in one store transaction,
independently of claim, commit, and ingress sizes. Larger batches are rejected
before any input is written. A full ingress channel falls back to durable Pending
insertion rather than rejecting a submission. Use
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
bounded local queue and per-worker Work concurrency quota. One dispatcher rotates
the lanes fairly. Work concurrency bounds effectful invocations; shared resident
input/output accounting also bounds Flow advancement.

These controls have different jobs:

| Control | What it bounds or groups |
|---|---|
| `TaskConf::concurrency(n)` | Work invocations through collection of their outcomes into the shared buffer |
| `TaskConf::db_concurrency(n)` | Runner-managed database operations through application of their responses; default 1 |
| `TaskLaneConf::new(lane, n)` | Per-runner lane Work concurrency and ordinary Work refill capacity |
| `TaskLaneConf::claim_batch_size(n)` | Maximum rows per lane claim; default 1,024 |
| `TaskConf::commit_batch_size(n)` | Maximum outcomes per shared flush; default 1,024, not output-buffer capacity |
| `TaskConf::ingress_capacity(n)` | Direct-submission channel capacity; default 4,096 |
| `#[bundles::work_batch]` | Matching, eligible inputs already queued locally; never waits |
| `TaskLaneLock::new(n).deadline(duration)` | Owner-held accumulation threshold before claiming a cohort |
| `TaskConf::max_all_children(n)` | Maximum fan-out per `all`, not execution concurrency |

All three size settings accept 1–10,000. A lane lock's cohort size must fit that
lane's claim batch size. Database bulk writes are chunked internally for backend
parameter limits; channel capacity does not control SQL chunk size.

Increasing a claim batch size does not override concurrency, rates, poll gates,
or the availability of ready work. Ordinary lanes keep independent input queues;
all lanes share the outcome buffer. Fan-out flushes additionally bound expanded
child writes without splitting an `all` group across transactions.

Work-only queue prefetch uses a fixed hysteresis rule: Vyuh refills a lane only when its
queued work is below half of that lane's concurrency quota, then claims up to
the lane's normal free capacity. Refill capacity subtracts running invocations,
queued tasks, and completed-but-uncommitted task outcomes from the lane quota.
This applies to every outcome, not just fan-out. Batch completion counts every
item until its outcome is acknowledged, although execution uses one handler slot.
Claims, flushes, and lane coordination are independent database operations.
A lane with no refill capacity is skipped without querying it. Pending claims
reserve capacity; buffered and in-flight outputs retain refill pressure until acknowledged.

Execution and completion collection continue while database calls are pending.
The process-local pipeline is:

```text
DB claim → lane queue → execution → completion channel → shared output buffer → DB commit
```

Work completion messages carry their global and lane permits; the collector releases
them only after buffering all accepted outcomes. Database permits are separate,
and external submissions and inspections do not consume that runner budget.

One FIFO output buffer serves all lanes. Its normal flush interval is independent
of input-lane idle and rate deadlines. With `db_concurrency` above one, eligible
claims and flushes can overlap. FIFO describes admission, not transaction commit
order. A flush never recursively executes a child or forces an immediate claim.
`commit_batch_size` is an upper bound, not a minimum: a due flush can send a partial
batch. It does not wait for every lane or for a full buffer.

Transient database errors retain their request and retry no earlier than the next
poll interval. An uncertain commit is reconciled using its retained batch and
current ownership/attempts before anything is dispatched or acknowledged.
Permanent database and internal protocol errors stop the runner.
Graceful shutdown stops new claims
and queued dispatch, lets executing handlers finish, and persists their outputs.
After all admitted database operations settle, one update returns this node's
remaining running claims to pending. Transient give-back failures retry on poll
boundaries; successful shutdown is reported only after acknowledgement.

A death watchdog applies while database operations are executing or unresolved.
It expires after six heartbeat intervals without a successful runner database
operation (72 seconds by default). Any acknowledged operation, including a
heartbeat, resets it. Idle standalone runners have no armed watchdog and perform
no extra liveness SQL. A successful heartbeat does not resolve another failed
operation or conceal its degraded health.

Expiry or a confirmed missing node stops the task runner, not the whole
application. Local work is discarded as unacknowledged; there is no automatic
re-registration. Already-issued SQL and external effects may still finish.
Ordinary successful operations gain no recovery queries. Error-path current
reads can wait for database locks, while the runner continues execution,
collection and independent operations.

Startup reclaim and shutdown give-back preserve checkpoints, resume input,
results, cancellation intent, join state, and retry counters. They clear task
ownership and advance Running tasks' internal readiness timer using database time.
Waiting Flow checkpoints retain their status and scheduled readiness. Returned
queued tasks retain their already-charged claim attempt; the next ordinary claim
still enforces retry exhaustion and cancellation. These boundary updates add no
hot-path queries and do not release or bypass durable lane locks: existing
token-fenced coordination and lease expiry still apply. External effects are not
undone. Clustered abandonment is handled by heartbeat retirement and ordinary
claims. Recovery preserves the same ownership fencing and atomic workflow writes.

```rust
use vyuh::prelude::*;
use std::time::Duration;
use vyuh::tasks::{TaskConf, TaskLane, TaskLaneConf, TaskRate,
    TaskRetry, DEFAULT_TASK_LANE};

const EMAIL: TaskLane = TaskLane::new("email");
const EXPORTS: TaskLane = TaskLane::new("exports");

let tasks = TaskConf::default()
    .concurrency(10)
    .commit_batch_size(100)
    .ingress_capacity(250)
    .poll_interval(Duration::from_secs(1))
    .fallback_poll_interval(Duration::from_secs(300))
    .lease_duration(Duration::from_secs(300))
    .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 6).claim_batch_size(100))
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
counter. Retries and abandoned-task reclaims do not reset it.
Both rate controls apply to Work, not Flow advancement. `rate_limit` is an
inexpensive in-memory token bucket owned by the local site runner.
`global_rate_limit` coordinates Work starts across workers sharing a durable
task store. Configure either one independently, or configure both when each
start must satisfy local smoothing and a shared external quota. Global permits
are reserved with claimed rows in the same transaction and in batches, so a
high-throughput lane does not require one rate-state write per task. The memory
store coordinates a global limit only among runners sharing that in-process
store. Restarting a runner restores its local burst, and adding processes
multiplies a local-only limit.

### Tuning Fan-Out And Batching

`all` is durable fan-out, not a single bulk handler call. A parent that creates
`N` one-step, non-batched children and then completes normally needs `N + 2` invocations,
before retries or additional checkpoints. Child insertion, result persistence,
parent counters, and claim-time result collection all have a cost. Completion
updates are batched by parent; there is no required SQL statement per child.

Tune the actual child lane before increasing `max_all_children` or `commit_batch_size`:

- Lane and global concurrency, rate permits, and uncommitted outcomes constrain
  refill capacity. A large claim bound cannot override those limits.
- `db_concurrency` permits overlapping eligible operations, but also increases
  demand on the database pool and database locks. Leave capacity for application
  queries; increasing it is not a guaranteed throughput improvement.
- A shorter poll interval reduces scheduling delay but can create more partial
  claims and flushes. Measure completed workflows as well as individual tasks.
- Use `work_batch` when the effect itself benefits from processing queued inputs
  together. Each item still has its own result, retry policy, and durable record;
  batching does not bypass the join or its combined resume-size limit.

For diagnosis, compare handler duration with queue and commit latency, actual
claim/flush sizes, and database contention. Keep parent and child lane capacity
visible separately. No setting removes the cost of persisting each child.

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
over after expiry, while the token prevents a stale worker from renewing lane ownership,
committing outcomes, applying hook results, or releasing the replacement's
lease. Handler and hook execution never holds a database transaction.
`TaskConf::lease_duration` controls this lane lease only; individual tasks use
node ownership and heartbeat-based abandonment recovery instead.

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

Lane lease renewal uses the central database-work pipeline and its semaphore.
The scheduler includes normal polling, useful-work, fallback, and half-lane-lease
deadlines; there is no separate per-lane poller or task-lease renewal stream.

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

## Adaptive Polling And Ownership

`poll_interval` is the short backlog interval. A lane whose candidate query
fills its requested batch is revisited after this interval when it has capacity.
`fallback_poll_interval` is the maximum idle recheck interval. Future
`ready_at`, lane-lease expiry, and rate-token deadlines wake the runner at their
store-relative database time when they are earlier. Deadlines are tracked per
lane, so activity in one lane does not force an idle or rate-limited lane to
query early.

Local submission, cancellation, resume, handler completion, and outcome commit mark the local
runner for its next eligible operation; they never create an early background store
query. PostgreSQL also supplies generic and owner-specific committed wake hints
through LISTEN/NOTIFY. These shorten discovery without bypassing the normal poll
gate. On other backends, or when a hint is missed, work submitted by another process
may wait until fallback polling; choose that interval accordingly.

Individual tasks have node ownership, not timed leases. Node retirement clears
ownership, after which ordinary claims can reclaim abandoned Running tasks.
Standalone startup instead returns prior running claims under its singleton
deployment contract. Reclaiming consumes another attempt and, for Work, a rate permit.
A stale node or attempt cannot commit an outcome.

When observability is enabled, Vyuh exports bounded-label task counters for
submission receipts and conflicts, claims and abandoned-task reclaims, handler starts
and lifecycle outcomes, node heartbeats, lane ownership loss, and store failures.
Batch handlers additionally expose invocation and item counters by registered
handler name.
Queue, handler, and outcome-commit durations are exported without dynamic
application-data labels. Handler and lane labels come only from the immutable
site registries.

## Runtime Health

Building a site configures its task store without querying task tables. This
keeps migration commands and producer-only sites independent of worker startup.
Submission does not lazily initialize workers. Starting the site runtime validates
retained task registrations and prepares rate/lane coordination rows, reclaims
standalone tasks, then registers the node before claims begin in both modes.
Startup failure prevents the worker from starting.

Task runtime health is updated from initialization and acknowledged runner database
operations; readiness checks never issue an additional task-store query. The default
requires successful task initialization and then tolerates transient operation
failures while recovery is pending:

```rust
use vyuh::tasks::{TaskConf, TaskReadiness};

let tasks = TaskConf::default()
    .readiness(TaskReadiness::startup_only());
```

For task-critical sites, make readiness fail after a consecutive failure
threshold. A successful database operation restores healthy status only once no
request remains awaiting retry or undergoing recovery:

```rust
let tasks = TaskConf::default()
    .readiness(TaskReadiness::after_failures(3));
```

`TaskReadiness::disabled()` keeps task health visible in metrics and the
console but excludes it from `/readyz`. Metrics expose the current readiness
gauge, consecutive store failures, and the last healthy database acknowledgement.
A successful heartbeat can reset the contact watchdog without clearing unresolved
operation failures. Terminal runner faults, retirement, and watchdog expiry mark
task health unhealthy even under `startup_only`; they do not restart the runner.
The console exposes only safe state and failure-class diagnostics.

## Stores

With a database backend feature enabled, Vyuh stores tasks durably:

- `postgres`: `vyuh_tasks`
- `mysql`: `vyuh_tasks`
- `sqlite`: `vyuh_tasks`

Durable stores use framework-owned tables for task lifecycle records,
node registrations (`vyuh_task_nodes`), idempotency ownership, per-lane rate
buckets, opt-in lane-owner leases, and durable schedule cursors.
The runtime has no runtime-policy table, deployment fingerprint checks,
or policy-row locks. Submission validation and task/lane ownership fencing remain.
Deployments are responsible for compatible worker configuration; version-based
admission is deferred. The lane-owner table has one primary-key row per configured locked lane and no foreign keys or secondary
indexes.

Claims and output flushes are independently paced, batched transactions.
Ordinary lanes never consult lane-lock storage. Optional rates, lane ownership,
and workflow transitions require additional bounded statements. Eligible ordinary
PostgreSQL claims combine selection, ownership assignment, returned snapshots,
and wake hints into one data statement inside a transaction. This is not a promise
of one statement for global-rate, locked-lane, or join-materialization paths, and
excludes transaction control. SQLite uses its transactional claim path.
Flushes apply transitions in Rust with batched Mool writes in one transaction;
they retain task ownership locks but do not read deployment policy.
Expired idempotency reservations are physically cleaned up periodically; logical
expiry still permits reuse immediately. Task renewal is replaced by node ownership;
the ownership FK adds engine-level validation and locking, not a per-claim node query.

Persistent task tables are migration-owned. Apply the application's Mool/Gaman
migrations before starting task workers; `Site::build` never creates or alters
task tables. This makes schema changes reviewable and prevents a replica from
changing production DDL during startup.

### Upgrading Existing Task Stores

Stop all workers and writers, back up, and integrate the backend-specific
[task-protocol templates](https://github.com/vivsh/vyuh/tree/main/vyuh/migrations/task_protocol)
`0013_reset_tasks` and `0014_node_pipeline` into the application's Gaman ledger.
This is a **destructive upgrade**: every old task, result, continuation, join and
idempotency reservation is deleted. Schedule cursors, rate balances and lane
lifecycle state are preserved. Existing lane ownership expires normally.

The schema migration replaces string task owners with nullable UUID node
ownership and its FK, and removes individual task lease columns and deployment
fingerprint storage. SQLite requires Gaman's predecessor-aware rebuild with FK
enforcement enabled; MySQL/MariaDB DDL is non-atomic. Both steps require offline
schema locks. Review application-specific foreign keys and indexes before applying.

There is no automatic downgrade, mixed-version operation, startup migration or
retained-task compatibility layer. Backups are the only recovery path for deleted
old tasks. Fresh installations generate only the current desired schema.

Public cancellation now returns `Result<(), TaskRuntimeError>` after terminal
persistence or an idempotent no-op; it never acknowledges stopped execution.
Task inspection exposes `node_id()` instead of an individual task lease deadline.
`FlowConf::revision` and `Flow::compatibility` have been removed because they
provided no admission guarantee. Keep workers' handler, checkpoint and lane
configuration compatible through coordinated deployments.

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
- Stale lane owners cannot renew lane ownership, commit outcomes, or apply lifecycle
  results after another owner takes over.
- Lane lifecycle hooks may repeat after lease recovery and therefore must be
  idempotent reconcilers rather than one-shot external effects.
- Clustered abandoned tasks become reclaimable after node retirement; lane gates
  and polling can delay execution. Standalone recovery requires the prior runner
  to have stopped. Reclaimed attempts may repeat external effects.
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
