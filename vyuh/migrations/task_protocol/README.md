# Coordinated task upgrades

## Current placement upgrade: local stickiness

Apply `0017_local_sticky` after `0016_sticky_tasks`, with **all workers and
writers stopped**. Sticky now means local delivery preference, not durable node
affinity. The migration clears `node_id` on non-Running rows and drops the
obsolete owned-Pending readiness index. It preserves Running ownership, task
status, scheduling, checkpoints, results, attempts, lineage and join metadata.
No tasks are deleted. Old/new placement runtimes must not share the store.

PostgreSQL/SQLite apply it atomically through the migration ledger. MySQL/MariaDB
DDL is non-atomic: if interrupted, inspect whether the data update and index drop
committed before reconciling the ledger. The data update is idempotent; the index
drop must not be blindly replayed. Back up before upgrading.

Historical `0015_sticky_flows` and `0016_sticky_tasks` identities are unchanged:
apply them in order before `0017` on retained installations. Fresh installations
use the desired schema directly and have no owned-Pending readiness index.
The ordinary node FK index remains for retirement cleanup. No runtime migration
or repair query is added, and notification transport is unchanged.

## Node-owned task pipeline upgrade


This is a **destructive, breaking upgrade** from the lease-owned runtime.
`0013_reset_tasks` deletes **every old task**, including pending/running work,
suspended workflows, join metadata, results and history. It also deletes their
idempotency reservations. There is no automatic downgrade or recovery of those
records: retain a verified backup if they may be needed.

1. Stop **all workers and writers**, including producer-only processes and
   task-managed emitters. Back up the database. Keep them stopped through both
   migrations; old and new runtimes cannot share the store.
2. Select your backend directory (`mysql` includes MariaDB). Integrate
   `0013_reset_tasks` and `0014_node_pipeline` into the application's existing
   Gaman ledger. The predecessor is the current task schema through
   `0012_flow_factories_protocol`; adapt that dependency to the actual application
   schema head **before first application**, never by editing applied identities.
3. Apply the reset transaction and its ledger entry atomically. Then apply the
   schema migration. It creates nodes, replaces `locked_by` with nullable UUID
   `node_id`, adds `ON DELETE SET NULL` ownership and its index, updates readiness
   indexing, and removes task-lease columns/index and fingerprint storage.
4. Deploy the new code, then start compatible workers and writers. Validate
   readiness and node registration. No migration runs during startup.

Schedule cursors, rate balances/refill timestamps, and lane lifecycle rows are
preserved. Existing lane leases expire through ordinary takeover. Do not reset
schedule cursors unintentionally: that can replay scheduled submissions.

The PostgreSQL readiness index uses `(lane_name, ready_at, created_at, id)` with
the partial predicate `node_id IS NULL AND status IN (0, 1)`. Keeping status out
of the ordering keys allows one ordered scan across pending and abandoned running
tasks. SQLite and MySQL retain their backend-specific index definitions.

The migrations take offline schema locks and must be rendered with the full
predecessor history. SQLite requires Gaman's context-aware table rebuild; do not
disable FK enforcement. PostgreSQL/SQLite schema migration is atomic. MySQL and
MariaDB DDL is non-atomic: after partial failure inspect which operations committed
and reconcile before retrying; do not blindly replay or mark the ledger complete.
The acknowledged task reset remains applied if the later schema migration fails.

These templates target the framework-owned schema. Review application-added
foreign keys, triggers and indexes before applying; unsupported dependencies
should fail visibly, not be removed with `CASCADE`. Fresh installations generate
only the current desired schema and do not run historical data conversions.

The following sections document **historical migrations only**. Their identities
and contents remain immutable for existing application ledgers. The current
runtime has no fingerprint compatibility methods, policy table or verification.

## Historical protocol upgrades (0001–0012)

These are Gaman migration templates, not startup migrations. Stop **all old task
workers and writers**, including processes that only submit or resume tasks.
Back up the database. Do not run old and new versions together.
Keep lane, rate, retry, and idempotency configuration unchanged through this
protocol upgrade. The migration retains a digest of the prior policy so new
workers can verify it, preserve global token buckets, and recover leased tasks.

1. Choose `postgres`, `sqlite`, or `mysql` (also MariaDB).
2. Integrate `0001_step_attempts.yaml` into your application's existing migration
   history, depending on its current task-schema head. If your generated schema
   migration already adds the column, use that migration instead.
3. Integrate `0002_resume_results.yaml` with a dependency on that schema migration.
   Assign it one stable migration identity and apply it through Gaman's ledger.
   Never rerun its raw SQL, rename an applied migration, or fake its application.
4. Integrate `0003_result_preflight.yaml`, `0004_result_columns.yaml`, and
   `0005_persisted_results.yaml` in dependency order. Do not edit or reapply the
   earlier migration identities on installations already using resume results.
5. Apply `0006_cancellation_column.yaml` and `0007_cancellation_protocol.yaml`.
   Existing rows receive `cancelled = false`. If your desired-schema migration
   already adds the column, use it instead of 0006 and adjust the dependency of
   0007 before first application. Existing result and resume bytes are unchanged.
6. Apply the Work/Flow and typed-return protocol templates below.
7. Apply the durable `all` templates below.
8. Deploy handlers using `WorkState::complete(value)` and
   `Option<Result<R, TaskFailure>>`, then start new workers.

The data migration and its ledger entry must commit atomically. For MySQL-family
databases, schema DDL is deliberately a separate non-atomic migration; the data
migration uses transactional tables. Verify your migration runner commits the
data and ledger entry in the same transaction before deployment.

Every non-null old value is wrapped, including `null`, arrays, and objects already
shaped like `{"Ok": ...}` or `{"Err": ...}`. Concatenation preserves the original
JSON bytes and never guesses whether a value was migrated. Invalid legacy JSON
must be repaired before migration. The existing runtime policy row receives a
protocol marker; new workers reject an unmarked old policy.

Suspended tasks start with zero step attempts. Other existing tasks conservatively
retain their lifetime attempt count as their step count. This upgrade is not
automatically reversible once new workflows have run.

## Persisted results and the 32 KiB limit

The result upgrade runs preflight before any column changes, then renames
`last_error` to `last_result`, and finally converts errors to JSON envelopes in a
ledger-backed data transaction. Existing resume envelopes are preserved byte for
byte; historical successful outputs remain unknown (`NULL`). The full
32,768-byte envelope fits ordinary text storage on every backend. The runtime
enforces that byte limit.

Run the backend's `result_preflight.sql` to identify incompatible task IDs,
including malformed PostgreSQL JSON (PostgreSQL 16+). Repair or explicitly archive
incompatible records before upgrading; the migration never truncates resume
values or converts them into task failures. Errors whose escaped envelope would
exceed 32 KiB also require explicit remediation. Back up these values first.

On a failed MySQL preflight, disconnect before retrying: its connection-local
temporary guard table is not transactionally rolled back. Keep all workers and
writers stopped throughout all three steps, including non-atomic MySQL DDL.
Never roll back only the rename after converting data. The runtime accepts only
the new protocol or a matching ledger-marked predecessor; it does not repair or
upgrade task data at startup.

## Store-owned cancellation

The cancellation upgrade requires the preceding result protocol. The new
ledger-tracked marker retains the predecessor policy digest, including when
upgrading straight through from older installations. New workers reject
unmarked predecessor policies. Run the schema and marker migrations with all
workers and writers stopped; MySQL/MariaDB column DDL is non-atomic and separate
from the transactional marker update. Startup never adds the column.

## Work and Flow classification

Apply `0008_work_flow_protocol` only after stopping all workers and writers and
explicitly choosing which handlers become synchronous `bundles::flow` registrations.
Continuable Work handlers may remain Work with the typed-return protocol below.
No schema column is added. The existing kind integers remain Work=0, Flow=1.

Insert an application-owned, ledger-tracked data migration before this template
and make the template depend on it. Its explicit handler-name list identifies
every converted Flow, across every retained status:

```sql
UPDATE vyuh_tasks SET kind = 1 WHERE name IN ('checkout', 'approve_document');
```

Do not select only suspended rows: pending, sleeping, running, and terminal
records also retain their handler classification. Preserve all other columns,
including checkpoint/result bytes, lineage, timestamps, leases and counters.
Do not infer classification from checkpoint JSON or task status.

Before applying, compare every retained active row with the application's
name/kind roster. Check that Work and Flow checkpoint and resume types remain
decodable; a checkpoint alone does not imply Flow. Report and resolve mismatches explicitly; do not clear
state or relabel unknown handlers. Repeat this preflight after reclassification
and before restarting any process.
Each backend includes `flow_preflight.sql`; replace its example roster with the
complete application registry. A zero-row result is necessary, but does not
replace application-specific decoding of checkpoint and resume types.

The template advances only recognized predecessor protocol markers and retains
their policy digest for startup verification. New runtime fingerprints include
registered name/kind pairs; mismatched deployments are rejected. Startup does
not migrate or repair task rows. Never run mixed old/new workers or writers.
Applied migration identities must not be edited or replayed.

## Typed returns and continuable Work

Apply `0009_typed_returns_protocol` after 0008, with workers/writers stopped.
This advances only the protocol marker to enable Work suspension. No task column,
result envelope, checkpoint, status, or schedule is rewritten. New workers verify
the exact predecessor policy digest; an unmarked v4 runtime is rejected.

Migrate handler signatures to `WorkState<T>` / `FlowState<T>`, infallible
`complete(value)`, and `Result<_, WorkError>` / `Result<_, FlowError>`.
Infrastructure APIs now return `TaskRuntimeError`. Keep input and output JSON
representations compatible with retained tasks. Work can extract Continuation and
suspend, but cannot sleep or spawn. No mixed-version deployment is supported.

## Durable all joins

Apply `0010_all_columns` followed by `0011_all_protocol` after 0009, with all
workers and writers stopped. The schema adds nullable `waiting_children` JSON
text and `remaining_completions` with a zero default; existing task payloads,
checkpoints, results, leases, lineage, and counters are unchanged. MySQL/MariaDB
uses MEDIUMTEXT to accommodate the maximum 10,000 UUID members. No indexes or
foreign keys are added. If an application schema migration adds these columns,
depend on it instead of applying 0010 twice.

The protocol template marks only recognized predecessors and preserves their
digest for runtime verification. New fingerprints include `max_all_children`.
Applied migration identities remain immutable, replay is ledger-controlled, and
startup does not repair schema. Do not run mixed-version workers or writers.
