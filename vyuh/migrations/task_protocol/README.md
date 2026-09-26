# Coordinated task protocol upgrade

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
5. Deploy handlers using `TaskState::complete(value)` and
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
