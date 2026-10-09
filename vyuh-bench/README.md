# Vyuh Bench

This workspace member compares Vyuh against Axum, Rocket, Actix, FastAPI, and
DRF with two tiers:

- Tier 1: raw HTTP/API boundary with SQLite.
- Tier 2: Postgres-backed live operations app with scheduled rollups and live
  delivery.

Each framework implementation is intentionally a single application source file.
Shared SQL, seed data, load scripts, and reporting scripts are outside LOC
counts.

## Layout

| File | Purpose |
| --- | --- |
| `src/bin/t1_*.rs` | Rust Tier 1 implementations |
| `src/bin/t2_*.rs` | Rust Tier 2 implementations |
| `python/t1_fastapi.py` and `python/t2_fastapi.py` | FastAPI implementations |
| `python/drf_t1/` and `python/drf_t2/` | Folder-based DRF implementations |
| `sql/t1_sqlite.sql` | Tier 1 schema and seed |
| `sql/t2_postgres.sql` | Tier 2 schema |
| `scripts/loc.py` | LOC counter for implementation files |
| `scripts/report.py` | Markdown summary and chart generator |

Generated output is kept outside the repository in
`~/Codex/vyuh/benchmarks/http/results/`: `raw/<timestamp>/` contains measurements,
`charts/<timestamp>/` contains SVG/PNG charts, and `summary.md` contains the report.
Set `VYUH_BENCH_RESULTS` to an absolute directory to override that location in
native, Docker, or Compose runs. Do not commit generated results or database files.

## Running

Run the full benchmark in Docker with one command:

```sh
./vyuh-bench/scripts/docker-run.sh
```

This builds a single container image with Rust, Python, Postgres tools, and the
PNG renderer. The container runs `scripts/run_bench.py`, starts an ephemeral
local Postgres cluster inside the container, writes raw results back to the host
under the external results directory, refreshes its `summary.md`, and writes
chart assets under its `charts/<timestamp>/` subdirectory.

Docker defaults to the Postgres-backed Tier 2 benchmark:

```sh
BENCH_TIERS=t2 ./vyuh-bench/scripts/docker-run.sh
```

Run both tiers explicitly when you also want the Tier 1 SQLite raw-boundary
baseline:

```sh
BENCH_TIERS=t1,t2 ./vyuh-bench/scripts/docker-run.sh
```

If your Docker installation includes Compose, this equivalent command is also
available:

```sh
docker compose -f vyuh-bench/docker-compose.yml up --build --abort-on-container-exit
```

Tier 1 Vyuh uses SQLite:

```sh
cargo run -p vyuh-bench --features vyuh-sqlite --bin t1_vyuh
```

Tier 2 Vyuh uses Postgres:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/vyuh_bench \
cargo run -p vyuh-bench --features vyuh-postgres --bin t2_vyuh
```

Other Rust bins do not require Vyuh features:

```sh
cargo run -p vyuh-bench --bin t1_axum --release
cargo run -p vyuh-bench --bin t2_axum --release
```

Python dependencies are listed in `requirements.txt`. Run Python apps with
production-style servers, no reload/debug mode:

```sh
uvicorn t1_fastapi:app --app-dir python --host 127.0.0.1 --port 8000
PYTHONPATH=python gunicorn drf_t1.wsgi:application --bind 127.0.0.1:8000
```

## Methodology

- Use the same machine, OS settings, Postgres version, Postgres config, seed
  data, and connection pool size.
- Recreate the database before each run.
- Run Rust apps as release binaries.
- Run Python apps as single-process/single-worker baselines first.
- Warm each server before measurement.
- Run each scenario at least five times and report median with min/max.
- Store raw JSON/CSV under the external results directory's `raw/<timestamp>/`.
- Store reusable SVG/PNG charts under its `charts/<timestamp>/` with a
  stable mirror at `charts/latest/`.

Recommended scenarios:

- `GET /health` at concurrency `1`, `16`, `64`, `256`.
- Tier 1 `POST /echo` and `GET /items/{id}` at the same concurrency levels.
- Tier 2 read, write, live-subscribe, and mixed workloads.
- Tier 2 scheduler drift while write and live-subscribe workloads are active.

## LOC Rules

Count only each single app source file, except DRF where the app folder is
counted because that is Django's natural project shape. Exclude comments, blank
lines, shared SQL, seed data, harness scripts, and reporting scripts. Report:

- total implementation LOC,
- scheduler/live-delivery LOC,
- add-on dependencies needed to match the feature set.

## Interpreting Results

The comparison should separate two claims:

- Tier 1 shows raw web boundary overhead, where Vyuh should track close to Axum
  because Vyuh is a thin Axum layer.
- Tier 2 shows framework surface area for scheduled work, signals/fanout,
  channels/live delivery, operation metadata, and introspection.
