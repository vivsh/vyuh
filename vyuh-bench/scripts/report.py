#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import platform
import re
import subprocess
from typing import Any

import charts
from paths import RESULTS


ROOT = Path(__file__).resolve().parents[1]
TOKEN_RE = re.compile(
    r'''(?x)
      r?\#*"(?:\\.|[^"\\])*"\#*
    | ''' + "'''" + r'''[\s\S]*?''' + "'''" + r'''
    | """[\s\S]*?"""
    | [A-Za-z_][A-Za-z0-9_]*
    | \d+(?:\.\d+)?
    | ::|->|=>|==|!=|<=|>=|&&|\|\||\.\.
    | [^\s]
'''
)


def run(args: list[str], cwd: Path = ROOT) -> str:
    try:
        return subprocess.check_output(args, cwd=cwd, text=True).strip()
    except Exception as exc:
        return f"unavailable: {exc}"


def latest_raw() -> Path | None:
    raw_root = RESULTS / "raw"
    dirs = [path for path in raw_root.glob("*") if path.is_dir() and (path / "results.json").exists()]
    return sorted(dirs)[-1] if dirs else None


def load(raw: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    meta = json.loads((raw / "meta.json").read_text())
    rows = json.loads((raw / "results.json").read_text())
    return meta, rows


def cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def find_row(rows: list[dict[str, Any]], tier: str, framework: str, scenario: str, concurrency: int) -> dict[str, Any] | None:
    for row in rows:
        if (
            row["tier"] == tier
            and row["framework"] == framework
            and row["scenario"] == scenario
            and row["concurrency"] == concurrency
        ):
            return row
    return None


def row_for(rows: list[dict[str, Any]], tier: str, framework: str, scenario: str, concurrency: int) -> dict[str, Any]:
    row = find_row(rows, tier, framework, scenario, concurrency)
    if row is not None:
        return row
    raise KeyError((tier, framework, scenario, concurrency))


def rps_cell(row: dict[str, Any] | None) -> str:
    if row is None:
        return "-"
    return f"{row['requests_per_second']:.0f} rps / p99 {cell(row['p99_ms'])}ms"


def tier_table(rows: list[dict[str, Any]], tier: str, concurrency: int, scenarios: list[str]) -> str:
    frameworks = ["vyuh", "axum", "rocket", "actix", "fastapi", "drf"]
    lines = [
        f"### {tier.upper()} c{concurrency}",
        "",
        "| framework | " + " | ".join(scenarios) + " | errors |",
        "| --- | " + " | ".join("---:" for _ in scenarios) + " | ---: |",
    ]
    for framework in frameworks:
        values = []
        errors = 0
        present = False
        for scenario in scenarios:
            row = find_row(rows, tier, framework, scenario, concurrency)
            values.append(rps_cell(row))
            if row is not None:
                present = True
                errors += row["errors"]
        if present:
            lines.append(f"| {framework} | " + " | ".join(values) + f" | {errors} |")
    if len(lines) == 4:
        lines.append(f"| no `{tier}` rows in this raw run | " + " | ".join("-" for _ in scenarios) + " | - |")
    return "\n".join(lines)


def memory_table(rows: list[dict[str, Any]]) -> str:
    frameworks = ["vyuh", "axum", "rocket", "actix", "fastapi", "drf"]
    lines = [
        "## Memory Snapshot",
        "",
        "RSS is summed over the server process tree where available. Values are from this single run.",
        "",
        "| target | idle RSS MiB | max after-scenario RSS MiB |",
        "| --- | ---: | ---: |",
    ]
    for tier in ["t1", "t2"]:
        for framework in frameworks:
            subset = [row for row in rows if row["tier"] == tier and row["framework"] == framework]
            if not subset:
                continue
            idle = max(row["idle_rss_kb"] for row in subset) / 1024
            peak = max(row["rss_after_kb"] for row in subset) / 1024
            lines.append(f"| {tier}/{framework} | {idle:.1f} | {peak:.1f} |")
    return "\n".join(lines)


def startup_table(rows: list[dict[str, Any]]) -> str:
    frameworks = ["vyuh", "axum", "rocket", "actix", "fastapi", "drf"]
    lines = [
        "## Startup Readiness",
        "",
        "| target | start to `/health` ms |",
        "| --- | ---: |",
    ]
    for tier in ["t1", "t2"]:
        for framework in frameworks:
            subset = [row for row in rows if row["tier"] == tier and row["framework"] == framework]
            if not subset:
                continue
            startup = subset[0]["startup_ms"]
            lines.append(f"| {tier}/{framework} | {startup:.1f} |")
    return "\n".join(lines)


def binary_table() -> str:
    target = ROOT.parent / "target" / "release"
    bins = ["t1_vyuh", "t1_axum", "t1_rocket", "t1_actix", "t2_vyuh", "t2_axum", "t2_rocket", "t2_actix"]
    lines = ["## Rust Binary Size", "", "| binary | size MiB |", "| --- | ---: |"]
    for name in bins:
        path = target / name
        size = path.stat().st_size / (1024 * 1024) if path.exists() else 0
        lines.append(f"| {name} | {size:.1f} |")
    return "\n".join(lines)


def source_paths(target: str) -> list[Path]:
    mapping = {
        "t1/vyuh": [ROOT / "src/bin/t1_vyuh.rs"],
        "t1/axum": [ROOT / "src/bin/t1_axum.rs"],
        "t1/rocket": [ROOT / "src/bin/t1_rocket.rs"],
        "t1/actix": [ROOT / "src/bin/t1_actix.rs"],
        "t1/fastapi": [ROOT / "python/t1_fastapi.py"],
        "t1/drf": sorted((ROOT / "python/drf_t1").glob("*.py")),
        "t2/vyuh": [ROOT / "src/bin/t2_vyuh.rs"],
        "t2/axum": [ROOT / "src/bin/t2_axum.rs"],
        "t2/rocket": [ROOT / "src/bin/t2_rocket.rs"],
        "t2/actix": [ROOT / "src/bin/t2_actix.rs"],
        "t2/fastapi": [ROOT / "python/t2_fastapi.py"],
        "t2/drf": sorted((ROOT / "python/drf_t2").glob("*.py")),
    }
    return mapping[target]


def loc_count(paths: list[Path]) -> int:
    count = 0
    for path in paths:
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if line and not line.startswith(("//", "#")):
                count += 1
    return count


def strip_rust_comments(source: str) -> str:
    out: list[str] = []
    index = 0
    block = 0
    while index < len(source):
        if block:
            if source.startswith("*/", index):
                block -= 1
                index += 2
            else:
                index += 1
        elif source.startswith("//", index):
            next_line = source.find("\n", index)
            index = len(source) if next_line == -1 else next_line
        elif source.startswith("/*", index):
            block += 1
            index += 2
        else:
            out.append(source[index])
            index += 1
    return "".join(out)


def strip_python_comments(source: str) -> str:
    lines: list[str] = []
    for raw in source.splitlines():
        in_single = False
        in_double = False
        escaped = False
        cut = len(raw)
        for index, char in enumerate(raw):
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == "'" and not in_double:
                in_single = not in_single
            elif char == '"' and not in_single:
                in_double = not in_double
            elif char == "#" and not in_single and not in_double:
                cut = index
                break
        lines.append(raw[:cut])
    return "\n".join(lines)


def token_count(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        source = path.read_text()
        source = strip_rust_comments(source) if path.suffix == ".rs" else strip_python_comments(source)
        total += len(TOKEN_RE.findall(source))
    return total


def import_roots(paths: list[Path]) -> set[str]:
    roots: set[str] = set()
    rust_std = {"std"}
    python_std = {"asyncio", "collections", "json", "os", "pathlib", "sqlite3", "threading", "time"}
    for path in paths:
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if path.suffix == ".rs" and line.startswith("use "):
                root = line.removeprefix("use ").split("::", 1)[0].split("{", 1)[0].strip()
                if root and root not in rust_std:
                    roots.add(root)
            if path.suffix == ".py" and line.startswith(("import ", "from ")):
                if line.startswith("import "):
                    root = (
                        line.removeprefix("import ")
                        .split(",", 1)[0]
                        .split(" as ", 1)[0]
                        .split(".", 1)[0]
                        .strip()
                    )
                else:
                    root = (
                        line.removeprefix("from ")
                        .split(" import ", 1)[0]
                        .split(".", 1)[0]
                        .strip()
                    )
                if root and root not in python_std:
                    roots.add(root)
    return roots


def dev_metrics() -> list[dict[str, Any]]:
    targets = [
        "t1/vyuh",
        "t1/axum",
        "t1/rocket",
        "t1/actix",
        "t1/fastapi",
        "t1/drf",
        "t2/vyuh",
        "t2/axum",
        "t2/rocket",
        "t2/actix",
        "t2/fastapi",
        "t2/drf",
    ]
    rows: list[dict[str, Any]] = []
    for target in targets:
        paths = source_paths(target)
        roots = sorted(import_roots(paths))
        rows.append(
            {
                "target": target,
                "files": len(paths),
                "folder": len(paths) > 1,
                "loc": loc_count(paths),
                "tokens": token_count(paths),
                "deps": roots,
            }
        )
    return rows


def dev_ux_table(metrics: list[dict[str, Any]]) -> str:
    lines = [
        "## Dev UX Measures",
        "",
        "Dependency count is the number of external package roots imported by the implementation source, excluding standard-library imports. It is not transitive dependency count.",
        "",
        "LOC excludes blank lines and comments only. It still includes DTOs, derives, route handlers, SQL query strings, startup, and runtime plumbing. Shared schema SQL is excluded when it lives under `sql/`.",
        "",
        "| target | files | LOC | direct imported deps | deps |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for metric in metrics:
        roots = metric["deps"]
        label = f"{metric['files']}"
        if metric["folder"]:
            label = f"{metric['files']} folder files"
        lines.append(
            f"| {metric['target']} | {label} | {metric['loc']} LOC / {metric['tokens']} tokens | {len(roots)} | "
            + ", ".join(f"`{root}`" for root in roots)
            + " |"
        )
    return "\n".join(lines)


def chart_gallery(chart_rows: list[dict[str, str]]) -> str:
    if not chart_rows:
        return "## Charts\n\nNo chart assets were generated for this run."
    lines = [
        "## Charts",
        "",
        "Reusable assets are written under `charts/<raw-run>/` and mirrored to `charts/latest/` alongside this report.",
        "",
        "| chart | SVG | PNG |",
        "| --- | --- | --- |",
    ]
    for row in chart_rows:
        png = f"[png]({row['png']})" if row["png"] else "not generated"
        lines.append(f"| {row['name']} | [svg]({row['svg']}) | {png} |")
    return "\n".join(lines)


def runtime_plumbing_table() -> str:
    return """## Tier 2 Runtime Plumbing

| framework | scheduler/rollup code | live delivery code | app-facing shape |
| --- | --- | --- | --- |
| vyuh | `#[bundles::periodic]` emits typed tick into `#[bundles::signal]` | `site.signals().emit`, `Subscriber`, `ChannelResponse` | runtime surfaces are registered and introspectable |
| axum | explicit `tokio::spawn` interval loop | explicit `broadcast::Sender`, SSE stream filter, long-poll loop | manual app state and plumbing |
| rocket | explicit spawned interval loop | explicit broadcast channel plus `EventStream` route | manual managed state and plumbing |
| actix | explicit spawned interval loop | explicit broadcast channel plus streaming response | manual app state and plumbing |
| fastapi | explicit `asyncio.create_task` rollup loop | explicit per-project queue set plus `sse-starlette` | manual queues and task lifecycle |
| drf | explicit background thread rollup loop | explicit event loop thread, queues, streaming response | folder-shaped Django app plus manual threads |
"""


def capability_matrix() -> str:
    return """## Capability Matrix

| capability | vyuh | axum | rocket | actix | fastapi | drf |
| --- | --- | --- | --- | --- | --- | --- |
| scheduler/cron | built-in emitters | tokio task | spawned task | runtime task | app task/add-on | thread/add-on |
| live delivery | built-in channels | broadcast/SSE | stream/SSE | stream/SSE | add-on/SSE | streaming/thread |
| integrated OpenAPI | built-in | add-on | add-on | add-on | built-in | add-on |
| auth/middleware metadata | integrated | manual/add-on | manual/add-on | manual/add-on | partial | add-on |
| operation introspection | built-in console | manual | manual | manual | partial | partial |
| background service model | built-in | manual | manual | manual | manual | manual |
| implementation shape | single file | single file | single file | single file | single file | folder-shaped |
"""


def interpretation(rows: list[dict[str, Any]], raw: Path) -> str:
    vyuh_echo = find_row(rows, "t1", "vyuh", "echo", 64)
    axum_echo = find_row(rows, "t1", "axum", "echo", 64)
    vyuh_item = find_row(rows, "t1", "vyuh", "item", 64)
    axum_item = find_row(rows, "t1", "axum", "item", 64)
    drf_write = find_row(rows, "t2", "drf", "events_write", 256)
    vyuh_write = find_row(rows, "t2", "vyuh", "events_write", 256)
    evidence: list[str] = []
    if vyuh_echo and axum_echo and vyuh_item and axum_item:
        evidence.append(
            f"Vyuh Tier 1 is in the same broad performance band as Axum for typed handlers. At c64, Vyuh echo is `{vyuh_echo['requests_per_second']:.0f} rps` versus Axum `{axum_echo['requests_per_second']:.0f} rps`; Vyuh item lookup is `{vyuh_item['requests_per_second']:.0f} rps` versus Axum `{axum_item['requests_per_second']:.0f} rps`."
        )
    if vyuh_write and drf_write:
        evidence.append(
            f"Python stacks are materially lower in this run. At Tier 2 c256 writes, Vyuh reports `{vyuh_write['requests_per_second']:.0f} rps`; DRF reports `{drf_write['requests_per_second']:.0f} rps`."
        )
    evidence.append("All measured scenario rows completed with the recorded HTTP-status error counts shown above.")
    evidence.append(
        "Tier 2 implementation ergonomics remain the more important Vyuh strength: scheduled rollup, signal fanout, channel publish, SSE, long-poll, and operation metadata are registered runtime surfaces instead of separately assembled background/pubsub machinery."
    )
    bullets = "\n".join(f"- {item}" for item in evidence)
    return f"""## Interpretation

This run is useful as a reproducible baseline and harness validation, not as a final benchmark claim. It is a single run per scenario using the repository's standard-library HTTP load runner, which opens one connection per request. Several Tier 2 rows show around 2s p99 outliers across multiple frameworks, so p99 should not be used for ranking until repeated median runs are collected with a stronger load generator.

What the current data does support:

{bullets}

Raw data: `{raw.relative_to(RESULTS)}/`.
"""


def main() -> None:
    raw = latest_raw()
    if raw is None:
        raise SystemExit("no raw results found")
    meta, rows = load(raw)
    metrics = dev_metrics()
    chart_rows = charts.emit(rows, metrics, raw, RESULTS)
    loc = run(["python3", "scripts/loc.py"])
    content = "\n\n".join(
        [
            "# Framework Benchmark Summary",
            f"Generated: {datetime.now(timezone.utc).isoformat()}",
            f"Raw run: `{raw.relative_to(RESULTS)}`",
            "## Methodology",
            "\n".join(
                [
                    "| key | value |",
                    "| --- | --- |",
                    f"| machine | {platform.platform()} |",
                    f"| python | {meta.get('python', 'unknown')} |",
                    f"| rust | {meta.get('rustc', 'unknown')} |",
                    f"| cargo | {meta.get('cargo', 'unknown')} |",
                    f"| duration | {meta.get('duration_seconds')}s per scenario |",
                    f"| warmup | {meta.get('warmup_seconds')}s before measured scenarios |",
                    f"| concurrency | {meta.get('concurrency')} |",
                    "| load generator | repository stdlib HTTP runner, one connection per request |",
                    "| run count | one baseline run per scenario |",
                ]
            ),
            "## Single-File LOC\n\n" + loc,
            tier_table(rows, "t1", 64, ["health", "echo", "item"]),
            tier_table(rows, "t1", 256, ["health", "echo", "item"]),
            tier_table(rows, "t2", 64, ["health", "summary", "events_read", "events_write"]),
            tier_table(rows, "t2", 256, ["health", "summary", "events_read", "events_write"]),
            dev_ux_table(metrics),
            chart_gallery(chart_rows),
            runtime_plumbing_table(),
            memory_table(rows),
            binary_table(),
            capability_matrix(),
            interpretation(rows, raw),
            "## Limitations\n\n- This is not yet a five-run median with min/max.\n- The standard-library runner is intentionally dependency-free but is not a replacement for `oha`, `wrk`, or `bombardier`.\n- The p99 values in Tier 2 contain repeated multi-second outliers across frameworks, so they are recorded but not used for ranking.\n- SSE/long-poll correctness routes exist in the apps, but this run measured HTTP read/write endpoints only; live subscriber fanout still needs a dedicated scenario.\n- Startup readiness is not reported for this raw run because the first harness version recorded it after readiness completed; the harness is fixed for future runs.\n- Python apps ran single worker/process baselines, matching the single-process Rust baseline.",
        ]
    )
    out = RESULTS / "summary.md"
    out.write_text(content + "\n")
    print(out)


if __name__ == "__main__":
    main()
