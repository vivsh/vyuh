#!/usr/bin/env python3
import asyncio
import json
import os
import signal
import socket
import statistics
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from paths import RESULTS

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
VENV = ROOT / ".venv" / "bin"
HOST = "127.0.0.1"
PORT = 9100
PG_PORT = 55432
CONCURRENCY = [1, 16, 64, 256]
DURATION = float(os.environ.get("BENCH_DURATION", "3"))
WARMUP = float(os.environ.get("BENCH_WARMUP", "1"))
TIMEOUT = float(os.environ.get("BENCH_TIMEOUT", "5"))
TIERS = frozenset(
    part.strip()
    for part in os.environ.get("BENCH_TIERS", "t1,t2").split(",")
    if part.strip()
)
FRAMEWORKS = frozenset(
    part.strip()
    for part in os.environ.get("BENCH_FRAMEWORKS", "vyuh,axum,rocket,actix,fastapi,drf").split(",")
    if part.strip()
)


@dataclass
class App:
    name: str
    tier: str
    command: list[str]
    env: dict[str, str]
    cwd: Path
    reset_sqlite: Path | None = None


def now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run(args: list[str], cwd: Path = WORKSPACE, env: dict[str, str] | None = None) -> None:
    subprocess.run(args, cwd=cwd, env=env, check=True)


def output(args: list[str], cwd: Path = WORKSPACE) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def build() -> None:
    run(["cargo", "build", "-p", "vyuh-bench", "--release", "--bins"])
    if "t1" in TIERS and "vyuh" in FRAMEWORKS:
        run(["cargo", "build", "-p", "vyuh-bench", "--release", "--features", "vyuh-sqlite", "--bin", "t1_vyuh"])
    if "t2" in TIERS and "vyuh" in FRAMEWORKS:
        run(["cargo", "build", "-p", "vyuh-bench", "--release", "--features", "vyuh-postgres", "--bin", "t2_vyuh"])


def start_pg(raw: Path) -> tuple[Path, str]:
    pgdata = raw / "pgdata"
    socket_dir = Path("/tmp") / f"vyuh-bench-pg-{raw.name}"
    log = raw / "postgres.log"
    socket_dir.mkdir(parents=True, exist_ok=True)
    run(["initdb", "-D", str(pgdata), "-A", "trust", "-U", "postgres"])
    run(["pg_ctl", "-D", str(pgdata), "-o", f"-p {PG_PORT} -k {socket_dir}", "-l", str(log), "start"])
    deadline = time.time() + 20
    while time.time() < deadline:
        probe = subprocess.run(["pg_isready", "-h", HOST, "-p", str(PG_PORT)], capture_output=True)
        if probe.returncode == 0:
            break
        time.sleep(0.2)
    db_url = f"postgres://postgres@{HOST}:{PG_PORT}/vyuh_bench"
    run(["createdb", "-h", HOST, "-p", str(PG_PORT), "-U", "postgres", "vyuh_bench"])
    return pgdata, db_url


def stop_pg(pgdata: Path) -> None:
    subprocess.run(["pg_ctl", "-D", str(pgdata), "stop", "-m", "fast"], check=False)


def reset_pg() -> None:
    sql = "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"
    run(["psql", "-h", HOST, "-p", str(PG_PORT), "-U", "postgres", "-d", "vyuh_bench", "-c", sql])
    run([
        "psql",
        "-h",
        HOST,
        "-p",
        str(PG_PORT),
        "-U",
        "postgres",
        "-d",
        "vyuh_bench",
        "-f",
        str(ROOT / "sql" / "t2_postgres.sql"),
    ])


def reset_sqlite(path: Path) -> None:
    if path.exists():
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        db.executescript((ROOT / "sql" / "t1_sqlite.sql").read_text())


def process_ids(pid: int) -> list[int]:
    ids = [pid]
    pending = [pid]
    while pending:
        parent = pending.pop()
        proc = subprocess.run(["pgrep", "-P", str(parent)], capture_output=True, text=True)
        children = [int(line) for line in proc.stdout.splitlines() if line.strip().isdigit()]
        ids.extend(children)
        pending.extend(children)
    return ids


def rss_kb(pid: int) -> int:
    total = 0
    for child in process_ids(pid):
        proc = subprocess.run(["ps", "-o", "rss=", "-p", str(child)], capture_output=True, text=True)
        value = proc.stdout.strip()
        if value.isdigit():
            total += int(value)
    return total


def wait_ready() -> None:
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            status, _ = sync_request("GET", "/health")
            if 200 <= status < 300:
                return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError("server did not become healthy")


def start_app(app: App, raw: Path) -> tuple[subprocess.Popen, Path, float]:
    if app.reset_sqlite is not None:
        reset_sqlite(app.reset_sqlite)
    env = os.environ.copy()
    env.update(app.env)
    env["PORT"] = str(PORT)
    log_path = raw / f"{app.tier}_{app.name}.log"
    log = log_path.open("w")
    started = time.perf_counter()
    proc = subprocess.Popen(app.command, cwd=app.cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        wait_ready()
    except Exception:
        stop_app(proc)
        raise
    startup_ms = round((time.perf_counter() - started) * 1000, 3)
    return proc, log_path, startup_ms


def stop_app(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def sync_request(method: str, path: str, body: bytes = b"") -> tuple[int, bytes]:
    with socket.create_connection((HOST, PORT), timeout=TIMEOUT) as sock:
        sock.settimeout(TIMEOUT)
        req = make_request(method, path, body)
        sock.sendall(req)
        return read_response(sock)


def json_body(response: bytes) -> Any:
    headers, _, body = response.partition(b"\r\n\r\n")
    if b"transfer-encoding: chunked" in headers.lower():
        body = decode_chunked(body)
    return json.loads(body.decode())


def decode_chunked(body: bytes) -> bytes:
    output = bytearray()
    cursor = 0
    while cursor < len(body):
        line_end = body.find(b"\r\n", cursor)
        if line_end < 0:
            break
        size_text = body[cursor:line_end].split(b";", 1)[0]
        size = int(size_text, 16)
        cursor = line_end + 2
        if size == 0:
            break
        output.extend(body[cursor : cursor + size])
        cursor += size + 2
    return bytes(output)


def assert_json(method: str, path: str, body: bytes, check) -> None:
    status, response = sync_request(method, path, body)
    if status < 200 or status >= 300:
        raise RuntimeError(f"{method} {path} returned {status}")
    data = json_body(response)
    if not check(data):
        raise RuntimeError(f"{method} {path} returned unexpected body: {data!r}")


async def request(method: str, path: str, body: bytes = b"") -> tuple[int, float]:
    start = time.perf_counter()
    reader, writer = await asyncio.wait_for(asyncio.open_connection(HOST, PORT), timeout=TIMEOUT)
    try:
        writer.write(make_request(method, path, body))
        await writer.drain()
        status = await read_async_response(reader)
        return status, (time.perf_counter() - start) * 1000
    finally:
        writer.close()
        await writer.wait_closed()


def make_request(method: str, path: str, body: bytes) -> bytes:
    headers = [
        f"{method} {path} HTTP/1.1",
        f"Host: {HOST}:{PORT}",
        "Connection: close",
        "User-Agent: vyuh-bench",
    ]
    if body:
        headers.append("Content-Type: application/json")
        headers.append(f"Content-Length: {len(body)}")
    headers.append("")
    headers.append("")
    return "\r\n".join(headers).encode() + body


def read_response(sock: socket.socket) -> tuple[int, bytes]:
    data = bytearray()
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        data.extend(chunk)
    head = bytes(data).split(b"\r\n", 1)[0].decode(errors="replace")
    status = int(head.split()[1])
    return status, bytes(data)


async def read_async_response(reader: asyncio.StreamReader) -> int:
    line = await asyncio.wait_for(reader.readline(), timeout=TIMEOUT)
    if not line:
        return 0
    status = int(line.decode(errors="replace").split()[1])
    while await reader.read(65536):
        pass
    return status


async def workload(method: str, path: str, body: bytes, concurrency: int, duration: float) -> dict[str, Any]:
    latencies: list[float] = []
    errors = 0
    deadline = time.perf_counter() + duration

    async def worker() -> None:
        nonlocal errors
        while time.perf_counter() < deadline:
            try:
                status, elapsed = await request(method, path, body)
                latencies.append(elapsed)
                if status < 200 or status >= 300:
                    errors += 1
            except Exception:
                errors += 1

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    total = len(latencies) + errors
    return summarize(latencies, errors, total, duration)


def summarize(latencies: list[float], errors: int, total: int, duration: float) -> dict[str, Any]:
    values = sorted(latencies)
    return {
        "requests": total,
        "success": len(latencies),
        "errors": errors,
        "requests_per_second": total / duration,
        "p50_ms": percentile(values, 50),
        "p90_ms": percentile(values, 90),
        "p99_ms": percentile(values, 99),
    }


def percentile(values: list[float], pct: int) -> float | None:
    if not values:
        return None
    index = max(0, min(len(values) - 1, round((pct / 100) * (len(values) - 1))))
    return round(values[index], 3)


def scenarios(app: App) -> list[tuple[str, str, str, bytes]]:
    echo = json.dumps({"message": "hello", "count": 1}).encode()
    event = json.dumps({"value": 1}).encode()
    if app.tier == "t1":
        return [
            ("health", "GET", "/health", b""),
            ("echo", "POST", "/echo", echo),
            ("item", "GET", "/items/1", b""),
        ]
    return [
        ("health", "GET", "/health", b""),
        ("summary", "GET", "/projects/1/summary", b""),
        ("events_read", "GET", "/projects/1/events", b""),
        ("events_write", "POST", "/projects/1/events", event),
    ]


def prime_t2() -> None:
    assert_json(
        "POST",
        "/projects",
        json.dumps({"name": "bench"}).encode(),
        lambda data: data.get("id") == 1 and data.get("name") == "bench",
    )
    for _ in range(10):
        assert_json(
            "POST",
            "/projects/1/events",
            json.dumps({"value": 1}).encode(),
            lambda data: isinstance(data.get("id"), int) and data["id"] > 0,
        )
    assert_json(
        "GET",
        "/projects/1/events",
        b"",
        lambda data: isinstance(data, list) and len(data) >= 10 and data[0].get("value") == 1,
    )
    time.sleep(6)
    assert_json(
        "GET",
        "/projects/1/summary",
        b"",
        lambda data: data.get("project_id") == 1
        and data.get("event_count", 0) >= 10
        and data.get("event_sum", 0) >= 10,
    )


async def run_app(app: App, raw: Path) -> list[dict[str, Any]]:
    if app.tier == "t2":
        reset_pg()
    proc, log_path, startup_ms = start_app(app, raw)
    results: list[dict[str, Any]] = []
    try:
        if app.tier == "t2":
            prime_t2()
        idle = rss_kb(proc.pid)
        await asyncio.sleep(WARMUP)
        for name, method, path, body in scenarios(app):
            for concurrency in CONCURRENCY:
                before = rss_kb(proc.pid)
                result = await workload(method, path, body, concurrency, DURATION)
                after = rss_kb(proc.pid)
                result.update({
                    "framework": app.name,
                    "tier": app.tier,
                    "scenario": name,
                    "method": method,
                    "path": path,
                    "concurrency": concurrency,
                    "duration_seconds": DURATION,
                    "startup_ms": startup_ms,
                    "idle_rss_kb": idle,
                    "rss_before_kb": before,
                    "rss_after_kb": after,
                    "log": str(log_path.relative_to(raw)),
                })
                results.append(result)
    finally:
        stop_app(proc)
    return results


def apps(db_url: str, raw: Path) -> list[App]:
    target = WORKSPACE / "target" / "release"
    py = VENV / "python"
    env_pg = {"DATABASE_URL": db_url}
    sqlite_urls = {
        name: f"sqlite:{raw / f't1_{name}.sqlite3'}"
        for name in ["vyuh", "axum", "rocket", "actix"]
    }
    candidates = [
        App("vyuh", "t1", [str(target / "t1_vyuh")], {"T1_SQLITE_URL": sqlite_urls["vyuh"]}, WORKSPACE, raw / "t1_vyuh.sqlite3"),
        App("axum", "t1", [str(target / "t1_axum")], {"T1_SQLITE_URL": sqlite_urls["axum"]}, WORKSPACE, raw / "t1_axum.sqlite3"),
        App("rocket", "t1", [str(target / "t1_rocket")], {"T1_SQLITE_URL": sqlite_urls["rocket"]}, WORKSPACE, raw / "t1_rocket.sqlite3"),
        App("actix", "t1", [str(target / "t1_actix")], {"T1_SQLITE_URL": sqlite_urls["actix"]}, WORKSPACE, raw / "t1_actix.sqlite3"),
        App("fastapi", "t1", [str(py), "-m", "uvicorn", "t1_fastapi:app", "--app-dir", "python", "--host", HOST, "--port", str(PORT), "--workers", "1"], {"T1_SQLITE_PATH": str(raw / "t1_fastapi.sqlite3")}, ROOT, raw / "t1_fastapi.sqlite3"),
        App("drf", "t1", [str(py), "-m", "gunicorn", "drf_t1.wsgi:application", "--bind", f"{HOST}:{PORT}", "--workers", "1", "--threads", "1"], {"PYTHONPATH": "python", "T1_SQLITE_PATH": str(raw / "t1_drf.sqlite3")}, ROOT, raw / "t1_drf.sqlite3"),
        App("vyuh", "t2", [str(target / "t2_vyuh")], env_pg, WORKSPACE),
        App("axum", "t2", [str(target / "t2_axum")], env_pg, WORKSPACE),
        App("rocket", "t2", [str(target / "t2_rocket")], env_pg, WORKSPACE),
        App("actix", "t2", [str(target / "t2_actix")], env_pg, WORKSPACE),
        App("fastapi", "t2", [str(py), "-m", "uvicorn", "t2_fastapi:app", "--app-dir", "python", "--host", HOST, "--port", str(PORT), "--workers", "1"], env_pg, ROOT),
        App("drf", "t2", [str(py), "-m", "gunicorn", "drf_t2.wsgi:application", "--bind", f"{HOST}:{PORT}", "--workers", "1", "--threads", "1", "--timeout", "120"], {"PYTHONPATH": "python", **env_pg}, ROOT),
    ]
    return [app for app in candidates if app.tier in TIERS and app.name in FRAMEWORKS]


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True))


def echo_findings(app: App, results: list[dict[str, Any]]) -> None:
    if not results:
        print(f"findings {app.tier}/{app.name}: no results", flush=True)
        return
    max_errors = max(int(row["errors"]) for row in results)
    peak_rss = max(int(row["rss_after_kb"]) for row in results) / 1024
    samples = []
    for scenario, concurrency in [("echo", 64), ("item", 64), ("events_write", 256), ("summary", 256)]:
        row = next(
            (
                item
                for item in results
                if item["scenario"] == scenario and item["concurrency"] == concurrency
            ),
            None,
        )
        if row is not None:
            samples.append(f"{scenario}@c{concurrency}={row['requests_per_second']:.0f}rps")
    joined = ", ".join(samples) if samples else "no selected sample rows"
    print(
        f"findings {app.tier}/{app.name}: {joined}; "
        f"startup={results[0]['startup_ms']:.1f}ms; peak_rss={peak_rss:.1f}MiB; max_errors={max_errors}",
        flush=True,
    )


async def main() -> None:
    raw = RESULTS / "raw" / now_slug()
    raw.mkdir(parents=True)
    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": DURATION,
        "warmup_seconds": WARMUP,
        "concurrency": CONCURRENCY,
        "tiers": sorted(TIERS),
        "frameworks": sorted(FRAMEWORKS),
        "host": HOST,
        "port": PORT,
        "rustc": output(["rustc", "--version"]),
        "cargo": output(["cargo", "--version"]),
        "python": output([str(py_path()), "--version"], ROOT),
        "note": "Single-run baseline using the repository stdlib HTTP runner; not a publication-grade five-run median.",
    }
    write_json(raw / "meta.json", meta)
    build()
    pgdata: Path | None = None
    db_url = ""
    if "t2" in TIERS:
        pgdata, db_url = start_pg(raw)
    all_results: list[dict[str, Any]] = []
    try:
        selected = apps(db_url, raw)
        if not selected:
            raise RuntimeError("no benchmark apps selected")
        for app in selected:
            print(f"running {app.tier}/{app.name}", flush=True)
            results = await run_app(app, raw)
            all_results.extend(results)
            write_json(raw / f"{app.tier}_{app.name}.json", results)
            echo_findings(app, results)
    finally:
        if pgdata is not None:
            stop_pg(pgdata)
    write_json(raw / "results.json", all_results)
    print(raw)


def py_path() -> Path:
    return VENV / "python"


if __name__ == "__main__":
    asyncio.run(main())
