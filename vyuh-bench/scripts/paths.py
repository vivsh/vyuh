"""Keep generated benchmark evidence outside the source checkout."""

import os
from pathlib import Path

RESULTS = Path(
    os.environ.get("VYUH_BENCH_RESULTS", Path.home() / "Codex/vyuh/benchmarks/http/results")
).expanduser().resolve()
