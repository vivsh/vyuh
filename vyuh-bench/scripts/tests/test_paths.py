"""Verify benchmark artifacts stay outside the source tree."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import charts
import report


class ResultPaths(unittest.TestCase):
    """Keep native output defaults and report links consistent."""

    def resolve(self, override=None):
        """Load configuration in a fresh process without inherited overrides."""
        env = dict(os.environ)
        env.pop("VYUH_BENCH_RESULTS", None)
        if override is not None:
            env["VYUH_BENCH_RESULTS"] = override
        return subprocess.check_output(
            [sys.executable, "-c", "from paths import RESULTS; print(RESULTS)"],
            cwd=SCRIPTS, env=env, text=True,
        ).strip()

    def test_default_external(self):
        """Default evidence belongs in the external project guidance directory."""
        self.assertEqual(self.resolve(), str(Path.home() / "Codex/vyuh/benchmarks/http/results"))

    def test_override(self):
        """An explicit output directory works independently of the checkout."""
        with tempfile.TemporaryDirectory(prefix="vyuh benchmark ") as directory:
            self.assertEqual(self.resolve(directory), str(Path(directory).resolve()))

    def test_latest_external(self):
        """Reporting discovers completed runs only in the configured output root."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw" / "20261009T000000Z"
            raw.mkdir(parents=True)
            (raw / "results.json").write_text("[]")
            with patch.object(report, "RESULTS", root):
                self.assertEqual(report.latest_raw(), raw)

    def test_chart_links(self):
        """Generated chart links resolve relative to the external summary file."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw" / "20261009T000000Z"
            specs = [("sample", "Sample", "Test", "rps", [("vyuh", 1.0)])]
            with patch.object(charts, "chart_specs", return_value=specs), patch.object(charts, "png_chart", return_value=False):
                emitted = charts.emit([], [], raw, root)
            self.assertEqual(emitted[0]["svg"], "charts/20261009T000000Z/sample.svg")
            self.assertTrue((root / emitted[0]["svg"]).is_file())
            self.assertTrue((root / "charts/latest/sample.svg").is_file())
