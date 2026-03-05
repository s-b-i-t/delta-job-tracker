import copy
import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import generate_efficiency_report as ger


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, float(seconds))


class FakePopen:
    def __init__(self, clock: FakeClock, exit_after_seconds: float = 2.2, returncode: int = 0) -> None:
        self._clock = clock
        self._exit_after_seconds = exit_after_seconds
        self._target_returncode = returncode
        self.returncode = None

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        if self._clock.now >= self._exit_after_seconds:
            self.returncode = self._target_returncode
        return self.returncode

    def wait(self, timeout=None):
        _ = timeout
        if self.returncode is None:
            self._clock.now = max(self._clock.now, self._exit_after_seconds)
            self.returncode = self._target_returncode
        return self.returncode

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


class GenerateEfficiencyReportTest(unittest.TestCase):
    def test_writes_metrics_csv_and_readme_with_expected_schema(self) -> None:
        clock = FakeClock()
        snapshots = [
            {
                "schema_version": "db-snapshot-v1",
                "captured_at": "2026-03-04T00:00:00+00:00",
                "ok": True,
                "mode_requested": "auto",
                "mode_used": "direct",
                "metrics": {
                    "ats_endpoints_count": 10,
                    "companies_with_domain_count": 100,
                    "companies_with_ats_endpoint_count": 8,
                    "endpoints_by_type_map": {"WORKDAY": 8},
                },
            },
            {
                "schema_version": "db-snapshot-v1",
                "captured_at": "2026-03-04T00:00:01+00:00",
                "ok": True,
                "mode_requested": "auto",
                "mode_used": "direct",
                "metrics": {
                    "ats_endpoints_count": 14,
                    "companies_with_domain_count": 102,
                    "companies_with_ats_endpoint_count": 9,
                    "endpoints_by_type_map": {"WORKDAY": 11, "GREENHOUSE": 1},
                },
            },
            {
                "schema_version": "db-snapshot-v1",
                "captured_at": "2026-03-04T00:00:03+00:00",
                "ok": True,
                "mode_requested": "auto",
                "mode_used": "direct",
                "metrics": {
                    "ats_endpoints_count": 20,
                    "companies_with_domain_count": 105,
                    "companies_with_ats_endpoint_count": 11,
                    "endpoints_by_type_map": {"WORKDAY": 15, "GREENHOUSE": 2},
                },
            },
        ]
        state = {"idx": 0}

        def fake_snapshot(*args, **kwargs):
            _ = args, kwargs
            idx = min(state["idx"], len(snapshots) - 1)
            state["idx"] += 1
            return copy.deepcopy(snapshots[idx])

        with tempfile.TemporaryDirectory() as tmp:
            out_base = Path(tmp) / "out"

            with patch.object(ger.time, "monotonic", side_effect=clock.monotonic), patch.object(
                ger.time, "sleep", side_effect=clock.sleep
            ), patch.object(
                ger.subprocess,
                "Popen",
                side_effect=lambda *a, **k: FakePopen(clock, exit_after_seconds=2.2, returncode=0),
            ), patch.object(
                ger.dbsnap,
                "capture_db_snapshot",
                side_effect=fake_snapshot,
            ), patch.object(
                ger,
                "load_matplotlib",
                return_value=(None, "missing-matplotlib"),
            ):
                rc = ger.main(
                    [
                        "--out-dir",
                        str(out_base),
                        "--interval-seconds",
                        "1",
                        "--",
                        "python3",
                        "-c",
                        "print('ok')",
                    ]
                )

            self.assertEqual(rc, 0)
            report_dirs = list(out_base.glob("*/efficiency_report"))
            self.assertEqual(len(report_dirs), 1)
            report_dir = report_dirs[0]

            metrics_path = report_dir / "metrics.json"
            csv_path = report_dir / "metrics.csv"
            readme_path = report_dir / "README.md"

            self.assertTrue(metrics_path.exists())
            self.assertTrue(csv_path.exists())
            self.assertTrue(readme_path.exists())

            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual(metrics.get("schema_version"), "efficiency-report-v1")
            self.assertEqual(metrics.get("sampling", {}).get("sample_ok_count"), 4)
            self.assertEqual(metrics.get("throughput", {}).get("ats_endpoints_count", {}).get("delta"), 10)
            self.assertEqual(metrics.get("delta", {}).get("endpoints_by_type", {}).get("WORKDAY"), 7)
            self.assertFalse(metrics.get("plots", {}).get("enabled"))

            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(reader.fieldnames, ger.CSV_HEADERS)
                rows = list(reader)
                self.assertEqual(len(rows), metrics.get("sampling", {}).get("sample_count"))
                self.assertEqual(rows[0]["snapshot_ok"], "true")

            readme = readme_path.read_text(encoding="utf-8")
            self.assertIn("What this run proves", readme)
            self.assertIn("Plots were skipped", readme)

    def test_snapshot_capture_failures_still_produce_report_files(self) -> None:
        clock = FakeClock()

        with tempfile.TemporaryDirectory() as tmp:
            out_base = Path(tmp) / "out"

            with patch.object(ger.time, "monotonic", side_effect=clock.monotonic), patch.object(
                ger.time, "sleep", side_effect=clock.sleep
            ), patch.object(
                ger.subprocess,
                "Popen",
                side_effect=lambda *a, **k: FakePopen(clock, exit_after_seconds=0.2, returncode=0),
            ), patch.object(
                ger.dbsnap,
                "capture_db_snapshot",
                side_effect=RuntimeError("db unavailable"),
            ), patch.object(
                ger,
                "load_matplotlib",
                return_value=(None, "missing-matplotlib"),
            ):
                rc = ger.main(
                    [
                        "--out-dir",
                        str(out_base),
                        "--interval-seconds",
                        "1",
                        "--",
                        "python3",
                        "-c",
                        "print('ok')",
                    ]
                )

            self.assertEqual(rc, 0)
            report_dirs = list(out_base.glob("*/efficiency_report"))
            self.assertEqual(len(report_dirs), 1)
            report_dir = report_dirs[0]

            metrics = json.loads((report_dir / "metrics.json").read_text(encoding="utf-8"))
            sampling = metrics.get("sampling") or {}
            errors = metrics.get("error_breakdown") or {}

            self.assertGreaterEqual(sampling.get("sample_count", 0), 2)
            self.assertEqual(sampling.get("sample_ok_count"), 0)
            self.assertEqual(sampling.get("sample_failure_count"), sampling.get("sample_count"))
            self.assertGreaterEqual(errors.get("snapshot_failure_count", 0), 2)
            self.assertTrue((report_dir / "metrics.csv").exists())
            self.assertTrue((report_dir / "README.md").exists())


if __name__ == "__main__":
    unittest.main()
