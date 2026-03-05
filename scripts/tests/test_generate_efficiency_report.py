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

import generate_efficiency_report as eff


class _FakePopen:
    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.returncode = None
        self._poll_calls = 0

    def poll(self):
        self._poll_calls += 1
        if self._poll_calls < 3:
            return None
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def terminate(self):
        self.returncode = 143

    def kill(self):
        self.returncode = 137

    def wait(self, timeout=None):  # noqa: ARG002
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class GenerateEfficiencyReportTest(unittest.TestCase):
    def test_report_files_written_and_schema_stable(self) -> None:
        counter = {"i": 0}

        def fake_snapshot(repo_root, mode="auto"):  # noqa: ANN001, ARG001
            counter["i"] += 1
            i = counter["i"]
            return {
                "schema_version": "db-snapshot-v1",
                "captured_at": f"2026-03-05T00:00:{i:02d}Z",
                "ok": True,
                "mode_requested": mode,
                "mode_used": "docker",
                "metrics": {
                    "ats_endpoints_count": 100 + i,
                    "companies_with_ats_endpoint_count": 50 + i,
                    "companies_with_domain_count": 900 + i,
                    "endpoints_by_type_map": {"SMARTRECRUITERS": i},
                },
            }

        with tempfile.TemporaryDirectory() as tmp:
            with patch("generate_efficiency_report.subprocess.Popen", _FakePopen), patch(
                "generate_efficiency_report.time.sleep", lambda _x: None
            ), patch("generate_efficiency_report.dbsnap.capture_db_snapshot", side_effect=fake_snapshot):
                rc = eff.main([
                    "--out-dir",
                    tmp,
                    "--interval-seconds",
                    "1",
                    "--",
                    "python3",
                    "-c",
                    "print('ok')",
                ])

            self.assertEqual(rc, 0)
            run_dirs = list(Path(tmp).glob("*/efficiency_report"))
            self.assertEqual(len(run_dirs), 1)
            run_dir = run_dirs[0]

            metrics_path = run_dir / "metrics.json"
            csv_path = run_dir / "metrics.csv"
            readme_path = run_dir / "README.md"
            samples_path = run_dir / "samples.jsonl"
            self.assertTrue(metrics_path.exists())
            self.assertTrue(csv_path.exists())
            self.assertTrue(readme_path.exists())
            self.assertTrue(samples_path.exists())

            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual(metrics["schema_version"], "efficiency-report-v1")
            self.assertIn("summary", metrics)
            self.assertGreaterEqual(metrics["summary"]["sample_count"], 1)
            self.assertIsInstance(metrics["summary"]["delta"], dict)
            self.assertIn("ats_endpoints_count", metrics["summary"]["delta"])
            self.assertIn("companies_with_domain_count", metrics["summary"]["delta"])

            with csv_path.open("r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertGreaterEqual(len(rows), 1)
            self.assertIn("ats_endpoints_count", rows[0])
            self.assertIn("companies_with_domain_count", rows[0])

    def test_snapshot_failures_emit_error_stub(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch("generate_efficiency_report.subprocess.Popen", _FakePopen), patch(
                "generate_efficiency_report.time.sleep", lambda _x: None
            ), patch(
                "generate_efficiency_report.dbsnap.capture_db_snapshot",
                side_effect=RuntimeError("db offline"),
            ):
                rc = eff.main([
                    "--out-dir",
                    tmp,
                    "--interval-seconds",
                    "1",
                    "--",
                    "python3",
                    "-c",
                    "print('ok')",
                ])

            self.assertEqual(rc, 0)
            run_dirs = list(Path(tmp).glob("*/efficiency_report"))
            self.assertEqual(len(run_dirs), 1)
            run_dir = run_dirs[0]

            metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
            self.assertEqual(metrics["summary"]["sample_success_count"], 0)
            self.assertGreaterEqual(metrics["summary"]["sample_failure_count"], 1)
            self.assertGreaterEqual(len(metrics["errors"]), 1)
            first_error = metrics["errors"][0]["error"]
            self.assertEqual(first_error.get("exception_type"), "RuntimeError")

            readme_text = (run_dir / "README.md").read_text(encoding="utf-8")
            self.assertIn("Efficiency Report", readme_text)


if __name__ == "__main__":
    unittest.main()
