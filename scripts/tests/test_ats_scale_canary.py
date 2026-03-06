import json
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import run_ats_scale_canary as scale
import run_ats_validation_canary as canary


class AtsScaleCanaryFailureHandlingTests(unittest.TestCase):
    def test_api_client_wraps_socket_timeout(self) -> None:
        client = canary.ApiClient("http://localhost:8080", timeout_seconds=5)
        with patch.object(canary, "urlopen", side_effect=TimeoutError("timed out")):
            with self.assertRaises(RuntimeError) as ctx:
                client.get_json("/api/status")
        self.assertIn("Request timed out", str(ctx.exception))

    def test_frontier_seed_timeout_writes_structured_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "20260306T000000Z"
            out_dir.mkdir(parents=True, exist_ok=True)

            args = Namespace(
                base_url="http://localhost:8080",
                out_dir=tmp,
                domain_limit=500,
                max_sitemap_fetches=120,
                discover_limit=300,
                discover_batch_size=50,
                vendor_probe_only=False,
                max_wait_seconds=1200,
                poll_interval_seconds=5,
                http_timeout_seconds=300,
                postgres_container="delta-job-tracker-postgres",
                postgres_user="app",
                postgres_db="delta_job_tracker",
            )

            preflight_step = canary.StepResult("preflight_api_status", 0.0, 0.1, {"dbConnectivity": True})
            frontier_step = canary.StepResult(
                "frontier_seed",
                0.1,
                300.1,
                {"_error": "Request timed out for POST http://localhost:8080/api/frontier/seed after 300s"},
            )

            with patch.object(scale, "parse_args", return_value=args), patch.object(
                scale.canary, "ensure_out_dir", return_value=out_dir
            ), patch.object(scale.canary, "ApiClient", return_value=Mock()), patch.object(
                scale.canary, "DbClient", return_value=Mock()
            ), patch.object(
                scale.canary, "timed_step_safe", side_effect=[preflight_step, frontier_step]
            ):
                rc = scale.main()

            self.assertEqual(rc, 2)
            metrics_path = out_dir / "ats_scale_metrics.json"
            stage_diag_path = out_dir / "stage_diagnostics.json"
            self.assertTrue(metrics_path.exists())
            self.assertTrue(stage_diag_path.exists())
            payload = json.loads(metrics_path.read_text(encoding="utf-8"))
            stage_diag = json.loads(stage_diag_path.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("error"), "frontier_seed_failed")
            self.assertIn("Request timed out", payload.get("details", {}).get("_error", ""))
            self.assertEqual(payload.get("stage_failure_summary", {}).get("stage"), "frontier_seed")
            self.assertEqual(
                payload.get("stage_failure_summary", {}).get("request_context", {}).get("path"),
                "/api/frontier/seed",
            )
            self.assertEqual(payload.get("runtime_context", {}).get("limits", {}).get("domain_limit"), 500)
            self.assertEqual(payload.get("stage_timings_ms", {}).get("frontier_seed"), 300000)
            self.assertEqual(stage_diag.get("stage_failure_summary", {}).get("error_code"), "frontier_seed_failed")


if __name__ == "__main__":
    unittest.main()
