import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import run_e2e_pipeline_proof as proof


class E2EPipelineProofConfigTest(unittest.TestCase):
    def test_normalize_stop_reason_maps_expected_buckets(self) -> None:
        self.assertEqual(proof.normalize_stop_reason(None), "NONE")
        self.assertEqual(proof.normalize_stop_reason("request_budget_exceeded"), "REQUEST_BUDGET_EXCEEDED")
        self.assertEqual(proof.normalize_stop_reason("hard_timeout_reached"), "TIMEOUT")
        self.assertEqual(proof.normalize_stop_reason("host_failure_cutoff"), "HOST_FAILURE_CUTOFF")
        self.assertEqual(proof.normalize_stop_reason("FAILED"), "FAILED")

    def test_build_canonical_efficiency_report_has_scope_tags_and_rates(self) -> None:
        payload = proof.build_canonical_efficiency_report(
            run_root=Path("out/20260306T000000Z/e2e_pipeline_run"),
            preset="S",
            fetch_elapsed_seconds=120.0,
            fetch_attempts=60,
            fetch_successes=45,
            queued_before_fetch=100,
            queued_after_fetch=40,
            fetch_error_buckets={"timeout": 6, "robots": 3},
            ats_stage={
                "resolve_batches": [
                    {"attempted": 10, "resolvedCount": 6, "duration_seconds": 30.0},
                    {"attempted": 5, "resolvedCount": 2, "duration_seconds": 15.0},
                ],
                "discovery_wait_seconds": 180.0,
                "discovery_final": {
                    "companiesAttemptedCount": 12,
                    "succeededCount": 7,
                    "failedCount": 5,
                    "endpointExtractedCount": 4,
                    "stopReason": "request_budget_exceeded",
                },
            },
            ats_endpoints_created=3,
            domain_before=1000,
            domain_after=1008,
        )
        self.assertEqual(payload["schema_version"], "canonical-efficiency-report-v1")
        self.assertEqual(payload["phases"]["domain_resolution"]["scope_tag"], "run_scope")
        self.assertEqual(payload["phases"]["queue_fetch"]["scope_tag"], "run_scope")
        self.assertEqual(payload["phases"]["ats_discovery"]["scope_tag"], "run_scope")
        self.assertEqual(payload["global_snapshots"]["scope_tag"], "global_snapshot")
        self.assertEqual(payload["phases"]["queue_fetch"]["rates_per_min"]["urls_fetch_attempted_per_min"], 30.0)
        self.assertEqual(payload["phases"]["ats_discovery"]["stop_reason"]["normalized"], "REQUEST_BUDGET_EXCEEDED")
        self.assertEqual(payload["phases"]["queue_fetch"]["error_breakdown"]["error_rate_per_attempt"], 0.15)

    def test_preset_s_applies_expected_defaults(self) -> None:
        args = proof.parse_args(["--preset", "S"])
        cfg, applied = proof.resolve_effective_config(args, {"--preset"})

        self.assertEqual(cfg["seed_domain_limit"], proof.PRESETS["S"]["seed_domain_limit"])
        self.assertEqual(cfg["fetch_batch_size"], proof.PRESETS["S"]["fetch_batch_size"])
        self.assertEqual(cfg["sec_limit"], proof.PRESETS["S"]["sec_limit"])
        self.assertIn("preset:S", applied)

    def test_explicit_flags_override_preset(self) -> None:
        argv = ["--preset", "M", "--sec-limit", "100", "--discover-limit", "333"]
        explicit = proof.detect_explicit_cli_options(argv)
        args = proof.parse_args(argv)
        cfg, applied = proof.resolve_effective_config(args, explicit)

        self.assertEqual(cfg["sec_limit"], 100)
        self.assertEqual(cfg["discover_limit"], 333)
        self.assertIn("explicit:--sec-limit", applied)
        self.assertIn("explicit:--discover-limit", applied)

    def test_default_report_path_is_under_run_root(self) -> None:
        run_root = Path("out/20260305T000000Z/e2e_pipeline_run")
        paths = proof.compute_report_paths(run_root, "2026-03-05", write_docs_report=False)

        self.assertEqual(paths["run_root_report"], str(run_root / "release_report.md"))
        self.assertIsNone(paths["docs_report"])

    def test_write_docs_report_enables_docs_path(self) -> None:
        run_root = Path("out/20260305T000000Z/e2e_pipeline_run")
        paths = proof.compute_report_paths(run_root, "2026-03-05", write_docs_report=True)

        self.assertEqual(paths["run_root_report"], str(run_root / "release_report.md"))
        self.assertEqual(paths["docs_report"], "docs/release_e2e_pipeline_proof_2026-03-05.md")

    def test_db_snapshots_written_and_summary_contains_db_truth(self) -> None:
        start_snapshot = {
            "schema_version": "db-snapshot-v1",
            "captured_at": "2026-03-05T00:00:00Z",
            "ok": True,
            "mode_requested": "auto",
            "mode_used": "docker",
            "metrics": {
                "ats_endpoints_count": 10,
                "companies_with_ats_endpoint_count": 7,
                "companies_with_domain_count": 100,
                "endpoints_by_type": [{"ats_type": "WORKDAY", "count": 10}],
                "endpoints_by_type_map": {"WORKDAY": 10},
            },
        }
        end_snapshot = {
            "schema_version": "db-snapshot-v1",
            "captured_at": "2026-03-05T00:01:00Z",
            "ok": True,
            "mode_requested": "auto",
            "mode_used": "docker",
            "metrics": {
                "ats_endpoints_count": 12,
                "companies_with_ats_endpoint_count": 8,
                "companies_with_domain_count": 101,
                "endpoints_by_type": [{"ats_type": "WORKDAY", "count": 12}],
                "endpoints_by_type_map": {"WORKDAY": 12},
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(
                proof,
                "check_runtime_dependencies",
                return_value={"curl": {"ok": True}, "docker": {"ok": True}},
            ), patch.object(
                proof,
                "get_json",
                return_value={"dbConnectivity": True},
            ), patch.object(
                proof.dbsnap,
                "capture_db_snapshot",
                side_effect=[start_snapshot, end_snapshot],
            ):
                rc = proof.main(["--preset", "S", "--dry-run", "--out-dir", tmp])

            self.assertEqual(rc, 0)
            run_dirs = list(Path(tmp).glob("*/e2e_pipeline_run"))
            self.assertEqual(len(run_dirs), 1)
            run_dir = run_dirs[0]
            self.assertTrue((run_dir / "db_snapshot_start.json").exists())
            self.assertTrue((run_dir / "db_snapshot_end.json").exists())
            self.assertTrue((run_dir / "canonical_efficiency_report.json").exists())
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertIn("db_truth", summary)
            self.assertIn("canonical_efficiency_report", summary)
            self.assertTrue(summary["db_truth"]["enabled"])
            self.assertTrue(summary["db_truth"]["delta_ok"])
            self.assertEqual(summary["db_truth"]["delta"]["ats_endpoints_count"], 2)

    def test_db_snapshot_exception_is_structured_and_non_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(
                proof,
                "check_runtime_dependencies",
                return_value={"curl": {"ok": True}, "docker": {"ok": True}},
            ), patch.object(
                proof,
                "get_json",
                return_value={"dbConnectivity": True},
            ), patch.object(
                proof.dbsnap,
                "capture_db_snapshot",
                side_effect=RuntimeError("boom"),
            ):
                rc = proof.main(["--preset", "S", "--dry-run", "--out-dir", tmp])

            self.assertEqual(rc, 0)
            run_dirs = list(Path(tmp).glob("*/e2e_pipeline_run"))
            self.assertEqual(len(run_dirs), 1)
            run_dir = run_dirs[0]
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertIn("db_truth", summary)
            self.assertIn("canonical_efficiency_report", summary)
            self.assertTrue(summary["db_truth"]["enabled"])
            self.assertFalse(summary["db_truth"]["delta_ok"])
            self.assertIn("error", summary["db_truth"]["start"])
            self.assertEqual(
                summary["db_truth"]["start"]["error"]["exception_type"],
                "RuntimeError",
            )


if __name__ == "__main__":
    unittest.main()
