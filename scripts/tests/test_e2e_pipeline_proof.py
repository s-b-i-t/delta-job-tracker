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
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertIn("db_truth", summary)
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
            self.assertTrue(summary["db_truth"]["enabled"])
            self.assertFalse(summary["db_truth"]["delta_ok"])
            self.assertIn("error", summary["db_truth"]["start"])
            self.assertEqual(
                summary["db_truth"]["start"]["error"]["exception_type"],
                "RuntimeError",
            )


if __name__ == "__main__":
    unittest.main()
