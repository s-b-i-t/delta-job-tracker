import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import db_snapshot as dbsnap
import run_soak_test as soak


class DbSnapshotHarnessTest(unittest.TestCase):
    def test_no_db_snapshot_error_schema_and_soak_report_survives(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]

        def timeout_run(*args, **kwargs):
            cmd = kwargs.get("args") or (args[0] if args else [])
            timeout = kwargs.get("timeout", 1)
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)

        with patch("db_snapshot.subprocess.run", side_effect=timeout_run):
            snapshot = dbsnap.capture_db_snapshot(
                repo_root,
                mode="direct",
                query_timeout_seconds=1,
                discovery_timeout_seconds=1,
            )

        self.assertEqual(snapshot.get("schema_version"), "db-snapshot-v1")
        self.assertTrue(snapshot.get("captured_at"))
        self.assertFalse(snapshot.get("ok"))
        self.assertEqual(snapshot.get("mode_requested"), "direct")
        self.assertIsNone(snapshot.get("mode_used"))
        error = snapshot.get("error") or {}
        attempts = error.get("attempts") or []
        self.assertTrue(attempts)
        self.assertEqual(attempts[0].get("mode"), "direct")

        with tempfile.TemporaryDirectory() as tmp:
            out_base = Path(tmp)
            with patch.object(
                soak,
                "utc_stamp",
                return_value="20260304T120000Z",
            ), patch.object(
                soak,
                "run_phase",
                side_effect=[(True, False), (True, False)],
            ), patch.object(
                soak.dbsnap,
                "capture_db_snapshot",
                side_effect=[RuntimeError("start snapshot failure"), RuntimeError("end snapshot failure")],
            ):
                rc = soak.main([
                    "--out-base",
                    str(out_base),
                    "--warmup-hours",
                    "0",
                    "--soak-hours",
                    "0.0001",
                ])

            run_dir = out_base / "20260304T120000Z"
            report_path = run_dir / "soak_report.json"
            self.assertEqual(rc, 0)
            self.assertTrue((run_dir / "db_snapshot_start.json").exists())
            self.assertTrue((run_dir / "db_snapshot_end.json").exists())
            self.assertTrue(report_path.exists())
            start_snapshot = json.loads((run_dir / "db_snapshot_start.json").read_text(encoding="utf-8"))
            self.assertIn("error", start_snapshot)
            self.assertFalse(start_snapshot.get("ok"))
            self.assertEqual(start_snapshot.get("mode_requested"), "auto")

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertIn("db_truth", report)
            self.assertTrue((report["db_truth"].get("errors") or []))
            self.assertFalse(report["db_truth"].get("delta_ok"))
            self.assertIsNone(report["db_truth"].get("delta"))

    def test_build_db_truth_delta_math(self) -> None:
        start = {
            "schema_version": "db-snapshot-v1",
            "captured_at": "2026-03-04T00:00:00Z",
            "ok": True,
            "mode_requested": "auto",
            "mode_used": "direct",
            "metrics": {
                "ats_endpoints_count": 100,
                "companies_with_ats_endpoint_count": 80,
                "companies_with_domain_count": 1500,
                "endpoints_by_type_map": {
                    "WORKDAY": 60,
                    "GREENHOUSE": 20,
                },
            },
        }
        end = {
            "schema_version": "db-snapshot-v1",
            "captured_at": "2026-03-04T12:00:00Z",
            "ok": True,
            "mode_requested": "auto",
            "mode_used": "docker",
            "metrics": {
                "ats_endpoints_count": 128,
                "companies_with_ats_endpoint_count": 97,
                "companies_with_domain_count": 1544,
                "endpoints_by_type_map": {
                    "WORKDAY": 70,
                    "GREENHOUSE": 21,
                    "LEVER": 4,
                },
            },
        }

        truth = dbsnap.build_db_truth(start, end)
        self.assertTrue(truth["delta_ok"])
        delta = truth["delta"]

        self.assertEqual(delta["ats_endpoints_count"], 28)
        self.assertEqual(delta["companies_with_ats_endpoint_count"], 17)
        self.assertEqual(delta["companies_with_domain_count"], 44)
        self.assertEqual(
            delta["endpoints_by_type"],
            {
                "GREENHOUSE": 1,
                "LEVER": 4,
                "WORKDAY": 10,
            },
        )

    def test_delta_is_not_computed_when_snapshot_not_ok(self) -> None:
        start = {
            "schema_version": "db-snapshot-v1",
            "captured_at": "2026-03-04T00:00:00Z",
            "ok": False,
            "mode_requested": "auto",
            "mode_used": None,
            "error": {
                "message": "start failed",
                "attempts": [{"mode": "docker", "error": "timeout"}],
            },
        }
        end = {
            "schema_version": "db-snapshot-v1",
            "captured_at": "2026-03-04T12:00:00Z",
            "ok": True,
            "mode_requested": "auto",
            "mode_used": "docker",
            "metrics": {
                "ats_endpoints_count": 128,
                "companies_with_ats_endpoint_count": 97,
                "companies_with_domain_count": 1544,
                "endpoints_by_type_map": {
                    "WORKDAY": 70,
                },
            },
        }

        truth = dbsnap.build_db_truth(start, end)
        self.assertFalse(truth["delta_ok"])
        self.assertIsNone(truth["delta"])
        self.assertTrue(truth["errors"])

    def test_snapshot_success_includes_identity_fields(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]

        def fake_run_psql_csv(cmd, env=None, timeout_seconds=15):
            sql = cmd[-1]
            if "current_database()" in sql:
                return [
                    {
                        "current_database": "delta_job_tracker",
                        "inet_server_addr": "127.0.0.1",
                        "inet_server_port": "5432",
                        "version": "PostgreSQL 16.2",
                    }
                ]
            if "COUNT(*) AS value FROM ats_endpoints" in sql:
                return [{"value": "10"}]
            if "COUNT(DISTINCT company_id) AS value FROM ats_endpoints" in sql:
                return [{"value": "8"}]
            if "FROM ats_endpoints" in sql and "GROUP BY" in sql:
                return [{"ats_type": "WORKDAY", "count": "7"}]
            if "COUNT(DISTINCT company_id) AS value FROM company_domains" in sql:
                return [{"value": "50"}]
            return []

        with patch.dict(
            "os.environ",
            {
                "PGHOST": "localhost",
                "PGPORT": "5432",
                "PGUSER": "delta",
                "PGDATABASE": "delta_job_tracker",
            },
            clear=False,
        ), patch("db_snapshot.run_psql_csv", side_effect=fake_run_psql_csv):
            snapshot = dbsnap.capture_db_snapshot(
                repo_root,
                mode="direct",
                query_timeout_seconds=1,
                discovery_timeout_seconds=1,
            )

        self.assertTrue(snapshot.get("ok"))
        self.assertEqual(snapshot.get("mode_used"), "direct")
        self.assertEqual(snapshot.get("mode_requested"), "direct")
        self.assertEqual(snapshot.get("connection_target", {}).get("host"), "localhost")
        identity = snapshot.get("db_identity") or {}
        self.assertEqual(identity.get("current_database"), "delta_job_tracker")
        self.assertEqual(identity.get("inet_server_addr"), "127.0.0.1")
        self.assertEqual(identity.get("inet_server_port"), 5432)
        self.assertIn("PostgreSQL", identity.get("version") or "")


if __name__ == "__main__":
    unittest.main()
