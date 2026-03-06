import argparse
import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import efficiency_reporting_contract as contract
import run_ats_scale_canary as scale_canary
import run_ats_validation_canary as validation_canary


class ReportingConvergenceTest(unittest.TestCase):
    def test_stop_reason_normalization_contract(self) -> None:
        self.assertEqual(contract.normalize_stop_reason("request_budget_exceeded"), "REQUEST_BUDGET_EXCEEDED")
        self.assertEqual(contract.normalize_stop_reason("hard_timeout_reached"), "TIMEOUT")
        self.assertEqual(contract.normalize_stop_reason("host_failure_cutoff"), "HOST_FAILURE_CUTOFF")
        self.assertEqual(contract.normalize_stop_reason(""), "NONE")

    def test_validation_canary_builds_canonical_payload(self) -> None:
        args = argparse.Namespace(base_url="http://localhost:8080", discover_limit=50)
        payload = validation_canary.build_canonical_efficiency_report(
            args=args,
            out_dir=Path("out/20260306T000000Z"),
            domain_attempted=10,
            domain_resolved=6,
            domain_before=1000,
            domain_after=1008,
            domain_resolve_duration_ms=60000,
            probe_final_status={
                "companiesAttemptedCount": 12,
                "failedCount": 3,
                "succeededCount": 9,
                "endpointExtractedCount": 7,
                "stopReason": "request_budget_exceeded",
            },
            probe_duration_ms=120000,
            stage_failure_buckets={"HTTP_404": 2},
        )
        self.assertEqual(payload["schema_version"], "canonical-efficiency-report-v1")
        self.assertEqual(payload["phases"]["domain_resolution"]["scope_tag"], "run_scope")
        self.assertEqual(
            payload["phases"]["domain_resolution"]["rates_per_min"]["domains_discovered_per_min"], 8.0
        )
        self.assertEqual(
            payload["phases"]["ats_discovery"]["stop_reason"]["normalized"], "REQUEST_BUDGET_EXCEEDED"
        )

    def test_scale_canary_builds_canonical_payload(self) -> None:
        args = argparse.Namespace(base_url="http://localhost:8080", discover_limit=200)
        payload = scale_canary.build_canonical_efficiency_report(
            args=args,
            out_dir=Path("out/20260306T000000Z"),
            final_status={
                "requestBudget": 10000,
                "hardTimeoutSeconds": 900,
                "hostFailureCutoffCount": 6,
                "hostFailureCutoffSkips": 2,
            },
            frontier_seed_payload={"urlsEnqueued": 200, "urlsFetched": 120, "hostsSeen": 80},
            duration_seconds=300.0,
            attempted=150,
            endpoints_added=45,
            request_count=600,
            error_bucket_counts={"HTTP_404": 10},
            stop_reason_raw="hard_timeout_reached",
        )
        self.assertEqual(payload["schema_version"], "canonical-efficiency-report-v1")
        self.assertEqual(payload["phases"]["ats_discovery"]["scope_tag"], "run_scope")
        self.assertEqual(payload["phases"]["ats_discovery"]["rates_per_min"]["endpoints_created_per_min"], 9.0)
        self.assertEqual(payload["phases"]["ats_discovery"]["stop_reason"]["normalized"], "TIMEOUT")


if __name__ == "__main__":
    unittest.main()

