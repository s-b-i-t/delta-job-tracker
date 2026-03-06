import argparse
import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import efficiency_reporting_contract as erc
import run_ats_scale_canary as scale_canary
import run_ats_validation_canary as validation_canary


class ReportingConvergenceTests(unittest.TestCase):
    def test_stop_reason_normalization_contract(self) -> None:
        self.assertEqual(erc.normalize_stop_reason("timed out waiting for discovery run"), "TIMEOUT")
        self.assertEqual(erc.normalize_stop_reason("requestBudget exceeded"), "REQUEST_BUDGET_EXCEEDED")
        self.assertEqual(erc.normalize_stop_reason("host_failure_cutoff reached"), "HOST_FAILURE_GUARDRAIL")
        self.assertEqual(erc.normalize_stop_reason(None), "UNKNOWN")

    def test_validation_canary_queue_fetch_and_discovery_rates(self) -> None:
        args = argparse.Namespace(base_url="http://localhost:8080", discover_limit=50)
        payload = validation_canary.build_canonical_efficiency_report(
            args=args,
            out_dir=Path("out/20260306T000000Z"),
            domain_attempted=30,
            domain_resolved=18,
            domain_before=100,
            domain_after=112,
            domain_resolve_duration_ms=90_000,
            queue_fetch_counts_before={"crawl_url_attempts_count": 500, "crawl_urls_queued_count": 120, "crawl_urls_fetching_count": 3},
            queue_fetch_counts_after={"crawl_url_attempts_count": 560, "crawl_urls_queued_count": 100, "crawl_urls_fetching_count": 1},
            queue_fetch_window_duration_ms=120_000,
            probe_final_status={
                "companiesAttemptedCount": 40,
                "failedCount": 10,
                "succeededCount": 30,
                "endpointExtractedCount": 16,
                "lastError": "request limit exceeded",
            },
            probe_duration_ms=60_000,
            stage_failure_buckets={"HTTP_TIMEOUT": 3},
        )

        queue_fetch = payload["phases"]["queue_fetch"]
        self.assertEqual(queue_fetch["scope_tag"], "global_snapshot")
        self.assertEqual(queue_fetch["counters"]["crawl_url_attempts_delta"], 60)
        self.assertEqual(queue_fetch["rates_per_min"]["crawl_url_attempts_delta_per_min"], 30.0)
        self.assertEqual(payload["phases"]["ats_discovery"]["stop_reason"]["normalized"], "REQUEST_BUDGET_EXCEEDED")
        self.assertEqual(
            payload["phases"]["domain_resolution"]["rates_per_min"]["domains_discovered_per_min"],
            8.0,
        )

    def test_scale_canary_queue_fetch_rates_are_direct(self) -> None:
        args = argparse.Namespace(base_url="http://localhost:8080", discover_limit=200)
        payload = scale_canary.build_canonical_efficiency_report(
            args=args,
            out_dir=Path("out/20260306T000000Z"),
            final_status={"requestBudget": 5000, "hardTimeoutSeconds": 300},
            frontier_seed_payload={"urlsEnqueued": 90, "urlsFetched": 30, "hostsSeen": 25, "blockedByBackoff": 4},
            frontier_seed_duration_ms=30_000,
            duration_seconds=300.0,
            attempted=100,
            endpoints_added=35,
            request_count=300,
            error_bucket_counts={"HTTP_TIMEOUT": 5},
            stop_reason_raw="Timed out while polling",
        )

        queue_fetch = payload["phases"]["queue_fetch"]
        self.assertEqual(queue_fetch["scope_tag"], "run_scope")
        self.assertEqual(queue_fetch["rates_per_min"]["urls_enqueued_per_min"], 180.0)
        self.assertEqual(queue_fetch["rates_per_min"]["urls_fetched_per_min"], 60.0)
        self.assertEqual(payload["phases"]["ats_discovery"]["stop_reason"]["normalized"], "TIMEOUT")


if __name__ == "__main__":
    unittest.main()
