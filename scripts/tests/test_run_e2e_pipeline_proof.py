import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import run_e2e_pipeline_proof as e2e_proof


class E2ePipelineProofDiagnosticsTest(unittest.TestCase):
    def test_zero_endpoints_has_explicit_blocker_reason(self) -> None:
        frontier_stage = {"urlsEnqueued": 10}
        fetch_stage = {
            "urlsFetched": 0,
            "httpRequestCount": 5,
            "statusBucketCounts": {"ROBOTS_BLOCKED": 0},
        }
        ats_stage = {
            "selectionReturnedCount": 5,
            "attempted_companies": 5,
            "succeeded_count": 0,
            "vendor_detected_count": 0,
            "created_endpoints": 0,
            "error_type_histogram": {"discovery_fetch_failed": 5},
        }

        blockers = e2e_proof.classify_blockers(frontier_stage, fetch_stage, ats_stage)
        self.assertTrue(blockers)
        self.assertIn("all_fetch_attempts_failed", blockers)
        self.assertEqual(e2e_proof.build_go_no_go(ats_stage, blockers), "NO-GO")

    def test_unknown_zero_endpoints_fallback_blocker(self) -> None:
        blockers = e2e_proof.classify_blockers({}, {}, {"created_endpoints": 0})
        self.assertTrue(blockers)
        self.assertTrue(
            any(
                blocker in {"unknown_zero_endpoints", "no_eligible_companies", "no_frontier_candidates"}
                for blocker in blockers
            )
        )


if __name__ == "__main__":
    unittest.main()
