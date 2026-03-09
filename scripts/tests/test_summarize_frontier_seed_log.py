import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import summarize_frontier_seed_log as summary


class SummarizeFrontierSeedLogTest(unittest.TestCase):
    def test_summarizes_latest_run(self) -> None:
        log_text = """
12:20:07.694 [main] INFO x -- frontier.seed.start runId=seed-old requestedDomainLimit=1 requestedFetchLimit=1
12:20:07.800 [main] INFO x -- frontier.seed.summary runId=seed-old totalMs=10 dbMs=3 downstreamMs=2 parseMs=1 otherMs=4
12:20:08.000 [main] INFO x -- frontier.seed.start runId=seed-new requestedDomainLimit=2 requestedFetchLimit=1
12:20:08.050 [main] INFO x -- frontier.seed.scheduler.summary runId=seed-new urlsFetched=1 totalMs=25 claimNextDueUrlDbMs=4 findHostStateDbMs=3 countDueBlockedDbMs=0 enqueueDbMs=5 completeFetchDbMs=2 robotsCheckMs=4 httpFetchMs=3 parseMs=2 otherMs=2
12:20:08.070 [main] INFO x -- frontier.seed.summary runId=seed-new totalMs=70 dbMs=20 downstreamMs=12 parseMs=8 otherMs=30
""".strip()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "backend.log"
            log_path.write_text(log_text, encoding="utf-8")

            runs = summary.summarize_runs(log_path.read_text(encoding="utf-8").splitlines())
            selected = summary.select_run(runs, None)

        assert selected is not None
        self.assertEqual(selected["runId"], "seed-new")
        self.assertEqual(selected["seed"]["totalMs"], 70)
        self.assertEqual(selected["scheduler"]["enqueueDbMs"], 5)
        self.assertEqual(
            selected["eventSequence"],
            [
                "frontier.seed.start",
                "frontier.seed.scheduler.summary",
                "frontier.seed.summary",
            ],
        )

    def test_main_filters_by_run_id(self) -> None:
        log_text = """
12:20:07.694 [main] INFO x -- frontier.seed.start runId=seed-a requestedDomainLimit=1 requestedFetchLimit=1
12:20:07.800 [main] INFO x -- frontier.seed.summary runId=seed-a totalMs=10 dbMs=3 downstreamMs=2 parseMs=1 otherMs=4
12:20:08.000 [main] INFO x -- frontier.seed.start runId=seed-b requestedDomainLimit=2 requestedFetchLimit=1
12:20:08.070 [main] INFO x -- frontier.seed.summary runId=seed-b totalMs=70 dbMs=20 downstreamMs=12 parseMs=8 otherMs=30
""".strip()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "backend.log"
            log_path.write_text(log_text, encoding="utf-8")

            stdout_path = Path(tmp) / "stdout.json"
            saved_stdout = sys.stdout
            try:
                with stdout_path.open("w", encoding="utf-8") as handle:
                    sys.stdout = handle
                    rc = summary.main([str(log_path), "--run-id", "seed-a"])
            finally:
                sys.stdout = saved_stdout

            payload = json.loads(stdout_path.read_text(encoding="utf-8"))

        self.assertEqual(rc, 0)
        self.assertEqual(payload["runId"], "seed-a")
        self.assertEqual(payload["seed"]["dbMs"], 3)


if __name__ == "__main__":
    unittest.main()
