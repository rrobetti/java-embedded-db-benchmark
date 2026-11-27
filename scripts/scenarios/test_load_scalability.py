import json
from pathlib import Path
import tempfile
import unittest

import load_scalability as runner


class LoadScalabilityTest(unittest.TestCase):
    def test_arrival_accounting_and_target_evaluation(self):
        result = {
            "measurement": {
                "attempts": 100, "successes": 90, "failures": 3, "rejected": 7,
                "offeredWindowSeconds": 10, "throughputOpsPerSecond": 9,
                "latency": {"read": {"p95Nanos": 1_000_000, "p99Nanos": 2_000_000}},
            }
        }
        metrics = runner.extract(result, p99_ms=3, max_error_rate=0.1)
        self.assertEqual(metrics["offered"], 100)
        self.assertEqual(metrics["accepted"], 93)
        self.assertEqual(metrics["completed"], 93)
        self.assertEqual(metrics["rejected"], 7)
        self.assertEqual(metrics["actualOfferedPerSecond"], 10)
        self.assertEqual(metrics["failedOrRejectedRate"], 0.1)
        self.assertTrue(metrics["meetsTargets"])
        self.assertFalse(runner.extract(result, p99_ms=1, max_error_rate=0.1)["meetsTargets"])
        self.assertFalse(runner.extract(result, p99_ms=3, max_error_rate=0.09)["meetsTargets"])

    def test_boundary_requires_every_repetition_to_meet_targets(self):
        manifest = {"identity": {"engines": ["h2"], "rates": [10.0, 20.0], "repetitions": 2},
                    "runs": {
                        "h2-rate10-r1": {"status": "OK", "metrics": {"meetsTargets": True, "throughputOpsPerSecond": 9}},
                        "h2-rate10-r2": {"status": "OK", "metrics": {"meetsTargets": True, "throughputOpsPerSecond": 11}},
                        "h2-rate20-r1": {"status": "OK", "metrics": {"meetsTargets": True, "throughputOpsPerSecond": 19}},
                        "h2-rate20-r2": {"status": "OK", "metrics": {"meetsTargets": False, "throughputOpsPerSecond": 18}},
                    }}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            runner.write_reports(path, manifest)
            report = json.loads((path / "load-scalability.json").read_text(encoding="utf-8"))
            self.assertEqual(report["boundaries"]["h2"]["largestTargetRateMeetingTargets"], 10)
            self.assertEqual(report["boundaries"]["h2"]["maximumMeanSustainableThroughput"], 10)
            self.assertFalse(report["boundaries"]["h2"]["upperTestedRateMetTargets"])
            self.assertIn("<svg", (path / "load-scalability.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
