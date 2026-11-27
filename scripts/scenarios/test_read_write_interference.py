import json
from pathlib import Path
import tempfile
import unittest

import read_write_interference as runner


class InterferenceTest(unittest.TestCase):
    def test_per_category_throughput_and_conflict_counts(self):
        result = {"measurement": {"elapsedSeconds": 2, "offeredWindowSeconds": 2,
                                  "latency": {
                                      "read": {"count": 10, "p50Nanos": 1, "p95Nanos": 2, "p99Nanos": 3},
                                      "write": {"count": 4, "p50Nanos": 4, "p95Nanos": 5, "p99Nanos": 6}},
                                  "failures": 3, "rejected": 1, "retryCount": 0,
                                  "errors": {"SQL_LOCKED": 1, "SQL_BUSY": 1, "SERIALIZATION": 1}},
                  "measurementArrivalCounts": {"offeredReads": 11, "offeredWrites": 5}}
        metrics = runner.extract(result)
        self.assertEqual(metrics["readOpsPerSecond"], 5)
        self.assertEqual(metrics["writeOpsPerSecond"], 2)
        self.assertEqual(metrics["lockOrBusyErrors"], 2)
        self.assertEqual(metrics["transactionConflicts"], 1)
        self.assertEqual(metrics["arrivalCounts"]["offeredReads"], 11)

    def test_latency_ratio_requires_valid_read_only_baseline(self):
        manifest = {"identity": {"engines": ["h2"], "patterns": ["hot"], "workers": [1],
                                  "read_percentages": [100, 80], "repetitions": 1},
                    "runs": {
                        "h2-hot-t1-read100-r1": {"status": "OK", "metrics": {"readP95Nanos": 10}},
                        "h2-hot-t1-read80-r1": {"status": "OK", "metrics": {"readP95Nanos": 15}},
                    }}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            runner.write_reports(path, manifest)
            report = json.loads((path / "read-write-interference.json").read_text(encoding="utf-8"))
            self.assertEqual(report["runs"][1]["readP95RatioToReadOnly"], 1.5)
            self.assertIn("<svg", (path / "read-write-interference.html").read_text(encoding="utf-8"))
            manifest["runs"]["h2-hot-t1-read100-r1"]["status"] = "FAILED"
            runner.write_reports(path, manifest)
            report = json.loads((path / "read-write-interference.json").read_text(encoding="utf-8"))
            self.assertIsNone(report["runs"][1]["readP95RatioToReadOnly"])


if __name__ == "__main__":
    unittest.main()
