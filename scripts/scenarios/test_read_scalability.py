import json
from pathlib import Path
import tempfile
import unittest

import read_scalability as runner


class ReadScalabilityTest(unittest.TestCase):
    def test_scaling_uses_matching_successful_one_worker_repetition(self):
        manifest = {
            "identity": {"engines": ["h2"], "workloads": ["point"], "workers": [1, 2],
                         "repetitions": 2},
            "runs": {
                "h2-point-t1-r1": {"status": "OK", "metrics": {"throughputOpsPerSecond": 10, "p95Nanos": 1}},
                "h2-point-t2-r1": {"status": "OK", "metrics": {"throughputOpsPerSecond": 15, "p95Nanos": 2}},
                "h2-point-t1-r2": {"status": "FAILED"},
                "h2-point-t2-r2": {"status": "OK", "metrics": {"throughputOpsPerSecond": 40, "p95Nanos": 3}},
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            runner.write_reports(path, manifest)
            report = json.loads((path / "read-scalability.json").read_text(encoding="utf-8"))
            rows = {f"{x['workers']}-{x['repetition']}": x for x in report["runs"]}
            self.assertEqual(rows["2-1"]["scalabilityVsOneWorker"], 1.5)
            self.assertIsNone(rows["2-2"]["scalabilityVsOneWorker"])
            self.assertIn("serialized single-session", report["methodology"])
            self.assertIn("<svg", (path / "read-scalability.html").read_text(encoding="utf-8"))

    def test_extract_keeps_unavailable_cpu_and_rss_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "run").mkdir()
            (path / "run" / "resources.jsonl").write_text(
                '{"phase":"measurement","rssBytes":null}\n', encoding="utf-8")
            result = {"artifactDirectory": "run",
                      "measurement": {"throughputOpsPerSecond": 5, "successes": 2, "failures": 0,
                                      "errorRate": 0, "latency": {"read": {"p95Nanos": 4, "p99Nanos": 5}}},
                      "resourceDeltas": {"cpuCoreEquivalent": None}}
            metrics = runner.extract(result, path, "point")
            self.assertIsNone(metrics["throughputPerCpuCore"])
            self.assertIsNone(metrics["throughputPerGiBRss"])


if __name__ == "__main__":
    unittest.main()
