import json
from pathlib import Path
import tempfile
import unittest

import memory_capacity as capacity


class CapacityTest(unittest.TestCase):
    def test_memory_summary_uses_sampled_phases_and_seven_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "run").mkdir()
            samples = [
                {"phase": "loading", "rssBytes": 160},
                {"phase": "loading", "rssBytes": 180},
                {"phase": "validation", "rssBytes": 170},
                {"phase": "measurement", "rssBytes": 190},
                {"phase": "measurement", "rssBytes": 210},
            ]
            (path / "run" / "resources.jsonl").write_text(
                "\n".join(json.dumps(s) for s in samples), encoding="utf-8")
            result = {"artifactDirectory": "run", "configuration": {"rows": 10},
                      "stages": {"init": {"rssBytes": 100}}, "loadNanos": 3_000_000_000,
                      "logicalDatasetBytes": 400, "measurement": {"throughputOpsPerSecond": 4}}
            summary = capacity.memory_summary(result, path)
            self.assertEqual(summary["records"], 70)
            self.assertEqual(summary["loadSeconds"], 3)
            self.assertEqual(summary["sampledPeakLoadRssBytes"], 180)
            self.assertEqual(summary["postLoadRssBytes"], 170)
            self.assertEqual(summary["steadyMedianRssBytes"], 200)
            self.assertEqual(summary["steadyIncrementalRssBytes"], 100)
            self.assertEqual(summary["recordsPerGiBIncrementalRss"], 70 * capacity.GIB / 100)

    def test_boundary_does_not_claim_absolute_maximum(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            manifest = {
                "identity": {"engines": ["h2", "sqlite"], "sizes": [10, 20], "repetitions": 1},
                "runs": {
                    "h2-10-r1": {"status": "OK"},
                    "h2-20-r1": {"status": "FAILED"},
                    "sqlite-10-r1": {"status": "OK"},
                    "sqlite-20-r1": {"status": "OK"},
                },
            }
            capacity.write_reports(path, manifest)
            report = json.loads((path / "capacity.json").read_text(encoding="utf-8"))
            self.assertEqual(report["boundaries"]["h2"]["largestSuccessfulBaseRows"], 10)
            self.assertEqual(report["boundaries"]["h2"]["smallestFailedBaseRows"], 20)
            self.assertFalse(report["boundaries"]["h2"]["upperLimitReachedSuccessfully"])
            self.assertTrue(report["boundaries"]["sqlite"]["upperLimitReachedSuccessfully"])
            self.assertIn("not an absolute capacity", report["boundaries"]["sqlite"]["note"])

    def test_rejects_unsorted_sizes_and_unknown_engines(self):
        with self.assertRaises(SystemExit):
            capacity.parse_args(["--sizes", "20,10"])
        with self.assertRaises(SystemExit):
            capacity.parse_args(["--engines", "h2,unknown"])


if __name__ == "__main__":
    unittest.main()
