import json
from pathlib import Path
import tempfile
import unittest

import memory_reclamation as runner


class ReclamationTest(unittest.TestCase):
    def test_incomplete_stages_are_not_valid_measurements(self):
        result = {"reclamation": [{"status": "IN_PROGRESS"}]}
        with self.assertRaises(ValueError):
            runner.snapshot(result)

    def test_reports_distinct_delete_idle_and_reinsert_values(self):
        stage = {"status": "OK", "cycle": 1, "requestedPercent": 25, "removedBaseRows": 2,
                 "afterDelete": {"rssBytes": 150, "heapUsedBytes": 100},
                 "afterIdle": {"rssBytes": 140, "heapUsedBytes": 95},
                 "afterReinsert": {"rssBytes": 170, "heapUsedBytes": 110},
                 "deleteNanos": 10, "reinsertNanos": 20}
        manifest = {"identity": {"engines": ["h2"], "repetitions": 1},
                    "runs": {"h2-r1": {"status": "OK", "metrics": {
                        "initialized": {"rssBytes": 120}, "afterInitialLoad": {"rssBytes": 160},
                        "stages": [stage], "engineVersion": "test", "effectiveMemoryBudgetBytes": 200,
                        "effectiveConnectionMode": "dedicated"}}}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            runner.write_reports(path, manifest)
            report = json.loads((path / "memory-reclamation.json").read_text(encoding="utf-8"))
            row = report["stages"][0]
            self.assertEqual(row["afterDeleteRssBytes"], 150)
            self.assertEqual(row["afterIdleRssBytes"], 140)
            self.assertEqual(row["afterReinsertRssBytes"], 170)
            self.assertIn("does not establish a leak", report["methodology"])
            self.assertIn("<svg", (path / "memory-reclamation.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
