import json
from pathlib import Path
import tempfile
import unittest

import cache_refresh as runner


class CacheRefreshTest(unittest.TestCase):
    def test_missing_during_samples_are_not_fabricated(self):
        def phase(count, rate, p95):
            return {"successes": count, "throughputOpsPerSecond": rate,
                    "latency": {"read": {"p95Nanos": p95, "p99Nanos": p95, "maxNanos": p95}}}
        result = {
            "status": "OK", "artifactDirectory": "run",
            "cacheRefresh": {
                "cacheValid": True, "affectedEntries": 1, "cacheEntries": 10, "refreshNanos": 500,
                "readFailures": 0, "blockingDurationNanos": None,
                "beforeRefreshResources": {"rssBytes": 100}, "afterRefreshResources": {"rssBytes": 120},
                "afterReadersResources": {"rssBytes": 110},
                "reads": {"before": phase(10, 10, 100), "during": phase(0, 0, None),
                          "after": phase(10, 10, 120)},
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "run").mkdir()
            (path / "run" / "resources.jsonl").write_text(
                json.dumps({"phase": "refresh-during", "rssBytes": 125}) + "\n", encoding="utf-8")
            metrics = runner.extract(result, path)
            self.assertIsNone(metrics["duringP95Ratio"])
            self.assertIsNone(metrics["duringThroughputRatio"])
            self.assertIsNone(metrics["blockingDurationNanos"])
            self.assertEqual(metrics["sampledTemporaryRssGrowthBytes"], 25)
            result["cacheRefresh"]["reads"]["during"] = phase(1, 5, 200)
            result["status"] = "COMPLETED_WITH_ERRORS"
            self.assertIsNone(runner.extract(result, path)["duringP95Ratio"])


if __name__ == "__main__":
    unittest.main()
