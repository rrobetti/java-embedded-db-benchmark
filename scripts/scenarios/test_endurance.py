import json
from pathlib import Path
import tempfile
import unittest

import endurance


class EnduranceTest(unittest.TestCase):
    def test_duration_and_maintenance_bounds(self):
        self.assertEqual(endurance.parse_args(["--duration", "28800"]).duration, 28800)
        with self.assertRaises(SystemExit):
            endurance.parse_args(["--duration", "1", "--maintenance-interval", "1"])

    def test_extract_keeps_memory_change_and_missing_samples_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "run").mkdir()
            (path / "run" / "resources.jsonl").write_text("\n".join(json.dumps(x) for x in [
                {"phase": "loading", "rssBytes": 50},
                {"phase": "measurement", "rssBytes": 100},
                {"phase": "measurement-maintenance", "rssBytes": 150},
                {"phase": "measurement", "rssBytes": 120},
            ]), encoding="utf-8")
            result = {"artifactDirectory": "run", "workloadValid": True,
                      "endurance": {"maintenanceCompleted": 2},
                      "measurement": {"throughputOpsPerSecond": 10, "failures": 0,
                                      "latency": {"read": {"p95Nanos": 3, "p99Nanos": 4}}}}
            metrics = endurance.extract(result, path)
            self.assertEqual(metrics["sampledRssChangeBytes"], 20)
            self.assertEqual(metrics["peakSampledRssBytes"], 150)
            self.assertEqual(metrics["resourceSeries"], "run/resources.jsonl")
            result["endurance"]["maintenanceCompleted"] = 0
            with self.assertRaises(ValueError):
                endurance.extract(result, path)

    def test_chart_uses_actual_sample_time_and_omits_missing_values(self):
        series = [{"timestamp": "2025-01-01T00:00:00Z", "rssBytes": 1},
                  {"timestamp": "2025-01-01T00:00:10Z", "rssBytes": None},
                  {"timestamp": "2025-01-01T00:00:20Z", "rssBytes": 3}]
        chart = endurance.chart(series, "RSS", lambda sample: sample.get("rssBytes"))
        self.assertIn("20.0", chart)
        self.assertIn("60.0,220.0 660.0,40.0", chart)


if __name__ == "__main__":
    unittest.main()
