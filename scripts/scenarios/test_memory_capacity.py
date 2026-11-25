import json
from pathlib import Path
import tempfile
import unittest
import sys
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

import memory_capacity as capacity


class CapacityTest(unittest.TestCase):
    def test_missing_report_displays_supervisor_diagnostic(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            output = StringIO()
            with redirect_stderr(output):
                status = capacity.run_supervisor(
                    [sys.executable, "-c", "import sys; print('startup error'); sys.exit(2)"], path)
            self.assertEqual(status, 2)
            self.assertIn("code 2 without report.json", output.getvalue())
            self.assertIn("startup error", output.getvalue())
            self.assertIn(str(path / "supervisor.log"), output.getvalue())

    def test_failure_with_report_does_not_replace_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "report.json").write_text("{}", encoding="utf-8")
            output = StringIO()
            with redirect_stderr(output):
                status = capacity.run_supervisor([sys.executable, "-c", "raise SystemExit(1)"], path)
            self.assertEqual(status, 1)
            self.assertEqual(output.getvalue(), "")

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

    def test_progress_uses_last_complete_sample(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "runs" / "suite" / "child"
            path.mkdir(parents=True)
            (path / "resources.jsonl").write_text(json.dumps({
                "phase": "loading", "recordsLoaded": 70, "rssBytes": 123,
                "peakRssBytes": 150, "processCpuLoad": 0.25}) + "\n{\"phase\":",
                encoding="utf-8")
            output = StringIO()
            with redirect_stdout(output):
                capacity.print_progress(Path(tmp), 12)
            self.assertIn("phase=loading", output.getvalue())
            self.assertIn("recordsLoaded=70", output.getvalue())
            self.assertIn("childCpuLoad=25.0%", output.getvalue())


if __name__ == "__main__":
    unittest.main()
