import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import ingestion_comparison as comparison


class IngestionComparisonTest(unittest.TestCase):
    def test_incremental_interval_must_be_positive(self):
        with self.assertRaises(SystemExit):
            comparison.parse_args(["--incremental-interval", "0"])

    def test_reports_keep_failures_and_missing_modes_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            mode = output / "single-transaction"
            mode.mkdir()
            (mode / "capacity.json").write_text(json.dumps({"identity": {"loadCommitInterval": 0}, "runs": [
                {"engine": "h2", "baseRows": 10, "repetition": 1, "status": "FAILED",
                 "error": "TIMEOUT"}]}), encoding="utf-8")
            comparison.write_report(output, comparison.parse_args([]), {"incremental": "not executed"})
            report = json.loads((output / "ingestion-comparison.json").read_text(encoding="utf-8"))
            self.assertEqual(report["modesAvailable"], ["single-transaction"])
            self.assertEqual(report["runs"][0]["error"], "TIMEOUT")
            self.assertEqual(report["runs"][0]["commitInterval"], 0)
            self.assertEqual(report["modeErrors"]["incremental"], "not executed")

    def test_resume_only_passed_for_existing_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            args = comparison.parse_args(["--resume", "--rerun-failed"])
            self.assertFalse(comparison.capacity_args(args, 0, directory).resume)
            (directory / "manifest.json").write_text("{}", encoding="utf-8")
            config = comparison.capacity_args(args, 0, directory)
            self.assertTrue(config.resume)
            self.assertTrue(config.rerun_failed)

    def test_failed_mode_does_not_stop_other_mode_and_changed_resume_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "target").mkdir()
            (root / "target" / "benchmark.jar").write_bytes(b"fixture")
            output = root / "results"
            args = comparison.parse_args(["--output", str(output), "--engines", "h2"])
            with mock.patch.object(comparison.capacity, "ROOT", root), \
                    mock.patch.object(comparison.shutil, "which", return_value="/java"), \
                    mock.patch.dict(comparison.os.environ, {"JAVA_HOME": ""}), \
                    mock.patch.object(comparison.capacity, "experiment_identity",
                                      side_effect=lambda config, jar, java: {
                                          "loadCommitInterval": config.load_commit_interval}), \
                    mock.patch.object(comparison.capacity, "execute", side_effect=[1, 0]) as execute:
                self.assertEqual(comparison.execute(args), 1)
                self.assertEqual(execute.call_count, 2)
                changed = comparison.parse_args(["--output", str(output), "--engines", "sqlite", "--resume"])
                with self.assertRaisesRegex(RuntimeError, "Cannot resume"):
                    comparison.execute(changed)
                self.assertEqual(execute.call_count, 2)


if __name__ == "__main__":
    unittest.main()
