import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import run_research as research


class ResearchTest(unittest.TestCase):
    def test_quick_plan_counts_and_exclusions(self):
        args = research.parse_args(["--engines", "h2", "--sizes", "10", "--rows", "10",
                                    "--workers", "1", "--exclude-expensive"])
        plan = research.plan(args)
        self.assertEqual([item["name"] for item in plan], [
            "memory-capacity", "read-scalability", "read-write-interference", "cache-refresh", "endurance"])
        self.assertEqual(sum(item["count"] for item in plan), 7)
        self.assertEqual(sum(item["minimumSeconds"] for item in plan), 9)

    def test_cpu_affinity_limit_is_verified(self):
        args = research.parse_args(["--cpu-cores", "2"])
        with mock.patch.object(research.os, "sched_getaffinity", side_effect=[{2, 4, 6}, {2, 4}]), \
                mock.patch.object(research.os, "sched_setaffinity") as set_affinity:
            result = research.configure_cpu(args)
        set_affinity.assert_called_once_with(0, {2, 4})
        self.assertTrue(result["enforced"])
        self.assertEqual(result["effectiveCpuIds"], [2, 4])

    def test_comprehensive_interference_uses_representative_worker_counts(self):
        args = research.parse_args(["--level", "comprehensive", "--scenarios", "read-write-interference",
                                    "--engines", "h2"])
        entry = research.plan(args)[0]
        self.assertEqual(entry["count"], 2 * 4 * 8 * 3)

    def test_scope_verification_uses_restrictive_ancestor_limits(self):
        args = research.parse_args(["--systemd-memory-max", "2g", "--systemd-cpu-quota", "150"])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            leaf = root / "scope"
            leaf.mkdir()
            (root / "memory.max").write_text(str(1024 ** 3), encoding="utf-8")
            (root / "cpu.max").write_text("100000 100000", encoding="utf-8")
            (leaf / "memory.max").write_text(str(2 * 1024 ** 3), encoding="utf-8")
            (leaf / "cpu.max").write_text("150000 100000", encoding="utf-8")
            limits = research.verify_scope(args, leaf, root)
            self.assertTrue(limits["memoryEnforced"])
            self.assertTrue(limits["cpuEnforced"])
            self.assertEqual(limits["effectiveMemoryMaxBytes"], 1024 ** 3)
            self.assertEqual(limits["effectiveCpuQuotaCores"], 1)
            (root / "cpu.max").unlink()
            self.assertIsNone(research.verify_scope(args, leaf, root)["effectiveCpuQuotaCores"])
            (leaf / "memory.max").write_text("max", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "not enforced"):
                research.verify_scope(args, leaf, root)

    def test_scope_failure_has_no_unbounded_fallback(self):
        args = research.parse_args(["--systemd-memory-max", "2g"])
        with mock.patch.object(research.shutil, "which", return_value="/usr/bin/systemd-run"), \
                mock.patch.object(research.subprocess, "run", return_value=mock.Mock(returncode=1)) as run:
            self.assertEqual(research.launch_scope(args, ["--systemd-memory-max", "2g"]), 1)
        command = run.call_args.args[0]
        self.assertIn("MemoryMax=2147483648", command)
        self.assertIn("--inside-systemd-scope", command)

    def test_classifies_unsupported_incorrect_and_incomplete(self):
        self.assertEqual(research.classification({"status": "FAILED", "error": "UNSUPPORTED"}), "UNSUPPORTED")
        self.assertEqual(research.classification({"status": "FAILED", "error": "CORRECTNESS"}), "INCORRECT")
        self.assertEqual(research.classification({"status": "RUNNING"}), "INCOMPLETE")
        for error, expected in (("OOM", "MEMORY_EXHAUSTION"), ("NATIVE_ALLOCATION", "MEMORY_EXHAUSTION"),
                                ("MEMORY_BUDGET", "MEMORY_BUDGET"), ("MEMORY_ADMISSION", "MEMORY_ADMISSION"),
                                ("CHILD_DEADLINE", "CHILD_DEADLINE"), ("HEARTBEAT_TIMEOUT", "HEARTBEAT_TIMEOUT"),
                                ("INACTIVITY_TIMEOUT", "INACTIVITY_TIMEOUT"), ("JVM_CRASH", "JVM_CRASH")):
            with self.subTest(error=error):
                self.assertEqual(research.classification({"status": "FAILED", "error": error}), expected)

    def test_report_masks_failed_comparison_and_preserves_raw_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            directory = output / "memory-capacity"
            directory.mkdir()
            (directory / "capacity.json").write_text(json.dumps({
                "identity": {"repetitions": 1},
                "runs": [{"engine": "h2", "baseRows": 10, "repetition": 1, "status": "FAILED",
                          "recordsPerGiBTotalRss": 999}],
            }), encoding="utf-8")
            manifest = {"identity": {"plan": [{"name": "memory-capacity", "count": 1}]},
                        "scenarios": {"memory-capacity": {"status": "FAILED", "includeEvidence": True}}}
            research.write_reports(output, manifest)
            text = (output / "research.md").read_text(encoding="utf-8")
            self.assertNotIn("999", text)
            self.assertIn("FAILED", text)
            report = json.loads((output / "research.json").read_text(encoding="utf-8"))
            self.assertEqual(report["evidence"]["memory-capacity"]["runs"][0]["recordsPerGiBTotalRss"], 999)
            self.assertIn("No experiment evidence", text)


if __name__ == "__main__":
    unittest.main()
