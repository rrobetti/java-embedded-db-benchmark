#!/usr/bin/env python3
"""Delete and restore relational dataset suffixes while observing memory."""

import argparse
import csv
import hashlib
import html
import json
import math
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

import memory_capacity as common


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", default=",".join(common.ENGINES), type=lambda x: common.csv_list(x, str))
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--idle-seconds", type=float, default=30)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int, default=3600)
    parser.add_argument("--load-commit-interval", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/memory-reclamation"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(engine not in common.ENGINES for engine in args.engines):
        parser.error("unknown engine")
    if (min(args.rows, args.cycles, args.repetitions, args.child_deadline) < 1
            or args.cycles > 1000 or args.load_commit_interval < 0):
        parser.error("invalid positive count or negative commit interval")
    if (not math.isfinite(args.idle_seconds) or not 0 <= args.idle_seconds <= 14400
            or not math.isfinite(args.sample_interval) or not 0.1 <= args.sample_interval <= 3600):
        parser.error("invalid idle time or sample interval")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    return args


def identity(args, java, jar):
    result = {key: value for key, value in vars(args).items()
              if key not in ("output", "resume", "rerun_failed", "dry_run")}
    result.update({
        "scenario": "memory-reclamation",
        "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "runnerSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "commonSha256": hashlib.sha256(Path(common.__file__).read_bytes()).hexdigest(),
        "gitCommit": common.command_output(["git", "--no-pager", "rev-parse", "HEAD"]),
        "gitDirty": common.command_output(["git", "--no-pager", "status", "--porcelain", "--untracked-files=no"]),
        "javaVersion": common.command_output([java, "-version"]),
        "machine": {"os": platform.platform(), "architecture": platform.machine(),
                    "logicalProcessors": os.cpu_count(), "processor": platform.processor(), "node": platform.node()},
    })
    return result


def run_id(engine, repetition):
    return f"{engine}-r{repetition}"


def snapshot(result):
    stages = result["reclamation"]
    if not all(stage.get("status") == "OK" for stage in stages):
        raise ValueError("Incomplete reclamation stages")
    return {
        "initialized": result["stages"]["init"],
        "afterInitialLoad": result["stages"]["load"],
        "stages": stages,
        "engineVersion": result.get("engineVersion"),
        "effectiveMemoryBudgetBytes": result.get("effectiveMemoryBudgetBytes"),
        "effectiveConnectionMode": result.get("effectiveConnectionMode"),
    }


def memory_chart(rows, engine, repetition):
    stages = [row for row in rows if row["engine"] == engine and row["repetition"] == repetition and row["status"] == "OK"]
    values = []
    for row in stages:
        for key, label in (("afterDeleteRssBytes", "delete"), ("afterIdleRssBytes", "idle"),
                           ("afterReinsertRssBytes", "reinsert")):
            value = row.get(key)
            if isinstance(value, (int, float)):
                values.append((f"c{row['cycle']}-{row['deletePercent']}%-{label}", value))
    if not values:
        return ""
    ymax = max(value for _, value in values) or 1
    width = max(720, 45 * len(values) + 100)
    parts = [f'<h3>{html.escape(engine)} repetition {repetition}: process RSS (bytes)</h3>',
             f'<svg viewBox="0 0 {width} 320" role="img" aria-label="Memory recovery after deletion">'
             f'<rect width="{width}" height="320" fill="white"/><path d="M65 20V250H{width - 20}" fill="none" stroke="#57606a"/>']
    coordinates = []
    for index, (label, value) in enumerate(values):
        x = 65 + (width - 85) * index / max(1, len(values) - 1)
        y = 250 - 220 * value / ymax
        coordinates.append((x, y))
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="#0969da">'
                     f'<title>{html.escape(label)}: {value}</title></circle>')
    parts.append('<polyline fill="none" stroke="#0969da" points="'
                 + " ".join(f"{x:.1f},{y:.1f}" for x, y in coordinates) + '"/>')
    parts.append(f'<text x="58" y="24" text-anchor="end">{ymax:.2g}</text>'
                 '<text x="58" y="250" text-anchor="end">0</text>'
                 '<text x="330" y="292" text-anchor="middle">Delete → idle → reinsert, per cycle and percentage</text></svg>')
    return "".join(parts)


def write_reports(output, manifest):
    runs = []
    rows = []
    for engine in manifest["identity"]["engines"]:
        for repetition in range(1, manifest["identity"]["repetitions"] + 1):
            entry = manifest["runs"].get(run_id(engine, repetition))
            if not entry:
                continue
            runs.append({"engine": engine, "repetition": repetition, **entry})
            if entry["status"] != "OK":
                rows.append({"engine": engine, "repetition": repetition, "status": entry["status"],
                             "error": entry.get("error"), "artifactDirectory": entry.get("artifactDirectory")})
                continue
            metrics = entry["metrics"]
            for stage in metrics["stages"]:
                row = {"engine": engine, "repetition": repetition, "status": "OK",
                       "cycle": stage["cycle"], "deletePercent": stage["requestedPercent"],
                       "removedBaseRows": stage["removedBaseRows"],
                       "initialRssBytes": metrics["initialized"].get("rssBytes"),
                       "loadedRssBytes": metrics["afterInitialLoad"].get("rssBytes"),
                       "afterDeleteRssBytes": stage["afterDelete"].get("rssBytes"),
                       "afterIdleRssBytes": stage["afterIdle"].get("rssBytes"),
                       "afterReinsertRssBytes": stage["afterReinsert"].get("rssBytes"),
                       "afterDeleteHeapBytes": stage["afterDelete"].get("heapUsedBytes"),
                       "afterIdleHeapBytes": stage["afterIdle"].get("heapUsedBytes"),
                       "afterReinsertHeapBytes": stage["afterReinsert"].get("heapUsedBytes"),
                       "deleteNanos": stage["deleteNanos"], "reinsertNanos": stage["reinsertNanos"],
                       "engineVersion": metrics["engineVersion"],
                       "effectiveMemoryBudgetBytes": metrics["effectiveMemoryBudgetBytes"],
                       "artifactDirectory": entry.get("artifactDirectory")}
                rows.append(row)
    note = ("Each stage deletes 25%, 50%, or 100% of base rows across all six tables, including the boundary "
            "order item required for foreign-key integrity, then reinserts and validates the exact seeded dataset. "
            "Each percentage starts from the fully restored dataset. RSS is process-wide and sampled; falling RSS "
            "indicates observed OS-level release, while retained RSS alone does not establish a leak. "
            "No explicit GC, vacuum, or compaction is performed. PGlite uses a custom loopback transport "
            "and serialized single-session execution.")
    common.write_json(output / "memory-reclamation.json",
                      {"identity": manifest["identity"], "runs": runs, "stages": rows, "methodology": note})
    columns = ["engine", "repetition", "status", "error", "cycle", "deletePercent", "removedBaseRows",
               "initialRssBytes", "loadedRssBytes", "afterDeleteRssBytes", "afterIdleRssBytes",
               "afterReinsertRssBytes", "afterDeleteHeapBytes", "afterIdleHeapBytes", "afterReinsertHeapBytes",
               "deleteNanos", "reinsertNanos", "engineVersion", "effectiveMemoryBudgetBytes", "artifactDirectory"]
    with (output / "memory-reclamation.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    text = ["# Memory reclamation", "", note, "",
            "| Engine | Rep. | Status | Cycle | Deleted % | Initial RSS | Loaded RSS | Delete RSS | Idle RSS | Reinsert RSS |",
            "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        text.append("| " + " | ".join(str(row.get(k, "")) for k in (
            "engine", "repetition", "status", "cycle", "deletePercent", "initialRssBytes",
            "loadedRssBytes", "afterDeleteRssBytes", "afterIdleRssBytes", "afterReinsertRssBytes")) + " |")
    markdown = "\n".join(text) + "\n"
    (output / "memory-reclamation.md").write_text(markdown, encoding="utf-8")
    charts = "".join(memory_chart(rows, engine, repetition)
                     for engine in manifest["identity"]["engines"]
                     for repetition in range(1, manifest["identity"]["repetitions"] + 1))
    (output / "memory-reclamation.html").write_text(
        "<!doctype html><meta charset=\"utf-8\"><title>Memory reclamation</title>"
        "<style>body{font:15px system-ui;max-width:1000px;margin:2em auto}svg{max-width:100%}"
        "pre{white-space:pre-wrap}</style>" + charts + "<pre>" + html.escape(markdown) + "</pre>", encoding="utf-8")


def execute(args):
    total = len(args.engines) * args.repetitions
    print(f"Planned: {total} fresh-JVM runs; minimum idle time "
          f"{total * args.cycles * 3 * args.idle_seconds:.1f}s (excluding load, deletion and validation).", flush=True)
    if args.dry_run:
        for engine in args.engines:
            print(f"{engine}: rows={args.rows}, cycles={args.cycles}, repetitions={args.repetitions}")
        return 0
    jar = common.ROOT / "target" / "benchmark.jar"
    if not jar.is_file():
        raise RuntimeError("Build target/benchmark.jar first with ./mvnw package")
    java = str(Path(os.environ["JAVA_HOME"]) / "bin" / ("java.exe" if os.name == "nt" else "java")) \
        if os.environ.get("JAVA_HOME") else shutil.which("java")
    if not java:
        raise RuntimeError("Java not found")
    config = identity(args, java, jar)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        if not args.resume:
            raise RuntimeError("Manifest exists; use --resume or a new output directory")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["identity"] != config:
            raise RuntimeError("Cannot resume: experiment configuration, build, Git state or machine changed")
    elif args.resume:
        raise RuntimeError("No manifest exists to resume")
    else:
        manifest = {"identity": config, "runs": {}}
        common.write_json(manifest_path, manifest)
    write_reports(output, manifest)
    for engine in args.engines:
        for repetition in range(1, args.repetitions + 1):
            key = run_id(engine, repetition)
            previous = manifest["runs"].get(key, {})
            if previous.get("status") == "OK" or previous.get("status") in ("FAILED", "UNSUPPORTED") and not args.rerun_failed:
                continue
            attempt = previous.get("attempt", 0) + 1
            directory = output / "runs" / engine / f"r{repetition}-attempt{attempt}"
            entry = {"status": "RUNNING", "attempt": attempt, "artifactDirectory": str(directory.relative_to(output))}
            manifest["runs"][key] = entry
            common.write_json(manifest_path, manifest)
            directory.mkdir(parents=True, exist_ok=False)
            native = directory / "native"
            native.mkdir()
            done = sum(item.get("status") != "RUNNING" for item in manifest["runs"].values())
            print(f"{key} rows={args.rows} cycles={args.cycles} completed={done}/{total}", flush=True)
            command = [java, "-Xmx512m", f"-Djava.io.tmpdir={native}", f"-Dorg.sqlite.tmpdir={native}",
                       "-jar", str(jar), "--profile", "smoke", "--engine", engine, "--workloads", "memory-reclamation",
                       "--rows", str(args.rows), "--reclamation-cycles", str(args.cycles),
                       "--idle-seconds", str(args.idle_seconds), "--load-commit-interval", str(args.load_commit_interval),
                       "--sample-interval", str(args.sample_interval), "--iterations", "1", "--seed", str(args.seed),
                       "--memory-budget", args.memory_budget, "--jvm-heap", args.jvm_heap,
                       "--child-deadline", str(args.child_deadline), "--output", str(directory)]
            start = time.monotonic()
            try:
                exit_code = common.run_supervisor(command, directory)
                entry["exitCode"] = exit_code
                report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
                result = report["results"][0]
                if exit_code == 0 and result.get("status") == "OK" and result.get("valid") is True:
                    entry["metrics"] = snapshot(result)
                    if len(entry["metrics"]["stages"]) != args.cycles * 3:
                        raise ValueError("Unexpected number of reclamation stages")
                    entry["status"] = "OK"
                else:
                    entry["status"] = "UNSUPPORTED" if result.get("status") == "UNSUPPORTED" else "FAILED"
                    entry["error"] = str(result.get("errorClass") or result.get("status"))
            except (OSError, ValueError, KeyError, IndexError, TypeError) as error:
                entry["status"] = "FAILED"
                entry["error"] = f"Missing or invalid benchmark evidence: {error}"
            entry["wallSeconds"] = time.monotonic() - start
            common.write_json(manifest_path, manifest)
            write_reports(output, manifest)
            print(f"{key}: {entry['status']} {entry.get('error', '')}", flush=True)
    return 1 if any(item["status"] != "OK" for item in manifest["runs"].values()) else 0


def main():
    try:
        return execute(parse_args())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"memory-reclamation: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
