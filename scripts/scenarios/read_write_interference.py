#!/usr/bin/env python3
"""Compare independent and hot-account reads under configurable write pressure."""

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
import statistics
import subprocess
import sys
import time

import memory_capacity as common


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", default=",".join(common.ENGINES), type=lambda x: common.csv_list(x, str))
    parser.add_argument("--workers", default="1,4,16,32", type=lambda x: common.csv_list(x, int))
    parser.add_argument("--read-percentages", default="100,99,95,90,80,50,20,0",
                        type=lambda x: common.csv_list(x, int))
    parser.add_argument("--patterns", default="independent,hot", type=lambda x: common.csv_list(x, str))
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--fixed-read-rate", type=float, default=0,
                        help="Read arrivals/s in open-loop mode; 0 uses closed-loop")
    parser.add_argument("--worker-groups", action="store_true",
                        help="Assign dedicated closed-loop readers/writers by rounded worker proportion")
    parser.add_argument("--queue-capacity", type=int, default=256)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int, default=1800)
    parser.add_argument("--load-commit-interval", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/read-write"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(x not in common.ENGINES for x in args.engines) or any(x not in ("independent", "hot") for x in args.patterns):
        parser.error("unknown engine or access pattern")
    if (args.workers != sorted(args.workers) or any(x < 1 or x > 256 for x in args.workers)
            or any(x < 0 or x > 100 for x in args.read_percentages)):
        parser.error("workers must be increasing in 1..256 and percentages in 0..100")
    if min(args.rows - 3, args.repetitions, args.queue_capacity, args.child_deadline) < 1 or args.load_commit_interval < 0:
        parser.error("rows must be >=4 and counts positive")
    if (not all(math.isfinite(x) for x in (args.warmup, args.duration, args.sample_interval, args.fixed_read_rate))
            or args.warmup < 0 or args.duration <= 0 or not 0.1 <= args.sample_interval <= 3600
            or args.fixed_read_rate < 0 or args.queue_capacity > 1_000_000):
        parser.error("invalid timing, rate or queue capacity")
    if args.fixed_read_rate and (args.worker_groups or any(
            args.fixed_read_rate * 100 / p > 1_000_000 for p in args.read_percentages if p > 0)):
        parser.error("fixed rate is incompatible with worker groups or exceeds the total rate limit")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    return args


def identity(args, java, jar):
    result = {key: value for key, value in vars(args).items()
              if key not in ("output", "resume", "rerun_failed", "dry_run")}
    result.update({
        "scenario": "read-write-interference",
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


def run_id(engine, pattern, workers, read_percent, repetition):
    return f"{engine}-{pattern}-t{workers}-read{read_percent}-r{repetition}"


def extract(result):
    measurement = result["measurement"]
    seconds = measurement["elapsedSeconds"]
    reads = measurement["latency"]["read"]
    writes = measurement["latency"]["write"]
    errors = measurement["errors"]
    arrivals = result.get("measurementArrivalCounts")
    return {
        "readOps": reads["count"], "writeOps": writes["count"],
        "readOpsPerSecond": reads["count"] / seconds if seconds > 0 else None,
        "writeOpsPerSecond": writes["count"] / seconds if seconds > 0 else None,
        "readP50Nanos": reads["p50Nanos"], "readP95Nanos": reads["p95Nanos"], "readP99Nanos": reads["p99Nanos"],
        "writeP50Nanos": writes["p50Nanos"], "writeP95Nanos": writes["p95Nanos"], "writeP99Nanos": writes["p99Nanos"],
        "failures": measurement["failures"], "rejected": measurement["rejected"],
        "lockOrBusyErrors": sum(errors.get(key, 0) for key in ("SQL_LOCKED", "SQL_BUSY", "DEADLOCK")),
        "transactionConflicts": errors.get("SERIALIZATION", 0), "timeouts": errors.get("TIMEOUT", 0),
        "retries": measurement["retryCount"], "arrivalCounts": arrivals,
        "offeredWindowSeconds": measurement["offeredWindowSeconds"],
        "cpuCoreEquivalent": (result.get("resourceDeltas") or {}).get("cpuCoreEquivalent"),
        "steadyRssBytes": ((result.get("stages") or {}).get("steady") or {}).get("rssBytes"),
        "engineVersion": result.get("engineVersion"),
        "effectiveMemoryBudgetBytes": result.get("effectiveMemoryBudgetBytes"),
        "effectiveConnectionMode": result.get("effectiveConnectionMode"),
        "effectiveConnections": result.get("effectiveConnections"),
    }


def latency_chart(rows, pattern, workers):
    points = [row for row in rows if row["pattern"] == pattern and row["workers"] == workers
              and row["status"] == "OK" and isinstance(row.get("readP95Nanos"), (int, float))]
    if not points:
        return ""
    xs = sorted({row["writePercent"] for row in points})
    ymax = max(row["readP95Nanos"] for row in points) or 1
    colors = ("#0969da", "#1a7f37", "#8250df", "#bc4c00")
    parts = [f'<h3>Read p95 latency (ns): {html.escape(pattern)}, {workers} workers</h3>',
             '<svg viewBox="0 0 720 300" role="img" aria-label="Read latency versus write percentage">'
             '<rect width="720" height="300" fill="white"/><path d="M65 20V250H600" fill="none" stroke="#57606a"/>']
    for index, engine in enumerate(common.ENGINES):
        coordinates = []
        for percentage in xs:
            values = [row["readP95Nanos"] for row in points
                      if row["engine"] == engine and row["writePercent"] == percentage]
            if values:
                x = 65 + 535 * xs.index(percentage) / max(1, len(xs) - 1)
                y = 250 - 220 * statistics.mean(values) / ymax
                coordinates.append((x, y))
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{colors[index]}"/>')
        if coordinates:
            parts.append('<polyline fill="none" stroke="' + colors[index] + '" points="'
                         + " ".join(f"{x:.1f},{y:.1f}" for x, y in coordinates) + '"/>')
            parts.append(f'<text x="610" y="{35 + 20 * index}" fill="{colors[index]}">{engine}</text>')
    for index, percentage in enumerate(xs):
        x = 65 + 535 * index / max(1, len(xs) - 1)
        parts.append(f'<text x="{x:.1f}" y="270" text-anchor="middle">{percentage}</text>')
    parts.append(f'<text x="58" y="24" text-anchor="end">{ymax:.2g}</text>'
                 '<text x="58" y="250" text-anchor="end">0</text>'
                 '<text x="330" y="292" text-anchor="middle">Configured write percentage</text></svg>')
    return "".join(parts)


def write_reports(output, manifest):
    identity_data = manifest["identity"]
    rows = []
    for engine in identity_data["engines"]:
        for pattern in identity_data["patterns"]:
            for workers in identity_data["workers"]:
                for percentage in identity_data["read_percentages"]:
                    for repetition in range(1, identity_data["repetitions"] + 1):
                        entry = manifest["runs"].get(run_id(engine, pattern, workers, percentage, repetition))
                        if entry:
                            rows.append({"engine": engine, "pattern": pattern, "workers": workers,
                                         "readPercent": percentage, "writePercent": 100 - percentage,
                                         "repetition": repetition, "status": entry["status"], "error": entry.get("error"),
                                         "readerWorkers": entry.get("readerWorkers"),
                                         "artifactDirectory": entry.get("artifactDirectory"), **entry.get("metrics", {})})
    for row in rows:
        baseline = next((base for base in rows if base["engine"] == row["engine"] and base["pattern"] == row["pattern"]
                         and base["workers"] == row["workers"] and base["repetition"] == row["repetition"]
                         and base["readPercent"] == 100 and base["status"] == "OK"), None)
        p95 = baseline.get("readP95Nanos") if baseline else None
        current = row.get("readP95Nanos") if row["status"] == "OK" else None
        row["readP95RatioToReadOnly"] = current / p95 if current is not None and p95 and p95 > 0 else None
    note = ("Independent reads use odd account IDs while writes transfer between even accounts; hot reads and writes "
            "share account 1. Transfers are atomic and conserve the total balance. Fixed read rate schedules separate "
            "read/write streams; a shared bounded queue can reject either stream. A write-only fixed-rate run uses "
            "the configured read rate as its total write rate. Worker groups assign readers by rounded worker count, "
            "not by achieved operation mix. PGlite serializes its single session via a custom loopback transport. "
            "Failed or correctness-invalid runs are never used as baselines.")
    common.write_json(output / "read-write-interference.json",
                      {"identity": identity_data, "runs": rows, "methodology": note})
    columns = ["engine", "pattern", "workers", "readPercent", "writePercent", "repetition", "status", "error",
               "readerWorkers", "readOps", "writeOps", "readOpsPerSecond", "writeOpsPerSecond",
               "readP50Nanos", "readP95Nanos", "readP99Nanos", "writeP50Nanos", "writeP95Nanos", "writeP99Nanos",
               "readP95RatioToReadOnly", "failures", "rejected", "lockOrBusyErrors", "transactionConflicts",
               "timeouts", "retries", "arrivalCounts", "offeredWindowSeconds", "cpuCoreEquivalent", "steadyRssBytes",
               "engineVersion", "effectiveMemoryBudgetBytes", "effectiveConnectionMode", "effectiveConnections",
               "artifactDirectory"]
    with (output / "read-write-interference.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    text = ["# Read/write interference", "", note, "",
            "| Engine | Pattern | Workers | Writes % | Rep. | Status | Read ops/s | Write ops/s | Read p95 ns | Read p99 ns | p95 ratio |",
            "|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|"]
    for row in rows:
        text.append("| " + " | ".join(str(row.get(k, "")) for k in (
            "engine", "pattern", "workers", "writePercent", "repetition", "status", "readOpsPerSecond",
            "writeOpsPerSecond", "readP95Nanos", "readP99Nanos", "readP95RatioToReadOnly")) + " |")
    markdown = "\n".join(text) + "\n"
    (output / "read-write-interference.md").write_text(markdown, encoding="utf-8")
    charts = "".join(latency_chart(rows, pattern, workers)
                     for pattern in identity_data["patterns"] for workers in identity_data["workers"])
    (output / "read-write-interference.html").write_text(
        "<!doctype html><meta charset=\"utf-8\"><title>Read/write interference</title>"
        "<style>body{font:15px system-ui;max-width:1000px;margin:2em auto}svg{max-width:100%}"
        "pre{white-space:pre-wrap}</style>" + charts + "<pre>" + html.escape(markdown) + "</pre>", encoding="utf-8")


def execute(args):
    total = len(args.engines) * len(args.patterns) * len(args.workers) * len(args.read_percentages) * args.repetitions
    print(f"Planned: {total} fresh-JVM runs; minimum warmup and measurement "
          f"{total * (args.warmup + args.duration):.1f}s (excluding load/validation).", flush=True)
    if args.dry_run:
        for engine in args.engines:
            for pattern in args.patterns:
                print(f"{engine} {pattern}: rows={args.rows}, workers={args.workers}, "
                      f"read percentages={args.read_percentages}, repetitions={args.repetitions}")
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
        for pattern in args.patterns:
            for workers in args.workers:
                for percentage in args.read_percentages:
                    for repetition in range(1, args.repetitions + 1):
                        key = run_id(engine, pattern, workers, percentage, repetition)
                        previous = manifest["runs"].get(key, {})
                        if previous.get("status") == "OK" or previous.get("status") in ("FAILED", "UNSUPPORTED", "COMPLETED_WITH_ERRORS") and not args.rerun_failed:
                            continue
                        attempt = previous.get("attempt", 0) + 1
                        directory = output / "runs" / engine / pattern / f"t{workers}-read{percentage}-r{repetition}-attempt{attempt}"
                        readers = (workers * percentage + 50) // 100 if args.worker_groups else None
                        entry = {"status": "RUNNING", "attempt": attempt, "readerWorkers": readers,
                                 "artifactDirectory": str(directory.relative_to(output))}
                        manifest["runs"][key] = entry
                        common.write_json(manifest_path, manifest)
                        directory.mkdir(parents=True, exist_ok=False)
                        native = directory / "native"
                        native.mkdir()
                        done = sum(item.get("status") != "RUNNING" for item in manifest["runs"].values())
                        print(f"{key} rows={args.rows} completed={done}/{total}", flush=True)
                        command = [java, "-Xmx512m", f"-Djava.io.tmpdir={native}", f"-Dorg.sqlite.tmpdir={native}",
                                   "-jar", str(jar), "--profile", "smoke", "--engine", engine, "--workloads", "rw-" + pattern,
                                   "--rows", str(args.rows), "--threads", str(workers), "--read-percent", str(percentage),
                                   "--queue-capacity", str(args.queue_capacity), "--load-commit-interval", str(args.load_commit_interval),
                                   "--sample-interval", str(args.sample_interval), "--warmup", str(args.warmup),
                                   "--duration", str(args.duration), "--iterations", "1", "--seed", str(args.seed),
                                   "--memory-budget", args.memory_budget, "--jvm-heap", args.jvm_heap,
                                   "--child-deadline", str(args.child_deadline), "--output", str(directory)]
                        if args.fixed_read_rate > 0:
                            command += (["--fixed-read-rate", str(args.fixed_read_rate)] if percentage > 0
                                        else ["--request-rate", str(args.fixed_read_rate)])
                        elif readers is not None:
                            command += ["--reader-workers", str(readers)]
                        start = time.monotonic()
                        try:
                            exit_code = common.run_supervisor(command, directory)
                            entry["exitCode"] = exit_code
                            report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
                            result = report["results"][0]
                            if result.get("valid") is True and result.get("measurement") is not None:
                                entry["metrics"] = extract(result)
                            if exit_code == 0 and result.get("status") == "OK" and result.get("valid") is True:
                                entry["status"] = "OK"
                            else:
                                entry["status"] = result.get("status") if result.get("status") in (
                                    "UNSUPPORTED", "COMPLETED_WITH_ERRORS") else "FAILED"
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
        print(f"read-write-interference: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
