#!/usr/bin/env python3
"""Run bounded, periodically maintained cache workloads in independent JVMs."""

import argparse
import csv
from datetime import datetime
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
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--warmup", type=float, default=60)
    parser.add_argument("--duration", type=float, default=3600, help="Seconds; e.g. 1800, 3600, 14400 or 28800")
    parser.add_argument("--maintenance-interval", type=float, default=60)
    parser.add_argument("--read-percent", type=int, default=95)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--load-commit-interval", type=int, default=10000)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/endurance"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(engine not in common.ENGINES for engine in args.engines):
        parser.error("unknown engine")
    if min(args.rows, args.workers, args.repetitions) < 1 or args.workers > 256 or args.load_commit_interval < 0:
        parser.error("invalid counts")
    if (not all(math.isfinite(x) for x in (args.warmup, args.duration, args.maintenance_interval, args.sample_interval))
            or not 0 <= args.warmup <= 14400 or not 0 < args.duration <= 28800
            or not 0.1 <= args.maintenance_interval < args.duration
            or not 0.1 <= args.sample_interval <= 3600 or not 0 <= args.read_percent <= 100):
        parser.error("invalid timing or read percentage")
    if args.child_deadline is None:
        args.child_deadline = math.ceil(args.warmup + args.duration) + 3600
    if args.child_deadline <= args.warmup + args.duration:
        parser.error("child deadline must allow warmup and measurement")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    return args


def identity(args, java, jar):
    config = {key: value for key, value in vars(args).items()
              if key not in ("output", "resume", "rerun_failed", "dry_run")}
    config.update({
        "scenario": "endurance",
        "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "runnerSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "commonSha256": hashlib.sha256(Path(common.__file__).read_bytes()).hexdigest(),
        "gitCommit": common.command_output(["git", "--no-pager", "rev-parse", "HEAD"]),
        "gitDirty": common.command_output(["git", "--no-pager", "status", "--porcelain", "--untracked-files=no"]),
        "javaVersion": common.command_output([java, "-version"]),
        "machine": {"os": platform.platform(), "architecture": platform.machine(),
                    "logicalProcessors": os.cpu_count(), "processor": platform.processor(), "node": platform.node()},
    })
    return config


def samples(result, directory):
    path = directory / result["artifactDirectory"] / "resources.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def extract(result, directory):
    if not result.get("workloadValid") or result["endurance"]["maintenanceCompleted"] < 1:
        raise ValueError("Endurance validation or maintenance did not complete")
    series = [sample for sample in samples(result, directory)
              if sample.get("phase") in ("measurement", "measurement-maintenance")]
    rss = [sample["rssBytes"] for sample in series if isinstance(sample.get("rssBytes"), int)]
    measurement = result["measurement"]
    read = measurement["latency"]["read"]
    return {"firstSampledRssBytes": rss[0] if rss else None, "lastSampledRssBytes": rss[-1] if rss else None,
            "peakSampledRssBytes": max(rss) if rss else None,
            "sampledRssChangeBytes": rss[-1] - rss[0] if len(rss) > 1 else None,
            "throughputOpsPerSecond": measurement["throughputOpsPerSecond"],
            "readP95Nanos": read["p95Nanos"], "readP99Nanos": read["p99Nanos"],
            "failures": measurement["failures"], "maintenance": result["endurance"],
            "engineVersion": result.get("engineVersion"),
            "effectiveMemoryBudgetBytes": result.get("effectiveMemoryBudgetBytes"),
            "effectiveConnectionMode": result.get("effectiveConnectionMode"),
            "effectiveConnections": result.get("effectiveConnections"),
            "resourceSeries": str(Path(result["artifactDirectory"]) / "resources.jsonl")}


def chart(series, title, getter):
    points = []
    for item in series:
        value = getter(item)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or not item.get("timestamp"):
            continue
        timestamp = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00")).timestamp()
        points.append((timestamp, value))
    if not points:
        return ""
    points = points[::max(1, math.ceil(len(points) / 1000))]
    low, high = min(value for _, value in points), max(value for _, value in points)
    span = high - low or 1
    start = points[0][0]
    duration = max(1, points[-1][0] - start)
    coordinates = " ".join(f"{60 + 600 * (timestamp - start) / duration:.1f},{220 - 180 * (value - low) / span:.1f}"
                           for timestamp, value in points)
    return (f'<h4>{html.escape(title)}</h4><svg viewBox="0 0 700 260" role="img" '
            f'aria-label="{html.escape(title, quote=True)}"><path d="M60 20V220H660" '
            f'fill="none" stroke="#57606a"/><polyline fill="none" stroke="#0969da" points="{coordinates}"/>'
            f'<text x="55" y="40" text-anchor="end">{high:.2g}</text>'
            f'<text x="55" y="220" text-anchor="end">{low:.2g}</text>'
            f'<text x="360" y="250" text-anchor="middle">Elapsed seconds (up to {duration:.1f})</text></svg>')


def write_reports(output, manifest):
    rows = [{"run": key, **entry} for key, entry in manifest["runs"].items()]
    report = {"identity": manifest["identity"], "runs": rows,
              "methodology": "RSS includes JVM and native process memory, not database-only memory. "
              "A change or retained memory is not proof of a leak. Inspect raw time series, maintenance "
              "boundaries, GC and repetitions; short tests cannot establish long-term stability."}
    common.write_json(output / "endurance.json", report)
    columns = ["run", "status", "error", "firstSampledRssBytes", "lastSampledRssBytes",
               "sampledRssChangeBytes", "peakSampledRssBytes", "throughputOpsPerSecond",
               "readP95Nanos", "readP99Nanos", "failures", "engineVersion",
               "effectiveMemoryBudgetBytes", "effectiveConnectionMode", "artifactDirectory"]
    with (output / "endurance.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({**row, **row.get("metrics", {})} for row in rows)
    text = ["# Endurance", "", report["methodology"], "",
            "| Run | Status | Sampled RSS change (bytes) | Read p99 (ns) |",
            "|---|---|---:|---:|"]
    for row in rows:
        metrics = row.get("metrics", {})
        text.append(f"| {row['run']} | {row['status']} | {metrics.get('sampledRssChangeBytes')} "
                    f"| {metrics.get('readP99Nanos')} |")
    markdown = "\n".join(text) + "\n"
    (output / "endurance.md").write_text(markdown, encoding="utf-8")
    charts = []
    for row in rows:
        if row.get("status") != "OK":
            continue
        series_path = output / row["artifactDirectory"] / row["metrics"]["resourceSeries"]
        series = [json.loads(line) for line in series_path.read_text(encoding="utf-8").splitlines()
                  if line.strip()]
        series = [item for item in series if item.get("phase") in ("measurement", "measurement-maintenance")]
        charts.append("<h3>" + html.escape(row["run"]) + "</h3>")
        for title, getter in [
            ("Process RSS bytes", lambda s: s.get("rssBytes")),
            ("Process CPU load", lambda s: s.get("processCpuLoad")),
            ("Throughput ops/sec", lambda s: (s.get("performance") or {}).get("throughputOpsPerSecond")),
            ("Read p99 latency (ns)", lambda s: (((s.get("performance") or {}).get("categories") or {})
                                                 .get("read") or {}).get("p99Nanos")),
            ("Cumulative GC time (ms)", lambda s: s.get("gcMillis")),
        ]:
            charts.append(chart(series, title, getter))
    (output / "endurance.html").write_text(
        '<!doctype html><meta charset="utf-8"><title>Endurance</title><style>'
        'body{font:15px system-ui;max-width:1000px;margin:2em auto}svg{max-width:100%}pre{white-space:pre-wrap}'
        '</style>' + "".join(charts) + "<pre>" + html.escape(markdown) + "</pre>", encoding="utf-8")


def execute(args):
    total = len(args.engines) * args.repetitions
    print(f"Planned: {total} fresh-JVM runs; minimum warmup and measurement "
          f"{total * (args.warmup + args.duration):.1f}s (excluding load/validation).", flush=True)
    if args.dry_run:
        for engine in args.engines:
            print(f"{engine}: rows={args.rows}, workers={args.workers}, repetitions={args.repetitions}")
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
    path = output / "manifest.json"
    if path.exists():
        if not args.resume:
            raise RuntimeError("Manifest exists; use --resume or a new output directory")
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest["identity"] != config:
            raise RuntimeError("Cannot resume: experiment configuration, build, Git state or machine changed")
    elif args.resume:
        raise RuntimeError("No manifest exists to resume")
    else:
        manifest = {"identity": config, "runs": {}}
        common.write_json(path, manifest)
    write_reports(output, manifest)
    for engine in args.engines:
        for repetition in range(1, args.repetitions + 1):
            key = f"{engine}-r{repetition}"
            previous = manifest["runs"].get(key, {})
            if previous.get("status") == "OK" or previous.get("status") == "FAILED" and not args.rerun_failed:
                continue
            attempt = previous.get("attempt", 0) + 1
            directory = output / "runs" / engine / f"r{repetition}-attempt{attempt}"
            entry = {"status": "RUNNING", "attempt": attempt, "artifactDirectory": str(directory.relative_to(output))}
            manifest["runs"][key] = entry
            common.write_json(path, manifest)
            directory.mkdir(parents=True, exist_ok=False)
            native = directory / "native"
            native.mkdir()
            done = sum(item.get("status") in ("OK", "FAILED") for item in manifest["runs"].values())
            print(f"{key} rows={args.rows} workers={args.workers} completed={done}/{total}", flush=True)
            command = [java, "-Xmx512m", f"-Djava.io.tmpdir={native}", f"-Dorg.sqlite.tmpdir={native}",
                       "-jar", str(jar), "--profile", "smoke", "--engine", engine, "--workloads", "endurance-cache",
                       "--rows", str(args.rows), "--threads", str(args.workers), "--read-percent", str(args.read_percent),
                       "--load-commit-interval", str(args.load_commit_interval),
                       "--maintenance-interval", str(args.maintenance_interval),
                       "--sample-interval", str(args.sample_interval), "--warmup", str(args.warmup),
                       "--duration", str(args.duration), "--iterations", "1", "--seed", str(args.seed),
                       "--memory-budget", args.memory_budget, "--jvm-heap", args.jvm_heap,
                       "--child-deadline", str(args.child_deadline), "--output", str(directory)]
            start = time.monotonic()
            try:
                entry["exitCode"] = common.run_supervisor(command, directory)
                result = json.loads((directory / "report.json").read_text(encoding="utf-8"))["results"][0]
                if entry["exitCode"] == 0 and result.get("status") == "OK" and result.get("valid") is True:
                    entry["metrics"] = extract(result, directory)
                    entry["status"] = "OK"
                else:
                    entry["status"] = "FAILED"
                    entry["error"] = str(result.get("errorClass") or result.get("status"))
                    entry["diagnostics"] = {
                        "measurementErrors": (result.get("measurement") or {}).get("errors"),
                        "warmupErrors": (result.get("warmup") or {}).get("errors"),
                        "maintenance": result.get("endurance"),
                    }
            except (OSError, ValueError, KeyError, IndexError, TypeError) as error:
                entry["status"] = "FAILED"
                entry["error"] = f"Missing or invalid benchmark evidence: {error}"
            entry["wallSeconds"] = time.monotonic() - start
            common.write_json(path, manifest)
            write_reports(output, manifest)
            print(f"{key}: {entry['status']} {entry.get('error', '')}", flush=True)
    return 1 if any(item["status"] != "OK" for item in manifest["runs"].values()) else 0


def main():
    try:
        return execute(parse_args())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"endurance: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
