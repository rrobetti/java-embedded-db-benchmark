#!/usr/bin/env python3
"""Measure readers before, during and after JDBC cache refresh operations."""

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


STRATEGIES = ("record-update", "record-invalidate", "bulk-invalidate", "incremental-refresh",
              "full-table-refresh", "replacement", "expiration")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", default=",".join(common.ENGINES), type=lambda x: common.csv_list(x, str))
    parser.add_argument("--strategies", default=",".join(STRATEGIES), type=lambda x: common.csv_list(x, str))
    parser.add_argument("--percentages", default="1,10,50,100", type=lambda x: common.csv_list(x, int))
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--before", type=float, default=10)
    parser.add_argument("--after", type=float, default=10)
    parser.add_argument("--read-rate", type=float, default=0, help="Paced closed-loop reads/sec; 0 means unpaced")
    parser.add_argument("--transaction-size", type=int, default=1000)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int, default=1800)
    parser.add_argument("--inactivity-timeout", type=float, default=0)
    parser.add_argument("--load-commit-interval", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/cache-refresh"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(x not in common.ENGINES for x in args.engines) or any(x not in STRATEGIES for x in args.strategies):
        parser.error("unknown engine or refresh strategy")
    if any(x < 1 or x > 100 for x in args.percentages):
        parser.error("percentages must be between 1 and 100")
    if (min(args.rows, args.workers, args.repetitions, args.transaction_size, args.child_deadline) < 1
            or args.workers > 256 or args.load_commit_interval < 0):
        parser.error("invalid positive count or negative commit interval")
    if (not all(math.isfinite(x) for x in (args.before, args.after, args.read_rate, args.sample_interval))
            or args.before < 0 or args.after < 0 or args.read_rate < 0 or args.read_rate > 1_000_000
            or not 0.1 <= args.sample_interval <= 3600):
        parser.error("invalid timing, read rate or sample interval")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    common.validate_inactivity(parser, args)
    return args


def identity(args, java, jar):
    result = {key: value for key, value in vars(args).items()
              if key not in ("output", "resume", "rerun_failed", "dry_run")}
    result.update({
        "scenario": "cache-refresh",
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


def run_id(engine, strategy, percentage, repetition):
    return f"{engine}-{strategy}-p{percentage}-r{repetition}"


def extract(result, directory):
    refresh = result["cacheRefresh"]
    if not refresh.get("cacheValid"):
        raise ValueError("Cache validation did not pass")
    metrics = {
        "affectedEntries": refresh["affectedEntries"], "cacheEntries": refresh["cacheEntries"],
        "refreshNanos": refresh["refreshNanos"], "readFailures": refresh["readFailures"],
        "blockingDurationNanos": refresh["blockingDurationNanos"],
        "beforeRssBytes": refresh["beforeRefreshResources"].get("rssBytes"),
        "afterRssBytes": refresh["afterRefreshResources"].get("rssBytes"),
        "retainedRssBytes": refresh["afterReadersResources"].get("rssBytes"),
        "engineVersion": result.get("engineVersion"),
        "effectiveMemoryBudgetBytes": result.get("effectiveMemoryBudgetBytes"),
        "effectiveConnectionMode": result.get("effectiveConnectionMode"),
        "effectiveConnections": result.get("effectiveConnections"),
        "transactionIsolation": refresh.get("transactionIsolation"),
    }
    for phase in ("before", "during", "after"):
        report = refresh["reads"][phase]
        latency = report["latency"]["read"]
        metrics[phase + "ReadOps"] = report["completedReadOperations"]
        metrics[phase + "OverlappingReadOps"] = report["successes"]
        metrics[phase + "ReadOpsPerSecond"] = report["throughputOpsPerSecond"]
        metrics[phase + "ReadP95Nanos"] = latency["p95Nanos"]
        metrics[phase + "ReadP99Nanos"] = latency["p99Nanos"]
        metrics[phase + "ReadMaxNanos"] = latency["maxNanos"]
    baseline = metrics["beforeReadP95Nanos"]
    metrics["duringP95Ratio"] = metrics["duringReadP95Nanos"] / baseline \
        if baseline and metrics["duringReadP95Nanos"] is not None else None
    before_rate = metrics["beforeReadOpsPerSecond"]
    metrics["duringThroughputRatio"] = metrics["duringReadOpsPerSecond"] / before_rate \
        if before_rate and metrics["duringReadOpsPerSecond"] is not None else None
    if result.get("status") != "OK":
        metrics["duringP95Ratio"] = None
        metrics["duringThroughputRatio"] = None
    samples_path = directory / result["artifactDirectory"] / "resources.jsonl"
    samples = [json.loads(line) for line in samples_path.read_text(encoding="utf-8").splitlines()]
    rss = [sample["rssBytes"] for sample in samples
           if sample.get("phase") == "refresh-during" and isinstance(sample.get("rssBytes"), int)]
    metrics["sampledDuringPeakRssBytes"] = max(rss) if rss else None
    metrics["sampledTemporaryRssGrowthBytes"] = max(rss) - metrics["beforeRssBytes"] \
        if rss and metrics["beforeRssBytes"] is not None else None
    return metrics


def latency_chart(row):
    phases = ("before", "during", "after")
    values = [row.get(phase + "ReadP95Nanos") for phase in phases]
    known = [value for value in values if isinstance(value, (int, float))]
    if row["status"] != "OK" or not known:
        return ""
    ymax = max(known) or 1
    title = f"{row['engine']} {row['strategy']} {row['percentage']}% r{row['repetition']}"
    parts = [f'<h3>{html.escape(title)} — read p95 (ns)</h3>',
             '<svg viewBox="0 0 640 280" role="img" aria-label="Read latency before during and after refresh">'
             '<rect width="640" height="280" fill="white"/><path d="M65 20V220H590" fill="none" stroke="#57606a"/>']
    for index, phase in enumerate(phases):
        x = 130 + index * 190
        value = values[index]
        parts.append(f'<text x="{x}" y="245" text-anchor="middle">{phase}</text>')
        if isinstance(value, (int, float)):
            height = 190 * value / ymax
            parts.append(f'<rect x="{x - 30}" y="{220 - height:.1f}" width="60" height="{height:.1f}" fill="#0969da">'
                         f'<title>{value} ns</title></rect>')
        else:
            parts.append(f'<text x="{x}" y="210" text-anchor="middle">N/A</text>')
    parts.append(f'<text x="58" y="24" text-anchor="end">{ymax:.2g}</text>'
                 '<text x="58" y="220" text-anchor="end">0</text></svg>')
    return "".join(parts)


def write_reports(output, manifest):
    config = manifest["identity"]
    rows = []
    for engine in config["engines"]:
        for strategy in config["strategies"]:
            for percentage in config["percentages"]:
                for repetition in range(1, config["repetitions"] + 1):
                    entry = manifest["runs"].get(run_id(engine, strategy, percentage, repetition))
                    if entry:
                        rows.append({"engine": engine, "strategy": strategy, "percentage": percentage,
                                     "repetition": repetition, "status": entry["status"], "error": entry.get("error"),
                                     "artifactDirectory": entry.get("artifactDirectory"), **entry.get("metrics", {})})
    note = ("Each run adds a separate cache table with one entry per base row. Readers perform single-row lookups; "
            "replacement is atomic for its selected range and readers must not observe missing entries. "
            "Other strategies expose their documented old/new or invalidated states. Overlapping reads are "
            "attributed to the refresh interval for latency; throughput uses successful completions sampled at "
            "phase boundaries, which can distort very short intervals. No overlapping reads means latency "
            "degradation is unavailable, not zero. "
            "Exact lock-blocking duration is unavailable. RSS peaks are sampled and may miss spikes. "
            "PGlite uses a custom loopback transport and serialized single-session execution.")
    common.write_json(output / "cache-refresh.json", {"identity": config, "runs": rows, "methodology": note})
    columns = ["engine", "strategy", "percentage", "repetition", "status", "error", "affectedEntries",
               "cacheEntries", "refreshNanos", "beforeReadOps", "duringReadOps", "afterReadOps",
               "beforeOverlappingReadOps", "duringOverlappingReadOps", "afterOverlappingReadOps",
               "beforeReadOpsPerSecond", "duringReadOpsPerSecond", "afterReadOpsPerSecond",
               "beforeReadP95Nanos", "duringReadP95Nanos", "afterReadP95Nanos", "duringP95Ratio",
               "beforeReadP99Nanos", "duringReadP99Nanos", "afterReadP99Nanos",
               "duringReadMaxNanos", "duringThroughputRatio", "readFailures", "blockingDurationNanos",
               "beforeRssBytes", "sampledDuringPeakRssBytes", "sampledTemporaryRssGrowthBytes", "afterRssBytes",
               "retainedRssBytes", "engineVersion", "effectiveMemoryBudgetBytes", "effectiveConnectionMode",
               "effectiveConnections", "transactionIsolation", "artifactDirectory"]
    with (output / "cache-refresh.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    lines = ["# Cache refresh", "", note, "",
             "| Engine | Strategy | % | Rep. | Status | Refresh ns | Before p95 ns | During p95 ns | After p95 ns | During/before |",
             "|---|---|---:|---:|---|---:|---:|---:|---:|---:|"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(k, "")) for k in (
            "engine", "strategy", "percentage", "repetition", "status", "refreshNanos", "beforeReadP95Nanos",
            "duringReadP95Nanos", "afterReadP95Nanos", "duringP95Ratio")) + " |")
    markdown = "\n".join(lines) + "\n"
    (output / "cache-refresh.md").write_text(markdown, encoding="utf-8")
    charts = "".join(latency_chart(row) for row in rows)
    (output / "cache-refresh.html").write_text(
        "<!doctype html><meta charset=\"utf-8\"><title>Cache refresh</title>"
        "<style>body{font:15px system-ui;max-width:1000px;margin:2em auto}svg{max-width:100%}"
        "pre{white-space:pre-wrap}</style>" + charts + "<pre>" + html.escape(markdown) + "</pre>", encoding="utf-8")


def execute(args):
    total = len(args.engines) * len(args.strategies) * len(args.percentages) * args.repetitions
    print(f"Planned: {total} fresh-JVM runs; minimum before/after read time "
          f"{total * (args.before + args.after):.1f}s (excluding refresh, load and validation).", flush=True)
    if args.dry_run:
        for engine in args.engines:
            print(f"{engine}: rows={args.rows}, readers={args.workers}, strategies={args.strategies}, "
                  f"percentages={args.percentages}, repetitions={args.repetitions}")
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
        for strategy in args.strategies:
            for percentage in args.percentages:
                for repetition in range(1, args.repetitions + 1):
                    key = run_id(engine, strategy, percentage, repetition)
                    previous = manifest["runs"].get(key, {})
                    if previous.get("status") == "OK" or previous.get("status") in ("FAILED", "UNSUPPORTED", "COMPLETED_WITH_ERRORS") and not args.rerun_failed:
                        continue
                    attempt = previous.get("attempt", 0) + 1
                    directory = output / "runs" / engine / strategy / f"p{percentage}-r{repetition}-attempt{attempt}"
                    entry = {"status": "RUNNING", "attempt": attempt, "artifactDirectory": str(directory.relative_to(output))}
                    manifest["runs"][key] = entry
                    common.write_json(manifest_path, manifest)
                    directory.mkdir(parents=True, exist_ok=False)
                    native = directory / "native"
                    native.mkdir()
                    done = sum(item.get("status") != "RUNNING" for item in manifest["runs"].values())
                    print(f"{key} rows={args.rows} readers={args.workers} completed={done}/{total}", flush=True)
                    command = [java, "-Xmx512m", f"-Djava.io.tmpdir={native}", f"-Dorg.sqlite.tmpdir={native}",
                               "-jar", str(jar), "--profile", "smoke", "--engine", engine,
                               "--workloads", "cache-refresh-concurrent", "--rows", str(args.rows),
                               "--threads", str(args.workers), "--refresh-strategy", strategy,
                               "--refresh-percent", str(percentage), "--refresh-before", str(args.before),
                               "--refresh-after", str(args.after), "--refresh-read-rate", str(args.read_rate),
                               "--transaction-size", str(args.transaction_size),
                               "--load-commit-interval", str(args.load_commit_interval),
                               "--sample-interval", str(args.sample_interval), "--iterations", "1",
                               "--seed", str(args.seed), "--memory-budget", args.memory_budget,
                               "--jvm-heap", args.jvm_heap, "--child-deadline", str(args.child_deadline),
                               "--inactivity-timeout", str(args.inactivity_timeout),
                               "--output", str(directory)]
                    start = time.monotonic()
                    try:
                        exit_code = common.run_supervisor(command, directory)
                        entry["exitCode"] = exit_code
                        report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
                        result = report["results"][0]
                        if result.get("valid") is True and result.get("cacheRefresh") is not None:
                            entry["metrics"] = extract(result, directory)
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
        print(f"cache-refresh: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
