#!/usr/bin/env python3
"""Progressively offer open-loop requests using a bounded benchmark queue."""

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
    parser.add_argument("--rates", default="10,100,500,1000,5000", type=lambda x: common.csv_list(x, float))
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--queue-capacity", type=int, default=256)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--p99-ms", type=float, default=100)
    parser.add_argument("--max-error-rate", type=float, default=0.01)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int, default=1800)
    parser.add_argument("--load-commit-interval", type=int, default=10000)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/load-scalability"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(x not in common.ENGINES for x in args.engines):
        parser.error("unknown engine")
    if (any(not math.isfinite(x) or x <= 0 or x > 1_000_000 for x in args.rates)
            or args.rates != sorted(args.rates)):
        parser.error("rates must be strictly increasing and in (0, 1000000]")
    if (min(args.rows, args.workers, args.queue_capacity, args.repetitions, args.child_deadline) < 1
            or args.workers > 256 or args.queue_capacity > 1_000_000 or args.load_commit_interval < 0):
        parser.error("invalid positive count or negative commit interval")
    if (not all(math.isfinite(x) for x in (args.warmup, args.duration, args.sample_interval,
                                            args.p99_ms, args.max_error_rate))
            or args.warmup < 0 or args.duration <= 0 or args.p99_ms <= 0
            or not 0 <= args.max_error_rate <= 1 or not 0.1 <= args.sample_interval <= 3600):
        parser.error("invalid timing, target or sample interval")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    return args


def identity(args, java, jar):
    result = {key: value for key, value in vars(args).items()
              if key not in ("output", "resume", "rerun_failed", "dry_run")}
    result.update({
        "scenario": "load-scalability",
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


def run_id(engine, rate, repetition):
    return f"{engine}-rate{rate:g}-r{repetition}"


def extract(result, p99_ms, max_error_rate):
    measurement = result["measurement"]
    offered = measurement["attempts"]
    failures = measurement["failures"]
    rejected = measurement["rejected"]
    successful = measurement["successes"]
    completed = successful + failures
    seconds = measurement["offeredWindowSeconds"]
    latency = measurement["latency"]["read"]
    p99 = latency["p99Nanos"]
    error_rate = (failures + rejected) / offered if offered else None
    sustainable = (offered > 0 and p99 is not None and p99 <= p99_ms * 1_000_000
                   and error_rate is not None and error_rate <= max_error_rate)
    return {
        "offered": offered, "accepted": completed, "completed": completed, "successful": successful,
        "failed": failures, "rejected": rejected, "offeredWindowSeconds": seconds,
        "actualOfferedPerSecond": offered / seconds if seconds > 0 else None,
        "throughputOpsPerSecond": measurement["throughputOpsPerSecond"],
        "p95Nanos": latency["p95Nanos"], "p99Nanos": p99,
        "failedOrRejectedRate": error_rate, "meetsTargets": sustainable,
        "engineVersion": result.get("engineVersion"),
        "effectiveMemoryBudgetBytes": result.get("effectiveMemoryBudgetBytes"),
        "effectiveConnectionMode": result.get("effectiveConnectionMode"),
    }


def write_reports(output, manifest):
    identity_data = manifest["identity"]
    rows = []
    for engine in identity_data["engines"]:
        for rate in identity_data["rates"]:
            for repetition in range(1, identity_data["repetitions"] + 1):
                entry = manifest["runs"].get(run_id(engine, rate, repetition))
                if entry:
                    rows.append({"engine": engine, "targetRate": rate, "repetition": repetition,
                                 "status": entry["status"], "error": entry.get("error"),
                                 "artifactDirectory": entry.get("artifactDirectory"), **entry.get("metrics", {})})
    boundaries = {}
    for engine in identity_data["engines"]:
        eligible = []
        throughput = []
        for rate in identity_data["rates"]:
            entries = [manifest["runs"].get(run_id(engine, rate, n))
                       for n in range(1, identity_data["repetitions"] + 1)]
            if all(e and e["status"] == "OK" and e["metrics"]["meetsTargets"] for e in entries):
                eligible.append(rate)
                throughput.append(statistics.mean(e["metrics"]["throughputOpsPerSecond"] for e in entries))
        boundaries[engine] = {"largestTargetRateMeetingTargets": max(eligible) if eligible else None,
                              "maximumMeanSustainableThroughput": max(throughput) if throughput else None,
                              "upperTestedRateMetTargets": bool(eligible and max(eligible) == identity_data["rates"][-1]),
                              "note": "Largest tested target meeting thresholds in all repetitions, not an absolute maximum."}
    note = ("Open-loop point reads use scheduled-arrival-to-completion latency, including bounded queueing. "
            "Offered counts include rejected arrivals; accepted/completed counts exclude rejected requests and "
            "include failed operations after queue drain. Error target includes failed and rejected arrivals. "
            "A rate qualifies only when every requested repetition is valid and meets both targets. "
            "PGlite uses a custom loopback transport and serialized single-session execution.")
    common.write_json(output / "load-scalability.json",
                      {"identity": identity_data, "runs": rows, "boundaries": boundaries, "methodology": note})
    columns = ["engine", "targetRate", "repetition", "status", "error", "offered", "accepted", "completed",
               "successful", "failed", "rejected", "actualOfferedPerSecond", "throughputOpsPerSecond",
               "p95Nanos", "p99Nanos", "failedOrRejectedRate", "meetsTargets",
               "engineVersion", "effectiveMemoryBudgetBytes", "effectiveConnectionMode", "artifactDirectory"]
    with (output / "load-scalability.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    text = ["# Load scalability", "", note, "",
            "| Engine | Target req/s | Repetition | Status | Offered | Accepted | Completed | Rejected | p99 ns | Meets targets |",
            "|---|---:|---:|---|---:|---:|---:|---:|---:|---|"]
    for row in rows:
        text.append("| " + " | ".join(str(row.get(k, "")) for k in (
            "engine", "targetRate", "repetition", "status", "offered", "accepted",
            "completed", "rejected", "p99Nanos", "meetsTargets")) + " |")
    text.extend(["", "## Observed bounds", ""])
    for engine, bound in boundaries.items():
        text.append(f"- {engine}: {bound['largestTargetRateMeetingTargets']} req/s; "
                    f"upper tested rate met targets: {bound['upperTestedRateMetTargets']}.")
    markdown = "\n".join(text) + "\n"
    (output / "load-scalability.md").write_text(markdown, encoding="utf-8")
    points = [row for row in rows if row["status"] == "OK"
              and isinstance(row.get("throughputOpsPerSecond"), (int, float))]
    chart = ""
    if points:
        rates = sorted({row["targetRate"] for row in points})
        ymax = max(row["throughputOpsPerSecond"] for row in points) or 1
        parts = ['<h2>Throughput versus offered target rate</h2>'
                 '<svg viewBox="0 0 720 300" role="img" aria-label="Throughput versus offered target rate">'
                 '<rect width="720" height="300" fill="white"/><path d="M65 20V250H600" fill="none" stroke="#57606a"/>']
        colors = ("#0969da", "#1a7f37", "#8250df", "#bc4c00")
        for index, engine in enumerate(common.ENGINES):
            coordinates = []
            for rate in rates:
                values = [row["throughputOpsPerSecond"] for row in points
                          if row["engine"] == engine and row["targetRate"] == rate]
                if values:
                    x = 65 + 535 * rates.index(rate) / max(1, len(rates) - 1)
                    y = 250 - 220 * statistics.mean(values) / ymax
                    coordinates.append((x, y))
                    parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{colors[index]}"/>')
            if coordinates:
                parts.append('<polyline fill="none" stroke="' + colors[index] + '" points="'
                             + " ".join(f"{x:.1f},{y:.1f}" for x, y in coordinates) + '"/>')
                parts.append(f'<text x="610" y="{35 + 20 * index}" fill="{colors[index]}">{engine}</text>')
        for index, rate in enumerate(rates):
            x = 65 + 535 * index / max(1, len(rates) - 1)
            parts.append(f'<text x="{x:.1f}" y="270" text-anchor="middle">{rate:g}</text>')
        parts.append(f'<text x="58" y="24" text-anchor="end">{ymax:.2g}</text>'
                     '<text x="58" y="250" text-anchor="end">0</text>'
                     '<text x="330" y="292" text-anchor="middle">Target requests per second</text></svg>')
        chart = "".join(parts)
    (output / "load-scalability.html").write_text(
        "<!doctype html><meta charset=\"utf-8\"><title>Load scalability</title>"
        "<style>body{font:15px system-ui;max-width:1000px;margin:2em auto}svg{max-width:100%}"
        "pre{white-space:pre-wrap}</style>" + chart + "<pre>" + html.escape(markdown) + "</pre>", encoding="utf-8")


def execute(args):
    total = len(args.engines) * len(args.rates) * args.repetitions
    print(f"Planned: {total} fresh-JVM runs; minimum warmup and measurement "
          f"{total * (args.warmup + args.duration):.1f}s (excluding load/validation).", flush=True)
    if args.dry_run:
        for engine in args.engines:
            print(f"{engine}: rows={args.rows}, workers={args.workers}, rates={args.rates}, repetitions={args.repetitions}")
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
        for rate in args.rates:
            for repetition in range(1, args.repetitions + 1):
                key = run_id(engine, rate, repetition)
                previous = manifest["runs"].get(key, {})
                if previous.get("status") == "OK" or previous.get("status") == "FAILED" and not args.rerun_failed:
                    continue
                attempt = previous.get("attempt", 0) + 1
                directory = output / "runs" / engine / f"rate{rate:g}-r{repetition}-attempt{attempt}"
                entry = {"status": "RUNNING", "attempt": attempt,
                         "artifactDirectory": str(directory.relative_to(output))}
                manifest["runs"][key] = entry
                common.write_json(manifest_path, manifest)
                directory.mkdir(parents=True, exist_ok=False)
                native = directory / "native"
                native.mkdir()
                done = sum(item.get("status") in ("OK", "FAILED") for item in manifest["runs"].values())
                print(f"{key} rows={args.rows} workers={args.workers} completed={done}/{total}", flush=True)
                command = [java, "-Xmx512m", f"-Djava.io.tmpdir={native}", f"-Dorg.sqlite.tmpdir={native}",
                           "-jar", str(jar), "--profile", "smoke", "--engine", engine, "--workloads", "point",
                           "--rows", str(args.rows), "--threads", str(args.workers), "--request-rate", str(rate),
                           "--queue-capacity", str(args.queue_capacity),
                           "--load-commit-interval", str(args.load_commit_interval),
                           "--sample-interval", str(args.sample_interval), "--warmup", str(args.warmup),
                           "--duration", str(args.duration), "--iterations", "1", "--seed", str(args.seed),
                           "--memory-budget", args.memory_budget, "--jvm-heap", args.jvm_heap,
                           "--child-deadline", str(args.child_deadline), "--output", str(directory)]
                start = time.monotonic()
                try:
                    exit_code = common.run_supervisor(command, directory)
                    entry["exitCode"] = exit_code
                    report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
                    result = report["results"][0]
                    if result.get("valid") is True and result.get("measurement") is not None:
                        entry["metrics"] = extract(result, args.p99_ms, args.max_error_rate)
                    if exit_code == 0 and result.get("status") == "OK" and result.get("valid") is True:
                        entry["status"] = "OK"
                    else:
                        entry["status"] = "FAILED"
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
        print(f"load-scalability: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
