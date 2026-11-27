#!/usr/bin/env python3
"""Run independent, sequential read-concurrency experiments."""

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


WORKLOADS = ("point", "indexed-range", "joins", "aggregate")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", default=",".join(common.ENGINES), type=lambda x: common.csv_list(x, str))
    parser.add_argument("--workloads", default=",".join(WORKLOADS), type=lambda x: common.csv_list(x, str))
    parser.add_argument("--workers", default="1,2,4,8,16,32,64,128", type=lambda x: common.csv_list(x, int))
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--payload", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--load-commit-interval", type=int, default=10000)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--duration", type=float, default=120)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int, default=1800)
    parser.add_argument("--inactivity-timeout", type=float, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/read-scalability"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(x not in common.ENGINES for x in args.engines):
        parser.error("unknown engine")
    if any(x not in WORKLOADS for x in args.workloads):
        parser.error("unknown read workload")
    if any(x < 1 or x > 256 for x in args.workers) or args.workers != sorted(args.workers):
        parser.error("workers must be strictly increasing and between 1 and 256")
    if min(args.rows, args.payload, args.batch_size, args.repetitions, args.child_deadline) < 1 or args.load_commit_interval < 0:
        parser.error("invalid positive count or negative commit interval")
    if (not all(math.isfinite(x) for x in (args.warmup, args.duration, args.sample_interval))
            or args.warmup < 0 or args.duration <= 0 or not 0.1 <= args.sample_interval <= 3600):
        parser.error("invalid warmup, duration or sample interval")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    common.validate_inactivity(parser, args)
    return args


def identity(args, java, jar):
    params = {key: value for key, value in vars(args).items()
              if key not in ("output", "resume", "rerun_failed", "dry_run")}
    params.update({
        "scenario": "read-scalability",
        "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "runnerSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "commonSha256": hashlib.sha256(Path(common.__file__).read_bytes()).hexdigest(),
        "gitCommit": common.command_output(["git", "--no-pager", "rev-parse", "HEAD"]),
        "gitDirty": common.command_output(["git", "--no-pager", "status", "--porcelain", "--untracked-files=no"]),
        "javaVersion": common.command_output([java, "-version"]),
        "machine": {"os": platform.platform(), "architecture": platform.machine(),
                    "logicalProcessors": os.cpu_count(), "processor": platform.processor(), "node": platform.node()},
    })
    return params


def run_id(engine, workload, workers, repetition):
    return f"{engine}-{workload}-t{workers}-r{repetition}"


def extract(result, directory, workload):
    samples_path = directory / result["artifactDirectory"] / "resources.jsonl"
    samples = [json.loads(line) for line in samples_path.read_text(encoding="utf-8").splitlines()]
    rss = [sample["rssBytes"] for sample in samples
           if sample.get("phase") == "measurement" and isinstance(sample.get("rssBytes"), int)]
    median_rss = statistics.median(rss) if rss else None
    measurement = result["measurement"]
    throughput = measurement["throughputOpsPerSecond"]
    cpu = (result.get("resourceDeltas") or {}).get("cpuCoreEquivalent")
    category = "analytical" if workload == "aggregate" else "read"
    latency = measurement["latency"][category]
    return {
        "throughputOpsPerSecond": throughput, "p95Nanos": latency["p95Nanos"],
        "p99Nanos": latency["p99Nanos"], "successes": measurement["successes"],
        "failures": measurement["failures"], "errorRate": measurement["errorRate"],
        "cpuCoreEquivalent": cpu, "steadyMedianRssBytes": median_rss,
        "throughputPerCpuCore": throughput / cpu if cpu and cpu > 0 else None,
        "throughputPerGiBRss": throughput * common.GIB / median_rss if median_rss and median_rss > 0 else None,
        "effectiveConnections": result.get("effectiveConnections"),
        "effectiveConnectionMode": result.get("effectiveConnectionMode"),
        "capabilities": result.get("capabilities"), "engineVersion": result.get("engineVersion"),
        "effectiveMemoryBudgetBytes": result.get("effectiveMemoryBudgetBytes"),
    }


def svg_chart(rows, workload, field, title):
    points = [row for row in rows if row["workload"] == workload and row["status"] == "OK"
              and isinstance(row.get(field), (int, float))]
    if not points:
        return ""
    xs = sorted({row["workers"] for row in points})
    ymax = max(row[field] for row in points) or 1
    colors = ("#0969da", "#1a7f37", "#8250df", "#bc4c00")
    parts = [f'<h3>{html.escape(title)} — {html.escape(workload)}</h3>',
             '<svg viewBox="0 0 720 300" role="img" aria-label="'
             + html.escape(title, quote=True) + '"><rect width="720" height="300" fill="white"/>',
             '<path d="M65 20V250H600" fill="none" stroke="#57606a"/>']
    for index, engine in enumerate(common.ENGINES):
        values = []
        for x in xs:
            samples = [row[field] for row in points if row["engine"] == engine and row["workers"] == x]
            if samples:
                px = 65 + 535 * xs.index(x) / max(1, len(xs) - 1)
                py = 250 - 220 * statistics.mean(samples) / ymax
                values.append((px, py))
                parts.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="4" fill="{colors[index]}"/>')
        if values:
            parts.append('<polyline fill="none" stroke="' + colors[index] + '" points="'
                         + " ".join(f"{x:.1f},{y:.1f}" for x, y in values) + '"/>')
            parts.append(f'<text x="610" y="{35 + index * 20}" fill="{colors[index]}">{engine}</text>')
    for i, x in enumerate(xs):
        px = 65 + 535 * i / max(1, len(xs) - 1)
        parts.append(f'<text x="{px:.1f}" y="270" text-anchor="middle">{x}</text>')
    parts.append(f'<text x="58" y="24" text-anchor="end">{ymax:.2g}</text>'
                 '<text x="58" y="250" text-anchor="end">0</text>'
                 '<text x="330" y="292" text-anchor="middle">Workers</text></svg>')
    return "".join(parts)


def write_reports(output, manifest):
    identity_data = manifest["identity"]
    rows = []
    for engine in identity_data["engines"]:
        for workload in identity_data["workloads"]:
            for workers in identity_data["workers"]:
                for repetition in range(1, identity_data["repetitions"] + 1):
                    entry = manifest["runs"].get(run_id(engine, workload, workers, repetition))
                    if entry:
                        rows.append({"engine": engine, "workload": workload, "workers": workers,
                                     "repetition": repetition, "status": entry["status"], "error": entry.get("error"),
                                     "artifactDirectory": entry.get("artifactDirectory"), **entry.get("metrics", {})})
    for row in rows:
        baseline = next((candidate for candidate in rows if candidate["engine"] == row["engine"]
                         and candidate["workload"] == row["workload"] and candidate["workers"] == 1
                         and candidate["repetition"] == row["repetition"] and candidate["status"] == "OK"), None)
        base = baseline.get("throughputOpsPerSecond") if baseline else None
        value = row.get("throughputOpsPerSecond") if row["status"] == "OK" else None
        row["scalabilityVsOneWorker"] = value / base if value is not None and base and base > 0 else None
    note = ("One independent JVM per run. Ratios compare the same repetition to its one-worker result. "
            "CPU core-equivalent and RSS are process-level measurements; missing metrics remain unavailable. "
            "PGlite uses a custom loopback transport and serialized single-session execution, not independent concurrent sessions. "
            "Chart points show arithmetic means of successful repetitions, not confidence intervals.")
    common.write_json(output / "read-scalability.json", {"identity": identity_data, "runs": rows, "methodology": note})
    columns = ["engine", "workload", "workers", "repetition", "status", "error", "throughputOpsPerSecond",
               "p95Nanos", "p99Nanos", "successes", "failures", "errorRate", "scalabilityVsOneWorker",
               "cpuCoreEquivalent", "steadyMedianRssBytes", "throughputPerCpuCore", "throughputPerGiBRss",
               "effectiveConnections", "effectiveConnectionMode", "engineVersion", "effectiveMemoryBudgetBytes",
               "artifactDirectory"]
    with (output / "read-scalability.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    lines = ["# Read scalability", "", note, "",
             "| Engine | Workload | Workers | Repetition | Status | Ops/s | p95 ns | p99 ns | Relative to 1 | Sessions |",
             "|---|---|---:|---:|---|---:|---:|---:|---:|---|"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(k, "")) for k in (
            "engine", "workload", "workers", "repetition", "status", "throughputOpsPerSecond",
            "p95Nanos", "p99Nanos", "scalabilityVsOneWorker", "effectiveConnectionMode")) + " |")
    markdown = "\n".join(lines) + "\n"
    (output / "read-scalability.md").write_text(markdown, encoding="utf-8")
    charts = "".join(svg_chart(rows, workload, field, title)
                     for workload in identity_data["workloads"]
                     for field, title in (("throughputOpsPerSecond", "Throughput (ops/s)"),
                                          ("p95Nanos", "p95 latency (ns)")))
    (output / "read-scalability.html").write_text(
        '<!doctype html><meta charset="utf-8"><title>Read scalability</title>'
        '<style>body{font:15px system-ui;max-width:1000px;margin:2em auto;color:#24292f}'
        'svg{max-width:100%;border:1px solid #d0d7de}pre{white-space:pre-wrap}</style>'
        + charts + "<pre>" + html.escape(markdown) + "</pre>", encoding="utf-8")


def execute(args):
    total = len(args.engines) * len(args.workloads) * len(args.workers) * args.repetitions
    print(f"Planned: {total} fresh-JVM runs; minimum warmup and measurement "
          f"{total * (args.warmup + args.duration):.1f}s (excluding load/validation).", flush=True)
    if args.dry_run:
        for engine in args.engines:
            for workload in args.workloads:
                print(f"{engine} {workload}: rows={args.rows}, workers={args.workers}, repetitions={args.repetitions}")
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
        for workload in args.workloads:
            for workers in args.workers:
                for repetition in range(1, args.repetitions + 1):
                    key = run_id(engine, workload, workers, repetition)
                    previous = manifest["runs"].get(key, {})
                    if previous.get("status") == "OK" or previous.get("status") == "FAILED" and not args.rerun_failed:
                        continue
                    attempt = previous.get("attempt", 0) + 1
                    directory = output / "runs" / engine / workload / f"t{workers}-r{repetition}-attempt{attempt}"
                    entry = {"status": "RUNNING", "attempt": attempt,
                             "artifactDirectory": str(directory.relative_to(output))}
                    manifest["runs"][key] = entry
                    common.write_json(manifest_path, manifest)
                    directory.mkdir(parents=True, exist_ok=False)
                    native = directory / "native"
                    native.mkdir()
                    done = sum(item.get("status") in ("OK", "FAILED") for item in manifest["runs"].values())
                    print(f"{key} rows={args.rows} completed={done}/{total} remaining={total - done}", flush=True)
                    command = [java, "-Xmx512m", f"-Djava.io.tmpdir={native}", f"-Dorg.sqlite.tmpdir={native}",
                               "-jar", str(jar), "--profile", "smoke", "--engine", engine, "--workloads", workload,
                               "--rows", str(args.rows), "--payload", str(args.payload), "--threads", str(workers),
                               "--batch", str(args.batch_size), "--load-commit-interval", str(args.load_commit_interval),
                               "--sample-interval", str(args.sample_interval), "--warmup", str(args.warmup),
                               "--duration", str(args.duration), "--iterations", "1", "--seed", str(args.seed),
                               "--memory-budget", args.memory_budget, "--jvm-heap", args.jvm_heap,
                               "--child-deadline", str(args.child_deadline),
                               "--inactivity-timeout", str(args.inactivity_timeout), "--output", str(directory)]
                    start = time.monotonic()
                    try:
                        exit_code = common.run_supervisor(command, directory)
                        entry["exitCode"] = exit_code
                        report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
                        result = report["results"][0]
                        if exit_code == 0 and result.get("status") == "OK" and result.get("valid") is True:
                            entry["metrics"] = extract(result, directory, workload)
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
        print(f"read-scalability: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
