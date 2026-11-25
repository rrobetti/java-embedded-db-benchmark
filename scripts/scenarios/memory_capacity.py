#!/usr/bin/env python3
"""Progressive, sequential memory-capacity experiments using the benchmark CLI."""

import argparse
import csv
import hashlib
import html
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SIZES = "1000,10000,50000,100000,250000,500000,1000000,2000000,5000000"
ENGINES = ("h2", "hsqldb", "sqlite", "pglite")
GIB = 1024 ** 3


def csv_list(value, parse):
    try:
        items = [parse(x.strip()) for x in value.split(",")]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not items or len(set(items)) != len(items):
        raise argparse.ArgumentTypeError("values must be nonempty and unique")
    return items


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", default=",".join(ENGINES),
                        type=lambda s: csv_list(s, str))
    parser.add_argument("--sizes", default=DEFAULT_SIZES,
                        type=lambda s: csv_list(s, int), help="Ascending base-table row counts")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--payload", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--load-commit-interval", type=int, default=10000)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int, default=1800)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/memory-capacity"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(engine not in ENGINES for engine in args.engines):
        parser.error("engines must be drawn from " + ",".join(ENGINES))
    if any(size < 1 for size in args.sizes) or args.sizes != sorted(args.sizes):
        parser.error("sizes must be positive and strictly increasing")
    if (args.repetitions < 1 or args.payload < 1 or args.batch_size < 1
            or args.load_commit_interval < 0 or args.child_deadline < 1):
        parser.error("invalid positive count or negative commit interval")
    import math
    if (not all(math.isfinite(x) for x in (args.warmup, args.duration, args.sample_interval))
            or args.warmup < 0 or args.duration <= 0 or not 0.1 <= args.sample_interval <= 3600):
        parser.error("invalid warmup, duration or sample interval")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    return args


def command_output(command, cwd=ROOT):
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=True)
    return (result.stdout + result.stderr).strip()


def experiment_identity(args, jar, java):
    def git(*args):
        return command_output(["git", "--no-pager", *args])

    return {
        "scenario": "memory-capacity", "engines": args.engines, "sizes": args.sizes,
        "repetitions": args.repetitions, "warmup": args.warmup, "duration": args.duration,
        "payload": args.payload, "batchSize": args.batch_size, "loadCommitInterval": args.load_commit_interval,
        "sampleInterval": args.sample_interval, "memoryBudget": args.memory_budget,
        "jvmHeap": args.jvm_heap, "childDeadline": args.child_deadline, "seed": args.seed,
        "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "runnerSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "gitCommit": git("rev-parse", "HEAD"), "gitDirty": git("status", "--porcelain", "--untracked-files=no"),
        "javaVersion": command_output([java, "-version"]),
        "machine": {"os": platform.platform(), "architecture": platform.machine(),
                    "logicalProcessors": os.cpu_count(), "processor": platform.processor(), "node": platform.node()},
    }


def write_json(path, value):
    tmp = path.with_suffix(path.suffix + ".pending")
    tmp.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def run_id(engine, size, repetition):
    return f"{engine}-{size}-r{repetition}"


def memory_summary(result, directory):
    stages = result.get("stages", {})
    baseline = (stages.get("init") or {}).get("rssBytes")
    series = directory / result["artifactDirectory"] / "resources.jsonl"
    samples = [json.loads(line) for line in series.read_text(encoding="utf-8").splitlines()]
    loading = [s.get("rssBytes") for s in samples if s.get("phase") == "loading"
               and isinstance(s.get("rssBytes"), int)]
    retained = next((s.get("rssBytes") for s in samples if s.get("phase") == "validation"
                     and isinstance(s.get("rssBytes"), int)), None)
    if retained is not None:
        loading.append(retained)
    steady = [s.get("rssBytes") for s in samples if s.get("phase") == "measurement"
              and isinstance(s.get("rssBytes"), int)]
    steady_rss = statistics.median(steady) if steady else None
    incremental = steady_rss - baseline if steady_rss is not None and baseline is not None else None
    total = int(result["configuration"]["rows"]) * 7
    return {
        "records": total, "logicalDatasetBytes": result.get("logicalDatasetBytes"),
        "loadSeconds": result.get("loadNanos") / 1e9 if result.get("loadNanos") is not None else None,
        "initializedRssBytes": baseline, "sampledPeakLoadRssBytes": max(loading) if loading else None,
        "postLoadRssBytes": retained, "steadyMedianRssBytes": steady_rss,
        "steadyIncrementalRssBytes": incremental,
        "recordsPerGiBTotalRss": total * GIB / steady_rss if steady_rss and steady_rss > 0 else None,
        "bytesTotalRssPerRecord": steady_rss / total if steady_rss is not None else None,
        "recordsPerGiBIncrementalRss": total * GIB / incremental if incremental and incremental > 0 else None,
        "bytesIncrementalRssPerRecord": incremental / total if incremental is not None else None,
        "pointThroughputOpsPerSecond": (result.get("measurement") or {}).get("throughputOpsPerSecond"),
        "engineVersion": result.get("engineVersion"),
        "effectiveMemoryBudgetBytes": result.get("effectiveMemoryBudgetBytes"),
        "effectiveConnectionMode": result.get("effectiveConnectionMode"),
    }


def run_supervisor(command, directory):
    with (directory / "supervisor.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=os.name != "nt",
                                   creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0)
        try:
            return process.wait()
        except BaseException:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            else:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name != "nt":
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                process.kill()
                process.wait()
            raise


def write_reports(output, manifest):
    rows = []
    for engine in manifest["identity"]["engines"]:
        for size in manifest["identity"]["sizes"]:
            for repetition in range(1, manifest["identity"]["repetitions"] + 1):
                entry = manifest["runs"].get(run_id(engine, size, repetition))
                if entry:
                    rows.append({"engine": engine, "baseRows": size, "repetition": repetition,
                                 "status": entry["status"], "error": entry.get("error"),
                                 "artifactDirectory": entry.get("artifactDirectory"),
                                 **entry.get("metrics", {})})
    boundaries = {}
    for engine in manifest["identity"]["engines"]:
        success = []
        failed = []
        for size in manifest["identity"]["sizes"]:
            entries = [manifest["runs"].get(run_id(engine, size, n))
                       for n in range(1, manifest["identity"]["repetitions"] + 1)]
            if all(e and e["status"] == "OK" for e in entries):
                success.append(size)
            if any(e and e["status"] == "FAILED" for e in entries):
                failed.append(size)
        boundaries[engine] = {"largestSuccessfulBaseRows": max(success) if success else None,
                              "smallestFailedBaseRows": min(failed) if failed else None,
                              "upperLimitReachedSuccessfully": bool(success and max(success) == manifest["identity"]["sizes"][-1]),
                              "note": "Observed tested boundary, not an absolute capacity."}
    report = {"identity": manifest["identity"], "runs": rows, "boundaries": boundaries,
              "methodology": "RSS is sampled process memory, not database-only memory. "
              "Incremental RSS is steady median minus initialized process RSS; it is not exact native memory. "
              "Loading peaks may be missed between samples. A failed limit is not necessarily an OOM."}
    write_json(output / "capacity.json", report)
    columns = ["engine", "baseRows", "repetition", "status", "error", "records", "logicalDatasetBytes",
               "loadSeconds", "initializedRssBytes", "sampledPeakLoadRssBytes", "postLoadRssBytes",
               "steadyMedianRssBytes", "steadyIncrementalRssBytes", "recordsPerGiBTotalRss",
               "recordsPerGiBIncrementalRss", "bytesTotalRssPerRecord", "bytesIncrementalRssPerRecord",
               "pointThroughputOpsPerSecond", "engineVersion", "effectiveMemoryBudgetBytes",
               "effectiveConnectionMode", "artifactDirectory"]
    with (output / "capacity.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    text = ["# Memory capacity", "", report["methodology"], "",
            "| Engine | Largest successful base rows | First failed base rows | Configured upper limit reached |",
            "|---|---:|---:|---|"]
    for engine, data in boundaries.items():
        text.append(f"| {engine} | {data['largestSuccessfulBaseRows']} | {data['smallestFailedBaseRows']} "
                    f"| {data['upperLimitReachedSuccessfully']} |")
    text.extend(["", "Each base row creates seven records. See capacity.csv and raw per-run artifacts for "
                 "individual repetitions, versions, budgets and measurements."])
    markdown = "\n".join(text) + "\n"
    (output / "capacity.md").write_text(markdown, encoding="utf-8")
    (output / "capacity.html").write_text(
        "<!doctype html><meta charset=\"utf-8\"><title>Memory capacity</title><pre>"
        + html.escape(markdown) + "</pre>", encoding="utf-8")


def execute(args):
    count = len(args.engines) * len(args.sizes) * args.repetitions
    print(f"Planned maximum: {count} fresh-JVM runs; minimum measurement and warmup "
          f"{count * (args.duration + args.warmup):.1f}s (excluding load/validation).", flush=True)
    if args.dry_run:
        for engine in args.engines:
            for size in args.sizes:
                print(f"{engine}: {size} base rows, {size * 7} records, {args.repetitions} repetitions")
        return 0
    jar = ROOT / "target" / "benchmark.jar"
    if not jar.is_file():
        raise RuntimeError("Build target/benchmark.jar first with ./mvnw package")
    java = str(Path(os.environ["JAVA_HOME"]) / "bin" / ("java.exe" if os.name == "nt" else "java")) \
        if os.environ.get("JAVA_HOME") else shutil.which("java")
    if not java:
        raise RuntimeError("Java not found")
    identity = experiment_identity(args, jar, java)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        if not args.resume:
            raise RuntimeError("Manifest already exists; use --resume or a new output directory")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["identity"] != identity:
            raise RuntimeError("Cannot resume: experiment configuration, build, Git state or machine changed")
    elif args.resume:
        raise RuntimeError("No manifest exists to resume")
    else:
        manifest = {"identity": identity, "runs": {}}
        write_json(manifest_path, manifest)
    write_reports(output, manifest)
    for engine in args.engines:
        for size in args.sizes:
            failed = False
            for repetition in range(1, args.repetitions + 1):
                key = run_id(engine, size, repetition)
                previous = manifest["runs"].get(key, {})
                if previous.get("status") == "OK":
                    continue
                if previous.get("status") == "FAILED" and not args.rerun_failed:
                    failed = True
                    break
                attempt = previous.get("attempt", 0) + 1
                directory = output / "runs" / engine / str(size) / f"r{repetition}-attempt{attempt}"
                entry = {"status": "RUNNING", "attempt": attempt,
                         "artifactDirectory": str(directory.relative_to(output))}
                manifest["runs"][key] = entry
                write_json(manifest_path, manifest)
                directory.mkdir(parents=True, exist_ok=False)
                native = directory / "native"
                native.mkdir()
                complete = sum(item.get("status") in ("OK", "FAILED") for item in manifest["runs"].values())
                print(f"{engine} rows={size} repetition={repetition} completed={complete}/{count} "
                      f"remaining<={count - complete}", flush=True)
                command = [java, "-Xmx512m", f"-Djava.io.tmpdir={native}", f"-Dorg.sqlite.tmpdir={native}",
                           "-jar", str(jar), "--profile", "smoke", "--engine", engine, "--workloads", "point",
                           "--rows", str(size), "--payload", str(args.payload), "--threads", "1",
                           "--batch", str(args.batch_size), "--load-commit-interval", str(args.load_commit_interval),
                           "--sample-interval", str(args.sample_interval), "--warmup", str(args.warmup),
                           "--duration", str(args.duration), "--iterations", "1", "--seed", str(args.seed),
                           "--memory-budget", args.memory_budget, "--jvm-heap", args.jvm_heap,
                           "--child-deadline", str(args.child_deadline), "--output", str(directory)]
                started = time.monotonic()
                try:
                    exit_code = run_supervisor(command, directory)
                except OSError as error:
                    entry["status"] = "FAILED"
                    entry["error"] = f"Cannot start benchmark: {error}"
                    entry["wallSeconds"] = time.monotonic() - started
                    write_json(manifest_path, manifest)
                    write_reports(output, manifest)
                    print(f"{key}: FAILED {entry['error']}", flush=True)
                    failed = True
                    break
                entry["wallSeconds"] = time.monotonic() - started
                entry["exitCode"] = exit_code
                try:
                    report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
                    result = report["results"][0]
                    if exit_code == 0 and result.get("status") == "OK" and result.get("valid") is True:
                        entry["metrics"] = memory_summary(result, directory)
                        entry["status"] = "OK"
                    else:
                        entry["status"] = "FAILED"
                        entry["error"] = str(result.get("errorClass") or result.get("status"))
                except (OSError, ValueError, KeyError, IndexError, TypeError) as error:
                    entry["status"] = "FAILED"
                    entry["error"] = f"Missing or invalid benchmark evidence: {error}"
                write_json(manifest_path, manifest)
                write_reports(output, manifest)
                print(f"{key}: {entry['status']} {entry.get('error', '')}", flush=True)
                if entry["status"] != "OK":
                    failed = True
                    break
            if failed:
                break
    return 1 if any(item["status"] != "OK" for item in manifest["runs"].values()) else 0


def main():
    try:
        return execute(parse_args())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"memory-capacity: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
