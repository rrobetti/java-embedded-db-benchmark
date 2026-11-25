#!/usr/bin/env python3
"""Compare single-transaction and incremental ingestion in separate fresh JVMs."""

import argparse
import csv
import hashlib
import html
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import memory_capacity as capacity


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", default=",".join(capacity.ENGINES))
    parser.add_argument("--sizes", default="1000,10000,100000")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--payload", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--incremental-interval", type=int, default=10000)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--child-deadline", type=int, default=1800)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("research-results/ingestion-comparison"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.incremental_interval <= 0:
        parser.error("--incremental-interval must be positive")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    return args


def capacity_args(args, interval, directory):
    values = [
        "--engines", args.engines, "--sizes", args.sizes, "--repetitions", str(args.repetitions),
        "--warmup", str(args.warmup), "--duration", str(args.duration), "--payload", str(args.payload),
        "--batch-size", str(args.batch_size), "--load-commit-interval", str(interval),
        "--sample-interval", str(args.sample_interval), "--memory-budget", args.memory_budget,
        "--jvm-heap", args.jvm_heap, "--child-deadline", str(args.child_deadline),
        "--seed", str(args.seed), "--output", str(directory),
    ]
    if args.dry_run:
        values.append("--dry-run")
    if args.resume and (directory / "manifest.json").exists():
        values.append("--resume")
        if args.rerun_failed:
            values.append("--rerun-failed")
    return capacity.parse_args(values)


def write_report(output, args, problems):
    reports = {}
    for mode in ("single-transaction", "incremental"):
        path = output / mode / "capacity.json"
        if path.exists():
            reports[mode] = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for mode in ("single-transaction", "incremental"):
        for item in reports.get(mode, {}).get("runs", []):
            row = {"mode": mode, "commitInterval": reports[mode]["identity"]["loadCommitInterval"], **item}
            if row.get("artifactDirectory"):
                row["artifactDirectory"] = str(Path(mode) / row["artifactDirectory"])
            rows.append(row)
    report = {
        "methodology": "Independent fresh JVMs and identical seeded datasets. Loading RSS is sampled "
        "and can miss peaks; a failure is not necessarily memory exhaustion. Do not infer large-dataset "
        "capacity or OOM prevention from a small comparison. Consult each mode's manifest and raw artifacts.",
        "runs": rows,
        "modeErrors": problems,
        "modesAvailable": list(reports),
    }
    capacity.write_json(output / "ingestion-comparison.json", report)
    columns = ["mode", "commitInterval", "engine", "baseRows", "repetition", "status", "error",
               "loadSeconds", "sampledPeakLoadRssBytes", "postLoadRssBytes", "engineVersion",
               "effectiveMemoryBudgetBytes", "artifactDirectory"]
    with (output / "ingestion-comparison.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    lines = ["# Ingestion comparison", "", report["methodology"], "",
             "| Mode | Engine | Base rows | Repetition | Status | Load seconds | Sampled peak load RSS bytes |",
             "|---|---|---:|---:|---|---:|---:|"]
    for row in rows:
        lines.append("| {} | {} | {} | {} | {} | {} | {} |".format(
            row["mode"], row["engine"], row["baseRows"], row["repetition"], row["status"],
            row.get("loadSeconds", "unavailable"), row.get("sampledPeakLoadRssBytes", "unavailable")))
    if problems:
        lines.extend(["", "Mode errors:"] + [f"- {mode}: {error}" for mode, error in problems.items()])
    text = "\n".join(lines) + "\n"
    (output / "ingestion-comparison.md").write_text(text, encoding="utf-8")
    (output / "ingestion-comparison.html").write_text(
        '<!doctype html><meta charset="utf-8"><title>Ingestion comparison</title><pre>'
        + html.escape(text) + "</pre>", encoding="utf-8")


def execute(args):
    output = args.output.resolve()
    modes = [("single-transaction", 0), ("incremental", args.incremental_interval)]
    # Validate both modes before creating output or starting a run.
    configurations = [(name, capacity_args(args, interval, output / name)) for name, interval in modes]
    if not args.dry_run:
        jar = capacity.ROOT / "target" / "benchmark.jar"
        if not jar.is_file():
            raise RuntimeError("Build target/benchmark.jar first with ./mvnw package")
        java = str(Path(os.environ["JAVA_HOME"]) / "bin" / ("java.exe" if os.name == "nt" else "java")) \
            if os.environ.get("JAVA_HOME") else shutil.which("java")
        if not java:
            raise RuntimeError("Java not found")
        output.mkdir(parents=True, exist_ok=True)
        manifest_path = output / "comparison-manifest.json"
        identity = {key: value for key, value in vars(args).items()
                    if key not in ("output", "resume", "rerun_failed", "dry_run")}
        identity["runnerSha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        identity["modes"] = {name: capacity.experiment_identity(config, jar, java)
                             for name, config in configurations}
        if manifest_path.exists():
            if not args.resume:
                raise RuntimeError("Comparison manifest already exists; use --resume or a new output directory")
            if json.loads(manifest_path.read_text(encoding="utf-8")) != identity:
                raise RuntimeError("Cannot resume: comparison configuration, build, Git state or machine changed")
        elif args.resume:
            raise RuntimeError("No comparison manifest exists to resume")
        else:
            capacity.write_json(manifest_path, identity)
    failures = {}
    for name, configuration in configurations:
        print(f"\n{name}: commit interval {configuration.load_commit_interval}", flush=True)
        try:
            status = capacity.execute(configuration)
            if status:
                failures[name] = f"Runner returned status {status}; inspect mode report and logs"
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            failures[name] = str(error)
            print(f"{name}: {error}", file=sys.stderr, flush=True)
        if not args.dry_run:
            write_report(output, args, failures)
    return 1 if failures else 0


def main():
    try:
        return execute(parse_args())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"ingestion-comparison: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
