#!/usr/bin/env python3
"""Plan, run and consolidate sequential embedded-database research scenarios."""

import argparse
import csv
import hashlib
import html
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time

SCENARIO_DIR = Path(__file__).resolve().parent / "scenarios"
sys.path.insert(0, str(SCENARIO_DIR))
import memory_capacity as common
import read_scalability
import read_write_interference
import cache_refresh
import endurance
import load_scalability
import memory_reclamation
import ingestion_comparison


MODULES = {
    "memory-capacity": common,
    "read-scalability": read_scalability,
    "read-write-interference": read_write_interference,
    "cache-refresh": cache_refresh,
    "endurance": endurance,
    "load-scalability": load_scalability,
    "memory-reclamation": memory_reclamation,
    "ingestion-comparison": ingestion_comparison,
}
REPORTS = {"memory-capacity": "capacity", **{name: name for name in MODULES if name != "memory-capacity"}}
EXPENSIVE = {"load-scalability", "memory-reclamation", "ingestion-comparison"}
QUESTIONS = [
    ("Which database stores the most records per GB of RAM?", "memory-capacity"),
    ("Which database scales best with concurrent readers?", "read-scalability"),
    ("How much do concurrent writes affect read latency?", "read-write-interference"),
    ("Can cached datasets be refreshed without significantly disrupting readers?", "cache-refresh"),
    ("Does memory consumption remain stable over prolonged operation?", "endurance"),
]
FIELDS = {
    "memory-capacity": ("engine", "baseRows", "repetition", "recordsPerGiBTotalRss", "steadyIncrementalRssBytes"),
    "read-scalability": ("engine", "workload", "workers", "repetition", "throughputOpsPerSecond", "p95Nanos", "scalabilityVsOneWorker"),
    "read-write-interference": ("engine", "pattern", "workers", "readPercent", "repetition", "readP95Nanos", "readP95RatioToReadOnly"),
    "cache-refresh": ("engine", "strategy", "percentage", "repetition", "beforeReadP95Nanos", "duringReadP95Nanos", "duringP95Ratio"),
    "endurance": ("run", "sampledRssChangeBytes", "peakSampledRssBytes", "throughputOpsPerSecond", "readP99Nanos"),
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--level", choices=("quick", "standard", "comprehensive"), default="quick")
    parser.add_argument("--scenarios", type=lambda x: common.csv_list(x, str), default=list(MODULES))
    parser.add_argument("--engines", type=lambda x: common.csv_list(x, str), default=list(common.ENGINES))
    parser.add_argument("--sizes", type=lambda x: common.csv_list(x, int))
    parser.add_argument("--rows", type=int)
    parser.add_argument("--workers", type=lambda x: common.csv_list(x, int))
    parser.add_argument("--memory-budget", default="4g")
    parser.add_argument("--jvm-heap", default="768m")
    parser.add_argument("--cpu-cores", type=int, help="Enforce an integer CPU affinity limit on Linux")
    parser.add_argument("--inactivity-timeout", type=float, default=0,
                        help="Seconds without measurable progress or heartbeat; 0 disables")
    parser.add_argument("--systemd-memory-max", help="Enforce a Linux user-scope memory limit, e.g. 6g")
    parser.add_argument("--systemd-cpu-quota", type=float, help="Enforce a Linux user-scope CPU quota as a percentage")
    parser.add_argument("--inside-systemd-scope", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--warmup", type=float)
    parser.add_argument("--duration", type=float, help="Override ordinary measurement seconds")
    parser.add_argument("--endurance-duration", type=float)
    parser.add_argument("--output", type=Path, default=Path("research-results/research"))
    parser.add_argument("--exclude-expensive", action="store_true")
    parser.add_argument("--exclude-endurance", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if any(name not in MODULES for name in args.scenarios) or any(engine not in common.ENGINES for engine in args.engines):
        parser.error("unknown scenario or engine")
    if args.rows is not None and args.rows < 4:
        parser.error("rows must be at least four")
    if args.sizes and (any(x < 1 for x in args.sizes) or args.sizes != sorted(args.sizes)):
        parser.error("sizes must be positive and increasing")
    if args.workers and (any(x < 1 or x > 256 for x in args.workers) or args.workers != sorted(args.workers)):
        parser.error("workers must be increasing in 1..256")
    if args.repetitions is not None and args.repetitions < 1 or args.cpu_cores is not None and args.cpu_cores < 1:
        parser.error("repetitions and CPU cores must be positive")
    if any(value is not None and (not math.isfinite(value) or value < 0)
           for value in (args.warmup, args.duration, args.endurance_duration)):
        parser.error("invalid timing")
    if args.duration is not None and args.duration <= 0 or args.endurance_duration is not None and args.endurance_duration <= 0.1:
        parser.error("durations must be positive; endurance must exceed 0.1 seconds")
    if args.rerun_failed and not args.resume:
        parser.error("--rerun-failed requires --resume")
    if args.systemd_memory_max is not None:
        try:
            memory_bytes(args.systemd_memory_max)
        except ValueError as error:
            parser.error(str(error))
    if args.systemd_cpu_quota is not None and (not math.isfinite(args.systemd_cpu_quota)
                                                or not 0 < args.systemd_cpu_quota <= 100000):
        parser.error("systemd CPU quota must be a positive finite percentage")
    return args


def memory_bytes(value):
    match = re.fullmatch(r"([1-9][0-9]*)([kKmMgG]?)", value)
    if not match:
        raise ValueError("systemd memory limit must be a positive integer with optional k/m/g suffix")
    amount = int(match.group(1)) * {"": 1, "k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}[match.group(2).lower()]
    if amount > 2 ** 63 - 1:
        raise ValueError("systemd memory limit is too large")
    return amount


def cgroup_v2_path():
    lines = Path("/proc/self/cgroup").read_text(encoding="utf-8").splitlines()
    group = next((line[3:] for line in lines if line.startswith("0::")), None)
    if group is None:
        raise RuntimeError("Unified cgroup v2 membership is unavailable")
    for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
        if " - cgroup2 " not in line:
            continue
        fields = line.split()
        root = Path(fields[3].replace("\\040", " "))
        mount = Path(fields[4].replace("\\040", " "))
        try:
            relative = Path(group).relative_to(root)
        except ValueError:
            continue
        directory = mount / relative
        if directory.is_dir():
            return directory, mount
    raise RuntimeError("Current cgroup v2 directory is unavailable")


def verify_scope(args, directory, mount):
    requested_memory = memory_bytes(args.systemd_memory_max) if args.systemd_memory_max else None
    requested_cpu = args.systemd_cpu_quota / 100 if args.systemd_cpu_quota is not None else None
    def read_memory(path):
        value = (path / "memory.max").read_text(encoding="utf-8").strip()
        return None if value == "max" else int(value)
    def read_cpu(path):
        quota, period = (path / "cpu.max").read_text(encoding="utf-8").split()
        return None if quota == "max" else int(quota) / int(period)
    local_memory = read_memory(directory) if requested_memory is not None else None
    local_cpu = read_cpu(directory) if requested_cpu is not None else None
    if requested_memory is not None and (local_memory is None or local_memory > requested_memory):
        raise RuntimeError("Requested systemd memory limit is not enforced in the current cgroup")
    if requested_cpu is not None and (local_cpu is None or local_cpu > requested_cpu + 1e-9):
        raise RuntimeError("Requested systemd CPU quota is not enforced in the current cgroup")
    ancestors = [directory, *directory.parents]
    if mount not in ancestors:
        raise RuntimeError("Current cgroup is outside the cgroup mount")
    ancestors = ancestors[:ancestors.index(mount) + 1]
    def effective(reader, requested):
        if requested is None:
            return None
        try:
            values = [value for path in ancestors if (value := reader(path)) is not None]
        except (OSError, ValueError, ZeroDivisionError):
            return None
        return min(values) if values else None
    return {"mechanism": "systemd user scope, cgroup v2", "memoryEnforced": requested_memory is not None,
            "cpuEnforced": requested_cpu is not None, "configuredMemoryBytes": requested_memory,
            "localMemoryMaxBytes": local_memory, "effectiveMemoryMaxBytes": effective(read_memory, requested_memory),
            "configuredCpuQuotaCores": requested_cpu, "localCpuQuotaCores": local_cpu,
            "effectiveCpuQuotaCores": effective(read_cpu, requested_cpu)}


def launch_scope(args, argv):
    if sys.platform != "linux" or not shutil.which("systemd-run"):
        raise RuntimeError("systemd user-scope limits require Linux and systemd-run")
    command = ["systemd-run", "--user", "--scope", "--quiet", "--no-ask-password"]
    if args.systemd_memory_max:
        command += ["-p", f"MemoryMax={memory_bytes(args.systemd_memory_max)}"]
    if args.systemd_cpu_quota is not None:
        command += ["-p", f"CPUQuota={args.systemd_cpu_quota}%"]
    command += [sys.executable, str(Path(__file__).resolve()), *argv, "--inside-systemd-scope"]
    status = subprocess.run(command, check=False).returncode
    if status:
        print("research: scope launch or scoped research failed; no unbounded fallback was started", file=sys.stderr)
    return status


def plan(args):
    quick = args.level == "quick"
    comprehensive = args.level == "comprehensive"
    sizes = args.sizes or ([100, 1000] if quick else
                           [1000, 10000, 50000, 100000, 250000, 500000, 1000000, 2000000, 5000000]
                           if comprehensive else [1000, 10000, 50000, 100000])
    rows = args.rows or (100 if quick else 100000)
    workers = args.workers or ([1, 2] if quick else [1, 2, 4, 8, 16, 32, 64, 128]
                               if comprehensive else [1, 4, 8, 16])
    rw_workers = workers if args.workers or quick else [1, 4, 16, 32] if comprehensive else [1, 4, 16]
    fixed_workers = max(workers) if args.workers or quick else 4
    repetitions = args.repetitions or (1 if quick else 3)
    warmup = args.warmup if args.warmup is not None else (0 if quick else 30)
    duration = args.duration if args.duration is not None else (1 if quick else 60)
    endurance_duration = args.endurance_duration if args.endurance_duration is not None else (2 if quick else 3600)
    output = args.output.resolve()
    common_args = ["--engines", ",".join(args.engines), "--repetitions", str(repetitions),
                   "--memory-budget", args.memory_budget, "--jvm-heap", args.jvm_heap,
                   "--inactivity-timeout", str(args.inactivity_timeout)]
    time_args = ["--warmup", str(warmup), "--duration", str(duration)]
    result = []
    for name in args.scenarios:
        if args.exclude_endurance and name == "endurance" or args.exclude_expensive and name in EXPENSIVE:
            continue
        directory = output / name
        options = common_args + ["--output", str(directory)]
        if name == "memory-capacity":
            options += ["--sizes", ",".join(map(str, sizes))] + time_args
        elif name == "read-scalability":
            options += ["--rows", str(rows), "--workers", ",".join(map(str, workers)),
                        "--workloads", "point,indexed-range" if quick else "point,indexed-range,joins,aggregate"] + time_args
        elif name == "read-write-interference":
            options += ["--rows", str(rows), "--workers", ",".join(map(str, rw_workers)),
                        "--patterns", "independent" if quick else "independent,hot",
                        "--read-percentages", "100,95" if quick else "100,99,95,90,80,50,20,0" if comprehensive
                        else "100,95,50"] + time_args
        elif name == "cache-refresh":
            options += ["--rows", str(rows), "--workers", str(fixed_workers),
                        "--strategies", "replacement" if quick else "record-update,bulk-invalidate,incremental-refresh,replacement",
                        "--percentages", "50" if quick else "10,100", "--before", str(duration),
                        "--after", str(duration)]
        elif name == "endurance":
            options += ["--rows", str(rows), "--workers", str(fixed_workers), "--warmup", str(warmup),
                        "--duration", str(endurance_duration), "--maintenance-interval",
                        str(min(60, max(0.1, endurance_duration / 4)))]
        elif name == "load-scalability":
            options += ["--rows", str(rows), "--workers", str(fixed_workers),
                        "--rates", "10,100" if quick else "10,100,500,1000,5000"] + time_args
        elif name == "memory-reclamation":
            options += ["--rows", str(rows), "--cycles", "1" if quick else "3",
                        "--idle-seconds", "0.1" if quick else "30"]
        else:
            options += ["--sizes", ",".join(map(str, sizes))] + time_args
        parsed = MODULES[name].parse_args(options)
        e, r = len(args.engines), repetitions
        if name == "memory-capacity":
            count, minimum = e * len(sizes) * r, warmup + duration
        elif name == "read-scalability":
            count, minimum = e * len(parsed.workloads) * len(workers) * r, warmup + duration
        elif name == "read-write-interference":
            count = e * len(parsed.patterns) * len(parsed.workers) * len(parsed.read_percentages) * r
            minimum = warmup + duration
        elif name == "cache-refresh":
            count, minimum = e * len(parsed.strategies) * len(parsed.percentages) * r, 2 * duration
        elif name == "endurance":
            count, minimum = e * r, warmup + endurance_duration
        elif name == "load-scalability":
            count, minimum = e * len(parsed.rates) * r, warmup + duration
        elif name == "memory-reclamation":
            count, minimum = e * r, parsed.cycles * 3 * parsed.idle_seconds
        else:
            count, minimum = 2 * e * len(sizes) * r, warmup + duration
        result.append({"name": name, "options": options, "count": count, "minimumSeconds": count * minimum})
    if not result:
        raise ValueError("No scenarios selected after exclusions")
    return result


def configure_cpu(args):
    if args.cpu_cores is None:
        return {"configuredCores": None, "enforced": False, "mechanism": "inherited", "effectiveCpuIds":
                sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None}
    if not hasattr(os, "sched_getaffinity") or not hasattr(os, "sched_setaffinity"):
        raise RuntimeError("CPU affinity limits are unavailable on this platform")
    allowed = sorted(os.sched_getaffinity(0))
    if args.cpu_cores > len(allowed):
        raise RuntimeError("Requested CPU cores exceed the inherited affinity")
    chosen = set(allowed[:args.cpu_cores])
    os.sched_setaffinity(0, chosen)
    effective = sorted(os.sched_getaffinity(0))
    if set(effective) != chosen:
        raise RuntimeError("CPU affinity verification failed")
    return {"configuredCores": args.cpu_cores, "enforced": True, "mechanism": "Linux process affinity inherited by children",
            "effectiveCpuIds": effective}


def identity(args, schedule, cpu, scope):
    jar = common.ROOT / "target" / "benchmark.jar"
    if not jar.is_file():
        raise RuntimeError("Build target/benchmark.jar first with ./mvnw package")
    java = str(Path(os.environ["JAVA_HOME"]) / "bin" / ("java.exe" if os.name == "nt" else "java")) \
        if os.environ.get("JAVA_HOME") else shutil.which("java")
    if not java:
        raise RuntimeError("Java not found")
    version = common.command_output([java, "-version"])
    match = re.search(r'version "(\d+)(?:[."+_-]|$)', version)
    if not match or int(match.group(1)) < 25:
        raise RuntimeError(f"Research requires JDK 25 or later; set JAVA_HOME or PATH to it.\n{version}")
    try:
        help_text = common.command_output([java, "-jar", str(jar), "--help"])
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"Cannot start {jar} with {java}: {(error.stderr or error.stdout or '').strip()}") from error
    missing = [option for option in ("--load-commit-interval", "--sample-interval", "--inactivity-timeout")
               if option not in help_text]
    if missing:
        raise RuntimeError(f"Benchmark jar lacks {', '.join(missing)}; rebuild target/benchmark.jar from this checkout")
    config = {key: value for key, value in vars(args).items()
              if key not in ("output", "resume", "rerun_failed", "dry_run", "inside_systemd_scope")}
    config.update({"plan": schedule, "cpu": cpu, "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
                   "systemdScope": scope,
                   "runnerSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   "scenarioSha256": {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                                      for name, module in MODULES.items()},
                   "gitCommit": common.command_output(["git", "--no-pager", "rev-parse", "HEAD"]),
                   "gitDirty": common.command_output(["git", "--no-pager", "status", "--porcelain", "--untracked-files=no"]),
                   "javaVersion": version,
                   "pythonVersion": platform.python_version(),
                   "machine": {"os": platform.platform(), "architecture": platform.machine(),
                               "logicalProcessors": os.cpu_count(), "node": platform.node()}})
    return config


def capacity_chart(data, field, title):
    rows = [row for row in data.get("runs", []) if row.get("status") == "OK"
            and isinstance(row.get("baseRows"), (int, float))
            and isinstance(row.get(field), (int, float)) and math.isfinite(row[field])]
    if not rows:
        return ""
    sizes = sorted({row["baseRows"] for row in rows})
    ymax = max(row[field] for row in rows)
    if ymax <= 0:
        return ""
    colors = ("#0969da", "#1a7f37", "#8250df", "#bc4c00")
    parts = [f'<h3>{html.escape(title)}</h3><svg viewBox="0 0 760 300" role="img" '
             f'aria-label="{html.escape(title, quote=True)}"><path d="M75 20V250H630" fill="none" stroke="#57606a"/>']
    for index, engine in enumerate(common.ENGINES):
        points = []
        for size in sizes:
            values = [row[field] for row in rows if row.get("engine") == engine and row["baseRows"] == size]
            if values:
                x = 75 + 555 * sizes.index(size) / max(1, len(sizes) - 1)
                y = 250 - 220 * (sum(values) / len(values)) / ymax
                points.append(f"{x:.1f},{y:.1f}")
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{colors[index]}"/>')
        if points:
            parts.append(f'<polyline fill="none" stroke="{colors[index]}" points="{" ".join(points)}"/>')
            parts.append(f'<text x="640" y="{35 + index * 20}" fill="{colors[index]}">{engine}</text>')
    for index, size in enumerate(sizes):
        x = 75 + 555 * index / max(1, len(sizes) - 1)
        parts.append(f'<text x="{x:.1f}" y="270" text-anchor="middle">{size}</text>')
    parts.append(f'<text x="65" y="24" text-anchor="end">{ymax:.2g}</text>'
                 '<text x="65" y="250" text-anchor="end">0</text>'
                 '<text x="350" y="292" text-anchor="middle">Base rows (categorical)</text></svg>')
    return "".join(parts)


def capacity_boundaries(data):
    boundaries = data.get("boundaries", {})
    measured = [(engine, boundaries[engine]["largestSuccessfulBaseRows"])
                for engine in common.ENGINES if boundaries.get(engine, {}).get("largestSuccessfulBaseRows") is not None]
    if not measured:
        return ""
    maximum = max(size for _, size in measured)
    parts = ['<h3>Largest successfully tested dataset</h3><p>Observed tested bounds, not absolute capacity.</p>'
             '<svg viewBox="0 0 760 220" role="img" aria-label="Largest successfully tested dataset">']
    for index, (engine, size) in enumerate(measured):
        y = 20 + index * 45
        parts.append(f'<text x="100" y="{y + 17}" text-anchor="end">{engine}</text>'
                     f'<rect x="110" y="{y}" width="{500 * size / maximum:.1f}" height="22" fill="#0969da"/>'
                     f'<text x="{120 + 500 * size / maximum:.1f}" y="{y + 17}">{size} base rows</text>')
    parts.append("</svg>")
    return "".join(parts)


def classification(row):
    status = row.get("status", "INCOMPLETE")
    if status == "OK":
        return "OK"
    if status in ("RUNNING", "PENDING", "INTERRUPTED"):
        return "INCOMPLETE"
    error = str(row.get("error") or row.get("errorClass") or status).upper()
    if "UNSUPPORTED" in error:
        return "UNSUPPORTED"
    if "CORRECTNESS" in error or "INCORRECT" in error:
        return "INCORRECT"
    if error in ("OOM", "NATIVE_ALLOCATION"):
        return "MEMORY_EXHAUSTION"
    if error in ("MEMORY_BUDGET", "MEMORY_ADMISSION", "CHILD_DEADLINE",
                 "HEARTBEAT_TIMEOUT", "INACTIVITY_TIMEOUT", "TIMEOUT", "JVM_CRASH", "CHILD_EXIT"):
        return error
    return "FAILED"


def write_reports(output, manifest):
    evidence = {}
    report_errors = {}
    for name, state in manifest["scenarios"].items():
        if not state.get("includeEvidence"):
            continue
        path = output / name / (REPORTS[name] + ".json")
        if path.exists():
            try:
                evidence[name] = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as error:
                report_errors[name] = str(error)
    report = {"identity": manifest["identity"], "scenarios": manifest["scenarios"], "evidence": evidence,
              "scopeAttempts": manifest.get("scopeAttempts", []),
              "reportErrors": report_errors,
              "caveats": "Results describe only the tested configurations and valid runs. Incomplete, failed "
              "and unsupported observations must not be ranked. RSS includes the process, not only the database; "
              "retained memory is not proof of a leak. PGlite uses a custom loopback transport and serialized "
              "single-session execution. Memory budgets are sampled watchdogs, not OS-enforced limits. "
              "Affinity limits logical CPU IDs, not an inherited cgroup CPU quota."}
    common.write_json(output / "research.json", report)
    rows = []
    for scenario, data in evidence.items():
        for original in data.get("runs", []):
            row = {**original, **(original.get("metrics") or {})}
            rows.append({"scenario": scenario, "run": row.get("run"), "engine": row.get("engine"),
                         "status": row.get("status"), "classification": classification(row), "error": row.get("error"),
                         "repetition": row.get("repetition"), "engineVersion": row.get("engineVersion"),
                         "artifactDirectory": row.get("artifactDirectory"),
                         "measurements": json.dumps(row, ensure_ascii=False, sort_keys=True)})
    with (output / "research.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["scenario", "run", "engine", "status", "classification", "error", "repetition",
                                                  "engineVersion", "artifactDirectory", "measurements"])
        writer.writeheader()
        writer.writerows(rows)
    lines = ["# Embedded database research", "", report["caveats"], "",
             "Configuration, versions, repetitions, limits and raw measurements are preserved in research.json "
             "and the linked scenario reports. No winner is inferred from incomplete or incomparable data.", "",
             "| Scenario | Status | Planned runs | Report |", "|---|---|---:|---|"]
    for item in manifest["identity"]["plan"]:
        name = item["name"]
        state = manifest["scenarios"].get(name, {}).get("status", "PENDING")
        link = f"[details]({name}/{REPORTS[name]}.html)" if name in evidence else "unavailable"
        lines.append(f"| {name} | {state} | {item['count']} | {link} |")
    if report_errors:
        lines.extend(["", "Unreadable scenario evidence:"] + [
            f"- {name}: {error}" for name, error in report_errors.items()])
    dimensions = {"run", "engine", "workload", "workers", "repetition", "pattern", "readPercent",
                  "strategy", "percentage", "baseRows"}
    for question, name in QUESTIONS:
        lines.extend(["", "## " + question, ""])
        data = evidence.get(name)
        if not data:
            lines.append("No experiment evidence is available for this question.")
            continue
        lines.append("Scenario configuration: `" + json.dumps(data.get("identity", {}), sort_keys=True) + "`")
        lines.append("")
        fields = FIELDS[name] + ("engineVersion", "effectiveMemoryBudgetBytes")
        lines.append("| " + " | ".join(fields) + " | status |")
        lines.append("|" + "|".join("---" for _ in range(len(fields) + 1)) + "|")
        for original in data.get("runs", []):
            row = {**original, **(original.get("metrics") or {})}
            values = []
            for field in fields:
                value = row.get(field) if row.get("status") == "OK" or field in dimensions \
                    or field in ("engineVersion", "effectiveMemoryBudgetBytes") else None
                values.append(str(value if value is not None else "unavailable").replace("|", "\\|"))
            lines.append("| " + " | ".join(values) + " | " + classification(row) + " |")
        if not data.get("runs"):
            lines.append("| " + " | ".join("unavailable" for _ in fields) + " | INCOMPLETE |")
    text = "\n".join(lines) + "\n"
    (output / "research.md").write_text(text, encoding="utf-8")
    capacity = evidence.get("memory-capacity", {})
    charts = capacity_chart(capacity, "steadyMedianRssBytes", "Steady process RSS versus dataset size")
    charts += capacity_chart(capacity, "recordsPerGiBTotalRss", "Records per GiB of process RSS")
    charts += capacity_boundaries(capacity)
    links = "".join(f'<h2>{html.escape(name)}</h2><iframe title="{html.escape(name)}" '
                    f'src="{html.escape(name)}/{html.escape(REPORTS[name])}.html"></iframe>'
                    for name in evidence)
    (output / "research.html").write_text(
        '<!doctype html><meta charset="utf-8"><title>Embedded database research</title>'
        '<style>body{font:15px system-ui;max-width:1100px;margin:2em auto}pre{white-space:pre-wrap}'
        'iframe{width:100%;height:650px;border:1px solid #d0d7de}svg{max-width:100%}</style>'
        '<pre>' + html.escape(text) + '</pre>' + charts + links, encoding="utf-8")


def execute(args):
    schedule = plan(args)
    total = sum(item["count"] for item in schedule)
    minimum = sum(item["minimumSeconds"] for item in schedule)
    print(f"Planned maximum: {total} fresh-JVM runs; minimum measurement/idle time {minimum:.1f}s "
          "(excluding loading, validation and unpredictable refresh time).", flush=True)
    for item in schedule:
        print(f"  {item['name']}: {item['count']} runs, minimum {item['minimumSeconds']:.1f}s", flush=True)
        if args.dry_run:
            print("    " + " ".join(item["options"]), flush=True)
    if args.dry_run:
        return 0
    scope_path = None
    scope = {"mechanism": "none", "memoryEnforced": False, "cpuEnforced": False}
    if args.systemd_memory_max or args.systemd_cpu_quota is not None:
        if not args.inside_systemd_scope:
            raise RuntimeError("Requested systemd limits require launching through the user scope")
        directory, mount = cgroup_v2_path()
        scope_path = str(directory)
        scope = verify_scope(args, directory, mount)
    cpu = configure_cpu(args)
    config = identity(args, schedule, cpu, scope)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    path = output / "research-manifest.json"
    if path.exists():
        if not args.resume:
            raise RuntimeError("Research manifest exists; use --resume or a new output directory")
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest["identity"] != config:
            raise RuntimeError("Cannot resume: research configuration, build, Git state or machine changed")
    elif args.resume:
        raise RuntimeError("No research manifest exists to resume")
    else:
        manifest = {"identity": config, "scenarios": {}, "scopeAttempts": []}
        common.write_json(path, manifest)
    if scope_path is not None:
        manifest.setdefault("scopeAttempts", []).append({"path": scope_path, "startedAt": time.time()})
        common.write_json(path, manifest)
    write_reports(output, manifest)
    for item in schedule:
        name = item["name"]
        previous = manifest["scenarios"].get(name, {})
        if previous.get("status") == "OK" or previous.get("status") == "FAILED" and not args.rerun_failed:
            if previous.get("status") == "OK" and not (output / name / (REPORTS[name] + ".json")).is_file():
                manifest["scenarios"][name] = {"status": "FAILED", "error": "Completed scenario report is missing"}
                common.write_json(path, manifest)
                write_reports(output, manifest)
            continue
        marker = "comparison-manifest.json" if name == "ingestion-comparison" else "manifest.json"
        options = list(item["options"])
        if (output / name / marker).exists():
            options.append("--resume")
            if args.rerun_failed:
                options.append("--rerun-failed")
        manifest["scenarios"][name] = {"status": "RUNNING", "startedAt": time.time()}
        common.write_json(path, manifest)
        write_reports(output, manifest)
        print(f"\nResearch scenario: {name}", flush=True)
        try:
            status = MODULES[name].execute(MODULES[name].parse_args(options))
            entry = {"status": "OK" if status == 0 else "FAILED", "exitCode": status, "includeEvidence": True}
            report_path = output / name / (REPORTS[name] + ".json")
            if status == 0:
                try:
                    json.loads(report_path.read_text(encoding="utf-8"))
                except (OSError, ValueError) as error:
                    entry = {"status": "FAILED", "error": f"Missing or invalid scenario report: {error}"}
        except (OSError, RuntimeError, subprocess.SubprocessError, ValueError) as error:
            entry = {"status": "FAILED", "error": str(error)}
            print(f"{name}: {error}", file=sys.stderr, flush=True)
        except KeyboardInterrupt:
            manifest["scenarios"][name] = {"status": "INTERRUPTED"}
            common.write_json(path, manifest)
            write_reports(output, manifest)
            raise
        manifest["scenarios"][name] = entry
        common.write_json(path, manifest)
        write_reports(output, manifest)
    return 1 if any(entry["status"] != "OK" for entry in manifest["scenarios"].values()) else 0


def main(argv=None):
    try:
        arguments = list(sys.argv[1:] if argv is None else argv)
        args = parse_args(arguments)
        if (args.systemd_memory_max or args.systemd_cpu_quota is not None) and not args.inside_systemd_scope \
                and not args.dry_run:
            return launch_scope(args, arguments)
        return execute(args)
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError) as error:
        print(f"research: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("research: interrupted; resume with the same options", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
