# Metrics and interpretation

Aggregate outputs are `report.json`, `report.csv`, `report.md`, `report.html`.
`report-aggregates.csv` contains compatible repeated-run summaries.
Per-run output includes structured results, child logs and HdrHistogram
latency artifacts. Preserve the whole directory, not only a rendered table.

Throughput counts completed logical operations per measured second.
Latency is worker submission to completion in closed-loop, or scheduled
arrival to completion in open-loop, including scheduler lateness, queueing
and pooled/serialized-session contention. Separate database-only service and
queue-delay histograms are not emitted. Rejected arrivals are excluded from
success histograms. Averages hide tails; percentile values
must retain their units and sample count. Warmup samples must not be mixed
with measurement samples.

Errors, lock/busy failures, rejected requests, missing results and post-run
correctness failures are not interchangeable. A fast run with incorrect
balances, incomplete transactions or no successful operations is invalid.
Suite exit status is part of the result.
Missing child reports retain a bounded log tail for diagnosis. Explicit
JVM OOM, native-allocation and JVM-crash messages are distinguished; exit
code 137 alone does not prove an out-of-memory event.
Actual deadlock diagnostics are distinguished from a generic SQLState
40001 serialization conflict. A correctness-valid `COMPLETED_WITH_ERRORS`
run preserves its error evidence but is not an error-free performance
comparison.

JVM heap, garbage-collection counters and process resource sampling describe
different things. Heap is not RSS; RSS includes native/runtime pages and may
include shared pages. CPU sampling and OS-specific RSS availability differ
across platforms. An unavailable metric must remain unavailable, not become
a fabricated zero or a claim that no memory was used.

`--memory-budget` is a supervisor watchdog safety threshold.
`--jvm-heap` caps Java heap only. Neither measures database storage capacity,
guarantees a hard OS cap, nor counts just the engine's own memory.
PGlite's Chicory WASM linear memory consumes Java heap; SQLite native state
and JVM/native runtime overhead can be outside heap.

RSS uses Linux `/proc`, macOS `ps`, or Windows PowerShell process working-set
queries where available. Peak RSS is available through `/proc`/Windows but
not the macOS fallback. Disk read/write-byte counters currently require
Linux `/proc`. External process queries add platform-dependent sampling
overhead and may be denied. Raw JSON records `rssWatchdogAvailable`; absent
monitoring is not a successful hard cap.

Resource snapshots are taken at baseline, initialization, schema, load,
steady-state, explicit row deletion after validation, and shutdown.
Additionally, each child writes `resources.jsonl` with phase-labelled JVM
heap/non-heap, RSS/peak RSS, direct-buffer, CPU, GC, thread, free-memory and
swap counters. Sampling defaults to one second and can be changed with
`--sample-interval` (0.1 to 3600 seconds). Samples are also taken at phase
boundaries. CPU load fractions are JVM/OS-provided recent estimates, not
dedicated CPU quotas. Direct buffers are not all native allocations; WASM
memory is explicitly unavailable rather than inferred from RSS. Short spikes
can be missed. Monitoring does not provide per-interval operation or latency
data, supervisor process time series, or complete native-memory attribution.
Deletion is not compaction or proof that allocated pages were returned to
the OS. CPU/GC/I/O deltas refer to measurement, not startup/load. Normalized
throughput divides by reported logical processors or deterministic logical
dataset GiB; these are comparison aids, not actual database efficiency or
compressed bytes.

Latency outputs include min, mean, p50, p90, p95, p99, p99.9 and max.
Compatible valid repetitions are aggregated by merging raw histograms,
not averaging percentiles. Aggregate throughput is total successes divided
by total measured elapsed time. Inspect `aggregates` and its source run IDs
in JSON/HTML before interpreting it as repeated evidence.
Use per-run throughput and min/max/standard deviation to assess run-to-run
variability; a merged histogram is not a confidence interval. Compare the
largest **successful, valid** tested dataset under the same memory budget,
workload and policy, not the largest requested/admission-rejected size.
That observation is a lower bound on a tested configuration, not a universal
engine capacity limit.
Raw results also identify the actual JDBC URL, driver version and execution
mode. For default PGlite results, retain the explicit adapter-transport
deviation label alongside performance values; a different bridge is not an
unmodified-upstream-driver result.
Initialization separates actual driver class initialization and adapter
creation. Database start includes engine boot, anchor connection and XA
probe where applicable. The first worker connection is not necessarily the
first chronological JDBC connection. Read `timingSemantics` with the raw
timings; legacy field names are retained but do not imply narrower scopes.

Fresh JVM repetitions do not imply independent host caches or guarantee
statistical significance. No synthetic performance findings are checked
in. Derive any comparison from valid emitted reports and disclose resource
sampling, workload semantics and policy differences.
