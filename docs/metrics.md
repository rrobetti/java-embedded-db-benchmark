# Metrics and interpretation

Aggregate outputs are `report.json`, `report.csv`, `report.md`, `report.html`.
Per-run output includes structured results, child logs and HdrHistogram
latency artifacts. Preserve the whole directory, not only a rendered table.

Throughput counts completed logical operations per measured second.
Latency is worker submission to completion in closed-loop, or scheduled
arrival to completion in open-loop, including scheduler lateness, queueing
and serialized-session contention. Separate database-only service and
queue-delay histograms are not emitted. Rejected arrivals are excluded from
success histograms. Averages hide tails; percentile values
must retain their units and sample count. Warmup samples must not be mixed
with measurement samples.

Errors, lock/busy failures, rejected requests, missing results and post-run
correctness failures are not interchangeable. A fast run with incorrect
balances, incomplete transactions or no successful operations is invalid.
Suite exit status is part of the result.

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

Fresh JVM repetitions do not imply independent host caches or guarantee
statistical significance. No synthetic performance findings are checked
in. Derive any comparison from valid emitted reports and disclose resource
sampling, workload semantics and policy differences.
