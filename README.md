# Java embedded relational database benchmark

A standalone **Java 25 / JDBC** comparison of H2, HSQLDB, SQLite and
PGlite4j. Each measured engine/workload runs in a fresh JVM. There is no
required PostgreSQL server, Docker, Node.js or system Maven installation.
This is a benchmark harness, not a precomputed leaderboard.

**PGlite transport deviation:** the working PGlite adapter runs real
PGlite4j 0.0.3 WASM through a repository-local framed loopback bridge and
pgjdbc 42.7.15. It is **not the unmodified PGlite4j JDBC driver** or its
`jdbc:pglite:memory:benchmark` path. See
[database configuration](docs/database-configuration.md) before comparing
these results to upstream PGlite4j.
An explicit `--databases pglite --db-profile upstream --load-mode prepared`
selects the original named URL/driver path instead; its batching and
SQL-error recovery limitations remain visible, not silently repaired.

## Start

Use **JDK 25 or later** to build the project, which compiles for Java 25;
record the exact JDK when comparing benchmark results. Setup invokes the
Maven wrapper with the environment's `java` and `JAVA_HOME` (if set), without
pre-validating the JDK. Maven startup files are skipped during setup.

```bash
bash scripts/setup.sh
bash scripts/benchmark.sh
```

PowerShell 7 (including Windows):

```powershell
pwsh -File scripts/setup.ps1
pwsh -File scripts/benchmark.ps1
```

Setup prints detected OS, architecture, logical CPU count and physical RAM,
resolves Maven dependencies with the wrapper, packages/tests the project and
connects to **all four real engines**. First setup requires network access;
subsequent runs with a built jar do not.

```bash
bash scripts/benchmark.sh --offline --profile smoke \
  --databases h2,hsqldb,sqlite,pglite --workloads point \
  --rows 100 --warmup 0 --duration 0.2 --output benchmark-results/smoke
```

`--offline` belongs to the scripts, not the Java CLI. It prevents dependency
downloads when an absent jar must be built; the Maven distribution and all
dependencies must already be cached. PowerShell setup uses `-Offline`.

## Profiles and controls

The default `quick` uses 1,000 rows, 128-byte payloads, one worker/connection,
five seconds warmup and ten seconds measurement per selected combination.
It is intentionally small; it is **not** a meaningful production ranking.
All engines use the same default 768m child heap/4g budget for a fair resource
ceiling. Explicit heap/budget options override these values; an undersized
PGlite heap can fail during initialization. The script's supervisor heap
defaults to 512m.

The effective
limit is still capped at half the host RAM and 80% of a detected container
memory limit; passing admission does not guarantee a run will fit in memory.
In particular, HSQLDB can exhaust the Java heap with the `full` dataset, even
when the heap is larger than the default. Increase `--jvm-heap` for all engines
in the comparison and set `--memory-budget` high enough for the heap, native
memory and supervisor, within the available host/container memory, or reduce
`--rows`. The memory budget alone does not increase the child heap.

| Profile | Rows | Warmup seconds | Measurement seconds | Fresh-JVM iterations |
|---|---:|---:|---:|---:|
| smoke | 100 | 0 | 1 | 1 |
| quick | 1,000 | 5 | 10 | 1 |
| standard | 100,000 | 30 | 120 | 3 |
| full | 100,000 | 30 | 120 | 5 |
| stress | 100,000 | 30 | 60 | 1 |
| endurance | 100,000 | 60 | 3,600 | 1 |

Rows are per base table; the dataset contains seven times as many records.
All sizes/times can be overridden. By default, `full` runs point, indexed-range,
joins, mixed and HTAP workloads; other profiles run point and mixed. With four
engines, one worker and default repetitions, `full` schedules 100 children
(at least 4 hours 10 minutes of warmup and measurement, excluding loading
and validation). The stress profile does not automatically increase sizes;
specify `--rows` for each capacity experiment. These profiles are not the
dedicated research scenarios. Inspect the definitive CLI:

```bash
java -jar target/benchmark.jar --help
bash scripts/benchmark.sh --offline --profile quick --databases h2 \
  --workloads point,mixed --rows 10000 --payload-size 128 \
  --threads 2 --connections 2 --batch-size 500 --transaction-size 10 \
  --rate 0 --warmup 1 --duration 2 --iterations 1 --seed 42 \
  --memory-budget 768m --jvm-heap 256m --db-profile default \
  --connection-mode auto --output benchmark-results/example
```

Duration/warmup are numeric **seconds**; heap/budget accept `k`, `m`, `g`
binary suffixes. `--iterations` means independent fresh-JVM repetitions and must be positive.
`--rate 0` selects closed-loop; positive rates select bounded open-loop.
`--repetitions` is an alias for `--iterations`; `--max-operations 0` means
duration-only, while positive values bound attempts per phase.
`--child-deadline` limits each individual child in seconds, including loading
and validation, not the whole suite.
large PGlite loads can be slow, so use fewer rows and iterations for diagnosis.
`--threads 1,4,8` runs a concurrency grid; omit `--connections` to match each
worker count, or set a connection count no larger than the smallest grid value.
Connections cannot exceed workers. `auto` uses a bounded pool when fewer
connections than workers are requested; `dedicated` requires equal counts.
Do not assume every connection policy or
workload is equally suitable for all engines.

`--load-commit-interval 10000` commits after every 10,000 inserted records,
independently of JDBC batch size and across table boundaries. The default `0`
preserves the single-transaction loader. Incremental loading requires an
auto-commit connection and can leave previously committed rows on failure;
the failed child database must not be reused. A fresh child/database is used
for every benchmark run. Incremental commits are not guaranteed to prevent
engine memory exhaustion.

### Progressive memory capacity

The memory-capacity runner requires Python 3 and a built benchmark jar. It
tests each engine sequentially with a fresh JVM for each size/repetition;
on failure it stops increasing sizes for that engine and continues with the
others. The default sequence is 1,000 through 5,000,000 base rows, with
10,000-record load commits. This can take hours and exhaust the configured
budget. Start with a dry run and small sizes:

```bash
bash scripts/scenarios/memory-capacity.sh --sizes 1000,10000 --dry-run
bash scripts/scenarios/memory-capacity.sh --engines h2,hsqldb,sqlite,pglite \
  --sizes 1000,10000 --warmup 1 --duration 2 --memory-budget 2g \
  --jvm-heap 768m --output research-results/capacity
```

PowerShell: `./scripts/scenarios/memory-capacity.ps1` accepts the same
arguments. Repeat the identical command with `--resume` to reuse completed
runs, and add `--rerun-failed` to retry failures. The manifest checks the
experiment options, runner and benchmark jar hashes, Git state, Java and
machine; changed configurations must use a new directory. Reports and raw
artifacts are written incrementally. `capacity.json`, `.csv`, `.md` and
`.html` record observed successful/failed boundaries, load time and sampled
RSS. The configured upper limit reached successfully is only a lower bound,
not an absolute capacity. Sampled peaks can miss spikes, admission failures
are not proof of OOM, and RSS deltas are not exact database-only memory.
The runner does not impose a kernel memory or CPU limit; external scopes are
described in [limitations](docs/limitations.md).

### Read scalability

The read runner executes point lookups, indexed ranges, joins and verified
account aggregations in independent fresh JVMs. Defaults are 100,000 base
rows, 1–128 workers, three repetitions, 30 seconds warmup and 120 seconds
measurement per run. That is 384 runs across all engines, at least 16 hours
of warmup/measurement plus loading and validation. Start with a smaller grid:

```bash
bash scripts/scenarios/read-scalability.sh --engines h2,sqlite \
  --rows 1000 --workers 1,2 --warmup 1 --duration 2 --repetitions 1 \
  --output research-results/reads
```

PowerShell: `./scripts/scenarios/read-scalability.ps1` accepts the same
arguments. `--dry-run`, `--resume` and `--rerun-failed` have the same
meaning as in the capacity runner. Include worker count 1 to calculate
per-repetition scalability ratios. Results include throughput, p95/p99,
errors, process CPU and RSS, and the effective session topology. PGlite's
logical workers serialize through a single physical session; they are not
independent concurrent database sessions.

### Open-loop load scalability

`scripts/scenarios/load-scalability.sh` (or `.ps1`) offers point reads at
progressively increasing rates using the bounded open-loop queue. Scheduled
arrival-to-completion latency includes queueing and scheduler delay. The
reports distinguish offered, accepted, completed, failed and rejected
requests; the observed sustainable bound requires every repetition at that
rate to meet the configured p99 and combined failure/rejection targets.
It is a tested bound, not an absolute maximum. For example:

```bash
bash scripts/scenarios/load-scalability.sh --engines h2,sqlite \
  --rates 10,100,500 --rows 1000 --workers 2 --warmup 1 --duration 5 \
  --p99-ms 100 --max-error-rate 0.01 --output research-results/load
```

Default rates are 10, 100, 500, 1,000 and 5,000 requests/s, with 30 seconds
warmup and 60 seconds measurement. `--queue-capacity` bounds queued arrivals;
`--dry-run`, `--resume` and `--rerun-failed` are supported. Select rates
appropriate to the machine rather than interpreting a short CI run as a
production saturation result.

On an otherwise idle 32 GiB, 12-logical-CPU Linux workstation, keep substantial
headroom for the OS and other processes, for example use `--memory-budget 6g
--jvm-heap 2g` and an externally configured CPU/memory scope. The benchmark
also limits its effective watchdog budget to a fraction of detected host or
container memory. Do not launch large experiments on a shared machine without
coordination.

## Results and safety

The output directory contains `report.json`, `report.csv`, `report.md`,
`report.html` and per-run artifacts/logs. Failed or incorrect runs remain
visible and cause a nonzero exit; do not rank invalid results.
Memory budgets are **watchdog limits, not OS-enforced reservations**.
JVM `-Xmx` caps Java heap, including Chicory WASM memory, not total RSS or
SQLite's native allocations. See
[limitations](docs/limitations.md) before increasing profiles or concurrency.

Native extraction failures: set `BENCHMARK_NATIVE_TMP` to a user-owned,
writable directory on an executable filesystem and retry. Scripts never
install privileged packages or relax system-wide permissions.

## Documentation

- [Architecture](docs/architecture.md)
- [Benchmark methodology](docs/benchmark-methodology.md)
- [Database configuration](docs/database-configuration.md)
- [Provisioning and native dependencies](docs/provisioning.md)
- [Workloads](docs/workloads.md)
- [Metrics](docs/metrics.md)
- [Reproducing results](docs/reproducing-results.md)
- [Limitations](docs/limitations.md)
- [OJP suitability](docs/ojp-suitability.md)
- [Contributing](CONTRIBUTING.md)

The repository is Apache-2.0 licensed. Bundled dependencies retain their own
licenses; see [provisioning](docs/provisioning.md).
