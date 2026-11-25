# Limitations and safety

- Default databases are memory-backed; no durability, fsync throughput,
  power-loss recovery or persistent WAL comparison is established.
- Direct JDBC measurements omit OJP, TCP, remote pools, authentication and
  production deployment topology.
- SQL portability tests cover selected NULL, DECIMAL, timestamp, constraint
  and transaction behavior, not full PostgreSQL/SQL-standard conformance.
- SQLite has one writer; its shared-cache memory mode and numeric affinity
  are meaningful differences, not defects to hide by silently changing SQL.
- PGlite4j is PostgreSQL-to-WASM, not a normal PostgreSQL server; threading,
  connection and XA claims must follow the adapter/runtime's actual limits.
- The working PGlite path uses a repository-local framed transport repair,
  not the unmodified upstream 0.0.3 JDBC driver or named URL. Upstream
  socket-framing and SQL-error/active-portal limitations cannot be silently
  reclassified as successful upstream-driver measurements.
  The explicit `upstream` profile retains the original driver/named URL;
  individual/prepared loading is tested, but unsafe batched pipelines and
  SQL-error recovery remain technical limitations with no silent fallback.
- H2/HSQLDB XA support does not make SQLite/PGlite XA capable. Local
  commit/rollback is not two-phase commit or crash recovery.
- Dataset loading, worker contention, GC and native allocations can dominate
  short runs. Fresh child JVMs do not flush CPU/OS caches.
- Histograms and resource sampling have overhead. Metrics unavailable on an
  OS are not equivalent to zero.

## Resource limits

`--memory-budget` is sampled watchdog enforcement, **not a kernel memory
limit**. Sampling can miss spikes. `--jvm-heap` is only Java heap. The child
deadline bounds elapsed execution but is not a host CPU quota. CPU count and
physical RAM detection do not override container restrictions.
RSS sampling uses Linux `/proc` with macOS `ps` and Windows PowerShell
working-set fallbacks. These may be denied or unavailable, and external
queries increase sampling overhead/interval. Reports disclose whether RSS
monitoring was available. Admission checks on all platforms are estimates,
not hard limits; macOS fallback does not provide peak RSS.

For a genuine hard limit, launch the whole script externally in a configured
cgroup/container/job object or an administrator-provisioned systemd scope.
Alternatively, the master research runner can request an unprivileged user
scope with `--systemd-memory-max 2g --systemd-cpu-quota 200` on Linux.
It requires a running user systemd instance and delegated cgroup v2 memory
and CPU controllers; it verifies the local cgroup limits before running and
fails without fallback if the scope or verification is unavailable. The
research manifest records configured and verified local limits and the
most restrictive readable ancestor limits, or null when the latter cannot
be determined. `--cpu-cores` is a separate affinity constraint, not a quota.
For example, on a Linux host whose user systemd instance delegates memory
and CPU controllers:

```bash
systemd-run --user --scope -p MemoryMax=2G -p CPUQuota=200% \
  bash scripts/benchmark.sh --offline --profile quick \
  --memory-budget 1536m --jvm-heap 768m --threads 1 --connections 1
```

This is an **optional external operation**, not performed by setup. Verify
controller delegation and effective limits independently; a rejected scope
must not be reported as OS enforcement. Windows requires an externally
configured Job Object or equivalent; a script heap cap is not one.
macOS launchers do not provide a portable hard RSS cap.

Start small, keep host headroom, do not run stress/endurance alongside other
users' workloads, and preserve limit-triggered runs as failures. None of
these scripts elevates privileges or installs OS packages.
