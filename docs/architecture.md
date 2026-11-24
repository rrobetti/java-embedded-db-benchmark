# Architecture

`BenchmarkCli` parses/resolves a suite, then supervises one child JVM for each
engine, workload and repetition. The shaded artifact is
`target/benchmark.jar`; the entry point is
`io.github.embeddedbench.harness.BenchmarkCli`. A separate
`io.github.embeddedbench.db.DatabaseSmoke` connects to all engines without
running a measured suite.

The DB package owns JDBC adapters, capabilities, schema and seeded relational
data. The workload package owns SQL execution and correctness invariants.
The harness owns scheduling, timing, histograms, process monitoring and
aggregate reports. It does not substitute mock adapters for native engines.

Each child initializes an anchor connection, creates/indexes the schema,
loads data, prepares workload baselines, warms up, measures and validates.
Workers use the configured connection policy. Engine shutdown belongs to
the child lifecycle. Process isolation prevents one engine's heap, native
state, static caches or fatal error from becoming another engine's state;
it does not clear the host page cache or eliminate thermal interference.
Engine order is seeded and randomized within a workload/repetition.

The supervisor records child termination and missing/invalid results,
continues remaining combinations and fails the suite if any result fails.
Reports retain failure reasons alongside successful runs. Sampling observes
resource use but does not enforce a kernel memory quota.

This is direct embedded JDBC, not an OJP deployment. Proxy/network overhead,
pool integration and distributed recovery must be assessed separately.
