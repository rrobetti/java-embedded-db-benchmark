# Contributing

Use JDK 21 and the checked-in Maven wrapper, not an arbitrary system Maven.
No external database, Docker or Node.js is needed.

```bash
bash scripts/setup.sh
bash ./mvnw -B test
bash -n scripts/setup.sh scripts/benchmark.sh
bash scripts/benchmark.sh --offline --profile smoke \
  --databases h2,hsqldb,sqlite,pglite --workloads point \
  --rows 100 --duration 0.2 --warmup 0 --output benchmark-results/contribution
```

On Windows use `.\mvnw.cmd -B test` and the PowerShell launchers. Parse
PowerShell scripts with `System.Management.Automation.Language.Parser`;
run them on Windows as well as checking syntax.

An adapter change needs actual connection/schema/load tests, SQL semantics
and rollback checks, not just driver discovery. A workload needs deterministic
inputs, explicit transaction boundaries and post-run invariants. Capture
monetary/count baselines before modifying data. Never silently map unsupported
features to success.

Keep JDBC connections worker-confined unless the selected policy explicitly
serializes access. Preserve independent child JVMs, bounded queues, bounded
runtime and invalid-result reporting. Timing changes must explain what is
inside the timer and how open-loop waiting is represented.

Dependency updates must pin versions, check GitHub advisories, document native
platform/license changes and run connectivity on Linux, Windows and macOS.
Pin workflow actions by verified immutable commit SHA. Do not add credential
material or require privileged package installation.

Keep documentation/CLI names in sync. Generated benchmark artifacts belong
outside source control. Performance claims must include raw reports,
configuration, hardware/runtime information and validity status. CI smoke
timings are not evidence of production performance.
