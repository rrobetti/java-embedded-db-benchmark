# Reproducing results

1. Record the repository commit and ensure a clean working tree.
2. Install JDK 21 or later, record vendor/version, then run setup.
3. Run real connectivity for all four engines.
4. Record CPU/architecture, physical and available/container RAM, OS,
   power settings, other load, JVM options and native temp path.
5. Execute explicit options into a fresh output directory and retain every
   report, child log and histogram, including failures.

```bash
bash scripts/setup.sh
bash scripts/benchmark.sh --offline --profile quick \
  --databases h2,hsqldb,sqlite,pglite --workloads point,mixed \
  --rows 10000 --payload-size 128 --threads 1 --connections 1 \
  --batch-size 500 --transaction-size 10 --rate 0 \
  --warmup 1 --duration 2 --iterations 3 --max-operations 0 --seed 42 \
  --memory-budget 2048m --jvm-heap 768m --db-profile default \
  --connection-mode auto --output benchmark-results/reproduction
```

On Windows use `pwsh -File scripts/benchmark.ps1` followed by the same options.
For diagnostic isolation select one database/workload first, but do not
publish it as an all-engine result.
Keep PGlite `upstream` and default/framed-repair results separate. Record
the profile, actual URL, driver/runtime versions and transport-deviation
label. An upstream failure is evidence of that configuration's limitation,
not permission to substitute the repair without relabeling the experiment.

`--help` documents accepted values. `smoke` is a correctness/launch check;
`standard`, `full`, `stress` and `endurance` require an explicit assessment
of memory and elapsed time. Preserve resolved options, not just the profile
name, because overrides change the experiment.

GitHub CI performs cross-platform tests, connectivity and small measured
runs; it uploads reports even after failures. Manual workflow dispatch
accepts a larger profile with explicit resource settings. Shared CI machines
are noisy and are not a substitute for controlled performance experiments.
Workflow actions are pinned to verified latest stable commits (checkout
7.0.1, setup-java 6.3.0, upload-artifact 7.0.2), advisory-checked before use.
They use the Node 24 action runtime supplied by current hosted runners;
Node is not an application/setup prerequisite. Self-hosted runners would
need at least Actions Runner 2.327.1, and are not configured here.

Archive the resolved Maven dependency tree and source revision for long-lived
publications. The first online setup resolves dependencies; built jar runs
need no dependency network access. Reproducing compiler/native toolchain
versions is additionally required after a custom native rebuild.
