# Benchmark methodology

## Experimental unit

A result is an engine/workload/configuration/repetition in a fresh JVM.
Use the same seeded dataset, logical SQL, payload, worker count, connection
count and transaction size for comparison. Report engine-specific settings,
serialization policies and capabilities rather than presenting them as
equivalent internals.

Loading and warmup are distinct from measurement. Startup and load costs
matter for ephemeral applications, but must not be confused with warmed
operation throughput. Post-run correctness is mandatory; an incorrect
operation is not a performance win.

Default memory-backed URLs compare volatile operation paths. They are
**not durable file-backed or crash-recovery benchmarks**. Changing a PRAGMA
or transaction-control mode is a new configuration, not a free optimization.

## Load generation

Closed-loop workers submit the next operation after completion; slow
operations reduce offered load. Positive `--rate` uses a bounded queue for
open-loop arrivals. Report offered/accepted/completed work, rejection and
queue delay where emitted. Service latency alone cannot represent user
latency at saturation. A transaction workload reports transaction operations,
not an invented count of individual SQL statements.

`--iterations` (alias `--repetitions`) starts fresh experimental units.
`--max-operations` bounds attempts per phase in addition to elapsed time;
zero means duration-only. Do not mix these concepts.
Batch load size is separate from workload transaction size.

## Publication

Run multiple repetitions on an otherwise idle machine. Preserve per-run
histograms and all failures; examine variability instead of selecting only
the fastest run. Include Java/vendor, OS/architecture, CPU and RAM,
dependency versions, seed, resolved options, connection policy and limit
mechanism. CI exists for compatibility/correctness, not stable rankings.

An engine may favor point OLTP, aggregation or constrained concurrency.
Make conclusions specific to the measured workload and configuration.
No overall winner, unsupported XA result or simulated native measurement is
supplied by this project.
