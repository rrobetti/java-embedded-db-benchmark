# OJP suitability: evidence, not a winner

This harness measures embedded JDBC. It does **not** run OpenJProxy (OJP),
measure proxy latency, certify OJP driver compatibility or test distributed
recovery. A recommendation must combine valid workload results with an
actual OJP integration test in the intended deployment topology.

The pinned PGlite4j 0.0.3 README explicitly warns against production use
and describes it as a testing/demo driver. Treat that as an upstream
support boundary, not a performance finding. Its optional zip persistence
is periodic snapshotting, not a durable PostgreSQL storage path; this
benchmark uses its memory URL.
The working benchmark uses a repository-local framing repair over that real
WASM runtime, not upstream's unmodified JDBC transport. That additional
integration/support surface must be considered for OJP; successful framed
results do not certify the original upstream driver.

| Candidate | Evidence to evaluate | Important boundary |
|---|---|---|
| H2 | Point/mixed latency, concurrent transaction correctness, Java deployment, XA tests | Embedded memory results do not establish server/proxy recovery |
| HSQLDB | Primary MVCC versus optional `locks`, contention and XA tests | Transaction-control mode changes the experiment |
| SQLite | JNI provisioning, single-writer contention, SQL affinity correctness | Local transactions are not XA; dedicated connections do not create parallel writers |
| PGlite4j | PostgreSQL SQL compatibility exercised here, WASM heap/cost, effective serialization | An internal loopback bridge is not an independent production PostgreSQL server or proven OJP/XA integration |

For OJP, first determine whether the application needs persistent storage,
poolable independent sessions, cancellation/timeouts, two-phase commit,
crash recovery or PostgreSQL-specific SQL. Exclude candidates only when a
required feature is genuinely unsupported or fails an integration test,
not because a different architecture is unfamiliar.

Next compare valid results under the relevant workload, transaction size,
concurrency, access policy and memory ceiling. Include startup/loading for
ephemeral deployments and tail latency/rejections for overload-sensitive
ones. Measure OJP overhead separately against direct JDBC on the same host.

No benchmark numbers or preferred engine are invented here. An engine
capability flag is a hypothesis boundary; test assertions and raw valid
reports are evidence. Successful local rollback does not establish XA;
successful XA enlistment does not establish power-loss recovery.
