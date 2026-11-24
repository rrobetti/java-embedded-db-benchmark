# Database configuration

The supported IDs are `h2`, `hsqldb`, `sqlite`, `pglite`.
Use `--db-profile default` unless explicitly testing a documented alternative.
Database configuration is applied by adapters, not by an external server.

| Engine | Default JDBC URL | Important semantics |
|---|---|---|
| H2 | `jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1` | JVM-local in-memory database; explicit SHUTDOWN releases the database |
| HSQLDB | `jdbc:hsqldb:mem:benchmark` | Primary MVCC transaction control; optional `locks` profile for engine-default LOCKS |
| SQLite | `jdbc:sqlite:file:benchmark?mode=memory&cache=shared` | Shared-cache memory; one writer; numeric affinity |
| PGlite4j, framed repair | pgjdbc loopback URL, dynamically allocated port | Real PGlite4j 0.0.3 WASM, repository-local transport repair; not upstream JDBC |
| PGlite4j, explicit `upstream` | `jdbc:pglite:memory:benchmark` | Unmodified upstream driver transport; serialized adapter leases and pinned pgjdbc override |

## PGlite: explicit upstream deviation

The upstream named URL is `jdbc:pglite:memory:benchmark`.
The working adapter instead uses `PgLiteWireBridge` with PGlite4j's real
core/WASM and pgjdbc 42.7.15. Its actual URL starts
`jdbc:postgresql://127.0.0.1:` and ends `/postgres`, with the allocated port
reported per run. This is a transport repair, **not a measurement of the
unmodified PGlite4j JDBC driver**. Never combine the two paths under an
unqualified PGlite performance claim.

`--db-profile default` or `framed` selects the explicitly labeled repair.
`--databases pglite --db-profile upstream` selects the original named
URL and unmodified upstream driver transport. Non-default PGlite profiles
must be used with only that database; H2/HSQLDB/SQLite reject them.
The upstream path has tested individual/prepared loading, but batched
protocol pipelines are unsafe in the pinned transport and may fail.
Failed upstream configurations remain failures/technical reasons in reports;
there is no fallback to the repaired transport.

```bash
bash scripts/benchmark.sh --offline --profile smoke --databases pglite \
  --db-profile upstream --load-mode prepared --rows 16 --batch-size 1 \
  --workloads point --warmup 0 --duration 0.2 --iterations 1 \
  --memory-budget 2048m --jvm-heap 768m --output benchmark-results/upstream-pglite
```

Upstream 0.0.3 forwards arbitrary socket reads into the WASM protocol buffer;
split/coalesced messages can break batches and close the connection.
The local bridge forwards complete PostgreSQL frames. SQL-error cleanup
also has an active-portal recovery limitation in the pinned WASM runtime;
an instance may need restarting after such errors. A repaired framing path
does not establish that this upstream SQL-error limitation is fixed.

H2/HSQLDB use an empty embedded password, not a network account. PGlite4j
starts its own loopback wire bridge; it does not require an external
PostgreSQL server. Its adapter uses the bridge's `postgres`/`password`
defaults, not production credentials. Do not expose this internal listener
as a service. A fresh child process separates identically named databases.

SQLite sets foreign keys ON and busy timeout 5000 ms per connection.
It also explicitly sets `temp_store=MEMORY`, `locking_mode=NORMAL` and
`read_uncommitted=0`.
`busy_timeout` does not make shared-cache `SQLITE_LOCKED` disappear or create
parallel writers. Errors/retries must remain visible in results. Shared-memory
SQLite cannot provide a normal file-backed WAL durability test. Its DECIMAL
affinity is not fixed-point storage; monetary invariants normalize values
to integer cents rather than accumulating binary floating-point balances.

HSQLDB `default` and `mvcc` profiles explicitly set transaction control to
MVCC; `--db-profile locks` compares the engine-default LOCKS model.
Select only HSQLDB for these non-default options; other adapters reject
unsupported profiles. Its in-memory schema uses MEMORY tables.
Customer payloads use inline `VARBINARY(2147483647)`, not `BLOB`: HSQLDB's
default LOB allocation reserves a 32 KiB block per nonempty small BLOB.
At one million customers, 128-byte BLOB payloads alone would require about
30.5 GiB of block storage. Inline storage preserves the bytes and JDBC
`setBytes`/`getBytes` behavior without this allocation amplification.
This is an engine-specific storage choice, not a BLOB performance test;
do not combine results from the old BLOB schema with these results.
H2/HSQLDB capabilities must not be treated as proof of cross-engine XA
interoperability or recovery.
Their adapter initialization probes real XA prepare/recover/commit/rollback
before advertising XA support. Tests also enlist H2 and HSQLDB resources
with Narayana for commit/rollback. SQLite/PGlite explicitly lack XA.
These checks are not a process-crash/distributed-recovery certification.

`--connection-mode auto`, `dedicated`, `pooled`, `serialized` select harness access
policies. SQLite's writer limits and PGlite's single-instance restrictions
still apply. Record the effective mode and contention instead of claiming
that equal thread counts imply equal database parallelism.
With fewer connections than workers, `auto` selects a bounded pool.
The lease spans the full logical operation/transaction, and pool wait is
included in latency. Explicit `dedicated` requires equal worker/connection
counts; it does not silently discard extra workers.

Both PGlite paths have one PostgreSQL/WASM session.
The adapter leases the entire connection/transaction and the harness
serializes auto mode; `dedicated` is rejected for that engine.
`preferQueryMode=simple` changes the protocol path;
PGlite JDBC prepared statements therefore must not be presented as equivalent
server-side preparation to other drivers. Adapter shutdown also evicts the
pinned runtime instance rather than merely closing JDBC sockets.

There is no universal `durable`, `wal` or `unsafe` profile silently applied
across all engines. File-backed recovery, persistent SQLite cache limits
and production server tuning are outside the default comparison.
