# Database configuration

The supported IDs are `h2`, `hsqldb`, `sqlite`, `pglite`.
Use `--db-profile default` unless explicitly testing a documented alternative.
Database configuration is applied by adapters, not by an external server.

| Engine | Default JDBC URL | Important semantics |
|---|---|---|
| H2 | `jdbc:h2:mem:benchmark` | JVM-local in-memory database; anchor keeps lifecycle explicit |
| HSQLDB | `jdbc:hsqldb:mem:benchmark` | Default LOCKS transaction control; optional `mvcc` profile |
| SQLite | `jdbc:sqlite:file:benchmark?mode=memory&cache=shared` | Shared-cache memory; one writer; numeric affinity |
| PGlite4j | `jdbc:pglite:memory:benchmark` | PostgreSQL compiled to WASM, in-process Java runtime with an internal loopback JDBC bridge |

H2/HSQLDB use an empty embedded password, not a network account. PGlite4j
starts its own loopback wire bridge; it does not require an external
PostgreSQL server. Its adapter uses the bridge's `postgres`/`password`
defaults, not production credentials. Do not expose this internal listener
as a service. A fresh child process separates identically named databases.

SQLite sets foreign keys ON and busy timeout 5000 ms per connection.
`busy_timeout` does not make shared-cache `SQLITE_LOCKED` disappear or create
parallel writers. Errors/retries must remain visible in results. Shared-memory
SQLite cannot provide a normal file-backed WAL durability test. Its DECIMAL
affinity is not fixed-point storage; monetary invariants use a tolerance
where required.

HSQLDB `--db-profile mvcc` changes transaction control to MVCC. Select only
HSQLDB for that option; other adapters reject unsupported profiles.
H2/HSQLDB capabilities must not be treated as proof of cross-engine XA
interoperability or recovery.
Their adapter initialization probes real XA prepare/recover/commit/rollback
before advertising XA support. Tests also enlist H2 and HSQLDB resources
with Narayana for commit/rollback. SQLite/PGlite explicitly lack XA.
These checks are not a process-crash/distributed-recovery certification.

`--connection-mode auto`, `dedicated`, `serialized` select harness access
policies. SQLite's writer limits and PGlite's single-instance restrictions
still apply. Record the effective mode and contention instead of claiming
that equal thread counts imply equal database parallelism.

PGlite4j 0.0.3 multiplexes handles onto one PostgreSQL/WASM session.
The adapter leases the entire connection/transaction and the harness
serializes auto mode; `dedicated` is rejected for that engine.
`preferQueryMode=simple` avoids the pinned bridge's extended-query portal
recovery problem following SQL errors. This changes the protocol path;
PGlite JDBC prepared statements therefore must not be presented as equivalent
server-side preparation to other drivers. Adapter shutdown also evicts the
pinned runtime instance rather than merely closing JDBC sockets.

There is no universal `durable`, `wal` or `unsafe` profile silently applied
across all engines. File-backed recovery, persistent SQLite cache limits
and production server tuning are outside the default comparison.
