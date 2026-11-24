# Workloads

Workload names are accepted by `--workloads` (comma-separated); unavailable
names fail. The registry exposes:

| Group | Names |
|---|---|
| Lookups/ranges | `point`, `zipf`, `hot`, `range`, `indexed-range` |
| Relational/analytical | `joins`, `relational`, `cte`, `htap` |
| Mixed/transactions | `mixed`, `transactions`, `rollback`, `update`, `batch-update`, `write`, `contended-write`, `contended-transactions`, `long-transactions`, `deadlocks` |
| Ingestion/mutation | `insert`, `ingestion`, `upsert`, `delete` |
| Relational cache lifecycle | `cache`, `cache-populate`, `cache-incremental`, `cache-refresh`, `cache-replace`, `cache-expire`, `cache-invalidate`, `cache-bulk-invalidate` |

The dataset models customers, accounts, products, orders, order items and
transactions with primary/foreign keys and indexes. Inputs and payloads are
deterministic under `--seed`; NULL notes, DECIMAL values and timestamps
exercise SQL semantics rather than only a one-column key/value table.
Dataset rows count logical entities, not total rows across all tables.
The logical-byte accounting is defined by `DeterministicDataset.logicalBytes`;
it is a deterministic representation-size estimate, not engine allocation
or compressed file size.

Point lookups and mixed OLTP are the default comparison. The implemented
registry also exposes range scans, joins/aggregation, updates, account
transfers, ingestion and hybrid work where supported. Each workload owns
its SQL and post-run checks. A prepared data load is not itself evidence
that all workload statements use the same preparation policy.

`--range-rows` controls scan breadth.
The `indexed-range` predicate uses the customer primary key.
The nonindexed `range` derives a numeric ID with
`CAST(REPLACE(name,'customer-','') AS BIGINT)` over unindexed customer names
and checks the same lower/upper bounds. Its latency includes string
transformation/cast CPU cost; this is not a plan hint or proof that every
optimizer executes an identical physical scan.

Mixed/hybrid proportions are configured
with `--read-percent` and `--analytical-percent`, each independently in
0–100. HTAP first selects analytical work; remaining operations use the
mixed read/write proportion. The percentages do not need to sum to 100.
Within default mixed writes, 20% insert, 20% delete and 60% transfer;
the default 80% read mix applies before that write selection.
Transfers use explicit commit/rollback and conserve account totals.
Correctness baselines are captured before mutations, so an intended update
is not incorrectly compared against an untouched seeded dataset.
Monetary checks compare integer cents, avoiding accumulated SQLite
floating-point representation noise.

`contended-write`/`contended-transactions` repeatedly target accounts 1 and 2
instead of randomly spreading writes. `long-transactions` deliberately
holds a multi-statement transaction with a 10 ms pause. Its latency therefore
includes intentional application hold time; do not attribute all of it to
engine SQL execution.
`deadlocks` deliberately reverses accounts 1/2 lock order between operations
and holds the first write for 10 ms before the second. It rolls back the
full transaction after a detected conflict and checks conserved balances.
That hold time is application behavior, not engine-only service cost.
SQLite's single-writer and PGlite's serialized session do not create genuine
concurrent-writer deadlocks; do not equate their outcome with a multi-writer
engine's deadlock detection/recovery.
The harness rejects unavailable topologies rather than reporting artificial
zero-deadlock stability: at least two accounts, workers and independent
connections, a nonserialized policy and concurrent-writer capability are
required. Even a feasible run observing zero deadlocks does not prove
stability.

Cache-lifecycle scenarios are relational surrogates, not a cache product
hit-rate benchmark. `cache-refresh`/`cache-replace` delete and rebuild the
entire `financial_transactions` table in one atomic transaction with bounded
insertion batches. `cache-expire` deletes by a deterministic 2024 timestamp
cutoff. Cache readers point-lookup customers, so concurrent maintenance
observes cross-table locking/resource impact rather than same-key TTL races.
Publish this distinction with cache-oriented results.

`upsert` chooses existing-row updates or unique new-row inserts with equal
probability. It uses portable JDBC UPDATE followed by conditional INSERT
in one transaction, not a vendor-specific conflict clause or silent retry.
Insertion counters count only successful new rows. Mutation/ingestion
changes dataset size during a run; normalized logical bytes refer to the
initial deterministic dataset, not all subsequently inserted state.

`--batch-size` controls loading/ingestion batching;
`--transaction-size` controls logical transaction operations. Worker and
connection counts are separate, with connections not exceeding workers.
`--load-mode individual`, `prepared`, `batch` compare literal individual
inserts, prepared individual inserts and prepared JDBC batches respectively.
All use deterministic logical data and explicit load transaction boundaries.
Driver protocol behavior (notably PGlite's simple-query mode) remains a
configuration difference.
When workers outnumber connections, `auto` uses a bounded connection pool
with a full-operation lease; queue/pool wait belongs to latency. PGlite's
single-session policy remains serialized regardless of handle count.
Serialized policies can make many workers contend on one database path:
that must be interpreted as serialized access, not multicore SQL scaling.

This is not TPC-C/TPC-H certification, a faithful production trace, or a
distributed transaction recovery workload. NULL/DECIMAL/timestamp, foreign
key and rollback assertions establish tested behavior only; they do not
prove complete SQL standard compliance.
