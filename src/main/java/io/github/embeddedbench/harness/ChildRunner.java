package io.github.embeddedbench.harness;

import io.github.embeddedbench.db.DatabaseAdapter;
import io.github.embeddedbench.db.DatabaseAdapters;
import io.github.embeddedbench.db.DeterministicDataset;
import io.github.embeddedbench.db.DatasetReclamation;
import io.github.embeddedbench.db.XaProbe;
import io.github.embeddedbench.workload.Workload;
import io.github.embeddedbench.workload.Workloads;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

final class ChildRunner {
    private ChildRunner() {}

    static Map<String, Object> run(BenchmarkOptions options) throws Exception {
        Path directory = options.output;
        Path databaseDirectory = directory.resolve("database");
        Files.createDirectories(databaseDirectory);
        System.setProperty("embeddedbench.db.dir", databaseDirectory.toString());
        var result = new LinkedHashMap<String, Object>();
        result.put("runId", options.runId);
        result.put("engine", options.engines.getFirst());
        result.put("scenario", options.scenarios.getFirst());
        result.put("configuration", options.asMap());
        result.put("environment", Resources.environment());
        result.put("status", "FAILED");
        result.put("valid", false);
        result.put("logicalDatasetBytes", DeterministicDataset.logicalBytes(options.rows, options.payload));
        var stages = new LinkedHashMap<String, Object>();
        for (String name : new String[]{"baseline", "init", "schema", "load", "steady", "delete", "shutdown"})
            stages.put(name, null);
        result.put("stages", stages);
        var timings = new LinkedHashMap<String, Object>();
        result.put("initializationNanos", timings);
        result.put("initializationTimingSemantics", Map.of(
                "driverInitialization", "Actual Class.forName driver initialization; excludes database creation",
                "adapterCreation", "DatabaseAdapters factory object construction only",
                "driverAndAdapter", "Combined driver initialization and adapter construction interval",
                "databaseStartAndAnchorConnection", "Database boot, anchor JDBC connection, and engine XA probes; inseparable composite",
                "databaseStart", "Alias of databaseStartAndAnchorConnection",
                "firstWorkerConnection", "First harness-owned worker connection; anchor/version connections opened earlier",
                "firstConnection", "Alias of firstWorkerConnection; not the first chronological JDBC connection"));
        stages.put("baseline", Resources.snapshot(databaseDirectory));
        ResourceSampler sampler = new ResourceSampler(directory.resolve("resources.jsonl"), options.sampleInterval);
        result.put("resourceTimeSeries", "resources.jsonl");
        DatabaseAdapter adapter = null;
        Workload workload = null;
        var connections = new ArrayList<Connection>();
        Throwable failure = null;
        try {
            long time = System.nanoTime();
            long driverStart = time;
            DatabaseAdapters.initializeDriver(options.engines.getFirst());
            timings.put("driverInitialization", System.nanoTime() - time);
            time = System.nanoTime();
            adapter = DatabaseAdapters.create(options.engines.getFirst(), options.dbProfile);
            timings.put("adapterCreation", System.nanoTime() - time);
            timings.put("driverAndAdapter", System.nanoTime() - driverStart);
            time = System.nanoTime();
            adapter.initialize();
            long boot = System.nanoTime() - time;
            timings.put("databaseStartAndAnchorConnection", boot);
            timings.put("databaseStart", boot);
            stages.put("init", Resources.snapshot(databaseDirectory));
            var xa = XaProbe.result(adapter);
            var xaResult = new LinkedHashMap<String, Object>();
            xaResult.put("supported", xa.supported()); xaResult.put("prepare", xa.prepare());
            xaResult.put("recover", xa.recover()); xaResult.put("commit", xa.commit());
            xaResult.put("rollback", xa.rollback()); xaResult.put("details", xa.details());
            result.put("xaProbe", xaResult);
            var capabilities = adapter.capabilities();
            var compatibility = new LinkedHashMap<String, Object>();
            compatibility.put("independentTransactions", capabilities.independentTransactions());
            compatibility.put("concurrentReaders", capabilities.concurrentReaders());
            compatibility.put("concurrentWriters", capabilities.concurrentWriters());
            compatibility.put("xa", capabilities.xa());
            compatibility.put("cte", capabilities.cte());
            compatibility.put("executionMode", capabilities.executionMode());
            compatibility.put("limitations", capabilities.limitations());
            result.put("capabilities", compatibility);
            result.put("transportDeviation", capabilities.limitations().contains("ADAPTER TRANSPORT DEVIATION"));
            if (options.engines.getFirst().equals("pglite"))
                result.put("requestedJdbcUrl", "jdbc:pglite:memory:benchmark");
            boolean serialized = options.connectionMode.equals("serialized") || !capabilities.independentTransactions();
            if (options.connectionMode.equals("dedicated") && !capabilities.independentTransactions())
                throw new UnsupportedOperationException("Engine does not expose independent JDBC transactions; use auto/serialized");
            boolean pooled = !serialized && (options.connectionMode.equals("pooled") || options.connections < options.threads);
            result.put("effectiveConnectionMode", serialized ? "serialized-single-session" : pooled ? "bounded-pool" : "dedicated");
            result.put("effectiveConnections", serialized ? 1 : options.connections);
            int workers = options.threads;
            result.put("effectiveWorkers", workers);
            if (options.scenarios.getFirst().equals("deadlocks")) {
                boolean feasible = options.rows >= 2 && workers >= 2 && !serialized && options.connections >= 2
                        && capabilities.independentTransactions() && capabilities.concurrentWriters();
                result.put("scenarioFeasible", feasible);
                result.put("scenarioExecutionNote", feasible
                        ? "Induced opposite account lock order with intentional hold; zero observed deadlocks is not proof of stability."
                        : "Genuine concurrent-writer deadlocks require rows>=2, workers>=2, independent connections>=2, and concurrent writer capability.");
                if (!feasible) throw new java.sql.SQLFeatureNotSupportedException("Deadlock scenario unavailable for this execution topology");
            }
            result.put("latencySemantics", options.requestRate > 0 || options.fixedReadRate > 0
                    ? "Scheduled arrival to completion, including scheduler lateness, queueing and session contention; rejected requests excluded."
                    : "Worker submission to completion, including serialized session contention; successes only.");
            time = System.nanoTime();
            result.put("engineVersion", adapter.version());
            timings.put("versionProbe", System.nanoTime() - time);
            time = System.nanoTime();
            Connection setup = adapter.openConnection();
            connections.add(setup);
            long firstWorkerConnection = System.nanoTime() - time;
            timings.put("firstWorkerConnection", firstWorkerConnection);
            timings.put("firstConnection", firstWorkerConnection);
            var metadata = setup.getMetaData();
            result.put("jdbcUrl", metadata.getURL());
            result.put("driverName", metadata.getDriverName());
            result.put("driverVersion", metadata.getDriverVersion());
            time = System.nanoTime();
            adapter.createSchema(setup);
            timings.put("schema", System.nanoTime() - time);
            stages.put("schema", Resources.snapshot(databaseDirectory));
            sampler.phase("loading");
            time = System.nanoTime();
            DeterministicDataset.load(setup, options.rows, options.payload, options.seed, options.batch, options.loadMode,
                    options.loadCommitInterval);
            long loadNanos = System.nanoTime() - time;
            result.put("loadNanos", loadNanos);
            result.put("loadLogicalBytesPerSecond", loadNanos > 0
                    ? DeterministicDataset.logicalBytes(options.rows, options.payload) * 1_000_000_000.0 / loadNanos : null);
            sampler.phase("validation");
            time = System.nanoTime();
            DeterministicDataset.validate(setup, options.rows, options.payload, options.seed);
            result.put("datasetValidationNanos", System.nanoTime() - time);
            result.put("datasetValid", true);
            stages.put("load", Resources.snapshot(databaseDirectory));
            if (options.scenarios.getFirst().equals("memory-reclamation")) {
                runReclamation(setup, options, databaseDirectory, sampler, result);
                result.put("valid", true);
                result.put("status", "OK");
                return result;
            }
            if (options.scenarios.getFirst().equals("cache-refresh-concurrent")) {
                result.put("effectiveConnections", serialized ? 1 : options.threads + 1);
                result.put("effectiveConnectionMode", serialized ? "serialized-single-session" : "dedicated-readers-and-writer");
                Map<String, Object> refresh = CacheRefreshRunner.run(adapter, setup, connections, serialized,
                        options, databaseDirectory, sampler);
                result.put("cacheRefresh", refresh);
                DeterministicDataset.validate(setup, options.rows, options.payload, options.seed);
                result.put("valid", true);
                result.put("status", ((Number) refresh.get("readFailures")).longValue() == 0
                        ? "OK" : "COMPLETED_WITH_ERRORS");
                return result;
            }
            workload = Workloads.create(options.scenarios.getFirst(), options.rows, options.rangeRows,
                    options.readPercent, options.analyticalPercent, options.transactionSize, options.seed);
            workload.prepare(setup);
            int actualConnections = serialized ? 1 : options.connections;
            for (int i = 1; i < actualConnections; i++) connections.add(adapter.openConnection());
            sampler.phase("warmup");
            executePhase(workload, connections, workers, serialized, options, options.warmup,
                    options.seed ^ 0x123456789abcdefL, "warmup", "warmup-latency", result, sampler);
            if (options.warmup > 0) {
                sampler.phase("validation");
                workload.validate(setup);
                result.put("warmupValid", true);
                if (!java.util.Set.of("point", "zipf", "hot", "range", "indexed-range", "joins", "relational", "cte", "aggregate", "rollback")
                        .contains(options.scenarios.getFirst())) {
                    time = System.nanoTime();
                    workload.close();
                    workload = null;
                    deleteDataset(setup);
                    sampler.phase("loading");
                    DeterministicDataset.load(setup, options.rows, options.payload, options.seed, options.batch, options.loadMode,
                            options.loadCommitInterval);
                    DeterministicDataset.validate(setup, options.rows, options.payload, options.seed);
                    workload = Workloads.create(options.scenarios.getFirst(), options.rows, options.rangeRows,
                            options.readPercent, options.analyticalPercent, options.transactionSize, options.seed);
                    workload.prepare(setup);
                    result.put("warmupResetNanos", System.nanoTime() - time);
                    stages.put("measuredLoad", Resources.snapshot(databaseDirectory));
                }
            }
            Map<String, Object> measuredBaseline = Resources.snapshot(databaseDirectory);
            stages.put("measurementBaseline", measuredBaseline);
            sampler.phase("measurement");
            var steady = executePhase(workload, connections, workers, serialized, options, options.duration, options.seed,
                    "measurement", "latency", result, sampler);
            Map<String, Object> steadyResources = Resources.snapshot(databaseDirectory);
            stages.put("steady", steadyResources);
            result.put("resourceDeltas", deltas(measuredBaseline, steadyResources, steady.elapsedSeconds()));
            result.put("measurement", steady.measurements().report(steady.elapsedSeconds(), steady.offeredSeconds()));
            steady.measurements().save(directory, "latency", steady.elapsedSeconds());
            var normalized = new LinkedHashMap<String, Object>();
            double throughput = ((Number) ((Map<?, ?>) result.get("measurement")).get("throughputOpsPerSecond")).doubleValue();
            long logicalBytes = DeterministicDataset.logicalBytes(options.rows, options.payload);
            normalized.put("throughputPerLogicalProcessor", throughput / Runtime.getRuntime().availableProcessors());
            normalized.put("throughputPerLogicalGiB", logicalBytes > 0 ? throughput * (1024.0 * 1024 * 1024) / logicalBytes : null);
            normalized.put("peakRssPerLogicalDatasetByte", steadyResources.get("peakRssBytes") instanceof Number rss
                    && logicalBytes > 0 ? rss.doubleValue() / logicalBytes : null);
            result.put("normalized", normalized);
            sampler.phase("validation");
            time = System.nanoTime();
            workload.validate(setup);
            result.put("workloadValidationNanos", System.nanoTime() - time);
            result.put("workloadValid", true);
            long successes = ((Number) ((Map<?, ?>) result.get("measurement")).get("successes")).longValue();
            if (successes == 0) throw new IllegalStateException("No successful measured operations; run cannot be compared");
            result.put("valid", true);
            long errors = ((Number) ((Map<?, ?>) result.get("measurement")).get("failures")).longValue();
            long warmupErrors = ((Number) ((Map<?, ?>) result.get("warmup")).get("failures")).longValue();
            result.put("status", errors + warmupErrors == 0 ? "OK" : "COMPLETED_WITH_ERRORS");
            time = System.nanoTime();
            sampler.phase("deletion");
            deleteDataset(setup);
            result.put("deleteNanos", System.nanoTime() - time);
            stages.put("delete", Resources.snapshot(databaseDirectory));
        } catch (Throwable error) {
            LoggerFactory.getLogger(ChildRunner.class).error("Child {} failed outside the measurement window", options.runId, error);
            failure = error;
            result.put("errorClass", Measurements.classify(error));
            result.put("error", Measurements.describe(error));
            result.put("status", Measurements.classify(error).equals("UNSUPPORTED") ? "UNSUPPORTED" : "FAILED");
        } finally {
            long time = System.nanoTime();
            try { sampler.phase("shutdown"); } catch (Throwable error) { failure = append(failure, error, result); }
            if (workload != null) {
                try { workload.close(); } catch (Throwable error) { failure = append(failure, error, result); }
            }
            for (Connection connection : connections) {
                try { connection.close(); } catch (Throwable error) { failure = append(failure, error, result); }
            }
            if (adapter != null) {
                try { adapter.close(); } catch (Throwable error) { failure = append(failure, error, result); }
            }
            timings.put("shutdown", System.nanoTime() - time);
            stages.put("shutdown", Resources.snapshot(databaseDirectory));
            try { sampler.close(); } catch (Throwable error) { failure = append(failure, error, result); }
            if (failure != null) result.put("valid", false);
            BenchmarkCli.writeJson(directory.resolve("result.json"), result);
        }
        return result;
    }

    private static Throwable append(Throwable failure, Throwable error, Map<String, Object> result) {
        result.put("status", "FAILED");
        result.put("shutdownError", Measurements.describe(error));
        if (failure != null) failure.addSuppressed(error);
        return failure == null ? error : failure;
    }

    private static void runReclamation(Connection connection, BenchmarkOptions options, Path databaseDirectory,
                                       ResourceSampler sampler, Map<String, Object> result) throws Exception {
        var cycles = new ArrayList<Map<String, Object>>();
        result.put("reclamation", cycles);
        result.put("reclamationSemantics", "Each stage deletes a suffix across all six tables in one transaction, "
                + "including a boundary order item required for foreign-key integrity, then restores and validates "
                + "the exact seeded dataset. No explicit GC, vacuum or compaction is performed.");
        for (int cycle = 1; cycle <= options.reclamationCycles; cycle++) {
            for (int percentage : new int[]{25, 50, 100}) {
                long removed = options.rows / 100 * percentage + (options.rows % 100 * percentage + 99) / 100;
                long first = options.rows - removed + 1;
                var stage = new LinkedHashMap<String, Object>();
                stage.put("cycle", cycle); stage.put("requestedPercent", percentage);
                stage.put("removedBaseRows", removed); stage.put("firstRemovedBaseRow", first);
                stage.put("status", "IN_PROGRESS");
                cycles.add(stage);
                sampler.phase("reclamation-deletion");
                long start = System.nanoTime();
                DatasetReclamation.deleteSuffix(connection, options.rows, first);
                stage.put("deleteNanos", System.nanoTime() - start);
                stage.put("afterDelete", Resources.snapshot(databaseDirectory));
                sampler.phase("reclamation-idle");
                Thread.sleep(java.time.Duration.ofNanos((long) (options.idleSeconds * 1_000_000_000L)));
                stage.put("afterIdle", Resources.snapshot(databaseDirectory));
                sampler.phase("reclamation-reinsert");
                start = System.nanoTime();
                DeterministicDataset.loadSuffix(connection, options.rows, first, options.payload, options.seed,
                        options.batch, options.loadMode, options.loadCommitInterval);
                stage.put("reinsertNanos", System.nanoTime() - start);
                stage.put("afterReinsert", Resources.snapshot(databaseDirectory));
                sampler.phase("validation");
                start = System.nanoTime();
                DeterministicDataset.validate(connection, options.rows, options.payload, options.seed);
                stage.put("validationNanos", System.nanoTime() - start);
                stage.put("status", "OK");
            }
        }
    }

    private static PhaseRunner.Result executePhase(Workload workload, java.util.List<Connection> connections, int workers,
            boolean serialized, BenchmarkOptions options, double seconds, long seed, String key, String prefix,
            Map<String, Object> result, ResourceSampler sampler) throws Exception {
        try {
            var phase = PhaseRunner.run(workload, connections, workers, serialized, options, seconds, seed, sampler);
            if (!phase.arrivalCounts().isEmpty()) result.put(key + "ArrivalCounts", phase.arrivalCounts());
            if (key.equals("warmup")) {
                result.put(key, phase.measurements().report(phase.elapsedSeconds(), phase.offeredSeconds()));
                phase.measurements().save(options.output, prefix, phase.elapsedSeconds());
            }
            return phase;
        } catch (PhaseRunner.Failure error) {
            var phase = error.partial();
            if (!phase.arrivalCounts().isEmpty()) result.put(key + "ArrivalCounts", phase.arrivalCounts());
            result.put(key, phase.measurements().report(phase.elapsedSeconds(), phase.offeredSeconds()));
            phase.measurements().save(options.output, prefix, phase.elapsedSeconds());
            Throwable cause = error.getCause();
            if (cause instanceof Exception exception) throw exception;
            if (cause instanceof Error fatal) throw fatal;
            throw error;
        }
    }

    private static Map<String, Object> deltas(Map<String, Object> baseline, Map<String, Object> steady, double seconds) {
        var result = new LinkedHashMap<String, Object>();
        for (String key : new String[]{"gcCollections", "gcMillis", "processCpuNanos", "processReadBytes", "processWriteBytes"}) {
            result.put(key, baseline.get(key) instanceof Number b && steady.get(key) instanceof Number s
                    ? s.longValue() - b.longValue() : null);
        }
        Object cpu = result.get("processCpuNanos");
        result.put("cpuCoreEquivalent", cpu instanceof Number n && seconds > 0 ? n.doubleValue() / (seconds * 1e9) : null);
        result.put("cpuUtilizationPercentOfLogicalProcessors", cpu instanceof Number n && seconds > 0
                ? n.doubleValue() * 100 / (seconds * 1e9 * Runtime.getRuntime().availableProcessors()) : null);
        return result;
    }

    private static void deleteDataset(Connection connection) throws java.sql.SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (var statement = connection.createStatement()) {
            for (String table : new String[]{"order_items", "financial_transactions", "orders", "accounts", "products", "customers"})
                statement.executeUpdate("DELETE FROM " + table);
            connection.commit();
        } catch (java.sql.SQLException error) {
            connection.rollback();
            throw error;
        } finally { connection.setAutoCommit(autoCommit); }
    }
}
