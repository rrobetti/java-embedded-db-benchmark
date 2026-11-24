package io.github.embeddedbench.harness;

import io.github.embeddedbench.db.DatabaseAdapter;
import io.github.embeddedbench.db.DatabaseAdapters;
import io.github.embeddedbench.db.DeterministicDataset;
import io.github.embeddedbench.db.XaProbe;
import io.github.embeddedbench.workload.Workload;
import io.github.embeddedbench.workload.Workloads;

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
        stages.put("baseline", Resources.snapshot(databaseDirectory));
        DatabaseAdapter adapter = null;
        Workload workload = null;
        var connections = new ArrayList<Connection>();
        Throwable failure = null;
        try {
            long time = System.nanoTime();
            adapter = DatabaseAdapters.create(options.engines.getFirst(), options.dbProfile);
            timings.put("driverAndAdapter", System.nanoTime() - time);
            time = System.nanoTime();
            adapter.initialize();
            timings.put("databaseStart", System.nanoTime() - time);
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
            boolean serialized = options.connectionMode.equals("serialized") || !capabilities.independentTransactions();
            if (options.connectionMode.equals("dedicated") && !capabilities.independentTransactions())
                throw new UnsupportedOperationException("Engine does not expose independent JDBC transactions; use auto/serialized");
            result.put("effectiveConnectionMode", serialized ? "serialized-single-session" : "dedicated");
            result.put("effectiveConnections", serialized ? 1 : options.connections);
            int workers = serialized ? options.threads : options.connections;
            result.put("effectiveWorkers", workers);
            result.put("latencySemantics", options.requestRate > 0
                    ? "Scheduled arrival to completion, including scheduler lateness, queueing and session contention; rejected requests excluded."
                    : "Worker submission to completion, including serialized session contention; successes only.");
            time = System.nanoTime();
            result.put("engineVersion", adapter.version());
            timings.put("versionProbe", System.nanoTime() - time);
            time = System.nanoTime();
            Connection setup = adapter.openConnection();
            connections.add(setup);
            timings.put("firstConnection", System.nanoTime() - time);
            time = System.nanoTime();
            adapter.createSchema(setup);
            timings.put("schema", System.nanoTime() - time);
            stages.put("schema", Resources.snapshot(databaseDirectory));
            time = System.nanoTime();
            DeterministicDataset.load(setup, options.rows, options.payload, options.seed, options.batch, options.loadMode);
            long loadNanos = System.nanoTime() - time;
            result.put("loadNanos", loadNanos);
            result.put("loadLogicalBytesPerSecond", loadNanos > 0
                    ? DeterministicDataset.logicalBytes(options.rows, options.payload) * 1_000_000_000.0 / loadNanos : null);
            time = System.nanoTime();
            DeterministicDataset.validate(setup, options.rows, options.payload, options.seed);
            result.put("datasetValidationNanos", System.nanoTime() - time);
            result.put("datasetValid", true);
            stages.put("load", Resources.snapshot(databaseDirectory));
            workload = Workloads.create(options.scenarios.getFirst(), options.rows, options.rangeRows,
                    options.readPercent, options.analyticalPercent, options.transactionSize, options.seed);
            workload.prepare(setup);
            int actualConnections = serialized ? 1 : options.connections;
            for (int i = 1; i < actualConnections; i++) connections.add(adapter.openConnection());
            var warmup = PhaseRunner.run(workload, connections, workers, serialized, options, options.warmup,
                    options.seed ^ 0x123456789abcdefL);
            result.put("warmup", warmup.measurements().report(warmup.elapsedSeconds(), warmup.offeredSeconds()));
            warmup.measurements().save(directory, "warmup-latency", warmup.elapsedSeconds());
            if (options.warmup > 0) {
                workload.validate(setup);
                result.put("warmupValid", true);
                if (!java.util.Set.of("point", "zipf", "hot", "range", "indexed-range", "joins", "relational", "cte", "rollback")
                        .contains(options.scenarios.getFirst())) {
                    time = System.nanoTime();
                    workload.close();
                    workload = null;
                    deleteDataset(setup);
                    DeterministicDataset.load(setup, options.rows, options.payload, options.seed, options.batch, options.loadMode);
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
            var steady = PhaseRunner.run(workload, connections, workers, serialized, options, options.duration, options.seed);
            result.put("measurement", steady.measurements().report(steady.elapsedSeconds(), steady.offeredSeconds()));
            steady.measurements().save(directory, "latency", steady.elapsedSeconds());
            Map<String, Object> steadyResources = Resources.snapshot(databaseDirectory);
            stages.put("steady", steadyResources);
            result.put("resourceDeltas", deltas(measuredBaseline, steadyResources, steady.elapsedSeconds()));
            var normalized = new LinkedHashMap<String, Object>();
            double throughput = ((Number) ((Map<?, ?>) result.get("measurement")).get("throughputOpsPerSecond")).doubleValue();
            long logicalBytes = DeterministicDataset.logicalBytes(options.rows, options.payload);
            normalized.put("throughputPerLogicalProcessor", throughput / Runtime.getRuntime().availableProcessors());
            normalized.put("throughputPerLogicalGiB", logicalBytes > 0 ? throughput * (1024.0 * 1024 * 1024) / logicalBytes : null);
            normalized.put("peakRssPerLogicalDatasetByte", steadyResources.get("peakRssBytes") instanceof Number rss
                    && logicalBytes > 0 ? rss.doubleValue() / logicalBytes : null);
            result.put("normalized", normalized);
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
            deleteDataset(setup);
            result.put("deleteNanos", System.nanoTime() - time);
            stages.put("delete", Resources.snapshot(databaseDirectory));
        } catch (Throwable error) {
            failure = error;
            result.put("errorClass", Measurements.classify(error));
            result.put("error", Measurements.describe(error));
            result.put("status", Measurements.classify(error).equals("UNSUPPORTED") ? "UNSUPPORTED" : "FAILED");
        } finally {
            long time = System.nanoTime();
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
