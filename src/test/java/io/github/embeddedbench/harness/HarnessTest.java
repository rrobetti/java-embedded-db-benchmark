package io.github.embeddedbench.harness;

import io.github.embeddedbench.db.DeterministicDataset;
import io.github.embeddedbench.workload.Workload;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class HarnessTest {
    private final List<Path> directories = new ArrayList<>();

    private Path directory() throws Exception {
        Path dir = Path.of("target", "harness-tests", UUID.randomUUID().toString()).toAbsolutePath();
        Files.createDirectories(dir);
        directories.add(dir);
        return dir;
    }

    @AfterEach void clean() throws Exception {
        for (Path dir : directories) {
            try (var paths = Files.walk(dir)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }

    @Test void jsonRoundTripEscapesAndNulls() {
        var value = new LinkedHashMap<String, Object>();
        value.put("\"\\\n\t\u0000😀", List.of(1L, true, "line\r\n<>&"));
        value.put("unavailable", null);
        assertEquals(value, Json.decode(Json.encode(value)));
        assertEquals("null", Json.encode(Double.NaN));
        assertEquals("null", Json.encode(Double.POSITIVE_INFINITY));
        for (String invalid : List.of("true false", "{\"a\":}", "[1,]", "\"\n\"", "+1", "01", "{"))
            assertThrows(IllegalArgumentException.class, () -> Json.decode(invalid), invalid);
    }

    @Test void commandAliasesProfilesAndThreadGrid() {
        var cli = new BenchmarkCli();
        new CommandLine(cli).parseArgs("--profile", "standard", "--databases", "h2,sqlite", "--workloads", "point",
                "--threads", "1,4,8", "--iterations", "3", "--payload-size", "64", "--rate", "10",
                "--read-percent", "90", "--analytical-percent", "90");
        cli.options.resolve();
        assertEquals(List.of(1, 4, 8), cli.options.threadCounts);
        assertEquals(3, cli.options.repetitions);
        assertEquals(30.0, cli.options.warmup);
        assertEquals(120.0, cli.options.duration);
        assertEquals(100_000L, cli.options.rows);
        assertEquals(0, cli.options.maxOperations);
        assertEquals(64, cli.options.payload);
        assertEquals(1024L * 1024 * 1024, BenchmarkOptions.memoryBytes("1g"));
        assertThrows(ArithmeticException.class, () -> BenchmarkOptions.memoryBytes("999999999999g"));
        cli.options.duration = Double.NaN;
        assertThrows(IllegalArgumentException.class, cli.options::resolve);
    }

    @Test void researchProfileDefaultsAndLoadCommitOption() {
        for (var entry : Map.of(
                "smoke", List.of(100L, 0.0, 1.0, 1),
                "quick", List.of(1_000L, 5.0, 10.0, 1),
                "standard", List.of(100_000L, 30.0, 120.0, 3),
                "full", List.of(100_000L, 30.0, 120.0, 5),
                "stress", List.of(100_000L, 30.0, 60.0, 1),
                "endurance", List.of(100_000L, 60.0, 3600.0, 1)).entrySet()) {
            var cli = new BenchmarkCli();
            new CommandLine(cli).parseArgs("--profile", entry.getKey(), "--load-commit-interval", "10000");
            cli.options.resolve();
            assertEquals(entry.getValue(), List.of(cli.options.rows, cli.options.warmup,
                    cli.options.duration, cli.options.repetitions));
            assertEquals(10_000L, cli.options.asMap().get("loadCommitInterval"));
        }
        var cli = new BenchmarkCli();
        new CommandLine(cli).parseArgs("--load-commit-interval", "-1");
        assertThrows(IllegalArgumentException.class, cli.options::resolve);
    }

    @Test void defaultBudgetProvidesFullProfileAdmissionHeadroomAndPreservesOverrides() {
        var cli = new BenchmarkCli();
        var command = new CommandLine(cli);
        command.parseArgs("--profile", "full", "--rows", "1000000");
        cli.options.resolve();
        long estimate = DeterministicDataset.logicalBytes(cli.options.rows, cli.options.payload) * 4
                + BenchmarkOptions.memoryBytes(cli.options.jvmHeap)
                + 128L * 1024 * 1024 + cli.options.threads * 2L * 1024 * 1024
                + 512L * 1024 * 1024;
        assertTrue(estimate < BenchmarkOptions.memoryBytes(cli.options.memoryBudget));

        command.parseArgs("--profile", "full", "--rows", "1000000", "--memory-budget", "2g");
        cli.options.resolve();
        assertEquals(2L * 1024 * 1024 * 1024, cli.options.asMap().get("memoryBudgetBytes"));
        assertTrue(estimate > BenchmarkOptions.memoryBytes(cli.options.memoryBudget));
    }

    @Test void independentHistogramCategoriesAndClassification() throws Exception {
        var measurements = new Measurements();
        measurements.success("read", 1, 1_000);
        measurements.success("write", 3, 90_000);
        measurements.failure(new SQLException("database table is locked", "HY000", 6));
        measurements.failure(new SQLException("serialization conflict", "40001"));
        measurements.reject();
        Map<String, Object> report = measurements.report(1, 1);
        assertEquals(5L, report.get("attempts"));
        assertEquals(2L, report.get("successes"));
        assertEquals(0, report.get("retryCount"));
        assertEquals(0.4, report.get("errorRate"));
        Map<?, ?> latency = (Map<?, ?>) report.get("latency");
        assertTrue(((Number) ((Map<?, ?>) latency.get("read")).get("p99Nanos")).longValue() < 2_000);
        assertNull(((Map<?, ?>) latency.get("analytical")).get("p99Nanos"));
        assertEquals("SQL_LOCKED", Measurements.classify(new SQLException("[SQLITE_LOCKED] database is locked", null, 6)));
        assertEquals("SQL_BUSY", Measurements.classify(new SQLException("[SQLITE_BUSY] database is locked", null, 5)));
        assertEquals("SERIALIZATION", Measurements.classify(new SQLException("transaction rolled back", "40001")));
        assertEquals("DEADLOCK", Measurements.classify(new SQLException("deadlock", "40P01")));
        assertEquals("CORRECTNESS", Measurements.classify(new SQLException("wrong bytes", "BENCH_CORRECTNESS")));
        Path directory = directory();
        measurements.save(directory, "latency", 2.5);
        try (var stream = Files.newInputStream(directory.resolve("latency-read.hgr"))) {
            var reader = new HistogramLogReader(stream);
            Histogram histogram = (Histogram) reader.nextIntervalHistogram();
            assertEquals(1, histogram.getTotalCount());
            assertEquals(2500, histogram.getEndTimeStamp() - histogram.getStartTimeStamp());
        }
        var anotherWorker = new Measurements();
        anotherWorker.success("read", 1, 2_000);
        anotherWorker.failure(new OutOfMemoryError("test classification"));
        measurements.merge(anotherWorker);
        Map<String, Object> merged = measurements.report(1, 1);
        assertEquals(7L, merged.get("attempts"));
        assertEquals(3L, merged.get("successes"));
        assertEquals(1L, ((Map<?, ?>) merged.get("errors")).get("OOM"));
        assertEquals(2L, ((Map<?, ?>) ((Map<?, ?>) merged.get("latency")).get("read")).get("count"));
        measurements.success("READ", 4, 3_000);
        measurements.success("custom-read", 9, 4_000);
        measurements.success("custom-read", 9, 5_000);
        Map<?, ?> categories = (Map<?, ?>) measurements.report(1, 1).get("latency");
        assertEquals(6L, ((Map<?, ?>) categories.get("read")).get("units"));
        assertEquals(18L, ((Map<?, ?>) categories.get("custom-read")).get("units"));
    }

    @Test void closedLoopLimitsAreGlobalAndWarmupSeparate() throws Exception {
        BenchmarkOptions options = options();
        options.threadSpec = "4"; options.maxOperations = 20; options.resolve();
        AtomicLong executed = new AtomicLong();
        Workload workload = workload(executed, 0);
        List<Connection> connections = Arrays.asList(new Connection[4]);
        var warmup = PhaseRunner.run(workload, connections, 4, false, options, 1, 1);
        var steady = PhaseRunner.run(workload, connections, 4, false, options, 1, 1);
        assertEquals(20L, warmup.measurements().report(1, 1).get("successes"));
        assertEquals(20L, steady.measurements().report(1, 1).get("successes"));
        assertEquals(40, executed.get());
    }

    @Test void openLoopQueueIsBoundedAndLatencyIncludesWaiting() throws Exception {
        BenchmarkOptions options = options();
        options.requestRate = 5_000; options.queueCapacity = 2; options.resolve();
        var result = PhaseRunner.run(workload(new AtomicLong(), 10), Arrays.asList(new Connection[1]),
                1, false, options, 0.08, 1);
        var report = result.measurements().report(result.elapsedSeconds(), result.offeredSeconds());
        assertTrue(((Number) report.get("rejected")).longValue() > 0);
        assertTrue(((Number) report.get("successes")).longValue() > 0);
        Map<?, ?> latency = (Map<?, ?>) report.get("latency");
        assertTrue(((Number) ((Map<?, ?>) latency.get("read")).get("p99Nanos")).longValue() > 15_000_000);
    }

    @Test void correctnessErrorsAreFatal() {
        BenchmarkOptions options = options(); options.resolve();
        Workload workload = new Workload() {
            @Override public Operation execute(Connection c, SplittableRandom r) throws SQLException {
                throw new SQLException("invariant failed", "BENCH_CORRECTNESS");
            }
            @Override public void validate(Connection c) {}
        };
        SQLException exception = assertThrows(SQLException.class, () -> PhaseRunner.run(workload,
                Arrays.asList(new Connection[1]), 1, false, options, 1, 1));
        assertEquals("BENCH_CORRECTNESS", exception.getSQLState());
    }

    @Test void boundedPoolLeasesWholeOperationsWithoutConcurrentConnectionUse() throws Exception {
        BenchmarkOptions options = options();
        options.threadSpec = "4"; options.connections = 2; options.maxOperations = 40; options.resolve();
        var busy = new java.util.concurrent.ConcurrentHashMap<Connection, Boolean>();
        var connectionList = new ArrayList<Connection>();
        for (int i = 0; i < 2; i++) {
            Connection connection = (Connection) java.lang.reflect.Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == args[0];
                        default -> null;
                    });
            connectionList.add(connection);
        }
        AtomicLong executed = new AtomicLong();
        Workload workload = new Workload() {
            @Override public Operation execute(Connection c, SplittableRandom r) throws SQLException {
                if (busy.putIfAbsent(c, true) != null) throw new SQLException("Concurrent use of leased connection", "BENCH_CORRECTNESS");
                try {
                    Thread.sleep(2);
                    executed.incrementAndGet();
                    return new Operation("transaction", 1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(e);
                } finally { busy.remove(c); }
            }
            @Override public void validate(Connection c) {}
        };
        var phase = PhaseRunner.run(workload, connectionList, 4, false, options, 1, 1);
        assertEquals(40L, phase.measurements().report(1, 1).get("successes"));
        assertEquals(40, executed.get());
        assertTrue(busy.isEmpty());
        options.connectionMode = "dedicated";
        assertThrows(IllegalArgumentException.class, options::resolve);
    }

    @Test void admissionFailurePersistsAndSuiteContinues() throws Exception {
        Path directory = directory();
        int code = new CommandLine(new BenchmarkCli()).execute("--profile", "smoke", "--databases", "h2,sqlite",
                "--workloads", "point", "--memory-budget", "64m", "--jvm-heap", "32m", "--output", directory.toString());
        assertEquals(1, code);
        Map<?, ?> report = (Map<?, ?>) Json.decode(Files.readString(directory.resolve("report.json")));
        List<?> results = (List<?>) report.get("results");
        assertEquals(2, results.size());
        for (Object item : results) {
            assertEquals("MEMORY_ADMISSION", ((Map<?, ?>) item).get("errorClass"));
            assertEquals(false, ((Map<?, ?>) item).get("valid"));
        }
        for (String extension : List.of("json", "csv", "md", "html"))
            assertTrue(Files.size(directory.resolve("report." + extension)) > 0);
    }

    @Test void noResultFailuresAreClassifiedFromLogsNotExitCodes() throws Exception {
        Path directory = directory();
        assertEquals("CHILD_EXIT", BenchmarkCli.childExitKind(directory));
        Files.writeString(directory.resolve("stderr.log"), "Terminating due to java.lang.OutOfMemoryError: Java heap space");
        assertEquals("OOM", BenchmarkCli.childExitKind(directory));
        Files.writeString(directory.resolve("stderr.log"), "Native memory allocation (malloc) failed to allocate 4096 bytes");
        assertEquals("NATIVE_ALLOCATION", BenchmarkCli.childExitKind(directory));
        Files.writeString(directory.resolve("stderr.log"), "# A fatal error has been detected by the Java Runtime Environment:\n# SIGSEGV");
        assertEquals("JVM_CRASH", BenchmarkCli.childExitKind(directory));
        Files.writeString(directory.resolve("stderr.log"), "Process exited with code 137");
        assertEquals("CHILD_EXIT", BenchmarkCli.childExitKind(directory));
        Files.writeString(directory.resolve("stdout.log"), "x".repeat(80_000) + "\njava.lang.OutOfMemoryError");
        assertEquals("OOM", BenchmarkCli.childExitKind(directory));
    }

    @Test void realH2ChildValidatesAndDeletesDataset() throws Exception {
        BenchmarkOptions options = options();
        options.output = directory(); options.rows = 10L; options.payload = 16; options.duration = 1.0;
        options.maxOperations = 20; options.engines = List.of("h2"); options.scenarios = List.of("point");
        options.loadMode = "prepared"; options.runId = "test"; options.resolve();
        Map<String, Object> result = ChildRunner.run(options);
        assertEquals("OK", result.get("status"));
        assertEquals(true, result.get("valid"));
        assertEquals(true, result.get("datasetValid"));
        assertEquals(true, result.get("workloadValid"));
        assertEquals(true, ((Map<?, ?>) result.get("capabilities")).get("xa"));
        assertNotNull(((Map<?, ?>) result.get("stages")).get("delete"));
        assertNotNull(result.get("resourceDeltas"));
        assertTrue(Files.isRegularFile(options.output.resolve("latency-read.hgr")));
    }

    @Test void reportingEscapesHtmlAndCsv() {
        var result = new LinkedHashMap<String, Object>();
        result.put("runId", "\"<script>alert('x')</script>,\n");
        result.put("engine", "h2"); result.put("scenario", "point"); result.put("status", "FAILED");
        result.put("valid", false); result.put("error", "a,\"b\"\r\n");
        String csv = ReportWriter.csv(List.of(result));
        assertTrue(csv.contains("\"a,\"\"b\"\"\r\n\""));
        String html = ReportWriter.html(Map.of("aggregates", List.of()), List.of(result));
        assertFalse(html.contains("<script>"));
        assertTrue(html.contains("&lt;script&gt;"));
        assertFalse(html.contains("<svg"));
        String[] lines = ReportWriter.csv(List.of(Map.of("runId", "run", "engine", "h2", "scenario", "point",
                "status", "FAILED", "valid", false))).split("\r\n");
        assertEquals(lines[0].split(",", -1).length, lines[1].split(",", -1).length);
    }

    @Test void mutatingWarmupIsResetBeforeMeasurement() throws Exception {
        BenchmarkOptions options = options();
        options.output = directory(); options.rows = 10L; options.payload = 16; options.duration = 1.0;
        options.warmup = 0.1; options.maxOperations = 10; options.readPercent = 0; options.analyticalPercent = 0;
        options.engines = List.of("h2"); options.scenarios = List.of("mixed"); options.runId = "reset-test"; options.resolve();
        Map<String, Object> result = ChildRunner.run(options);
        assertEquals("OK", result.get("status"));
        assertEquals(true, result.get("warmupValid"));
        assertTrue(((Number) result.get("warmupResetNanos")).longValue() > 0);
        assertNotNull(((Map<?, ?>) result.get("stages")).get("measuredLoad"));
        assertEquals(10L, ((Map<?, ?>) result.get("measurement")).get("successes"));
    }

    @Test void compatibleHistogramsAreMergedNotPercentilesAveraged() throws Exception {
        Path directory = directory();
        var results = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 2; i++) {
            Path run = directory.resolve("runs/r" + i);
            var measurements = new Measurements();
            for (int n = 0; n < (i == 0 ? 99 : 1); n++) measurements.success("read", 1, i == 0 ? 1_000 : 100_000);
            measurements.save(run, "latency", 1);
            var result = new LinkedHashMap<String, Object>();
            result.put("runId", "r" + i); result.put("engine", "h2"); result.put("scenario", "point");
            result.put("valid", true); result.put("status", "OK"); result.put("configuration", Map.of("rows", 100L));
            result.put("effectiveConnectionMode", "dedicated"); result.put("artifactDirectory", "runs/r" + i);
            result.put("measurement", measurements.report(1, 1)); results.add(result);
        }
        var aggregated = HistogramAggregation.merge(directory, results);
        assertEquals(1, aggregated.size());
        Map<?, ?> read = (Map<?, ?>) ((Map<?, ?>) aggregated.getFirst().get("latency")).get("read");
        assertEquals(100L, read.get("count"));
        assertTrue(((Number) read.get("p99Nanos")).longValue() < 2_000);
        assertTrue(((Number) read.get("maxNanos")).longValue() >= 100_000);
        assertEquals(50.0, aggregated.getFirst().get("throughputOpsPerSecond"));
        results.getLast().put("configuration", Map.of("rows", 200L));
        assertEquals(2, HistogramAggregation.merge(directory, results).size());
    }

    private static BenchmarkOptions options() {
        var options = new BenchmarkOptions();
        options.profile = "smoke"; options.childDeadline = 5L;
        return options;
    }

    private static Workload workload(AtomicLong count, long delayMillis) {
        return new Workload() {
            @Override public Operation execute(Connection c, SplittableRandom random) throws SQLException {
                try { if (delayMillis != 0) Thread.sleep(delayMillis); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SQLException(e); }
                count.incrementAndGet();
                return new Operation("read", 1);
            }
            @Override public void validate(Connection c) {}
        };
    }
}
