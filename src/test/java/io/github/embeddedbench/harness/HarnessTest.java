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

    @Test void openLoopUsesTheEntireOfferedWindowWithoutRoundingExtraArrivals() throws Exception {
        BenchmarkOptions options = options();
        options.requestRate = 30; options.queueCapacity = 8; options.resolve();
        var result = PhaseRunner.run(workload(new AtomicLong(), 0), Arrays.asList(new Connection[1]),
                1, false, options, 0.3, 1);
        var report = result.measurements().report(result.elapsedSeconds(), result.offeredSeconds());
        assertEquals(9L, report.get("attempts"));
        assertEquals(0.3, result.offeredSeconds(), 0.001);
        assertTrue(result.elapsedSeconds() >= 0.3);
    }

    @Test void fixedReadArrivalsRemainIndependentOfWriteArrivals() throws Exception {
        BenchmarkOptions options = options();
        options.scenarios = List.of("rw-independent"); options.fixedReadRate = 20;
        options.readPercent = 80; options.queueCapacity = 32; options.resolve();
        var reads = new AtomicLong();
        var writes = new AtomicLong();
        Workload workload = typedWorkload(reads, writes);
        var result = PhaseRunner.run(workload, Arrays.asList(new Connection[1]), 1, false, options, 0.4, 1);
        assertEquals(8L, result.arrivalCounts().get("offeredReads"));
        assertEquals(2L, result.arrivalCounts().get("offeredWrites"));
        assertEquals(8L, result.arrivalCounts().get("acceptedReads"));
        assertEquals(2L, result.arrivalCounts().get("acceptedWrites"));
        assertEquals(8L, reads.get());
        assertEquals(2L, writes.get());
    }

    @Test void dedicatedReaderAndWriterWorkersExecuteTheirAssignedOperations() throws Exception {
        BenchmarkOptions options = options();
        options.scenarios = List.of("rw-hot"); options.threadSpec = "2"; options.readerWorkers = 1; options.resolve();
        var reads = new AtomicLong();
        var writes = new AtomicLong();
        var result = PhaseRunner.run(typedWorkload(reads, writes), Arrays.asList(new Connection[2]),
                2, false, options, 0.1, 1);
        assertTrue(reads.get() > 0);
        assertTrue(writes.get() > 0);
        assertEquals(reads.get() + writes.get(), result.measurements().report(1, 1).get("successes"));
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

    @Test void samplerWritesPeriodicSamplesDuringAPhase() throws Exception {
        Path file = directory().resolve("samples.jsonl");
        try (var sampler = new ResourceSampler(file, 0.1)) {
            sampler.phase("loading");
            Thread.sleep(350);
        }
        long loading = Files.readAllLines(file).stream()
                .map(Json::decode).map(value -> (Map<?, ?>) value)
                .filter(value -> "loading".equals(value.get("phase"))).count();
        assertTrue(loading >= 3, "phase boundaries and periodic samples must be recorded");
    }

    @Test void supervisorSamplerSeparatesSupervisorAndChild() throws Exception {
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-version").start();
        Path file = directory().resolve("supervisor.jsonl");
        try (var sampler = new SupervisorSampler(file, child, System.nanoTime())) {
            sampler.sample();
        } finally {
            child.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
            if (child.isAlive()) child.destroyForcibly();
        }
        Map<?, ?> sample = (Map<?, ?>) Json.decode(Files.readAllLines(file).getFirst());
        assertEquals(ProcessHandle.current().pid(), ((Map<?, ?>) sample.get("supervisor")).get("pid"));
        assertEquals(child.pid(), ((Map<?, ?>) sample.get("child")).get("pid"));
        assertTrue(sample.containsKey("liveTreeRssBytes"));
        assertTrue(sample.containsKey("childCpuCoreEquivalent"));
    }

    @Test void progressWatchdogDistinguishesActivityFromLostHeartbeat() throws Exception {
        var watchdog = new ProgressWatchdog(2);
        watchdog.observe(Map.of("sequence", 1, "phase", "loading", "recordsLoaded", 0,
                "completedOperations", 0), 0);
        watchdog.observe(Map.of("sequence", 2, "phase", "loading", "recordsLoaded", 5,
                "completedOperations", 0), 1_000_000_000L);
        watchdog.observe(Map.of("sequence", 3, "phase", "loading", "recordsLoaded", 5,
                "completedOperations", 0), 3_100_000_000L);
        assertEquals("INACTIVITY_TIMEOUT", watchdog.timeout(3_100_000_000L));
        watchdog.observe(Map.of("sequence", 4, "phase", "validation", "recordsLoaded", 5,
                "completedOperations", 0), 3_200_000_000L);
        watchdog.observe(Map.of("sequence", 5, "phase", "validation", "recordsLoaded", 5,
                "completedOperations", 0), 8_000_000_000L);
        assertNull(watchdog.timeout(8_000_000_000L));
        assertEquals("HEARTBEAT_TIMEOUT", watchdog.timeout(10_100_000_000L));
        var paced = new ProgressWatchdog(2, 20);
        paced.observe(Map.of("sequence", 1, "phase", "measurement", "completedOperations", 1), 0);
        paced.observe(Map.of("sequence", 2, "phase", "measurement", "completedOperations", 1), 5_000_000_000L);
        assertNull(paced.timeout(5_000_000_000L));
    }

    @Test void samplerReportsIntervalOperationsAndIndependentLatencyHistograms() throws Exception {
        Path file = directory().resolve("performance.jsonl");
        var measurements = new Measurements();
        try (var sampler = new ResourceSampler(file, 0.1)) {
            sampler.phase("measurement");
            sampler.startPerformance(List.of(measurements));
            measurements.success("read", 1, 1_000);
            sampler.phase("measurement");
            measurements.success("write", 1, 90_000);
            measurements.failure(new java.sql.SQLTimeoutException("timeout"));
            measurements.reject();
            sampler.stopPerformance();
        }
        var intervals = Files.readAllLines(file).stream().map(Json::decode).map(value -> (Map<?, ?>) value)
                .map(value -> (Map<?, ?>) value.get("performance")).filter(java.util.Objects::nonNull).toList();
        assertEquals(2, intervals.size());
        assertEquals(1L, intervals.get(0).get("successes"));
        assertEquals(1L, intervals.get(1).get("successes"));
        assertEquals(1L, intervals.get(1).get("failures"));
        assertEquals(1L, intervals.get(1).get("rejected"));
        assertEquals(1L, intervals.get(1).get("timeouts"));
        assertEquals(0L, ((Number) intervals.get(1).get("retries")).longValue());
        var first = (Map<?, ?>) intervals.get(0).get("categories");
        var second = (Map<?, ?>) intervals.get(1).get("categories");
        assertEquals(1L, ((Map<?, ?>) first.get("read")).get("count"));
        assertEquals(0L, ((Map<?, ?>) first.get("write")).get("count"));
        assertEquals(1L, ((Map<?, ?>) second.get("write")).get("count"));
        assertEquals(0L, ((Map<?, ?>) second.get("read")).get("count"));
        assertTrue(((Number) ((Map<?, ?>) second.get("write")).get("p99Nanos")).longValue() >= 90_000);
        assertEquals(1L, ((Map<?, ?>) intervals.get(1).get("errors")).get("TIMEOUT"));
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
        List<String> samples = Files.readAllLines(options.output.resolve("resources.jsonl"));
        var phases = new java.util.HashSet<>();
        for (String line : samples) {
            Map<?, ?> sample = (Map<?, ?>) Json.decode(line);
            phases.add(sample.get("phase"));
            assertTrue(sample.containsKey("rssBytes"));
            assertTrue(sample.containsKey("wasmMemoryBytes"));
            assertNull(sample.get("wasmMemoryBytes"));
            assertNull(sample.get("nativeMemoryBytes"));
            assertNull(sample.get("gcPauseNanos"));
            assertTrue(sample.containsKey("processCpuCoreEquivalent"));
        }
        assertTrue(phases.containsAll(List.of("initialization", "loading", "validation",
                "warmup", "measurement", "deletion", "shutdown")));
        Map<?, ?> heartbeat = (Map<?, ?>) Json.decode(Files.readString(options.output.resolve("heartbeat.json")));
        assertTrue(((Number) heartbeat.get("sequence")).longValue() > 0);
        assertEquals(70L, heartbeat.get("recordsLoaded"));
        assertTrue(((Number) heartbeat.get("completedOperations")).longValue() > 0);
        assertTrue(samples.stream().map(Json::decode).map(sample -> (Map<?, ?>) sample)
                .anyMatch(sample -> "validation".equals(sample.get("phase"))
                        && Long.valueOf(70).equals(sample.get("recordsLoaded"))));
        long sampledSuccesses = samples.stream().map(Json::decode).map(sample -> (Map<?, ?>) sample)
                .filter(sample -> "measurement".equals(sample.get("phase")))
                .map(sample -> (Map<?, ?>) sample.get("performance"))
                .filter(java.util.Objects::nonNull)
                .mapToLong(sample -> ((Number) sample.get("successes")).longValue()).sum();
        assertEquals(((Map<?, ?>) result.get("measurement")).get("successes"), sampledSuccesses);
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

    @Test void reclamationChildRecordsValidatedDeleteIdleAndReloadStages() throws Exception {
        BenchmarkOptions options = options();
        options.output = directory(); options.rows = 7L; options.payload = 16;
        options.engines = List.of("h2"); options.scenarios = List.of("memory-reclamation");
        options.reclamationCycles = 2; options.idleSeconds = 0; options.loadCommitInterval = 5;
        options.runId = "reclamation"; options.resolve();
        Map<String, Object> result = ChildRunner.run(options);
        assertEquals("OK", result.get("status"));
        assertEquals(true, result.get("valid"));
        List<?> stages = (List<?>) result.get("reclamation");
        assertEquals(6, stages.size());
        for (Object value : stages) {
            Map<?, ?> stage = (Map<?, ?>) value;
            assertEquals("OK", stage.get("status"));
            assertNotNull(stage.get("afterDelete"));
            assertNotNull(stage.get("afterIdle"));
            assertNotNull(stage.get("afterReinsert"));
        }
        assertTrue(HistogramAggregation.merge(options.output, List.of(result)).isEmpty());
    }

    @Test void cacheRefreshStrategiesValidateWithConcurrentReaders() throws Exception {
        for (String strategy : List.of("record-update", "record-invalidate", "bulk-invalidate", "incremental-refresh",
                "full-table-refresh", "replacement", "expiration")) {
            BenchmarkOptions options = options();
            options.output = directory(); options.rows = 20L; options.payload = 16;
            options.engines = List.of("h2"); options.scenarios = List.of("cache-refresh-concurrent");
            options.threadSpec = "2"; options.refreshStrategy = strategy; options.refreshPercent = 50;
            options.refreshBefore = 0.1; options.refreshAfter = 0.1; options.runId = strategy; options.resolve();
            Map<String, Object> result = ChildRunner.run(options);
            assertEquals("OK", result.get("status"), strategy + ": " + result.get("error"));
            assertEquals(true, result.get("valid"));
            Map<?, ?> refresh = (Map<?, ?>) result.get("cacheRefresh");
            assertEquals(true, refresh.get("cacheValid"));
            assertEquals(10L, refresh.get("affectedEntries"));
            Map<?, ?> reads = (Map<?, ?>) refresh.get("reads");
            assertTrue(((Number) ((Map<?, ?>) reads.get("before")).get("successes")).longValue() > 0);
            assertTrue(((Number) ((Map<?, ?>) reads.get("after")).get("successes")).longValue() > 0);
            assertNull(refresh.get("blockingDurationNanos"));
            assertTrue(HistogramAggregation.merge(options.output, List.of(result)).isEmpty());
        }
    }

    @Test void enduranceMaintainsBoundedCacheDuringMeasuredOperations() throws Exception {
        BenchmarkOptions options = options();
        options.output = directory(); options.rows = 20L; options.payload = 16;
        options.engines = List.of("h2"); options.scenarios = List.of("endurance-cache");
        options.threadSpec = "2"; options.readPercent = 95; options.warmup = 0.1;
        options.duration = 0.8; options.maintenanceInterval = 0.1; options.runId = "endurance";
        options.resolve();
        Map<String, Object> result = ChildRunner.run(options);
        assertEquals("OK", result.get("status"), String.valueOf(result.get("error")));
        assertEquals(true, result.get("valid"));
        Map<?, ?> endurance = (Map<?, ?>) result.get("endurance");
        assertEquals(20L, endurance.get("cacheEntries"));
        assertTrue(((Number) endurance.get("maintenanceCompleted")).longValue() > 0);
        Map<?, ?> measurement = (Map<?, ?>) result.get("measurement");
        Map<?, ?> latency = (Map<?, ?>) measurement.get("latency");
        assertTrue(((Number) ((Map<?, ?>) latency.get("read")).get("count")).longValue() > 0);
        assertTrue(((Number) ((Map<?, ?>) latency.get("maintenance")).get("count")).longValue() > 0);
        assertEquals(0L, ((Number) measurement.get("failures")).longValue());
        String samples = java.nio.file.Files.readString(options.output.resolve("resources.jsonl"));
        assertTrue(samples.contains("\"phase\":\"measurement-maintenance\""));
    }

    @Test void enduranceWithoutMaintenanceIsNotSuccessful() throws Exception {
        BenchmarkOptions options = options();
        options.output = directory(); options.rows = 10L; options.payload = 16;
        options.engines = List.of("h2"); options.scenarios = List.of("endurance-cache");
        options.duration = 0.1; options.maintenanceInterval = 10; options.runId = "no-maintenance";
        options.resolve();
        Map<String, Object> result = ChildRunner.run(options);
        assertEquals("FAILED", result.get("status"));
        assertEquals(false, result.get("valid"));
        assertTrue(String.valueOf(result.get("error")).contains("No endurance maintenance cycle completed"));
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

    private static Workload typedWorkload(AtomicLong reads, AtomicLong writes) {
        return new Workload() {
            @Override public Operation execute(Connection c, SplittableRandom r) {
                throw new AssertionError("Expected explicit operation type");
            }
            @Override public Operation execute(Connection c, SplittableRandom r, OperationType type) {
                (type == OperationType.READ ? reads : writes).incrementAndGet();
                return new Operation(type == OperationType.READ ? "read" : "write", 1);
            }
            @Override public void validate(Connection c) {}
        };
    }
}
