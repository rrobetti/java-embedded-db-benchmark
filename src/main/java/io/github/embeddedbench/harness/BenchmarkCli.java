package io.github.embeddedbench.harness;

import io.github.embeddedbench.db.DeterministicDataset;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Command(name = "embedded-benchmark", mixinStandardHelpOptions = true, version = "1.0",
        description = "Isolated, seeded, sequential embedded JDBC benchmark. Each run uses a fresh JVM.")
public final class BenchmarkCli implements Callable<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkCli.class);
    @Mixin BenchmarkOptions options = new BenchmarkOptions();
    private String gitCommit;
    private String gitDirty;

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkCli()).execute(args));
    }

    @Override public Integer call() throws Exception {
        options.resolve();
        Files.createDirectories(options.output);
        if (options.child) {
            if (options.engines.size() != 1 || options.scenarios.size() != 1)
                throw new IllegalArgumentException("Child requires exactly one engine and scenario");
            return "OK".equals(ChildRunner.run(options).get("status")) ? 0 : 1;
        }
        var results = new ArrayList<Map<String, Object>>();
        var environment = Resources.environment();
        gitCommit = (String) environment.get("gitCommit");
        gitDirty = (String) environment.get("gitDirty");
        var random = new Random(options.seed);
        String suite = Instant.now().toString().replace(':', '-') + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path suiteDirectory = options.output.resolve("runs").resolve(suite);
        Files.createDirectories(suiteDirectory);
        int sequence = 0;
        for (int repetition = 0; repetition < options.repetitions; repetition++) {
            for (String scenario : options.scenarios) {
                for (int threadCount : options.threadCounts) {
                    options.threads = threadCount;
                    if (options.connectionsDefault) options.connections = threadCount;
                    var engines = new ArrayList<>(options.engines);
                    Collections.shuffle(engines, random);
                    for (String engine : engines) {
                        String runId = String.format("%04d-%s-%s-t%d-r%d", sequence++, engine, scenario, threadCount, repetition + 1);
                        Path directory = suiteDirectory.resolve(runId);
                        LOG.info("Running {}/{} threads={} repetition={}", engine, scenario, threadCount, repetition + 1);
                        Map<String, Object> result;
                        try { result = supervise(engine, scenario, runId, directory); }
                        catch (Exception error) {
                            LOG.error("Supervisor failed for {}", runId, error);
                            result = failure(engine, scenario, runId, "SUPERVISOR_ERROR", Measurements.describe(error));
                            Files.createDirectories(directory);
                            writeJson(directory.resolve("result.json"), result);
                        }
                        result.put("repetition", repetition + 1);
                        result.put("sequence", sequence - 1);
                        result.put("artifactDirectory", options.output.relativize(directory).toString().replace(File.separatorChar, '/'));
                        results.add(result);
                        ReportWriter.write(options.output, environment, options.asMap(), results);
                        LOG.info("{}: {}{}", runId, result.get("status"), result.containsKey("error") ? ": " + result.get("error") : "");
                    }
                }
            }
        }
        System.out.println("Reports: " + options.output.resolve("report.html"));
        return results.stream().allMatch(r -> "OK".equals(r.get("status"))) ? 0 : 1;
    }

    private Map<String, Object> supervise(String engine, String scenario, String runId, Path directory) throws Exception {
        Files.createDirectories(directory);
        String heap = options.jvmHeap;
        String memoryBudget = options.memoryBudget;
        long estimate;
        try {
            estimate = Math.addExact(Math.addExact(Math.multiplyExact(
                    DeterministicDataset.logicalBytes(options.rows, options.payload), 4),
                    BenchmarkOptions.memoryBytes(heap)), 128L * 1024 * 1024);
            estimate = Math.addExact(estimate, Math.multiplyExact(options.threads, 2L * 1024 * 1024));
        } catch (ArithmeticException error) {
            return persistFailure(directory, engine, scenario, runId, "MEMORY_ADMISSION", "Dataset estimate overflow");
        }
        long budget = effectiveBudget(BenchmarkOptions.memoryBytes(memoryBudget));
        Long supervisorRss = Resources.procValue(ProcessHandle.current().pid(), "VmRSS:");
        long totalEstimate = Math.addExact(estimate, supervisorRss == null ? 128L * 1024 * 1024 : supervisorRss);
        if (totalEstimate > budget)
            return persistFailure(directory, engine, scenario, runId, "MEMORY_ADMISSION",
                    "Conservative process estimate " + totalEstimate + " exceeds effective total RSS budget " + budget);
        Path nativeDirectory;
        String configuredTemp = System.getProperty("java.io.tmpdir");
        if (configuredTemp != null && !forbiddenTemp(Path.of(configuredTemp).toAbsolutePath().normalize()))
            nativeDirectory = Path.of(configuredTemp).toAbsolutePath().normalize();
        else nativeDirectory = directory.resolve("native");
        Files.createDirectories(nativeDirectory);
        List<String> command = childCommand(engine, scenario, runId, directory, nativeDirectory, heap, memoryBudget);
        long start = System.nanoTime();
        Process process = new ProcessBuilder(command).directory(directory.toFile())
                .redirectOutput(directory.resolve("stdout.log").toFile())
                .redirectError(directory.resolve("stderr.log").toFile()).start();
        String supervisorError = null;
        Long peakTotalRss = null;
        boolean watchdogAvailable = false;
        long incompleteWatchdogSamples = 0;
        Thread cleanupHook = new Thread(() -> {
            try { terminate(process); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
        }, "benchmark-child-shutdown");
        boolean hookRegistered = false;
        double effectiveOperationTimeout = options.inactivityTimeout;
        double arrivalRate = options.fixedReadRate > 0 ? options.fixedReadRate
                : options.requestRate > 0 ? options.requestRate : options.refreshReadRate;
        if (effectiveOperationTimeout > 0 && arrivalRate > 0) {
            double longestPhase = Math.max(Math.max(options.duration, options.warmup),
                    Math.max(options.refreshBefore, options.refreshAfter));
            double pacedAllowance = Math.min(2 / arrivalRate, longestPhase) + 2 * options.sampleInterval;
            effectiveOperationTimeout = Math.max(effectiveOperationTimeout, pacedAllowance);
        }
        try {
            Runtime.getRuntime().addShutdownHook(cleanupHook);
            hookRegistered = true;
            try (var sampler = new SupervisorSampler(directory.resolve("supervisor-resources.jsonl"), process, start)) {
                long nextSample = 0;
                long nextHeartbeat = 0;
                var watchdog = new ProgressWatchdog(options.inactivityTimeout, effectiveOperationTimeout);
                while (process.isAlive()) {
                    Long rss = totalRss(process);
                    if (rss != null) {
                        watchdogAvailable = true;
                        peakTotalRss = peakTotalRss == null ? rss : Math.max(peakTotalRss, rss);
                        if (rss > budget) { supervisorError = "MEMORY_BUDGET"; break; }
                    } else incompleteWatchdogSamples++;
                    long now = System.nanoTime();
                    if (now >= nextSample) {
                        sampler.sample();
                        nextSample = now + (long) (options.sampleInterval * 1_000_000_000L);
                    }
                    if (options.inactivityTimeout > 0 && now >= nextHeartbeat) {
                        supervisorError = watchdog.check(directory.resolve("heartbeat.json"), now);
                        if (supervisorError != null) break;
                        nextHeartbeat = now + 500_000_000L;
                    }
                    if (now - start > TimeUnit.SECONDS.toNanos(options.childDeadline)) {
                        supervisorError = "CHILD_DEADLINE"; break;
                    }
                    process.waitFor(50, TimeUnit.MILLISECONDS);
                }
                if (supervisorError != null) terminate(process);
                process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (Exception | Error failure) {
            try { terminate(process); }
            catch (InterruptedException cleanup) {
                failure.addSuppressed(cleanup);
                Thread.currentThread().interrupt();
            }
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            throw failure;
        } finally {
            if (hookRegistered) {
                try { Runtime.getRuntime().removeShutdownHook(cleanupHook); }
                catch (IllegalStateException shuttingDown) {
                    // The registered hook owns cleanup while the JVM shuts down.
                }
                catch (SecurityException error) { LOG.warn("Could not remove child shutdown hook", error); }
            }
        }
        Map<String, Object> result = null;
        Path resultFile = directory.resolve("result.json");
        if (Files.isRegularFile(resultFile)) {
            try {
                if (Files.size(resultFile) > 8_000_000) throw new IOException("Oversized child report");
                Object decoded = Json.decode(Files.readString(resultFile));
                if (decoded instanceof Map<?, ?> parsed) {
                    result = new LinkedHashMap<>();
                    for (var entry : parsed.entrySet()) result.put(entry.getKey().toString(), entry.getValue());
                }
            } catch (RuntimeException | IOException error) {
                supervisorError = supervisorError == null ? "INVALID_CHILD_REPORT" : supervisorError;
            }
        }
        if (result == null) {
            String exitKind = childExitKind(directory);
            result = failure(engine, scenario, runId, supervisorError == null ? exitKind : supervisorError,
                    "Child produced no complete result. Inspect stdout.log and stderr.log.");
            String tail = childLogTail(directory);
            result.put("childLogTail", tail.length() > 2_000 ? tail.substring(tail.length() - 2_000) : tail);
        }
        if (supervisorError != null || process.isAlive() || process.exitValue() != 0 && "OK".equals(result.get("status"))) {
            if (result.containsKey("error")) result.put("childError", result.get("error"));
            if (result.containsKey("errorClass")) result.put("childErrorClass", result.get("errorClass"));
            result.put("status", "FAILED"); result.put("valid", false);
            result.put("errorClass", supervisorError == null ? "CHILD_EXIT" : supervisorError);
            result.put("error", "Supervisor stopped or observed failed child; inspect child logs.");
        }
        result.put("childExitCode", process.isAlive() ? null : process.exitValue());
        result.put("supervisedWallSeconds", (System.nanoTime() - start) / 1_000_000_000.0);
        result.put("supervisorPeakTotalRssBytes", peakTotalRss);
        result.put("supervisorResourceTimeSeries", "supervisor-resources.jsonl");
        result.put("rssWatchdogAvailable", watchdogAvailable);
        result.put("rssWatchdogIncompleteSamples", incompleteWatchdogSamples);
        result.put("effectiveOperationInactivityTimeoutSeconds", effectiveOperationTimeout);
        result.put("effectiveMemoryBudgetBytes", budget);
        result.put("admissionEstimateBytes", totalEstimate);
        result.put("childCommand", command);
        if (!Boolean.TRUE.equals(result.get("datasetValid")) && !process.isAlive()) {
            try {
                discardIncompleteDatabase(directory);
                result.put("incompleteDatabaseDiscarded", true);
            } catch (IOException | java.io.UncheckedIOException | SecurityException error) {
                result.put("incompleteDatabaseDiscarded", false);
                result.put("cleanupError", Measurements.describe(error));
            }
        }
        writeJson(resultFile, result);
        return result;
    }

    static void discardIncompleteDatabase(Path runDirectory) throws IOException {
        Path database = runDirectory.resolve("database");
        if (!Files.exists(database, java.nio.file.LinkOption.NOFOLLOW_LINKS)) return;
        try (var paths = Files.walk(database)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }

    static String childExitKind(Path directory) {
        String tail = childLogTail(directory).toLowerCase(java.util.Locale.ROOT);
        if (tail.contains("outofmemoryerror") || tail.contains("out of memory")) return "OOM";
        if (tail.contains("native memory allocation") || tail.contains("could not reserve enough space")
                || tail.contains("unable to allocate native memory")) return "NATIVE_ALLOCATION";
        if (tail.contains("fatal error has been detected by the java runtime")
                || tail.contains("sigsegv") || tail.contains("sigbus") || tail.contains("sigabrt")
                || tail.contains("exception_access_violation")) return "JVM_CRASH";
        return "CHILD_EXIT";
    }

    private static String childLogTail(Path directory) {
        var text = new StringBuilder();
        for (String filename : List.of("stdout.log", "stderr.log")) {
            try (var channel = java.nio.channels.FileChannel.open(directory.resolve(filename))) {
                long size = channel.size();
                channel.position(Math.max(0, size - 65_536));
                var buffer = java.nio.ByteBuffer.allocate((int) Math.min(size, 65_536));
                channel.read(buffer);
                buffer.flip();
                text.append(filename).append(":\n").append(StandardCharsets.UTF_8.decode(buffer)).append('\n');
            } catch (IOException ignored) {}
        }
        return text.toString();
    }

    static long effectiveBudget(long requested) {
        long result = requested;
        Long cgroup = Resources.cgroupLimit();
        if (cgroup != null) result = Math.min(result, (long) (cgroup * 0.8));
        var os = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean extended)
            result = Math.min(result, extended.getTotalMemorySize() / 2);
        return result;
    }

    private static Long totalRss(Process process) {
        Long parent = Resources.procValue(ProcessHandle.current().pid(), "VmRSS:");
        Long child = Resources.procValue(process.pid(), "VmRSS:");
        if (parent == null || child == null) return null;
        long total = parent + child;
        for (ProcessHandle descendant : process.descendants().toList()) {
            Long rss = Resources.procValue(descendant.pid(), "VmRSS:");
            if (rss == null) return null;
            total += rss;
        }
        return total;
    }

    private static void terminate(Process process) throws InterruptedException {
        List<ProcessHandle> descendants = process.descendants().toList();
        for (ProcessHandle child : descendants) child.destroy();
        process.destroy();
        process.waitFor(2, TimeUnit.SECONDS);
        for (ProcessHandle child : descendants) if (child.isAlive()) child.destroyForcibly();
        if (process.isAlive()) process.destroyForcibly();
    }

    private List<String> childCommand(String engine, String scenario, String runId, Path directory, Path nativeDirectory,
                                      String heap, String memoryBudget) {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx" + BenchmarkOptions.memoryBytes(heap));
        command.add("-XX:+ExitOnOutOfMemoryError");
        command.add("-Duser.timezone=UTC");
        command.add("-Djava.io.tmpdir=" + nativeDirectory);
        String sqliteTemp = System.getProperty("org.sqlite.tmpdir");
        Path sqliteTempPath = sqliteTemp == null ? nativeDirectory : Path.of(sqliteTemp).toAbsolutePath().normalize();
        if (forbiddenTemp(sqliteTempPath)) sqliteTempPath = nativeDirectory;
        command.add("-Dorg.sqlite.tmpdir=" + sqliteTempPath);
        for (String key : List.of("org.sqlite.lib.path", "org.sqlite.lib.name", "java.library.path")) {
            String value = System.getProperty(key);
            if (value != null) command.add("-D" + key + "=" + value);
        }
        command.add("-Dembeddedbench.db.dir=" + directory.resolve("database"));
        if (gitCommit != null) command.add("-Dembeddedbench.gitCommit=" + gitCommit);
        if (gitDirty != null) command.add("-Dembeddedbench.gitDirty=" + gitDirty);
        command.add("-cp");
        command.add(Arrays.stream(System.getProperty("java.class.path").split(java.util.regex.Pattern.quote(File.pathSeparator)))
                .map(path -> Path.of(path).toAbsolutePath().normalize().toString())
                .collect(java.util.stream.Collectors.joining(File.pathSeparator)));
        command.add(BenchmarkCli.class.getName());
        Collections.addAll(command, "--child", "--run-id", runId, "--engine", engine, "--scenario", scenario);
        add(command, "--profile", options.profile); add(command, "--db-profile", options.dbProfile);
        add(command, "--connection-mode", options.connectionMode); add(command, "--rows", options.rows);
        add(command, "--payload", options.payload); add(command, "--threads", options.threads);
        add(command, "--connections", options.connections); add(command, "--batch", options.batch);
        add(command, "--load-commit-interval", options.loadCommitInterval);
        add(command, "--sample-interval", options.sampleInterval);
        add(command, "--inactivity-timeout", options.inactivityTimeout);
        add(command, "--reclamation-cycles", options.reclamationCycles);
        add(command, "--idle-seconds", options.idleSeconds);
        add(command, "--refresh-strategy", options.refreshStrategy); add(command, "--refresh-percent", options.refreshPercent);
        add(command, "--refresh-before", options.refreshBefore); add(command, "--refresh-after", options.refreshAfter);
        add(command, "--refresh-read-rate", options.refreshReadRate);
        add(command, "--maintenance-interval", options.maintenanceInterval);
        add(command, "--transaction-size", options.transactionSize); add(command, "--request-rate", options.requestRate);
        add(command, "--fixed-read-rate", options.fixedReadRate);
        if (options.readerWorkers != null) add(command, "--reader-workers", options.readerWorkers);
        add(command, "--queue-capacity", options.queueCapacity); add(command, "--warmup", options.warmup);
        add(command, "--duration", options.duration); add(command, "--max-operations", options.maxOperations);
        add(command, "--iterations", 1);
        add(command, "--seed", options.seed); add(command, "--memory-budget", memoryBudget);
        add(command, "--jvm-heap", heap); add(command, "--range-rows", options.rangeRows);
        add(command, "--read-percent", options.readPercent); add(command, "--analytical-percent", options.analyticalPercent);
        add(command, "--load-mode", options.loadMode); add(command, "--child-deadline", options.childDeadline);
        add(command, "--output", directory);
        return command;
    }

    private static boolean forbiddenTemp(Path path) {
        return path.startsWith(Path.of("/tmp")) || path.startsWith(Path.of("/var/tmp"));
    }

    private static void add(List<String> command, String key, Object value) {
        command.add(key); command.add(value.toString());
    }

    private Map<String, Object> failure(String engine, String scenario, String id, String kind, String message) {
        var result = new LinkedHashMap<String, Object>();
        result.put("runId", id); result.put("engine", engine); result.put("scenario", scenario);
        result.put("status", "FAILED"); result.put("valid", false); result.put("configuration", options.asMap());
        result.put("errorClass", kind); result.put("error", message);
        return result;
    }

    private Map<String, Object> persistFailure(Path dir, String engine, String scenario, String id, String kind, String message) throws IOException {
        Map<String, Object> result = failure(engine, scenario, id, kind, message);
        writeJson(dir.resolve("result.json"), result);
        return result;
    }

    static void writeJson(Path file, Object value) throws IOException {
        Files.createDirectories(file.getParent());
        Path pending = file.resolveSibling(file.getFileName() + ".pending");
        Files.writeString(pending, Json.encode(value) + "\n", StandardCharsets.UTF_8);
        try { Files.move(pending, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
        catch (java.nio.file.AtomicMoveNotSupportedException e) { Files.move(pending, file, StandardCopyOption.REPLACE_EXISTING); }
    }
}
