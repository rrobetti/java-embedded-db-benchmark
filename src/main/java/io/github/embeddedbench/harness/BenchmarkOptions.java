package io.github.embeddedbench.harness;

import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class BenchmarkOptions {
    @Option(names = "--profile", description = "smoke, quick (default), standard, full, stress, endurance")
    String profile = "quick";
    @Option(names = {"--databases", "--engines", "--engine"}, split = ",", description = "Comma-separated engines")
    List<String> engines = List.of("h2", "hsqldb", "sqlite", "pglite");
    @Option(names = {"--workloads", "--scenarios", "--scenario"}, split = ",", description = "Comma-separated workloads")
    List<String> scenarios;
    @Option(names = "--db-profile", description = "Adapter durability/configuration profile") String dbProfile = "default";
    @Option(names = "--connection-mode", description = "auto, dedicated, pooled, serialized") String connectionMode = "auto";
    @Option(names = "--rows") Long rows;
    @Option(names = {"--payload", "--payload-size"}) Integer payload;
    @Option(names = "--threads", description = "Worker count or comma-separated grid (e.g. 1,4,8)") String threadSpec = "1";
    Integer threads;
    List<Integer> threadCounts;
    boolean connectionsDefault;
    @Option(names = "--connections") Integer connections;
    @Option(names = {"--batch", "--batch-size"}) Integer batch;
    @Option(names = "--load-commit-interval", description = "Inserted records per load commit; 0 uses one transaction")
    long loadCommitInterval;
    @Option(names = "--sample-interval", description = "Resource sampling interval in seconds")
    double sampleInterval = 1;
    @Option(names = "--reclamation-cycles", description = "Repeated deletion/reinsertion cycles")
    int reclamationCycles = 1;
    @Option(names = "--idle-seconds", description = "Idle time after each reclamation deletion")
    double idleSeconds = 30;
    @Option(names = "--refresh-strategy", description = "Cache refresh strategy")
    String refreshStrategy = "incremental-refresh";
    @Option(names = "--refresh-percent", description = "Percentage of cache entries affected")
    int refreshPercent = 10;
    @Option(names = "--refresh-before", description = "Seconds of reads before refresh")
    double refreshBefore = 10;
    @Option(names = "--refresh-after", description = "Seconds of reads after refresh")
    double refreshAfter = 10;
    @Option(names = "--refresh-read-rate", description = "Paced closed-loop reads/sec across refresh readers; 0 is unpaced")
    double refreshReadRate;
    @Option(names = "--maintenance-interval", description = "Seconds between endurance maintenance cycles")
    double maintenanceInterval = 60;
    @Option(names = {"--transaction-size", "--transaction"}) Integer transactionSize;
    @Option(names = {"--request-rate", "--rate"}, description = "Requests/sec; zero selects closed-loop") double requestRate;
    @Option(names = "--fixed-read-rate", description = "Open-loop read arrivals/sec for rw-* workloads") double fixedReadRate;
    @Option(names = "--reader-workers", description = "Dedicated closed-loop reader workers for rw-* workloads")
    Integer readerWorkers;
    @Option(names = "--queue-capacity", description = "Bound on waiting open-loop requests") int queueCapacity = 256;
    @Option(names = "--warmup", description = "Warmup seconds") Double warmup;
    @Option(names = "--duration", description = "Measurement seconds") Double duration;
    @Option(names = "--max-operations", description = "Maximum attempts per phase; zero means duration only") long maxOperations;
    @Option(names = {"--iterations", "--repetitions"}, description = "Independent fresh-JVM repetitions") Integer repetitions;
    @Option(names = "--seed") long seed = 42;
    @Option(names = "--memory-budget", description = "Total supervisor/child RSS safety limit (e.g. 768m)") String memoryBudget;
    @Option(names = "--jvm-heap", description = "Child maximum Java heap (e.g. 256m)") String jvmHeap;
    @Option(names = "--output") Path output = Path.of("benchmark-results");
    @Option(names = "--range-rows") int rangeRows = 100;
    @Option(names = "--read-percent") int readPercent = 80;
    @Option(names = "--analytical-percent") int analyticalPercent = 10;
    @Option(names = "--load-mode", description = "batch (dataset loader); workload ingestion measures subsequent inserts")
    String loadMode = "batch";
    @Option(names = "--child-deadline", description = "Maximum seconds per child, including loading") Long childDeadline;
    @Option(names = "--inactivity-timeout", description = "Seconds without measurable progress or heartbeat; 0 disables")
    double inactivityTimeout;
    @Option(names = "--child", hidden = true) boolean child;
    @Option(names = "--run-id", hidden = true) String runId;

    void resolve() {
        long defaultRows;
        double defaultDuration;
        switch (profile.toLowerCase(Locale.ROOT)) {
            case "smoke" -> { defaultRows = 100; defaultDuration = 1; }
            case "quick" -> { defaultRows = 1_000; defaultDuration = 10; }
            case "standard" -> { defaultRows = 100_000; defaultDuration = 120; }
            case "full" -> { defaultRows = 100_000; defaultDuration = 120; }
            case "stress" -> { defaultRows = 100_000; defaultDuration = 60; }
            case "endurance" -> { defaultRows = 100_000; defaultDuration = 3600; }
            default -> throw new IllegalArgumentException("Unknown profile: " + profile);
        }
        rows = rows == null ? defaultRows : rows;
        if (scenarios == null) scenarios = profile.equalsIgnoreCase("full")
                ? List.of("point", "indexed-range", "joins", "mixed", "htap") : List.of("point", "mixed");
        payload = payload == null ? 128 : payload;
        threadCounts = java.util.Arrays.stream(threadSpec.split(",", -1)).map(Integer::valueOf).distinct().toList();
        threads = threadCounts.getFirst();
        connectionsDefault = connections == null;
        connections = connections == null ? threads : connections;
        batch = batch == null ? 500 : batch;
        transactionSize = transactionSize == null ? 10 : transactionSize;
        warmup = warmup == null ? (profile.equalsIgnoreCase("smoke") ? 0
                : profile.equalsIgnoreCase("quick") ? 5 : profile.equalsIgnoreCase("endurance") ? 60 : 30) : warmup;
        duration = duration == null ? defaultDuration : duration;
        repetitions = repetitions == null ? switch (profile.toLowerCase(Locale.ROOT)) {
            case "standard" -> 3;
            case "full" -> 5;
            default -> 1;
        } : repetitions;
        jvmHeap = jvmHeap == null ? "768m" : jvmHeap;
        memoryBudget = memoryBudget == null ? "4g" : memoryBudget;
        childDeadline = childDeadline == null ? Math.max(120, (long) Math.ceil(warmup + duration) + 120) : childDeadline;
        output = output.toAbsolutePath().normalize();
        if (rows < 1 || payload < 1 || threads < 1 || connections < 1 || connections > threads || batch < 1
                || transactionSize < 1 || loadCommitInterval < 0 || sampleInterval < 0.1 || sampleInterval > 3600
                || reclamationCycles < 1 || reclamationCycles > 1000 || idleSeconds < 0 || idleSeconds > 14400
                || !Double.isFinite(idleSeconds)
                || refreshPercent < 1 || refreshPercent > 100 || refreshBefore < 0 || refreshBefore > 14400
                || refreshAfter < 0 || refreshAfter > 14400 || !Double.isFinite(refreshBefore) || !Double.isFinite(refreshAfter)
                || refreshReadRate < 0 || refreshReadRate > 1_000_000 || !Double.isFinite(refreshReadRate)
                || maintenanceInterval < 0.1 || maintenanceInterval > 28800 || !Double.isFinite(maintenanceInterval)
                || !Double.isFinite(sampleInterval) || duration <= 0 || warmup < 0 || !Double.isFinite(duration)
                || !Double.isFinite(inactivityTimeout) || inactivityTimeout < 0 || inactivityTimeout > 86400
                || inactivityTimeout > 0 && inactivityTimeout < Math.max(2, 2 * sampleInterval)
                || !Double.isFinite(warmup) || maxOperations < 0 || repetitions < 1 || requestRate < 0
                || !Double.isFinite(requestRate) || requestRate > 1_000_000 || fixedReadRate < 0
                || !Double.isFinite(fixedReadRate) || queueCapacity < 1
                || childDeadline < 1 || rangeRows < 1 || readPercent < 0 || readPercent > 100
                || analyticalPercent < 0 || analyticalPercent > 100 || duration > 28_800 || warmup > 14_400)
            throw new IllegalArgumentException("Invalid benchmark bounds/percentages; connections must not exceed threads");
        if (!List.of("auto", "dedicated", "pooled", "serialized").contains(connectionMode))
            throw new IllegalArgumentException("connection-mode must be auto, dedicated, pooled or serialized");
        if (connectionMode.equals("dedicated") && threadCounts.stream().anyMatch(n -> connections != n && !connectionsDefault))
            throw new IllegalArgumentException("Dedicated mode requires connections equal to threads; use auto/pooled");
        if (!List.of("batch", "prepared", "individual").contains(loadMode))
            throw new IllegalArgumentException("load-mode must be batch, prepared or individual");
        if (!List.of("record-update", "record-invalidate", "bulk-invalidate", "incremental-refresh",
                "full-table-refresh", "replacement", "expiration").contains(refreshStrategy))
            throw new IllegalArgumentException("Unknown refresh strategy: " + refreshStrategy);
        if (engines.isEmpty() || scenarios.isEmpty()) throw new IllegalArgumentException("Select engines and scenarios");
        if (fixedReadRate > 0 && (requestRate > 0 || readPercent == 0
                || fixedReadRate * 100.0 / readPercent > 1_000_000))
            throw new IllegalArgumentException("Fixed read rate requires read-percent >0, no request-rate and total rate <=1000000");
        if (readerWorkers != null && (readerWorkers < 0 || threadCounts.stream().anyMatch(n -> readerWorkers > n)
                || requestRate > 0 || fixedReadRate > 0))
            throw new IllegalArgumentException("reader-workers must fit every thread count and uses closed-loop mode");
        if ((fixedReadRate > 0 || readerWorkers != null)
                && scenarios.stream().anyMatch(name -> !List.of("rw-independent", "rw-hot").contains(name)))
            throw new IllegalArgumentException("Fixed read rate and reader-workers require rw-independent or rw-hot workloads");
        for (String id : engines) safeIdentifier(id);
        for (String scenario : scenarios) safeIdentifier(scenario);
        if (memoryBytes(jvmHeap) < 32L * 1024 * 1024 || memoryBytes(memoryBudget) < 64L * 1024 * 1024)
            throw new IllegalArgumentException("Heap must be >=32m and memory budget >=64m");
        if (threadCounts.stream().anyMatch(n -> n < 1 || n > 256 || !connectionsDefault && connections > n)
                || connections > 256 || queueCapacity > 1_000_000)
            throw new IllegalArgumentException("Safety limit: threads/connections <=256; queue <=1000000");
    }

    static void safeIdentifier(String id) {
        if (!id.matches("[a-zA-Z0-9_-]+")) throw new IllegalArgumentException("Invalid identifier: " + id);
    }

    public static long memoryBytes(String value) {
        String s = value.toLowerCase(Locale.ROOT).trim();
        long multiplier = 1;
        if (s.endsWith("k") || s.endsWith("m") || s.endsWith("g")) {
            multiplier = switch (s.charAt(s.length() - 1)) {
                case 'k' -> 1024L;
                case 'm' -> 1024L * 1024;
                default -> 1024L * 1024 * 1024;
            };
            s = s.substring(0, s.length() - 1);
        }
        long bytes = Math.multiplyExact(Long.parseLong(s), multiplier);
        if (bytes <= 0) throw new IllegalArgumentException("Memory size must be positive");
        return bytes;
    }

    Map<String, Object> asMap() {
        var m = new LinkedHashMap<String, Object>();
        m.put("profile", profile); m.put("dbProfile", dbProfile); m.put("connectionMode", connectionMode);
        m.put("rows", rows); m.put("payloadBytes", payload); m.put("threads", threads);
        m.put("connections", connections); m.put("batchSize", batch); m.put("loadCommitInterval", loadCommitInterval);
        m.put("sampleIntervalSeconds", sampleInterval);
        m.put("reclamationCycles", reclamationCycles); m.put("idleSeconds", idleSeconds);
        m.put("refreshStrategy", refreshStrategy); m.put("refreshPercent", refreshPercent);
        m.put("refreshBeforeSeconds", refreshBefore); m.put("refreshAfterSeconds", refreshAfter);
        m.put("refreshReadRate", refreshReadRate);
        m.put("maintenanceIntervalSeconds", maintenanceInterval);
        m.put("transactionSize", transactionSize);
        m.put("requestRate", requestRate); m.put("fixedReadRate", fixedReadRate);
        m.put("readerWorkers", readerWorkers); m.put("queueCapacity", queueCapacity);
        m.put("warmupSeconds", warmup); m.put("durationSeconds", duration); m.put("iterations", repetitions);
        m.put("maxOperations", maxOperations); m.put("threadGrid", threadCounts);
        m.put("seed", seed); m.put("memoryBudgetBytes", memoryBytes(memoryBudget));
        m.put("jvmHeapBytes", memoryBytes(jvmHeap)); m.put("loadMode", loadMode);
        m.put("rangeRows", rangeRows); m.put("readPercent", readPercent);
        m.put("analyticalPercent", analyticalPercent); m.put("childDeadlineSeconds", childDeadline);
        m.put("inactivityTimeoutSeconds", inactivityTimeout);
        return m;
    }
}
