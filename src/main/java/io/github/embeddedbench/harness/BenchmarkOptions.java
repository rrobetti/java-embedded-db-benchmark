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
    List<String> scenarios = List.of("point", "mixed");
    @Option(names = "--db-profile", description = "Adapter durability/configuration profile") String dbProfile = "default";
    @Option(names = "--connection-mode", description = "auto, dedicated, serialized") String connectionMode = "auto";
    @Option(names = "--rows") Long rows;
    @Option(names = {"--payload", "--payload-size"}) Integer payload;
    @Option(names = "--threads", description = "Worker count or comma-separated grid (e.g. 1,4,8)") String threadSpec = "1";
    Integer threads;
    List<Integer> threadCounts;
    boolean connectionsDefault;
    boolean heapDefault;
    boolean budgetDefault;
    @Option(names = "--connections") Integer connections;
    @Option(names = {"--batch", "--batch-size"}) Integer batch;
    @Option(names = {"--transaction-size", "--transaction"}) Integer transactionSize;
    @Option(names = {"--request-rate", "--rate"}, description = "Requests/sec; zero selects closed-loop") double requestRate;
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
    @Option(names = "--child", hidden = true) boolean child;
    @Option(names = "--run-id", hidden = true) String runId;

    void resolve() {
        long defaultRows;
        double defaultDuration;
        switch (profile.toLowerCase(Locale.ROOT)) {
            case "smoke" -> { defaultRows = 100; defaultDuration = 0.2; }
            case "quick" -> { defaultRows = 10_000; defaultDuration = 2; }
            case "standard" -> { defaultRows = 100_000; defaultDuration = 120; }
            case "full" -> { defaultRows = 1_000_000; defaultDuration = 120; }
            case "stress" -> { defaultRows = 2_000_000; defaultDuration = 120; }
            case "endurance" -> { defaultRows = 100_000; defaultDuration = 1800; }
            default -> throw new IllegalArgumentException("Unknown profile: " + profile);
        }
        rows = rows == null ? defaultRows : rows;
        payload = payload == null ? 128 : payload;
        threadCounts = java.util.Arrays.stream(threadSpec.split(",", -1)).map(Integer::valueOf).distinct().toList();
        threads = threadCounts.getFirst();
        connectionsDefault = connections == null;
        connections = connections == null ? threads : connections;
        batch = batch == null ? 500 : batch;
        transactionSize = transactionSize == null ? 10 : transactionSize;
        warmup = warmup == null ? (profile.equalsIgnoreCase("smoke") ? 0
                : profile.equalsIgnoreCase("quick") ? 1 : 30) : warmup;
        duration = duration == null ? defaultDuration : duration;
        repetitions = repetitions == null ? (List.of("standard", "full").contains(profile.toLowerCase(Locale.ROOT)) ? 5 : 1) : repetitions;
        heapDefault = jvmHeap == null;
        budgetDefault = memoryBudget == null;
        jvmHeap = jvmHeap == null ? (defaultRows >= 1_000_000 ? "512m" : "256m") : jvmHeap;
        memoryBudget = memoryBudget == null ? (defaultRows >= 1_000_000 ? "2g" : "768m") : memoryBudget;
        childDeadline = childDeadline == null ? Math.max(120, (long) Math.ceil(warmup + duration) + 120) : childDeadline;
        output = output.toAbsolutePath().normalize();
        if (rows < 1 || payload < 1 || threads < 1 || connections < 1 || connections > threads || batch < 1
                || transactionSize < 1 || duration <= 0 || warmup < 0 || !Double.isFinite(duration)
                || !Double.isFinite(warmup) || maxOperations < 0 || repetitions < 1 || requestRate < 0
                || !Double.isFinite(requestRate) || requestRate > 1_000_000 || queueCapacity < 1
                || childDeadline < 1 || rangeRows < 1 || readPercent < 0 || readPercent > 100
                || analyticalPercent < 0 || analyticalPercent > 100 || duration > 14_400 || warmup > 14_400)
            throw new IllegalArgumentException("Invalid benchmark bounds/percentages; connections must not exceed threads");
        if (!List.of("auto", "dedicated", "serialized").contains(connectionMode))
            throw new IllegalArgumentException("connection-mode must be auto, dedicated or serialized");
        if (!List.of("batch", "prepared", "individual").contains(loadMode))
            throw new IllegalArgumentException("load-mode must be batch, prepared or individual");
        if (engines.isEmpty() || scenarios.isEmpty()) throw new IllegalArgumentException("Select engines and scenarios");
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
        m.put("connections", connections); m.put("batchSize", batch); m.put("transactionSize", transactionSize);
        m.put("requestRate", requestRate); m.put("queueCapacity", queueCapacity);
        m.put("warmupSeconds", warmup); m.put("durationSeconds", duration); m.put("iterations", repetitions);
        m.put("maxOperations", maxOperations); m.put("threadGrid", threadCounts);
        m.put("seed", seed); m.put("memoryBudgetBytes", memoryBytes(memoryBudget));
        m.put("jvmHeapBytes", memoryBytes(jvmHeap)); m.put("loadMode", loadMode);
        m.put("pgliteDefaultHeapBytes", heapDefault ? memoryBytes("768m") : null);
        m.put("pgliteDefaultMemoryBudgetBytes", budgetDefault ? memoryBytes("2g") : null);
        m.put("rangeRows", rangeRows); m.put("readPercent", readPercent);
        m.put("analyticalPercent", analyticalPercent); m.put("childDeadlineSeconds", childDeadline);
        return m;
    }
}
