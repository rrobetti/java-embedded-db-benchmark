package io.github.embeddedbench.harness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/** Samples process and JVM counters without traversing database files. */
final class ResourceSampler implements AutoCloseable {
    private final BufferedWriter writer;
    private final Path heartbeat;
    private final Thread thread;
    private final long intervalMillis;
    private volatile String phase = "initialization";
    private volatile boolean stopping;
    private volatile long recordsLoaded = -1;
    private IOException failure;
    private List<Measurements> performanceSources = List.of();
    private List<Measurements.Sample> previous = List.of();
    private long previousNanos;
    private long previousResourceNanos;
    private Long previousProcessCpuNanos;
    private long heartbeatSequence;
    private long completedOperations;

    ResourceSampler(Path path, double intervalSeconds) throws IOException {
        writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        heartbeat = path.resolveSibling("heartbeat.json");
        intervalMillis = Math.max(1, (long) (intervalSeconds * 1000));
        sample();
        thread = new Thread(this::run, "resource-sampler");
        thread.setDaemon(true);
        thread.start();
    }

    synchronized void phase(String phase) throws IOException {
        this.phase = phase;
        sample();
    }

    void recordsLoaded(long records) { recordsLoaded = records; }

    synchronized void startPerformance(List<Measurements> sources) throws IOException {
        if (!performanceSources.isEmpty()) throw new IllegalStateException("Performance phase already active");
        performanceSources = List.copyOf(sources);
        previous = performanceSources.stream().map(Measurements::sample).toList();
        previousNanos = System.nanoTime();
    }

    synchronized void stopPerformance() throws IOException {
        if (!performanceSources.isEmpty()) {
            try { sample(); }
            finally { performanceSources = List.of(); previous = List.of(); }
        }
    }

    private void run() {
        while (!stopping) {
            try {
                Thread.sleep(intervalMillis);
                if (!stopping) sample();
            } catch (InterruptedException error) {
                if (!stopping) Thread.currentThread().interrupt();
                return;
            } catch (IOException error) {
                synchronized (this) { failure = error; }
                return;
            } catch (RuntimeException error) {
                synchronized (this) { failure = new IOException("Resource sampling failed", error); }
                return;
            }
        }
    }

    private synchronized void sample() throws IOException {
        if (failure != null) throw failure;
        var m = new LinkedHashMap<String, Object>();
        m.put("timestamp", Instant.now().toString());
        m.put("phase", phase);
        m.put("recordsLoaded", recordsLoaded < 0 ? null : recordsLoaded);
        long pid = ProcessHandle.current().pid();
        m.put("rssBytes", Resources.procValue(pid, "VmRSS:"));
        m.put("peakRssBytes", Resources.procValue(pid, "VmHWM:"));
        var memory = ManagementFactory.getMemoryMXBean();
        m.put("heapUsedBytes", memory.getHeapMemoryUsage().getUsed());
        m.put("heapCommittedBytes", memory.getHeapMemoryUsage().getCommitted());
        m.put("nonHeapUsedBytes", memory.getNonHeapMemoryUsage().getUsed());
        m.put("nonHeapCommittedBytes", memory.getNonHeapMemoryUsage().getCommitted());
        m.put("directBufferBytes", ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                .filter(pool -> pool.getName().equals("direct")).map(BufferPoolMXBean::getMemoryUsed)
                .filter(value -> value >= 0).findFirst().orElse(null));
        m.put("nativeMemoryBytes", null);
        m.put("wasmMemoryBytes", null);
        m.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());
        long count = 0, millis = 0;
        boolean countKnown = false, timeKnown = false;
        for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getCollectionCount() >= 0) { count += gc.getCollectionCount(); countKnown = true; }
            if (gc.getCollectionTime() >= 0) { millis += gc.getCollectionTime(); timeKnown = true; }
        }
        m.put("gcCollections", countKnown ? count : null);
        m.put("gcMillis", timeKnown ? millis : null);
        m.put("gcPauseNanos", null);
        var bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean os) {
            Long cpu = nonnegative(os.getProcessCpuTime());
            long now = System.nanoTime();
            Double cores = cpu != null && previousProcessCpuNanos != null && cpu >= previousProcessCpuNanos
                    && now > previousResourceNanos
                    ? (cpu - previousProcessCpuNanos) / (double) (now - previousResourceNanos) : null;
            m.put("processCpuNanos", cpu);
            m.put("processCpuCoreEquivalent", cores);
            m.put("processCpuPercentOfJvmAvailableProcessors", cores == null ? null
                    : cores * 100 / Runtime.getRuntime().availableProcessors());
            previousProcessCpuNanos = cpu;
            previousResourceNanos = now;
            m.put("processCpuLoad", nonnegative(os.getProcessCpuLoad()));
            m.put("systemCpuLoad", nonnegative(os.getCpuLoad()));
            m.put("availableMemoryBytes", nonnegative(os.getFreeMemorySize()));
            long totalSwap = os.getTotalSwapSpaceSize(), freeSwap = os.getFreeSwapSpaceSize();
            m.put("usedSwapBytes", totalSwap >= 0 && freeSwap >= 0 && freeSwap <= totalSwap
                    ? totalSwap - freeSwap : null);
        } else {
            for (String key : new String[]{"processCpuNanos", "processCpuCoreEquivalent",
                    "processCpuPercentOfJvmAvailableProcessors", "processCpuLoad", "systemCpuLoad",
                    "availableMemoryBytes", "usedSwapBytes"}) m.put(key, null);
        }
        Map<String, Object> performance = performance();
        m.put("performance", performance);
        if (performance != null)
            for (String key : List.of("successes", "failures", "rejected"))
                completedOperations += ((Number) performance.get(key)).longValue();
        writer.write(Json.encode(m));
        writer.newLine();
        writer.flush();
        var progress = new LinkedHashMap<String, Object>();
        progress.put("sequence", ++heartbeatSequence);
        progress.put("phase", phase);
        progress.put("recordsLoaded", recordsLoaded < 0 ? null : recordsLoaded);
        progress.put("completedOperations", completedOperations);
        BenchmarkCli.writeJson(heartbeat, progress);
    }

    private Map<String, Object> performance() {
        if (performanceSources.isEmpty()) return null;
        long now = System.nanoTime();
        double seconds = (now - previousNanos) / 1_000_000_000.0;
        long successes = 0, failures = 0, rejected = 0;
        var errors = new LinkedHashMap<String, Long>();
        var histograms = new LinkedHashMap<String, Histogram>();
        var current = new ArrayList<Measurements.Sample>();
        for (int i = 0; i < performanceSources.size(); i++) {
            var sample = performanceSources.get(i).sample();
            var old = previous.get(i);
            current.add(sample);
            successes += sample.successes() - old.successes();
            failures += sample.failures() - old.failures();
            rejected += sample.rejected() - old.rejected();
            sample.errors().forEach((key, value) -> errors.merge(key,
                    value - old.errors().getOrDefault(key, 0L), Long::sum));
            sample.histograms().forEach((key, histogram) -> {
                Histogram delta = histogram.copy();
                Histogram prior = old.histograms().get(key);
                if (prior != null) delta.subtract(prior);
                histograms.computeIfAbsent(key, ignored -> {
                    Histogram h = new Histogram(3);
                    h.setAutoResize(true);
                    return h;
                }).add(delta);
            });
        }
        previous = current;
        previousNanos = now;
        var result = new LinkedHashMap<String, Object>();
        result.put("windowSeconds", seconds);
        result.put("successes", successes);
        result.put("failures", failures);
        result.put("rejected", rejected);
        result.put("errors", errors);
        result.put("timeouts", errors.getOrDefault("TIMEOUT", 0L));
        result.put("retries", 0);
        result.put("throughputOpsPerSecond", seconds > 0 ? successes / seconds : null);
        var categories = new LinkedHashMap<String, Object>();
        histograms.forEach((key, histogram) -> {
            var stats = new LinkedHashMap<String, Object>();
            stats.put("count", histogram.getTotalCount());
            stats.put("throughputOpsPerSecond", seconds > 0 ? histogram.getTotalCount() / seconds : null);
            stats.putAll(Measurements.histogramStats(histogram));
            categories.put(key, stats);
        });
        result.put("categories", categories);
        return result;
    }

    private static Long nonnegative(long value) { return value < 0 ? null : value; }
    private static Double nonnegative(double value) { return value < 0 || !Double.isFinite(value) ? null : value; }

    @Override public void close() throws IOException {
        stopping = true;
        thread.interrupt();
        try {
            thread.join(TimeUnit.SECONDS.toMillis(5));
            if (thread.isAlive()) throw new IOException("Resource sampler did not stop");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while stopping resource sampler", error);
        }
        IOException error = null;
        try { sample(); } catch (IOException e) { error = e; }
        try { writer.close(); } catch (IOException e) {
            if (error == null) error = e; else error.addSuppressed(e);
        }
        if (error != null) throw error;
    }
}
