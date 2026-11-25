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
import java.util.concurrent.TimeUnit;

/** Samples process and JVM counters without traversing database files. */
final class ResourceSampler implements AutoCloseable {
    private final BufferedWriter writer;
    private final Thread thread;
    private final long intervalMillis;
    private volatile String phase = "initialization";
    private volatile boolean stopping;
    private IOException failure;

    ResourceSampler(Path path, double intervalSeconds) throws IOException {
        writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
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
        var bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean os) {
            m.put("processCpuNanos", nonnegative(os.getProcessCpuTime()));
            m.put("processCpuLoad", nonnegative(os.getProcessCpuLoad()));
            m.put("systemCpuLoad", nonnegative(os.getCpuLoad()));
            m.put("availableMemoryBytes", nonnegative(os.getFreeMemorySize()));
            long totalSwap = os.getTotalSwapSpaceSize(), freeSwap = os.getFreeSwapSpaceSize();
            m.put("usedSwapBytes", totalSwap >= 0 && freeSwap >= 0 && freeSwap <= totalSwap
                    ? totalSwap - freeSwap : null);
        } else {
            for (String key : new String[]{"processCpuNanos", "processCpuLoad", "systemCpuLoad",
                    "availableMemoryBytes", "usedSwapBytes"}) m.put(key, null);
        }
        writer.write(Json.encode(m));
        writer.newLine();
        writer.flush();
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
