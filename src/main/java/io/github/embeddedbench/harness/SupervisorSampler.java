package io.github.embeddedbench.harness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/** Samples the supervisor and live child process tree independently of JDBC work. */
final class SupervisorSampler implements AutoCloseable {
    private final BufferedWriter writer;
    private final Process process;
    private final long start;
    private long previousNanos;
    private Long previousChildCpu;

    SupervisorSampler(Path path, Process process, long start) throws IOException {
        this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        this.process = process;
        this.start = start;
    }

    void sample() throws IOException {
        long now = System.nanoTime();
        Map<String, Object> supervisor = process(ProcessHandle.current());
        Map<String, Object> child = process(process.toHandle());
        var descendants = new ArrayList<Map<String, Object>>();
        for (ProcessHandle handle : process.descendants().toList()) descendants.add(process(handle));
        var result = new LinkedHashMap<String, Object>();
        result.put("timestamp", Instant.now().toString());
        result.put("elapsedSeconds", (now - start) / 1e9);
        result.put("supervisor", supervisor);
        result.put("child", child);
        result.put("descendants", descendants);
        var all = new ArrayList<Map<String, Object>>();
        all.add(supervisor); all.add(child); all.addAll(descendants);
        result.put("liveTreeRssBytes", sum(all, "rssBytes"));
        result.put("liveTreeCpuNanos", sum(all, "cpuNanos"));
        Long childCpu = (Long) child.get("cpuNanos");
        Double cores = childCpu != null && previousChildCpu != null
                && childCpu >= previousChildCpu && now > previousNanos
                ? (childCpu - previousChildCpu) / (double) (now - previousNanos) : null;
        result.put("childCpuCoreEquivalent", cores);
        result.put("childCpuPercentOfJvmAvailableProcessors", cores == null ? null
                : cores * 100 / Runtime.getRuntime().availableProcessors());
        var os = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        double load = os instanceof com.sun.management.OperatingSystemMXBean extended ? extended.getCpuLoad() : -1;
        result.put("systemCpuLoad", load >= 0 && Double.isFinite(load) ? load : null);
        previousChildCpu = childCpu;
        previousNanos = now;
        writer.write(Json.encode(result));
        writer.newLine();
        writer.flush();
    }

    private static Map<String, Object> process(ProcessHandle handle) {
        var value = new LinkedHashMap<String, Object>();
        value.put("pid", handle.pid());
        value.put("alive", handle.isAlive());
        value.put("rssBytes", Resources.procValue(handle.pid(), "VmRSS:"));
        value.put("peakRssBytes", Resources.procValue(handle.pid(), "VmHWM:"));
        value.put("cpuNanos", handle.info().totalCpuDuration().map(java.time.Duration::toNanos).orElse(null));
        return value;
    }

    private static Long sum(java.util.List<Map<String, Object>> processes, String key) {
        long result = 0;
        for (Map<String, Object> process : processes) {
            if (!(process.get(key) instanceof Number number)) return null;
            result = Math.addExact(result, number.longValue());
        }
        return result;
    }

    @Override public void close() throws IOException { writer.close(); }
}
