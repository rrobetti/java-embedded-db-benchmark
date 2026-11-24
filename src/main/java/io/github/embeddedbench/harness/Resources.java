package io.github.embeddedbench.harness;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class Resources {
    private Resources() {}

    static Map<String, Object> snapshot(Path directory) {
        var result = new LinkedHashMap<String, Object>();
        result.put("timestamp", Instant.now().toString());
        result.put("rssBytes", procValue(ProcessHandle.current().pid(), "VmRSS:"));
        result.put("peakRssBytes", procValue(ProcessHandle.current().pid(), "VmHWM:"));
        var memory = ManagementFactory.getMemoryMXBean();
        result.put("heapUsedBytes", memory.getHeapMemoryUsage().getUsed());
        result.put("heapCommittedBytes", memory.getHeapMemoryUsage().getCommitted());
        result.put("nonHeapUsedBytes", memory.getNonHeapMemoryUsage().getUsed());
        result.put("nonHeapCommittedBytes", memory.getNonHeapMemoryUsage().getCommitted());
        var threads = ManagementFactory.getThreadMXBean();
        result.put("threads", threads.getThreadCount());
        result.put("peakThreads", threads.getPeakThreadCount());
        long gcCount = 0, gcMillis = 0;
        boolean gcCountKnown = false, gcTimeKnown = false;
        for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getCollectionCount() >= 0) { gcCount += gc.getCollectionCount(); gcCountKnown = true; }
            if (gc.getCollectionTime() >= 0) { gcMillis += gc.getCollectionTime(); gcTimeKnown = true; }
        }
        result.put("gcCollections", gcCountKnown ? gcCount : null);
        result.put("gcMillis", gcTimeKnown ? gcMillis : null);
        var os = ManagementFactory.getOperatingSystemMXBean();
        result.put("processCpuNanos", os instanceof com.sun.management.OperatingSystemMXBean extended
                && extended.getProcessCpuTime() >= 0 ? extended.getProcessCpuTime() : null);
        result.put("processReadBytes", ioValue("read_bytes:"));
        result.put("processWriteBytes", ioValue("write_bytes:"));
        result.put("databaseFileBytes", directoryBytes(directory));
        return result;
    }

    static Long procValue(long pid, String key) {
        try {
            for (String line : Files.readAllLines(Path.of("/proc", Long.toString(pid), "status"))) {
                if (line.startsWith(key)) return Math.multiplyExact(Long.parseLong(line.substring(key.length()).trim().split("\\s+")[0]), 1024);
            }
        } catch (IOException | RuntimeException ignored) {
            // Unsupported platform, inaccessible process, or an exited process.
        }
        return platformRss(pid, key);
    }

    private static Long platformRss(long pid, String key) {
        String os = System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT);
        try {
            Process process;
            long multiplier;
            if (os.contains("mac") && key.equals("VmRSS:")) {
                process = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(pid))
                        .redirectError(ProcessBuilder.Redirect.DISCARD).start();
                multiplier = 1024;
            } else if (os.contains("windows")) {
                String property = key.equals("VmHWM:") ? "PeakWorkingSet64" : "WorkingSet64";
                process = new ProcessBuilder("powershell", "-NoProfile", "-NonInteractive", "-Command",
                        "(Get-Process -Id " + pid + ")." + property)
                        .redirectError(ProcessBuilder.Redirect.DISCARD).start();
                multiplier = 1;
            } else return null;
            if (!process.waitFor(2, TimeUnit.SECONDS)) { process.destroyForcibly(); return null; }
            if (process.exitValue() != 0) return null;
            return Math.multiplyExact(Long.parseLong(new String(process.getInputStream().readNBytes(128),
                    StandardCharsets.UTF_8).trim()), multiplier);
        } catch (IOException | RuntimeException | InterruptedException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            return null;
        }
    }

    private static Long ioValue(String key) {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/io")))
                if (line.startsWith(key)) return Long.parseLong(line.substring(key.length()).trim());
        } catch (IOException | RuntimeException ignored) {}
        return null;
    }

    static Long directoryBytes(Path dir) {
        if (!Files.exists(dir)) return 0L;
        try (var files = Files.walk(dir)) {
            long total = 0;
            for (Path file : files.filter(Files::isRegularFile).toList()) total = Math.addExact(total, Files.size(file));
            return total;
        } catch (IOException | RuntimeException ignored) { return null; }
    }

    static Map<String, Object> environment() {
        var m = new LinkedHashMap<String, Object>();
        m.put("timestamp", Instant.now().toString());
        m.put("javaVersion", System.getProperty("java.version"));
        m.put("javaVendor", System.getProperty("java.vendor"));
        m.put("vmName", System.getProperty("java.vm.name"));
        m.put("vmVersion", System.getProperty("java.vm.version"));
        m.put("jvmArguments", ManagementFactory.getRuntimeMXBean().getInputArguments());
        m.put("os", System.getProperty("os.name"));
        m.put("osVersion", System.getProperty("os.version"));
        m.put("architecture", System.getProperty("os.arch"));
        m.put("logicalProcessors", Runtime.getRuntime().availableProcessors());
        var os = ManagementFactory.getOperatingSystemMXBean();
        m.put("physicalRamBytes", os instanceof com.sun.management.OperatingSystemMXBean extended
                ? extended.getTotalMemorySize() : null);
        m.put("gitCommit", git("rev-parse", "HEAD"));
        m.put("gitDirty", git("status", "--porcelain"));
        m.put("cpuModel", cpuModel());
        m.put("containerMemoryLimitBytes", cgroupLimit());
        var dependencies = new LinkedHashMap<String, Object>();
        for (String name : new String[]{"picocli.CommandLine", "org.HdrHistogram.Histogram",
                "org.h2.Driver", "org.hsqldb.jdbc.JDBCDriver", "org.sqlite.JDBC", "org.postgresql.Driver",
                "io.roastedroot.pglite4j.jdbc.PgLiteDriver"}) {
            try {
                Class<?> type = Class.forName(name, false, Resources.class.getClassLoader());
                dependencies.put(name, type.getPackage().getImplementationVersion());
            } catch (LinkageError | ClassNotFoundException e) { dependencies.put(name, null); }
        }
        m.put("dependencyVersions", dependencies);
        m.put("declaredDependencyVersions", declaredDependencies());
        m.put("measurementNotes", "Linux /proc RSS and disk I/O are process counters; null means unavailable. "
                + "JVM metrics exclude native database heaps. File bytes are not an on-disk compression ratio.");
        return m;
    }

    private static Map<String, Object> declaredDependencies() {
        var versions = new LinkedHashMap<String, Object>();
        try (var stream = Resources.class.getResourceAsStream("/META-INF/benchmark/pom.xml")) {
            if (stream == null) return versions;
            var factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            var document = factory.newDocumentBuilder().parse(stream);
            var properties = new java.util.HashMap<String, String>();
            var propertyNodes = document.getElementsByTagName("properties");
            if (propertyNodes.getLength() > 0) {
                var children = propertyNodes.item(0).getChildNodes();
                for (int i = 0; i < children.getLength(); i++) {
                    var child = children.item(i);
                    if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE)
                        properties.put(child.getNodeName(), child.getTextContent().trim());
                }
            }
            var nodes = document.getElementsByTagName("dependency");
            for (int i = 0; i < nodes.getLength(); i++) {
                var node = (org.w3c.dom.Element) nodes.item(i);
                String group = tag(node, "groupId"), artifact = tag(node, "artifactId"), version = tag(node, "version");
                if (version != null && version.startsWith("${") && version.endsWith("}"))
                    version = properties.get(version.substring(2, version.length() - 1));
                if (group != null && artifact != null) versions.put(group + ":" + artifact, version);
            }
        } catch (Exception error) { versions.put("metadataReadError", error.getClass().getSimpleName()); }
        return versions;
    }

    private static String tag(org.w3c.dom.Element node, String name) {
        var matches = node.getElementsByTagName(name);
        return matches.getLength() == 0 ? null : matches.item(0).getTextContent().trim();
    }

    private static String git(String... arguments) {
        try {
            var command = new java.util.ArrayList<String>();
            command.add("git"); command.add("--no-pager"); command.addAll(java.util.List.of(arguments));
            Process p = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.DISCARD).start();
            if (!p.waitFor(2, TimeUnit.SECONDS)) { p.destroyForcibly(); return null; }
            return p.exitValue() == 0 ? new String(p.getInputStream().readNBytes(16_384), StandardCharsets.UTF_8).trim() : null;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            return null;
        }
    }

    private static String cpuModel() {
        try {
            return Files.readAllLines(Path.of("/proc/cpuinfo")).stream()
                    .filter(line -> line.startsWith("model name") || line.startsWith("Hardware"))
                    .map(line -> line.substring(line.indexOf(':') + 1).trim()).findFirst().orElse(null);
        } catch (IOException e) { return null; }
    }

    static Long cgroupLimit() {
        for (String path : new String[]{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"}) {
            try {
                String text = Files.readString(Path.of(path)).trim();
                if (!text.equals("max")) {
                    long value = Long.parseLong(text);
                    if (value > 0 && value < Long.MAX_VALUE / 2) return value;
                }
            } catch (IOException | NumberFormatException ignored) {}
        }
        return null;
    }
}
