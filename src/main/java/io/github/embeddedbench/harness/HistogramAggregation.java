package io.github.embeddedbench.harness;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class HistogramAggregation {
    private HistogramAggregation() {}

    static List<Map<String, Object>> merge(Path directory, List<Map<String, Object>> results) throws IOException {
        var groups = new LinkedHashMap<String, Group>();
        for (Map<String, Object> result : results) {
            if (!Boolean.TRUE.equals(result.get("valid")) || !"OK".equals(result.get("status"))) continue;
            if (!(result.get("measurement") instanceof Map<?, ?>)) continue;
            String key = result.get("engine") + "/" + result.get("scenario") + "/" + Json.encode(result.get("configuration"))
                    + "/" + result.get("effectiveConnectionMode");
            Group group = groups.computeIfAbsent(key, ignored -> new Group(result));
            group.sources.add(result.get("runId").toString());
            Map<?, ?> measurement = (Map<?, ?>) result.get("measurement");
            group.successes += ((Number) measurement.get("successes")).longValue();
            group.seconds += ((Number) measurement.get("elapsedSeconds")).doubleValue();
            group.rates.add(((Number) measurement.get("throughputOpsPerSecond")).doubleValue());
            Map<?, ?> categories = (Map<?, ?>) measurement.get("latency");
            Path artifacts = directory.resolve(result.get("artifactDirectory").toString()).normalize();
            if (!artifacts.startsWith(directory)) throw new IOException("Histogram path escapes output directory");
            for (var entry : categories.entrySet()) {
                String category = entry.getKey().toString();
                BenchmarkOptions.safeIdentifier(category);
                try (var stream = Files.newInputStream(artifacts.resolve("latency-" + category + ".hgr"))) {
                    var reader = new HistogramLogReader(stream);
                    Histogram histogram = (Histogram) reader.nextIntervalHistogram();
                    if (histogram == null) throw new IOException("Missing histogram interval");
                    Histogram existing = group.histograms.get(category);
                    if (existing == null) group.histograms.put(category, histogram.copy());
                    else existing.add(histogram);
                    Map<?, ?> stats = (Map<?, ?>) entry.getValue();
                    group.units.merge(category, ((Number) stats.get("units")).longValue(), Long::sum);
                }
            }
        }
        var reports = new ArrayList<Map<String, Object>>();
        if (groups.isEmpty()) return reports;
        Files.createDirectories(directory.resolve("aggregates"));
        int sequence = 0;
        for (Group group : groups.values()) {
            var result = new LinkedHashMap<String, Object>();
            result.put("engine", group.first.get("engine")); result.put("scenario", group.first.get("scenario"));
            result.put("configuration", group.first.get("configuration"));
            result.put("effectiveConnectionMode", group.first.get("effectiveConnectionMode"));
            result.put("sourceRunIds", group.sources); result.put("successes", group.successes);
            result.put("elapsedSeconds", group.seconds);
            result.put("throughputOpsPerSecond", group.seconds == 0 ? null : group.successes / group.seconds);
            double mean = group.rates.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            double variance = group.rates.stream().mapToDouble(rate -> (rate - mean) * (rate - mean)).average().orElseThrow();
            var variability = new LinkedHashMap<String, Object>();
            variability.put("runThroughputsOpsPerSecond", group.rates);
            variability.put("min", group.rates.stream().mapToDouble(Double::doubleValue).min().orElseThrow());
            variability.put("max", group.rates.stream().mapToDouble(Double::doubleValue).max().orElseThrow());
            variability.put("mean", mean); variability.put("populationStdDev", Math.sqrt(variance));
            result.put("throughputRunToRunVariability", variability);
            var latency = new LinkedHashMap<String, Object>();
            for (var entry : group.histograms.entrySet()) {
                var stats = new LinkedHashMap<String, Object>(Measurements.histogramStats(entry.getValue()));
                stats.put("count", entry.getValue().getTotalCount());
                stats.put("units", group.units.getOrDefault(entry.getKey(), 0L));
                String filename = "aggregates/merged-" + sequence + "-" + entry.getKey() + ".hgr";
                stats.put("histogramFile", filename);
                try (var stream = Files.newOutputStream(directory.resolve(filename))) {
                    var writer = new HistogramLogWriter(stream);
                    writer.outputComment("Merged compatible successful runs; nanoseconds; no percentile averaging");
                    writer.outputLogFormatVersion();
                    writer.outputIntervalHistogram(0, group.seconds, entry.getValue(), 1);
                    writer.close();
                }
                latency.put(entry.getKey(), stats);
            }
            result.put("latency", latency);
            reports.add(result);
            sequence++;
        }
        return reports;
    }

    private static final class Group {
        final Map<String, Object> first;
        final List<String> sources = new ArrayList<>();
        final Map<String, Histogram> histograms = new LinkedHashMap<>();
        final Map<String, Long> units = new LinkedHashMap<>();
        final List<Double> rates = new ArrayList<>();
        long successes;
        double seconds;
        Group(Map<String, Object> first) { this.first = first; }
    }
}
