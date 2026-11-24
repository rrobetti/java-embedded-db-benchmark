package io.github.embeddedbench.harness;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class Measurements {
    private final Map<String, Histogram> histograms = new LinkedHashMap<>();
    private final Map<String, Long> units = new LinkedHashMap<>();
    private final long[] standardUnits = new long[4];
    private final Map<String, String> categoryAliases = new java.util.HashMap<>();
    private final Map<String, Long> errors = new LinkedHashMap<>();
    private final Map<String, String> errorSamples = new LinkedHashMap<>();
    private long attempts;
    private long successes;
    private long rejected;
    private long failed;

    Measurements() {
        for (String category : new String[]{"read", "write", "analytical", "transaction"})
            histograms.put(category, histogram());
    }

    private static Histogram histogram() {
        Histogram h = new Histogram(3);
        h.setAutoResize(true);
        return h;
    }

    synchronized void success(String category, long count, long latencyNanos) {
        String normalized = category;
        Histogram h = histograms.get(category);
        if (h == null) {
            normalized = categoryAliases.get(category);
            if (normalized == null) {
                if (category == null || !category.matches("[a-zA-Z0-9_-]+"))
                    throw new IllegalArgumentException("Invalid operation category");
                normalized = category.toLowerCase(Locale.ROOT);
                categoryAliases.put(category, normalized);
            }
            h = histograms.computeIfAbsent(normalized, ignored -> histogram());
        }
        h.recordValue(Math.max(1, latencyNanos));
        int index = standardCategory(normalized);
        if (index >= 0) standardUnits[index] += count;
        else units.merge(normalized, count, Long::sum);
        attempts++; successes++;
    }

    private static int standardCategory(String category) {
        return switch (category) {
            case "read" -> 0;
            case "write" -> 1;
            case "analytical" -> 2;
            case "transaction" -> 3;
            default -> -1;
        };
    }

    private long unitsFor(String category) {
        int index = standardCategory(category);
        return index >= 0 ? standardUnits[index] : units.getOrDefault(category, 0L);
    }

    synchronized void failure(Throwable error) {
        attempts++; failed++;
        String kind = classify(error);
        errors.merge(kind, 1L, Long::sum);
        errorSamples.putIfAbsent(kind, describe(error));
    }

    synchronized void reject() {
        attempts++; rejected++;
        errors.merge("REJECTED", 1L, Long::sum);
    }

    synchronized void merge(Measurements other) {
        if (other == this) throw new IllegalArgumentException("Cannot merge a collector with itself");
        synchronized (other) {
            attempts += other.attempts; successes += other.successes;
            failed += other.failed; rejected += other.rejected;
            for (int i = 0; i < standardUnits.length; i++) standardUnits[i] += other.standardUnits[i];
            for (var entry : other.histograms.entrySet())
                histograms.computeIfAbsent(entry.getKey(), ignored -> histogram()).add(entry.getValue());
            other.units.forEach((key, value) -> units.merge(key, value, Long::sum));
            other.errors.forEach((key, value) -> errors.merge(key, value, Long::sum));
            other.errorSamples.forEach(errorSamples::putIfAbsent);
        }
    }

    synchronized Map<String, Object> report(double seconds, double offeredSeconds) {
        var result = new LinkedHashMap<String, Object>();
        result.put("attempts", attempts); result.put("successes", successes);
        result.put("failures", failed); result.put("rejected", rejected);
        result.put("elapsedSeconds", seconds); result.put("offeredWindowSeconds", offeredSeconds);
        result.put("throughputOpsPerSecond", seconds > 0 ? successes / seconds : null);
        result.put("errorRate", attempts == 0 ? null : (double) failed / attempts);
        result.put("timeoutRate", attempts == 0 ? null : (double) errors.getOrDefault("TIMEOUT", 0L) / attempts);
        result.put("rejectionRate", attempts == 0 ? null : (double) rejected / attempts);
        result.put("retryCount", 0);
        result.put("retryRate", attempts == 0 ? null : 0.0);
        result.put("errors", new LinkedHashMap<>(errors)); result.put("errorSamples", new LinkedHashMap<>(errorSamples));
        var latency = new LinkedHashMap<String, Object>();
        for (var entry : histograms.entrySet()) {
            Histogram h = entry.getValue();
            var stats = new LinkedHashMap<String, Object>();
            stats.put("count", h.getTotalCount()); stats.put("units", unitsFor(entry.getKey()));
            stats.putAll(histogramStats(h));
            latency.put(entry.getKey(), stats);
        }
        result.put("latency", latency);
        return result;
    }

    static Map<String, Object> histogramStats(Histogram h) {
        var stats = new LinkedHashMap<String, Object>();
        stats.put("minNanos", h.getTotalCount() == 0 ? null : h.getMinValue());
        stats.put("meanNanos", h.getTotalCount() == 0 ? null : h.getMean());
        stats.put("p50Nanos", h.getTotalCount() == 0 ? null : h.getValueAtPercentile(50));
        stats.put("p90Nanos", h.getTotalCount() == 0 ? null : h.getValueAtPercentile(90));
        stats.put("p95Nanos", h.getTotalCount() == 0 ? null : h.getValueAtPercentile(95));
        stats.put("p99Nanos", h.getTotalCount() == 0 ? null : h.getValueAtPercentile(99));
        stats.put("p999Nanos", h.getTotalCount() == 0 ? null : h.getValueAtPercentile(99.9));
        stats.put("maxNanos", h.getTotalCount() == 0 ? null : h.getMaxValue());
        return stats;
    }

    synchronized void save(Path directory, String prefix, double elapsedSeconds) throws IOException {
        Files.createDirectories(directory);
        for (var entry : histograms.entrySet()) {
            try (var out = Files.newOutputStream(directory.resolve(prefix + "-" + entry.getKey() + ".hgr"))) {
                var writer = new HistogramLogWriter(out);
                writer.outputComment("Latency in nanoseconds; success only; phase=" + prefix + "; category=" + entry.getKey());
                writer.outputLogFormatVersion();
                writer.outputIntervalHistogram(0.0, elapsedSeconds, entry.getValue(), 1.0);
                writer.close();
            }
        }
    }

    static String describe(Throwable error) {
        String text = error.getClass().getName() + ": " + String.valueOf(error.getMessage());
        if (error instanceof SQLException sql) text += " [SQLState=" + sql.getSQLState() + ", code=" + sql.getErrorCode() + "]";
        return text.length() > 2_000 ? text.substring(0, 2_000) : text;
    }

    static String classify(Throwable error) {
        if (error instanceof OutOfMemoryError) return "OOM";
        if (error instanceof SQLFeatureNotSupportedException || error instanceof UnsupportedOperationException) return "UNSUPPORTED";
        if (error instanceof InterruptedException || error instanceof java.util.concurrent.CancellationException) return "CANCELLED";
        String text = String.valueOf(error.getMessage()).toLowerCase(Locale.ROOT);
        String state = error instanceof SQLException sql ? sql.getSQLState() : null;
        if ("BENCH_CORRECTNESS".equals(state)) return "CORRECTNESS";
        if (text.contains("out of memory") || text.contains("outofmemory")) return "OOM";
        if (text.contains("deadlock") || "40P01".equals(state)) return "DEADLOCK";
        if ("40001".equals(state)) return "SERIALIZATION";
        int code = error instanceof SQLException sql ? sql.getErrorCode() & 0xff : -1;
        if (code == 6 || text.contains("sqlite_locked") || text.contains("table is locked")) return "SQL_LOCKED";
        if (code == 5 || text.contains("sqlite_busy") || text.contains("database is busy") || text.contains("database is locked")) return "SQL_BUSY";
        if (error instanceof java.sql.SQLTimeoutException || text.contains("timeout") || text.contains("timed out")
                || "57014".equals(state) || "HYT00".equals(state)) return "TIMEOUT";
        if (text.contains("unsupported") || text.contains("not supported") || (state != null && state.startsWith("0A"))) return "UNSUPPORTED";
        if (state != null && state.startsWith("23")) return "CONSTRAINT";
        if (state != null && state.startsWith("08")) return "CONNECTION";
        if (error instanceof SQLException) return "SQL_ERROR";
        return "ERROR";
    }
}
