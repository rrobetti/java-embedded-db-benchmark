package io.github.embeddedbench.harness;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class ReportWriter {
    private static final String NOTES = "Each row is one fresh JVM. Engine order is seeded and randomized within each scenario/repetition; "
            + "runs are sequential. Percentiles are per-category HdrHistogram measurements, never averages of percentiles. "
            + "Workers have independent latency/counter collectors merged after completion, with no shared metrics lock in the hot path. "
            + "Compatible repetitions are aggregated by merging raw histograms; aggregate throughput is total successes / total elapsed time. "
            + "Latency counts successful operations only. Open-loop latency includes scheduled-arrival queue delay; rejected arrivals "
            + "are counted separately. Warmup has separate counters and histograms and a full drain barrier before measurement. "
            + "Mutating warmup workloads are validated and reset to the identical seeded dataset before measurement; "
            + "reset cost is reported separately and excluded from the measurement window. "
            + "Driver initialization is measured separately; database boot plus the anchor connection and XA probes are an inseparable composite. "
            + "The first worker connection is not the first chronological JDBC connection. "
            + "Throughput uses actual measurement elapsed time including drain. null/N/A means unavailable or not exercised. "
            + "Correctness failures and unsupported operations invalidate comparisons; errors are never silently retried. "
            + "Architecture/capability declarations are not measured performance. PGlite's single shared session is not independent transactions.";

    private ReportWriter() {}

    static void write(Path directory, Map<String, Object> environment, Map<String, Object> configuration,
                      List<Map<String, Object>> results) throws IOException {
        var report = new LinkedHashMap<String, Object>();
        report.put("schemaVersion", 1);
        report.put("environment", environment); report.put("configuration", configuration);
        report.put("methodology", NOTES); report.put("results", results);
        var aggregates = HistogramAggregation.merge(directory, results);
        report.put("aggregates", aggregates);
        report.put("largestSuccessfulDatasetsObserved", largestDatasets(results));
        BenchmarkCli.writeJson(directory.resolve("report.json"), report);
        Files.writeString(directory.resolve("report.csv"), csv(results), StandardCharsets.UTF_8);
        Files.writeString(directory.resolve("report.md"), markdown(results) + aggregateMarkdown(aggregates), StandardCharsets.UTF_8);
        Files.writeString(directory.resolve("report-aggregates.csv"), aggregateCsv(aggregates), StandardCharsets.UTF_8);
        Files.writeString(directory.resolve("report.html"), html(report, results), StandardCharsets.UTF_8);
    }

    static String csv(List<Map<String, Object>> results) {
        var b = new StringBuilder();
        csvRow(b, List.of("run_id", "engine", "scenario", "repetition", "status", "valid", "connection_mode",
                "effective_connections", "actual_jdbc_url", "requested_jdbc_url", "driver_name", "driver_version", "transport_deviation",
                "category", "attempts", "successes", "failures", "rejected",
                "error_rate", "timeout_rate", "rejection_rate", "retry_count", "retry_rate",
                "elapsed_seconds", "throughput_ops_per_second", "category_count", "category_units",
                "min_ns", "mean_ns", "p50_ns", "p90_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns", "load_ns", "logical_dataset_bytes",
                "shutdown_peak_rss_bytes", "steady_database_file_bytes", "error_class", "error"));
        for (Map<String, Object> r : results) {
            Map<?, ?> measurement = map(r.get("measurement"));
            Map<?, ?> latency = map(measurement.get("latency"));
            List<String> categories = new ArrayList<>();
            for (Object key : latency.keySet()) categories.add(key.toString());
            if (categories.isEmpty()) categories.add("");
            for (String category : categories) {
                Map<?, ?> stats = map(latency.get(category));
                var values = new ArrayList<Object>();
                for (String key : List.of("runId", "engine", "scenario", "repetition", "status", "valid",
                        "effectiveConnectionMode", "effectiveConnections", "jdbcUrl", "requestedJdbcUrl", "driverName", "driverVersion",
                        "transportDeviation")) values.add(r.get(key));
                values.add(category);
                for (String key : List.of("attempts", "successes", "failures", "rejected", "errorRate", "timeoutRate",
                        "rejectionRate", "retryCount", "retryRate", "elapsedSeconds", "throughputOpsPerSecond"))
                    values.add(measurement.get(key));
                for (String key : List.of("count", "units", "minNanos", "meanNanos", "p50Nanos", "p90Nanos", "p95Nanos", "p99Nanos", "p999Nanos", "maxNanos"))
                    values.add(stats.get(key));
                values.add(r.get("loadNanos")); values.add(r.get("logicalDatasetBytes"));
                values.add(stage(r, "shutdown").get("peakRssBytes")); values.add(stage(r, "steady").get("databaseFileBytes"));
                values.add(r.get("errorClass")); values.add(r.get("error"));
                csvRow(b, values);
            }
        }
        return b.toString();
    }

    private static void csvRow(StringBuilder b, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i != 0) b.append(',');
            Object value = values.get(i);
            if (value != null) b.append('"').append(value.toString().replace("\"", "\"\"")).append('"');
        }
        b.append("\r\n");
    }

    private static String aggregateCsv(List<Map<String, Object>> aggregates) {
            var b = new StringBuilder();
            csvRow(b, List.of("engine", "scenario", "threads", "rows", "source_runs", "category", "count",
                    "min_ns", "mean_ns", "p50_ns", "p90_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns",
                    "throughput_ops_per_second", "run_throughput_min", "run_throughput_max", "run_throughput_population_stddev"));
            for (Map<String, Object> a : aggregates) {
                Map<?, ?> config = map(a.get("configuration"));
                Map<?, ?> variability = map(a.get("throughputRunToRunVariability"));
                for (var category : map(a.get("latency")).entrySet()) {
                    var values = new ArrayList<Object>();
                    values.add(a.get("engine")); values.add(a.get("scenario"));
                    values.add(config.get("threads")); values.add(config.get("rows")); values.add(a.get("sourceRunIds"));
                    values.add(category.getKey());
                    Map<?, ?> stats = map(category.getValue());
                    for (String key : List.of("count", "minNanos", "meanNanos", "p50Nanos", "p90Nanos", "p95Nanos", "p99Nanos", "p999Nanos", "maxNanos"))
                        values.add(stats.get(key));
                    values.add(a.get("throughputOpsPerSecond")); values.add(variability.get("min"));
                    values.add(variability.get("max")); values.add(variability.get("populationStdDev"));
                    csvRow(b, values);
                }
            }
            return b.toString();
        }

    private static String aggregateMarkdown(List<Map<String, Object>> aggregates) {
            var b = new StringBuilder("\n## Compatible repetition aggregates\n\n"
                    + "Raw histograms are merged only for identical configurations. Throughput is total successes / total elapsed time. "
                    + "Run-to-run standard deviation is population standard deviation of individual run throughputs, not a latency percentile average.\n\n"
                    + "| Engine | Workload | Threads | Rows | Runs | Ops/s | Min run ops/s | Max run ops/s | Stddev ops/s |\n"
                    + "|---|---|---:|---:|---:|---:|---:|---:|---:|\n");
            for (Map<String, Object> a : aggregates) {
                Map<?, ?> config = map(a.get("configuration"));
                Map<?, ?> variability = map(a.get("throughputRunToRunVariability"));
                b.append('|').append(md(a.get("engine"))).append('|').append(md(a.get("scenario"))).append('|')
                        .append(md(config.get("threads"))).append('|').append(md(config.get("rows"))).append('|')
                        .append(((List<?>) a.get("sourceRunIds")).size()).append('|')
                        .append(formatScaled(a.get("throughputOpsPerSecond"), 1)).append('|')
                        .append(formatScaled(variability.get("min"), 1)).append('|')
                        .append(formatScaled(variability.get("max"), 1)).append('|')
                        .append(formatScaled(variability.get("populationStdDev"), 1)).append("|\n");
            }
            return b.append("\nMerged latency percentiles and source histogram paths are in `report.json` and `report-aggregates.csv`.\n").toString();
        }
    static String markdown(List<Map<String, Object>> results) {
        var b = new StringBuilder("# Embedded JDBC benchmark\n\n").append(NOTES).append("\n\n");
        b.append("## Measured runs\n\n| Run | Engine | Scenario | Status | Valid | Ops/s | Failures | Rejected | Mode | Actual JDBC URL | Transport deviation |\n")
                .append("|---|---|---|---|---|---:|---:|---:|---|---|---|\n");
        for (Map<String, Object> r : results) {
            Map<?, ?> m = map(r.get("measurement"));
            b.append('|');
            for (Object value : new Object[]{r.get("runId"), r.get("engine"), r.get("scenario"), r.get("status"),
                    r.get("valid"), m.get("throughputOpsPerSecond"), m.get("failures"), m.get("rejected"), r.get("effectiveConnectionMode"),
                    r.get("jdbcUrl"), r.get("transportDeviation")})
                b.append(' ').append(md(value)).append(" |");
            b.append('\n');
        }
        b.append("\n## Architectural compatibility (declared, not measured)\n\n")
                .append("| Engine | Independent transactions | Concurrent readers | Concurrent writers | XA | CTE | Execution | Limitations |\n")
                .append("|---|---|---|---|---|---|---|---|\n");
        for (Map<String, Object> r : uniqueCapabilities(results)) {
            Map<?, ?> c = map(r.get("capabilities"));
            b.append('|').append(md(r.get("engine"))).append('|');
            for (String key : List.of("independentTransactions", "concurrentReaders", "concurrentWriters", "xa", "cte", "executionMode", "limitations"))
                b.append(md(c.get(key))).append('|');
            b.append('\n');
        }
        b.append("\n## Per-category latency (milliseconds, successful operations only)\n\n")
                .append("| Run | Category | Count | Min | Mean | p50 | p90 | p95 | p99 | p99.9 | Max |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (Map<String, Object> r : results) {
            for (var entry : map(map(r.get("measurement")).get("latency")).entrySet()) {
                Map<?, ?> s = map(entry.getValue());
                b.append('|').append(md(r.get("runId"))).append('|').append(md(entry.getKey())).append('|').append(md(s.get("count"))).append('|');
                for (String key : List.of("minNanos", "meanNanos", "p50Nanos", "p90Nanos", "p95Nanos", "p99Nanos", "p999Nanos", "maxNanos"))
                    b.append(formatScaled(s.get(key), 1_000_000)).append('|');
                b.append('\n');
            }
        }
        b.append("\nRaw JSON contains initialization timings, all resource stages, errors, warmup counters, environment, "
                + "and complete configuration. See the standalone HTML for measured charts and the run directories "
                + "for raw `.hgr` histograms and subprocess logs. Delete snapshots follow explicit deletion of dataset rows after validation; "
                + "shutdown does not imply deletion/compaction of backing files.\n");
        for (Map<String, Object> r : results) {
            if (r.get("error") != null)
                b.append("\n- ").append(md(r.get("runId"))).append(": ").append(md(r.get("errorClass"))).append(" — ").append(md(r.get("error"))).append('\n');
        }
        return b.toString();
    }

    static String html(Map<String, Object> report, List<Map<String, Object>> results) {
        var b = new StringBuilder("""
                <!doctype html><html lang="en"><head><meta charset="utf-8">
                <meta name="viewport" content="width=device-width,initial-scale=1">
                <title>Embedded JDBC benchmark</title><style>
                :root{color-scheme:light dark}body{font:15px system-ui,sans-serif;margin:0 auto;padding:2rem;max-width:1500px}
                h1,h2{letter-spacing:-.03em}p{line-height:1.6}table{border-collapse:collapse;width:100%;font-size:13px}
                th,td{text-align:left;padding:.6rem;border-bottom:1px solid #8886;vertical-align:top}
                th{background:#8882}code,pre{font:12px ui-monospace,monospace}pre{white-space:pre-wrap;overflow-wrap:anywhere}
                .scroll{overflow:auto}.charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:1.5rem}
                .chart{padding:1rem;border:1px solid #8885;border-radius:10px}svg{width:100%;height:auto}
                svg text{fill:currentColor;font:12px system-ui}a{color:#478ef0}.bad{color:#d65e52}.ok{color:#309367}
                footer{margin-top:2rem;border-top:1px solid #8886}details{margin:1rem 0}
                </style></head><body><h1>Embedded JDBC benchmark</h1>
                """);
        b.append("<p>").append(escape(NOTES)).append("</p>");
        b.append("<h2>Measured runs</h2><div class=\"scroll\"><table><thead><tr>");
        for (String h : List.of("Run", "Engine / transport", "Workload", "Status / correctness", "Ops/s", "Failures / rejected", "Effective execution", "Actual JDBC URL", "Artifacts"))
            b.append("<th>").append(h).append("</th>");
        b.append("</tr></thead><tbody>");
        for (Map<String, Object> r : results) {
            Map<?, ?> m = map(r.get("measurement"));
            b.append("<tr>");
            cell(b, r.get("runId")); cell(b, r.get("engine") + (Boolean.TRUE.equals(r.get("transportDeviation"))
                    ? " — ADAPTER TRANSPORT DEVIATION" : "")); cell(b, r.get("scenario"));
            b.append("<td class=\"").append(Boolean.TRUE.equals(r.get("valid")) && "OK".equals(r.get("status")) ? "ok" : "bad")
                    .append("\">").append(escape(r.get("status"))).append(" / ").append(escape(r.get("valid"))).append("</td>");
            cell(b, formatScaled(m.get("throughputOpsPerSecond"), 1));
            cell(b, shown(m.get("failures")) + " / " + shown(m.get("rejected")));
            cell(b, shown(r.get("effectiveConnectionMode")) + " (" + shown(r.get("effectiveConnections")) + " connections; "
                    + shown(r.get("effectiveWorkers")) + " workers)");
            cell(b, r.get("jdbcUrl"));
            b.append("<td>");
            if (r.get("artifactDirectory") != null) {
                String path = escape(r.get("artifactDirectory"));
                b.append("<a href=\"").append(path).append("/result.json\">JSON</a> · <a href=\"")
                        .append(path).append("/stderr.log\">stderr</a>");
            } else b.append("N/A");
            b.append("</td></tr>");
        }
        b.append("</tbody></table></div><h2>Collected measurements</h2><p>Charts include only correctness-valid, error-free runs. "
                + "No cross-run percentile averaging or inferred results.</p><div class=\"charts\">");
        b.append(chart("Throughput", "ops/s", results, r -> numeric(map(r.get("measurement")).get("throughputOpsPerSecond")), 1));
        b.append(chart("Dataset ingestion logical throughput", "MiB/s", results,
                r -> numeric(r.get("loadLogicalBytesPerSecond")), 1024 * 1024));
        for (String category : List.of("read", "write", "analytical", "transaction"))
            b.append(chart(category + " latency p99", "ms", results,
                    r -> numeric(map(map(map(r.get("measurement")).get("latency")).get(category)).get("p99Nanos")), 1_000_000));
        b.append(chart("Process peak resident memory", "MiB", results,
                r -> numeric(stage(r, "shutdown").get("peakRssBytes")), 1024 * 1024));
        b.append(chart("Database backing-file size after measurement", "MiB", results,
                r -> numeric(stage(r, "steady").get("databaseFileBytes")), 1024 * 1024));
        b.append(chart("Observed operation error rate", "fraction", results,
                r -> numeric(map(r.get("measurement")).get("errorRate")), 1, false));
        b.append(chart("Observed rejection rate", "fraction", results,
                r -> numeric(map(r.get("measurement")).get("rejectionRate")), 1, false));
        b.append("</div><p>Error and rejection charts include errored runs when counters were collected; other charts require valid error-free runs. "
                + "Labels show measured worker count (t), logical base rows (n), and repetition (r).</p>"
                + "<h2>Per-category latency</h2><div class=\"scroll\"><table><thead><tr>");
        for (String h : List.of("Run", "Category", "Count", "Min ms", "Mean ms", "p50 ms", "p90 ms", "p95 ms", "p99 ms", "p99.9 ms", "Max ms"))
            b.append("<th>").append(h).append("</th>");
        b.append("</tr></thead><tbody>");
        for (Map<String, Object> r : results) {
            for (var entry : map(map(r.get("measurement")).get("latency")).entrySet()) {
                Map<?, ?> s = map(entry.getValue());
                b.append("<tr>"); cell(b, r.get("runId")); cell(b, entry.getKey()); cell(b, s.get("count"));
                for (String key : List.of("minNanos", "meanNanos", "p50Nanos", "p90Nanos", "p95Nanos", "p99Nanos", "p999Nanos", "maxNanos"))
                    cell(b, formatScaled(s.get(key), 1_000_000));
                b.append("</tr>");
            }
        }
        b.append("</tbody></table></div><h2>Merged compatible repetitions</h2>"
                + "<p>Only identical engine, workload, configuration and execution mode are merged. "
                + "Counts and raw latency histograms are added; percentiles are recomputed, never averaged.</p>");
        for (Object item : (List<?>) report.getOrDefault("aggregates", List.of())) {
            Map<?, ?> aggregate = map(item);
            b.append("<details><summary>").append(escape(aggregate.get("engine"))).append("/")
                    .append(escape(aggregate.get("scenario"))).append(" — ").append(escape(aggregate.get("sourceRunIds")))
                    .append("</summary><pre>").append(escape(Json.encode(aggregate))).append("</pre></details>");
        }
        b.append("<h2>Architectural compatibility — declarations, not measurements</h2>"
                + "<p>A JDBC connection handle need not be an independent native database session. "
                + "PGlite is reported as a serialized shared session and is not compared as independent transaction concurrency.</p>"
                + "<div class=\"scroll\"><table><thead><tr>");
        for (String h : List.of("Engine", "Independent transactions", "Concurrent readers", "Concurrent writers", "XA", "CTE", "Execution", "Limitations"))
            b.append("<th>").append(h).append("</th>");
        b.append("</tr></thead><tbody>");
        for (Map<String, Object> r : uniqueCapabilities(results)) {
            Map<?, ?> c = map(r.get("capabilities"));
            b.append("<tr>"); cell(b, r.get("engine"));
            for (String key : List.of("independentTransactions", "concurrentReaders", "concurrentWriters", "xa", "cte", "executionMode", "limitations"))
                cell(b, c.get(key));
            b.append("</tr>");
        }
        b.append("</tbody></table></div><h2>Resource stages and initialization</h2>"
                + "<p>Baseline/init/schema/load/steady/delete/shutdown are actual snapshots. Delete follows explicit row deletion after validation. "
                + "Java heap/nonheap/GC/CPU/thread counters and Linux RSS/disk-I/O counters are distinct. "
                + "A null counter is unsupported, not zero. Database files are not deleted at shutdown.</p>");
        for (Map<String, Object> r : results) {
            b.append("<details><summary>").append(escape(r.get("runId"))).append(" — resource stages, warmup and errors</summary><pre>")
                    .append(escape(Json.encode(r))).append("</pre></details>");
        }
        b.append("<h2>Largest successful datasets observed</h2><p>These are observations in this suite under each recorded memory/workload/concurrency configuration, "
                + "not extrapolated capacity limits.</p><pre>").append(escape(Json.encode(report.get("largestSuccessfulDatasetsObserved")))).append("</pre>");
        b.append("<h2>Reproducibility</h2><details open><summary>Environment and exact configuration</summary><pre>")
                .append(escape(Json.encode(report.get("environment")))).append("\n\n")
                .append(escape(Json.encode(report.get("configuration")))).append("</pre></details>"
                        + "<footer><p>Standalone HTML: no CDN, remote fonts, scripts, or external chart libraries. "
                        + "Raw reports: <a href=\"report.json\">JSON</a> · <a href=\"report.csv\">CSV</a> · <a href=\"report.md\">Markdown</a>.</p></footer>"
                        + "</body></html>\n");
        return b.toString();
    }

    @FunctionalInterface private interface Metric { Double value(Map<String, Object> result); }
    private static String chart(String title, String unit, List<Map<String, Object>> results, Metric metric, double divisor) {
        return chart(title, unit, results, metric, divisor, true);
    }
    private static String chart(String title, String unit, List<Map<String, Object>> results, Metric metric, double divisor, boolean requireValid) {
        var labels = new ArrayList<String>();
        var values = new ArrayList<Double>();
        for (Map<String, Object> r : results) {
            if (requireValid && (!Boolean.TRUE.equals(r.get("valid")) || !"OK".equals(r.get("status")))) continue;
            Double value = metric.value(r);
            if (value == null || !Double.isFinite(value) || value < 0) continue;
            Map<?, ?> configuration = map(r.get("configuration"));
            labels.add(r.get("engine") + "/" + r.get("scenario") + "/t" + configuration.get("threads")
                    + "/n" + configuration.get("rows") + "/r" + r.get("repetition"));
            values.add(value / divisor);
        }
        if (values.isEmpty()) return "";
        double maximum = values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
        int height = 45 + labels.size() * 30;
        var b = new StringBuilder("<section class=\"chart\"><h3>").append(escape(title)).append(" (").append(unit)
                .append(")</h3><svg role=\"img\" aria-label=\"").append(escape(title)).append("\" viewBox=\"0 0 650 ")
                .append(height).append("\"><title>").append(escape(title)).append("</title>");
        for (int i = 0; i < values.size(); i++) {
            int y = 15 + 30 * i;
            double width = maximum == 0 ? 0 : 330 * values.get(i) / maximum;
            b.append("<text x=\"0\" y=\"").append(y + 15).append("\">").append(escape(labels.get(i))).append("</text>");
            b.append("<rect x=\"220\" y=\"").append(y).append("\" width=\"").append(String.format(Locale.ROOT, "%.3f", width))
                    .append("\" height=\"21\" rx=\"3\" fill=\"#478ef0\"><title>").append(formatScaled(values.get(i), 1))
                    .append(' ').append(unit).append("</title></rect>");
            b.append("<text x=\"560\" y=\"").append(y + 15).append("\">").append(formatScaled(values.get(i), 1)).append("</text>");
        }
        return b.append("</svg></section>").toString();
    }

    private static List<Map<String, Object>> uniqueCapabilities(List<Map<String, Object>> results) {
        var unique = new LinkedHashMap<String, Map<String, Object>>();
        for (Map<String, Object> result : results) {
            if (result.get("capabilities") != null) {
                Map<?, ?> configuration = map(result.get("configuration"));
                unique.putIfAbsent(result.get("engine") + "/" + configuration.get("dbProfile"), result);
            }
        }
        return new ArrayList<>(unique.values());
    }

    private static List<Map<String, Object>> largestDatasets(List<Map<String, Object>> results) {
        var largest = new LinkedHashMap<String, Map<String, Object>>();
        for (Map<String, Object> r : results) {
            if (!Boolean.TRUE.equals(r.get("valid")) || !"OK".equals(r.get("status"))) continue;
            Map<?, ?> config = map(r.get("configuration"));
            if (!(config.get("rows") instanceof Number rows)) continue;
            String key = r.get("engine") + "/" + r.get("scenario") + "/" + config.get("jvmHeapBytes")
                    + "/" + r.get("effectiveMemoryBudgetBytes") + "/" + config.get("threads") + "/"
                    + r.get("effectiveConnectionMode") + "/" + config.get("requestRate");
            Map<String, Object> previous = largest.get(key);
            if (previous == null || ((Number) previous.get("rows")).longValue() < rows.longValue()) {
                var observation = new LinkedHashMap<String, Object>();
                observation.put("engine", r.get("engine")); observation.put("scenario", r.get("scenario"));
                observation.put("threads", config.get("threads")); observation.put("requestRate", config.get("requestRate"));
                observation.put("effectiveConnectionMode", r.get("effectiveConnectionMode"));
                observation.put("jvmHeapBytes", config.get("jvmHeapBytes"));
                observation.put("effectiveMemoryBudgetBytes", r.get("effectiveMemoryBudgetBytes"));
                observation.put("rows", rows); observation.put("logicalDatasetBytes", r.get("logicalDatasetBytes"));
                observation.put("sourceRunId", r.get("runId"));
                observation.put("interpretation", "Largest successful dataset observed in this suite, not an extrapolated capacity limit");
                largest.put(key, observation);
            }
        }
        return new ArrayList<>(largest.values());
    }

    private static Map<?, ?> stage(Map<String, Object> result, String name) { return map(map(result.get("stages")).get(name)); }
    private static Map<?, ?> map(Object value) { return value instanceof Map<?, ?> m ? m : Map.of(); }
    private static Double numeric(Object value) { return value instanceof Number n ? n.doubleValue() : null; }
    private static String shown(Object value) { return value == null ? "N/A" : value.toString(); }
    private static String md(Object value) { return shown(value).replace("|", "\\|").replace("\n", " ").replace("\r", " "); }
    private static String formatScaled(Object value, double divisor) {
        Double n = numeric(value);
        return n == null ? "N/A" : String.format(Locale.ROOT, "%.3f", n / divisor);
    }
    private static void cell(StringBuilder b, Object value) { b.append("<td>").append(escape(shown(value))).append("</td>"); }
    static String escape(Object value) {
        return shown(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#39;");
    }
}
