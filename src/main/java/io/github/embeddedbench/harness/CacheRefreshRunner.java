package io.github.embeddedbench.harness;

import io.github.embeddedbench.db.DatabaseAdapter;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** A bounded cache fixture with readers running across a real JDBC refresh. */
final class CacheRefreshRunner {
    private CacheRefreshRunner() {}

    static Map<String, Object> run(DatabaseAdapter adapter, Connection setup, List<Connection> connections,
                                   boolean serialized, BenchmarkOptions options, Path directory,
                                   ResourceSampler sampler) throws Exception {
        var result = new LinkedHashMap<String, Object>();
        result.put("semantics", "A separate cache_entries table contains one extra record per base row. "
                + "Reads are single-row transactions. Updates may expose old or new values; invalidations may "
                + "expose absence. Replacement deletes and reinserts its selected range in one transaction; "
                + "readers must never observe a missing replacement row. Overlapping reads are attributed to "
                + "the refresh interval. Optional pacing is closed-loop, not an open-loop arrival guarantee. "
                + "Blocking duration is unavailable, not inferred from latency.");
        result.put("cacheEntries", options.rows);
        result.put("transactionIsolation", setup.getTransactionIsolation());
        sampler.phase("cache-initialization");
        long start = System.nanoTime();
        initialize(setup, options);
        result.put("cacheInitializationNanos", System.nanoTime() - start);
        validate(setup, options.rows, 0, false);
        result.put("afterCacheInitializationResources", Resources.snapshot(directory));
        if (!serialized) {
            if (options.connections != options.threads)
                throw new UnsupportedOperationException("Cache refresh requires one dedicated connection per reader plus a writer");
            for (int i = 0; i < options.threads; i++) connections.add(adapter.openConnection());
        }
        List<Connection> readers = serialized ? List.of(setup) : connections.subList(1, connections.size());
        long affected = options.rows / 100 * options.refreshPercent
                + (options.rows % 100 * options.refreshPercent + 99) / 100;
        result.put("affectedEntries", affected);
        boolean invalidation = isInvalidation(options.refreshStrategy);
        var readGroups = new ArrayList<List<Measurements>>();
        for (int phase = 0; phase < 3; phase++) {
            var group = new ArrayList<Measurements>();
            for (int worker = 0; worker < options.threads; worker++) group.add(new Measurements());
            readGroups.add(group);
        }
        var sources = new ArrayList<Measurements>();
        readGroups.forEach(sources::addAll);
        var alive = new AtomicBoolean(true);
        var fatal = new AtomicReference<Throwable>();
        var refreshStart = new AtomicLong();
        var refreshEnd = new AtomicLong();
        var ready = new CountDownLatch(options.threads);
        var begin = new CountDownLatch(1);
        Object lock = new Object();
        var workers = new ArrayList<Thread>();
        for (int i = 0; i < options.threads; i++) {
            final int index = i;
            Connection connection = serialized ? setup : readers.get(i);
            Thread worker = new Thread(() -> readLoop(connection, serialized, lock, options, invalidation,
                    readGroups, index, alive, fatal, refreshStart, refreshEnd, ready, begin), "cache-reader-" + i);
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
        }
        long beforeStart = 0, duringStart = 0, afterStart = 0, end = 0;
        Throwable failure = null;
        try {
            ready.await();
            throwIfFatal(fatal);
            sampler.phase("refresh-before");
            sampler.startPerformance(sources);
            beforeStart = System.nanoTime();
            begin.countDown();
            sleep(options.refreshBefore);
            throwIfFatal(fatal);
            result.put("beforeRefreshResources", Resources.snapshot(directory));
            sampler.phase("refresh-during");
            duringStart = System.nanoTime();
            refreshStart.set(duringStart);
            if (serialized) {
                synchronized (lock) { refresh(setup, options, affected); }
            } else refresh(setup, options, affected);
            afterStart = System.nanoTime();
            refreshEnd.set(afterStart);
            result.put("refreshNanos", afterStart - duringStart);
            result.put("afterRefreshResources", Resources.snapshot(directory));
            sampler.phase("refresh-after");
            sleep(options.refreshAfter);
            throwIfFatal(fatal);
            end = System.nanoTime();
        } catch (Throwable error) {
            failure = error;
            throw error;
        } finally {
            alive.set(false);
            begin.countDown();
            Throwable cleanup = null;
            for (Thread worker : workers) {
                try {
                    worker.join(TimeUnit.SECONDS.toMillis(5));
                    if (worker.isAlive()) {
                        worker.interrupt();
                        cleanup = append(cleanup, new SQLException("Cache reader did not stop", "HYT00"));
                    }
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    cleanup = append(cleanup, error);
                }
            }
            try { sampler.stopPerformance(); } catch (Throwable error) { cleanup = append(cleanup, error); }
            if (cleanup != null) {
                if (failure != null) failure.addSuppressed(cleanup);
                else if (cleanup instanceof Exception exception) throw exception;
                else throw (Error) cleanup;
            }
        }
        throwIfFatal(fatal);
        result.put("afterReadersResources", Resources.snapshot(directory));
        sampler.phase("validation");
        validate(setup, options.rows, affected, invalidation);
        result.put("cacheValid", true);
        var summaries = new LinkedHashMap<String, Object>();
        String[] names = {"before", "during", "after"};
        double[] seconds = {(duringStart - beforeStart) / 1e9, (afterStart - duringStart) / 1e9,
                (end - afterStart) / 1e9};
        long failures = 0;
        for (int phase = 0; phase < 3; phase++) {
            var merged = new Measurements();
            for (Measurements local : readGroups.get(phase)) merged.merge(local);
            var report = merged.report(seconds[phase], seconds[phase]);
            failures += ((Number) report.get("failures")).longValue();
            summaries.put(names[phase], report);
            merged.save(options.output, "refresh-" + names[phase], seconds[phase]);
        }
        result.put("reads", summaries);
        result.put("readFailures", failures);
        result.put("blockingDurationNanos", null);
        return result;
    }

    private static void readLoop(Connection connection, boolean serialized, Object lock, BenchmarkOptions options,
                                 boolean allowMissing, List<List<Measurements>> groups, int index, AtomicBoolean alive,
                                 AtomicReference<Throwable> fatal, AtomicLong refreshStart, AtomicLong refreshEnd,
                                 CountDownLatch ready, CountDownLatch begin) {
        PreparedStatement statement = null;
        try {
            if (serialized) {
                synchronized (lock) { statement = connection.prepareStatement("SELECT id,cache_value FROM cache_entries WHERE id=?"); }
            } else statement = connection.prepareStatement("SELECT id,cache_value FROM cache_entries WHERE id=?");
            ready.countDown();
            begin.await();
            var random = new SplittableRandom(options.seed + 0x9e3779b97f4a7c15L * index);
            while (alive.get() && fatal.get() == null) {
                long submitted = System.nanoTime();
                Throwable error = null;
                try {
                    long id = random.nextLong(options.rows) + 1;
                    if (serialized) {
                        synchronized (lock) { read(statement, id, allowMissing, options, refreshStart); }
                    } else read(statement, id, allowMissing, options, refreshStart);
                } catch (Throwable exception) { error = exception; }
                long completed = System.nanoTime();
                long from = refreshStart.get(), to = refreshEnd.get();
                int phase = from == 0 || completed < from ? 0
                        : submitted <= (to == 0 ? Long.MAX_VALUE : to) ? 1 : 2;
                Measurements measurement = groups.get(phase).get(index);
                if (error == null) measurement.success("read", 1, completed - submitted);
                else {
                    measurement.failure(error);
                    String kind = Measurements.classify(error);
                    if (!(error instanceof SQLException) || kind.equals("CORRECTNESS") || kind.equals("UNSUPPORTED")) {
                        fatal.compareAndSet(null, error);
                        break;
                    }
                }
                if (options.refreshReadRate > 0) {
                    double period = options.threads * 1_000_000_000.0 / options.refreshReadRate;
                    long delay = period >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) period;
                    long remaining = delay - (System.nanoTime() - submitted);
                    while (remaining > 0 && alive.get() && fatal.get() == null) {
                        java.util.concurrent.locks.LockSupport.parkNanos(Math.min(remaining, 10_000_000L));
                        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Cache reader interrupted");
                        remaining = delay - (System.nanoTime() - submitted);
                    }
                }
            }
        } catch (Throwable error) {
            fatal.compareAndSet(null, error);
            ready.countDown();
        } finally {
            if (statement != null) {
                try {
                    if (serialized) synchronized (lock) { statement.close(); }
                    else statement.close();
                } catch (SQLException error) { fatal.compareAndSet(null, error); }
            }
        }
    }

    private static void read(PreparedStatement statement, long id, boolean allowMissing, BenchmarkOptions options,
                             AtomicLong refreshStart) throws SQLException {
        statement.setLong(1, id);
        try (ResultSet rs = statement.executeQuery()) {
            long affected = options.rows / 100 * options.refreshPercent
                    + (options.rows % 100 * options.refreshPercent + 99) / 100;
            if (!rs.next()) {
                if (!allowMissing || id > affected || refreshStart.get() == 0)
                    throw incorrect("Reader observed an absent cache entry " + id);
                return;
            }
            String value = rs.getString(2);
            boolean initial = ("initial-" + id).equals(value);
            boolean refreshed = !allowMissing && ("refresh-" + id).equals(value)
                    && id <= affected && refreshStart.get() != 0;
            if (rs.getLong(1) != id || !initial && !refreshed || rs.next())
                throw incorrect("Reader observed an invalid cache entry " + id);
        }
    }

    private static void initialize(Connection connection, BenchmarkOptions options) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE cache_entries (id BIGINT PRIMARY KEY, cache_value VARCHAR(100) NOT NULL, expires_at BIGINT NOT NULL)");
        }
        transaction(connection, () -> {
            try (var statement = connection.prepareStatement("INSERT INTO cache_entries VALUES (?,?,?)")) {
                long sinceCommit = 0;
                int pending = 0;
                for (long id = 1; id <= options.rows; id++) {
                    bindCache(statement, id, false);
                    statement.addBatch();
                    pending++;
                    if (pending == options.batch || options.loadCommitInterval > 0
                            && sinceCommit + pending == options.loadCommitInterval) {
                        checkBatch(statement.executeBatch(), pending);
                        statement.clearBatch();
                        sinceCommit += pending;
                        pending = 0;
                        if (options.loadCommitInterval > 0 && sinceCommit == options.loadCommitInterval) {
                            connection.commit();
                            sinceCommit = 0;
                        }
                    }
                }
                if (pending > 0) checkBatch(statement.executeBatch(), pending);
            }
        });
    }

    private static void refresh(Connection connection, BenchmarkOptions options, long affected) throws SQLException {
        switch (options.refreshStrategy) {
            case "record-update" -> update(connection, 1, affected, false, options.batch);
            case "record-invalidate" -> {
                try (var statement = connection.prepareStatement("DELETE FROM cache_entries WHERE id=?")) {
                    for (long id = 1; id <= affected; id++) {
                        statement.setLong(1, id);
                        if (statement.executeUpdate() != 1) throw incorrect("Individual invalidation count");
                    }
                }
            }
            case "bulk-invalidate", "expiration" -> transaction(connection, () -> {
                String predicate = options.refreshStrategy.equals("expiration") ? "expires_at<=?" : "id<=?";
                try (var statement = connection.prepareStatement("DELETE FROM cache_entries WHERE " + predicate)) {
                    statement.setLong(1, affected);
                    if (statement.executeUpdate() != affected) throw incorrect("Bulk invalidation count");
                }
            });
            case "incremental-refresh" -> {
                for (long first = 1; first <= affected; first += options.transactionSize) {
                    long from = first, last = Math.min(affected, first + options.transactionSize - 1L);
                    transaction(connection, () -> update(connection, from, last, true, options.batch));
                }
            }
            case "full-table-refresh" -> transaction(connection, () -> update(connection, 1, affected, true, options.batch));
            case "replacement" -> transaction(connection, () -> {
                try (var statement = connection.prepareStatement("DELETE FROM cache_entries WHERE id<=?")) {
                    statement.setLong(1, affected);
                    if (statement.executeUpdate() != affected) throw incorrect("Replacement deletion count");
                }
                try (var statement = connection.prepareStatement("INSERT INTO cache_entries VALUES (?,?,?)")) {
                    int pending = 0;
                    for (long id = 1; id <= affected; id++) {
                        bindCache(statement, id, true);
                        statement.addBatch();
                        if (++pending == options.batch) {
                            checkBatch(statement.executeBatch(), pending);
                            statement.clearBatch();
                            pending = 0;
                        }
                    }
                    if (pending > 0) checkBatch(statement.executeBatch(), pending);
                }
            });
            default -> throw new IllegalArgumentException("Unknown refresh strategy");
        }
    }

    private static void update(Connection connection, long first, long last, boolean batch, int batchSize) throws SQLException {
        try (var statement = connection.prepareStatement("UPDATE cache_entries SET cache_value=? WHERE id=?")) {
            int pending = 0;
            for (long id = first; id <= last; id++) {
                statement.setString(1, "refresh-" + id);
                statement.setLong(2, id);
                if (!batch) {
                    if (statement.executeUpdate() != 1) throw incorrect("Individual update count");
                } else {
                    statement.addBatch();
                    if (++pending == batchSize) {
                        checkBatch(statement.executeBatch(), pending);
                        statement.clearBatch();
                        pending = 0;
                    }
                }
            }
            if (pending > 0) checkBatch(statement.executeBatch(), pending);
        }
    }

    private static void validate(Connection connection, long rows, long affected, boolean deleted) throws SQLException {
        try (var statement = connection.createStatement()) {
            for (long first = 1; first <= rows; first += 256) {
                long last = Math.min(rows, first + 255);
                try (var rs = statement.executeQuery("SELECT id,cache_value,expires_at FROM cache_entries WHERE id>="
                        + first + " AND id<=" + last + " ORDER BY id")) {
                    for (long id = first; id <= last; id++) {
                        if (deleted && id <= affected) continue;
                        String expected = (id <= affected ? "refresh-" : "initial-") + id;
                        if (!rs.next() || rs.getLong(1) != id || !expected.equals(rs.getString(2)) || rs.getLong(3) != id)
                            throw incorrect("Cache validation mismatch at " + id);
                    }
                    if (rs.next()) throw incorrect("Unexpected cache row");
                }
            }
            try (var rs = statement.executeQuery("SELECT COUNT(*) FROM cache_entries")) {
                if (!rs.next() || rs.getLong(1) != rows - (deleted ? affected : 0) || rs.next())
                    throw incorrect("Cache total count mismatch");
            }
        }
    }

    private static void bindCache(PreparedStatement statement, long id, boolean refreshed) throws SQLException {
        statement.setLong(1, id);
        statement.setString(2, (refreshed ? "refresh-" : "initial-") + id);
        statement.setLong(3, id);
    }

    private static void checkBatch(int[] counts, int expected) throws SQLException {
        if (counts.length != expected) throw incorrect("Unexpected cache batch length");
        for (int count : counts)
            if (count != 1 && count != java.sql.Statement.SUCCESS_NO_INFO) throw incorrect("Unexpected cache batch result");
    }

    private static boolean isInvalidation(String strategy) {
        return List.of("record-invalidate", "bulk-invalidate", "expiration").contains(strategy);
    }

    @FunctionalInterface private interface SqlAction { void run() throws SQLException; }

    private static void transaction(Connection connection, SqlAction action) throws SQLException {
        if (!connection.getAutoCommit()) throw new SQLException("Cache transaction requires auto-commit");
        connection.setAutoCommit(false);
        Throwable failure = null;
        try {
            action.run();
            connection.commit();
        } catch (SQLException | RuntimeException | Error error) {
            failure = error;
            try { connection.rollback(); } catch (SQLException rollback) { error.addSuppressed(rollback); }
            throw error;
        } finally {
            try { connection.setAutoCommit(true); }
            catch (SQLException | RuntimeException cleanup) {
                if (failure == null) throw cleanup;
                failure.addSuppressed(cleanup);
            }
        }
    }

    private static void sleep(double seconds) throws InterruptedException {
        Thread.sleep(Duration.ofNanos((long) (seconds * 1_000_000_000L)));
    }

    private static SQLException incorrect(String message) { return new SQLException(message, "BENCH_CORRECTNESS"); }

    private static void throwIfFatal(AtomicReference<Throwable> failure) throws Exception {
        Throwable error = failure.get();
        if (error instanceof Exception exception) throw exception;
        if (error instanceof Error fatal) throw fatal;
    }

    private static Throwable append(Throwable original, Throwable next) {
        if (original == null) return next;
        original.addSuppressed(next);
        return original;
    }
}
