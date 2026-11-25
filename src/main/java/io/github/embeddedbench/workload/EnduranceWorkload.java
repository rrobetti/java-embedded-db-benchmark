package io.github.embeddedbench.workload;

import io.github.embeddedbench.db.DeterministicDataset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Bounded cache workload with transactional periodic maintenance. */
public final class EnduranceWorkload implements Workload {
    private final long rows, seed, commitInterval, maintenanceNanos;
    private final int payload, batch, readPercent;
    private final Map<Connection, Map<String, PreparedStatement>> statements = new IdentityHashMap<>();
    private final AtomicBoolean maintenanceActive = new AtomicBoolean();
    private final AtomicLong nextMaintenance = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maintenanceAttempts = new AtomicLong(), maintenanceCompleted = new AtomicLong();
    private MaintenanceObserver observer = active -> {};

    @FunctionalInterface public interface MaintenanceObserver { void phase(boolean active) throws SQLException; }

    public EnduranceWorkload(long rows, int payload, long seed, int batch, long commitInterval,
                             int readPercent, double maintenanceSeconds) {
        this.rows = rows;
        this.payload = payload;
        this.seed = seed;
        this.batch = batch;
        this.commitInterval = commitInterval;
        this.readPercent = readPercent;
        this.maintenanceNanos = (long) (maintenanceSeconds * 1_000_000_000L);
    }

    @Override public void prepare(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE endurance_cache (id BIGINT PRIMARY KEY, generation BIGINT NOT NULL, expires_at BIGINT NOT NULL)");
        }
        transaction(connection, () -> {
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO endurance_cache VALUES (?,0,?)")) {
                int pending = 0;
                long committed = 0;
                for (long id = 1; id <= rows; id++) {
                    insert.setLong(1, id);
                    insert.setLong(2, id);
                    insert.addBatch();
                    pending++;
                    if (pending == batch || commitInterval > 0 && committed + pending == commitInterval) {
                        checkBatch(insert.executeBatch(), pending);
                        insert.clearBatch();
                        committed += pending;
                        pending = 0;
                        if (commitInterval > 0 && committed == commitInterval) {
                            connection.commit();
                            committed = 0;
                        }
                    }
                }
                if (pending > 0) checkBatch(insert.executeBatch(), pending);
            }
        });
        validateCache(connection);
    }

    public void resetAfterWarmup(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (statement.executeUpdate("UPDATE endurance_cache SET generation=0") != rows)
                throw incorrect("Warmup reset changed an unexpected number of cache entries");
        }
        validateCache(connection);
    }

    public void startMeasurement() {
        nextMaintenance.set(System.nanoTime() + maintenanceNanos);
    }

    public void setMaintenanceObserver(MaintenanceObserver observer) {
        this.observer = observer;
    }

    public Map<String, Object> metadata() {
        var result = new LinkedHashMap<String, Object>();
        result.put("cacheEntries", rows);
        result.put("maintenanceAttempts", maintenanceAttempts.get());
        result.put("maintenanceCompleted", maintenanceCompleted.get());
        result.put("semantics", "A separate cache table adds one record per base row. Periodic maintenance "
                + "refreshes retained entries, then transactionally expires and bulk-invalidates the first 10%, "
                + "reinserts them, and checks count and ID sum. Reads never permit absent rows. "
                + "Maintenance is performed by a workload worker and is measured separately; resource snapshots "
                + "at its boundaries add overhead to the maintenance latency. No GC is forced.");
        return result;
    }

    @Override public Operation execute(Connection connection, SplittableRandom random) throws SQLException {
        long now = System.nanoTime();
        if (now >= nextMaintenance.get() && maintenanceActive.compareAndSet(false, true)) {
            if (now >= nextMaintenance.get()) return scheduledMaintenance(connection);
            maintenanceActive.set(false);
        }
        long id = random.nextLong(rows) + 1;
        if (random.nextInt(100) < readPercent) {
            PreparedStatement statement = statement(connection, "SELECT id,generation,expires_at FROM endurance_cache WHERE id=?");
            statement.setLong(1, id);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || result.getLong(1) != id || result.getLong(2) < 0
                        || result.getLong(3) != id || result.next())
                    throw incorrect("Invalid or missing cache entry " + id);
            }
            return new Operation("read", 1);
        }
        PreparedStatement statement = statement(connection, "UPDATE endurance_cache SET generation=generation+1 WHERE id=?");
        statement.setLong(1, id);
        if (statement.executeUpdate() != 1) throw incorrect("Cache update did not affect exactly one entry");
        return new Operation("write", 1);
    }

    private Operation scheduledMaintenance(Connection connection) throws SQLException {
        Throwable failure = null;
        try {
            observer.phase(true);
            maintenanceAttempts.incrementAndGet();
            maintenance(connection);
            maintenanceCompleted.incrementAndGet();
            return new Operation("maintenance", 1);
        } catch (SQLException | RuntimeException | Error error) {
            failure = error;
            throw error;
        } finally {
            try { observer.phase(false); }
            catch (SQLException | RuntimeException | Error cleanup) {
                if (failure == null) throw cleanup;
                failure.addSuppressed(cleanup);
            } finally {
                nextMaintenance.set(System.nanoTime() + maintenanceNanos);
                maintenanceActive.set(false);
            }
        }
    }

    private void maintenance(Connection connection) throws SQLException {
        long affected = Math.max(1, rows / 10);
        transaction(connection, () -> {
            try (PreparedStatement refresh = connection.prepareStatement(
                    "UPDATE endurance_cache SET generation=generation+1 WHERE id>?")) {
                refresh.setLong(1, affected);
                if (refresh.executeUpdate() != rows - affected) throw incorrect("Refresh count mismatch");
            }
            try (PreparedStatement expire = connection.prepareStatement(
                    "DELETE FROM endurance_cache WHERE id<=? AND expires_at<=?")) {
                expire.setLong(1, affected);
                expire.setLong(2, affected);
                if (expire.executeUpdate() != affected) throw incorrect("Expiration count mismatch");
            }
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO endurance_cache VALUES (?,0,?)")) {
                int pending = 0;
                for (long id = 1; id <= affected; id++) {
                    insert.setLong(1, id);
                    insert.setLong(2, id);
                    insert.addBatch();
                    if (++pending == batch) {
                        checkBatch(insert.executeBatch(), pending);
                        insert.clearBatch();
                        pending = 0;
                    }
                }
                if (pending > 0) checkBatch(insert.executeBatch(), pending);
            }
            validateCache(connection);
        });
    }

    @Override public void validate(Connection connection) throws SQLException {
        validateCache(connection);
        DeterministicDataset.validate(connection, rows, payload, seed);
    }

    private void validateCache(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COUNT(*),SUM(id),MIN(generation),"
                     + "SUM(CASE WHEN expires_at<>id THEN 1 ELSE 0 END),"
                     + "SUM(CASE WHEN id<1 OR id>" + rows + " THEN 1 ELSE 0 END) FROM endurance_cache")) {
            long expectedSum = Math.multiplyExact(rows, Math.addExact(rows, 1)) / 2;
            if (!result.next() || result.getLong(1) != rows || result.getLong(2) != expectedSum
                    || result.getLong(3) < 0 || result.getLong(4) != 0 || result.getLong(5) != 0 || result.next())
                throw incorrect("Cache aggregate invariant failed");
        }
    }

    private synchronized PreparedStatement statement(Connection connection, String sql) throws SQLException {
        Map<String, PreparedStatement> bySql = statements.computeIfAbsent(connection, ignored -> new LinkedHashMap<>());
        PreparedStatement existing = bySql.get(sql);
        if (existing != null) return existing;
        PreparedStatement prepared = connection.prepareStatement(sql);
        bySql.put(sql, prepared);
        return prepared;
    }

    @Override public synchronized void close() throws SQLException {
        SQLException failure = null;
        for (var bySql : statements.values()) {
            for (PreparedStatement statement : bySql.values()) {
                try { statement.close(); }
                catch (SQLException error) {
                    if (failure == null) failure = error; else failure.addSuppressed(error);
                }
            }
        }
        if (failure != null) throw failure;
    }

    private static void checkBatch(int[] results, int expected) throws SQLException {
        if (results.length != expected) throw incorrect("Batch result count mismatch");
        for (int count : results)
            if (count != 1 && count != Statement.SUCCESS_NO_INFO) throw incorrect("Batch insert failed");
    }

    @FunctionalInterface private interface SqlAction { void run() throws SQLException; }

    private static void transaction(Connection connection, SqlAction action) throws SQLException {
        if (!connection.getAutoCommit()) throw new SQLException("Endurance transaction requires auto-commit");
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
            catch (SQLException error) {
                if (failure == null) throw error;
                failure.addSuppressed(error);
            }
        }
    }

    private static SQLException incorrect(String message) { return new SQLException(message, "BENCH_CORRECTNESS"); }
}
