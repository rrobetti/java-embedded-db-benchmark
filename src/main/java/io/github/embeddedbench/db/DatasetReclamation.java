package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.SQLException;

/** Deletes a deterministic suffix in foreign-key order for reclamation experiments. */
public final class DatasetReclamation {
    private DatasetReclamation() {}

    public static void deleteSuffix(Connection connection, long rows, long first) throws SQLException {
        if (rows < 1 || first < 1 || first > rows)
            throw new IllegalArgumentException("Invalid dataset suffix");
        if (!connection.getAutoCommit())
            throw new IllegalArgumentException("Reclamation requires an auto-commit connection");
        connection.setAutoCommit(false);
        Throwable failure = null;
        try {
            long removed = rows - first + 1;
            delete(connection, "DELETE FROM order_items WHERE order_id>=? OR product_id>=?", first,
                    Math.addExact(Math.multiplyExact(removed, 2), first > 1 ? 1 : 0), true);
            for (String table : new String[]{"financial_transactions", "orders", "accounts", "products", "customers"})
                delete(connection, "DELETE FROM " + table + " WHERE id>=?", first, removed, false);
            long retained = first - 1;
            for (String table : new String[]{"financial_transactions", "orders", "accounts", "products", "customers"})
                count(connection, "SELECT COUNT(*) FROM " + table, retained);
            count(connection, "SELECT COUNT(*) FROM order_items", retained == 0 ? 0 : retained * 2 - 1);
            count(connection, "SELECT COUNT(*) FROM order_items i LEFT JOIN orders o ON i.order_id=o.id "
                    + "LEFT JOIN products p ON i.product_id=p.id WHERE o.id IS NULL OR p.id IS NULL", 0);
            count(connection, "SELECT COUNT(*) FROM financial_transactions t LEFT JOIN accounts a ON t.account_id=a.id "
                    + "WHERE a.id IS NULL", 0);
            count(connection, "SELECT COUNT(*) FROM accounts a LEFT JOIN customers c ON a.customer_id=c.id "
                    + "WHERE c.id IS NULL", 0);
            count(connection, "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id=c.id "
                    + "WHERE c.id IS NULL", 0);
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

    private static void delete(Connection connection, String sql, long first, long expected, boolean twoParameters)
            throws SQLException {
        try (var statement = connection.prepareStatement(sql)) {
            statement.setLong(1, first);
            if (twoParameters) statement.setLong(2, first);
            if (statement.executeUpdate() != expected)
                throw new SQLException("Unexpected deleted row count: " + sql, "BENCH_CORRECTNESS");
        }
    }

    private static void count(Connection connection, String sql, long expected) throws SQLException {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
            if (!result.next() || result.getLong(1) != expected || result.next())
                throw new SQLException("Unexpected retained row count: " + sql, "BENCH_CORRECTNESS");
        }
    }
}
