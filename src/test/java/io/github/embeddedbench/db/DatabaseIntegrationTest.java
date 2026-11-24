package io.github.embeddedbench.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 120, unit = TimeUnit.SECONDS)
class DatabaseIntegrationTest {
    @ParameterizedTest(name = "{0} / {1}")
    @CsvSource({"h2,default", "hsqldb,default", "hsqldb,mvcc", "sqlite,default", "pglite,default"})
    void realEngineRoundTripConstraintsTransactionsAndCapabilities(String id, String profile) throws Exception {
        try (DatabaseAdapter adapter = DatabaseAdapters.create(id, profile)) {
            assertThrows(SQLException.class, adapter::openConnection);
            adapter.initialize();
            adapter.initialize();
            assertFalse(adapter.version().isBlank());
            if ("h2".equals(id) || "hsqldb".equals(id)) {
                XaProbe.Result probe = XaProbe.result(adapter);
                assertTrue(probe.supported() && probe.prepare() && probe.recover() && probe.commit() && probe.rollback());
            }
            try (Connection connection = adapter.openConnection()) {
                adapter.createSchema(connection);
                DeterministicDataset.load(connection, 37, 257, 987654321, 8);
                DeterministicDataset.validate(connection, 37, 257, 987654321);
                assertJoinAndCte(connection);
                assertConstraints(connection);
                assertRollbackAndSavepoint(connection, id);
                assertCommittedUpdate(connection);
                assertPrecision(connection, id);
                if ("sqlite".equals(id)) {
                    assertPragmas(connection);
                }
            }
            if (adapter.capabilities().independentTransactions()) {
                assertIndependentTransactions(adapter);
                if (adapter.capabilities().concurrentWriters()) {
                    assertConcurrentNonconflictingWritersAndSnapshotReader(adapter);
                }
            } else {
                assertEquals("pglite", id);
                assertEquals("serialized-shared-session", adapter.capabilities().executionMode());
                assertFalse(adapter.capabilities().concurrentReaders());
                assertFalse(adapter.capabilities().concurrentWriters());
                assertFalse(adapter.capabilities().xa());
                assertSerializedLease(adapter);
            }
            if ("sqlite".equals(id)) {
                assertWriterConflictClassification(adapter);
            }
            adapter.close();
            adapter.close();
            assertThrows(SQLException.class, adapter::openConnection);
            adapter.initialize();
            try (Connection connection = adapter.openConnection()) {
                // A clean shutdown must release the named in-memory database.
                adapter.createSchema(connection);
                DeterministicDataset.load(connection, 0, 0, 1, 1);
                DeterministicDataset.validate(connection, 0, 0, 1);
            }
        }
    }

    @ParameterizedTest(name = "{0} ingestion {1}")
    @CsvSource({"h2,individual", "h2,prepared", "h2,batch", "hsqldb,individual", "hsqldb,prepared",
            "hsqldb,batch", "sqlite,individual", "sqlite,prepared", "sqlite,batch",
            "pglite,individual", "pglite,prepared", "pglite,batch"})
    void ingestionModesProduceIdenticalExactDataset(String engine, String mode) throws Exception {
        try (DatabaseAdapter adapter = DatabaseAdapters.create(engine, "default")) {
            adapter.initialize();
            try (Connection connection = adapter.openConnection()) {
                adapter.createSchema(connection);
                DeterministicDataset.load(connection, 100, 128, Long.MIN_VALUE, 500, mode);
                DeterministicDataset.validate(connection, 100, 128, Long.MIN_VALUE);
                assertTrue(connection.getAutoCommit());
            }
        }
    }

    @Test
    void streamingLoaderModesLogicalAccountingAndCorruptionDetection() throws Exception {
        assertEquals(0, DeterministicDataset.logicalBytes(0, 32));
        assertEquals(211, DeterministicDataset.logicalBytes(1, 0));
        assertEquals(1_032, DeterministicDataset.logicalBytes(4, 48));
        assertThrows(IllegalArgumentException.class, () -> DeterministicDataset.logicalBytes(-1, 0));
        assertThrows(ArithmeticException.class, () -> DeterministicDataset.logicalBytes(Long.MAX_VALUE / 2, 1));
        try (DatabaseAdapter adapter = new H2DatabaseAdapter()) {
            adapter.initialize();
            try (Connection connection = adapter.openConnection()) {
                adapter.createSchema(connection);
                connection.setAutoCommit(false);
                DeterministicDataset.load(connection, 12, 0, Long.MIN_VALUE, 3, "prepared");
                assertFalse(connection.getAutoCommit(), "loader must not commit caller-owned transactions");
                DeterministicDataset.validate(connection, 12, 0, Long.MIN_VALUE);
                connection.rollback();
                DeterministicDataset.validate(connection, 0, 0, Long.MIN_VALUE);
                connection.setAutoCommit(true);
                DeterministicDataset.load(connection, 123, 129, 42, 11);
                DeterministicDataset.validate(connection, 123, 129, 42);
                assertThrows(SQLException.class, () -> DeterministicDataset.load(connection, 123, 129, 42, 11));
                assertTrue(connection.getAutoCommit());
                DeterministicDataset.validate(connection, 123, 129, 42);
                String[] corruptions = {
                        "UPDATE customers SET name='bad' WHERE id=1",
                        "UPDATE customers SET notes='bad' WHERE id=4",
                        "UPDATE customers SET payload=X'00' WHERE id=1",
                        "UPDATE accounts SET balance=balance+0.01 WHERE id=1",
                        "UPDATE products SET price=price+0.01 WHERE id=1",
                        "UPDATE orders SET created_at=TIMESTAMP '2000-01-01 00:00:00' WHERE id=1",
                        "UPDATE order_items SET quantity=quantity+1 WHERE id=1",
                        "UPDATE financial_transactions SET amount=amount+0.01 WHERE id=1"
                };
                for (String sql : corruptions) {
                    connection.setAutoCommit(false);
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate(sql);
                    }
                    SQLException exception = assertThrows(SQLException.class,
                            () -> DeterministicDataset.validate(connection, 123, 129, 42), sql);
                    assertTrue(exception.getMessage().contains("Dataset validation failed"));
                    connection.rollback();
                    connection.setAutoCommit(true);
                    DeterministicDataset.validate(connection, 123, 129, 42);
                }
            }
        }
    }

    private static void assertJoinAndCte(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("WITH line_counts AS (SELECT order_id, COUNT(*) n "
                     + "FROM order_items GROUP BY order_id) "
                     + "SELECT COUNT(*), SUM(l.n) FROM customers c JOIN orders o ON o.customer_id=c.id "
                     + "JOIN line_counts l ON l.order_id=o.id")) {
            assertTrue(result.next());
            assertEquals(37, result.getLong(1));
            assertEquals(74, result.getLong(2));
        }
    }

    private static void assertConstraints(Connection connection) throws SQLException {
        String[] invalid = {
                "INSERT INTO customers VALUES (1,'duplicate','new@example.test',NULL,NULL)",
                "INSERT INTO products VALUES (1,'duplicate',1.00)",
                "INSERT INTO accounts VALUES (900,999999,1.00)",
                "INSERT INTO orders VALUES (900,999999,CURRENT_TIMESTAMP,'NEW')",
                "INSERT INTO order_items VALUES (900,999999,1,1)",
                "INSERT INTO order_items VALUES (900,1,999999,1)",
                "INSERT INTO financial_transactions VALUES (900,999999,1.00,CURRENT_TIMESTAMP)",
                "UPDATE customers SET email='customer-2@example.test' WHERE id=1",
                "DELETE FROM customers WHERE id=1",
                "DELETE FROM products WHERE id=1"
        };
        for (String sql : invalid) {
            try (Statement statement = connection.createStatement()) {
                assertThrows(SQLException.class, () -> statement.executeUpdate(sql), sql);
            }
        }
    }

    private static void assertRollbackAndSavepoint(Connection connection, String engine) throws SQLException {
        BigDecimal initialPrice;
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT price FROM products WHERE id=1")) {
            assertTrue(result.next());
            initialPrice = result.getBigDecimal(1);
        }
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE accounts SET balance=balance+0.01 WHERE id=1");
        }
        var savepoint = connection.setSavepoint();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE products SET price=price+0.01 WHERE id=1");
        }
        connection.rollback(savepoint);
        if ("pglite".equals(engine)) {
            try (Statement statement = connection.createStatement()) {
                SQLException limitation = assertThrows(SQLException.class,
                        () -> statement.executeQuery("SELECT price FROM products WHERE id=1"));
                assertTrue(limitation.getMessage().contains("cannot drop active portal"),
                        "PGlite 0.0.3 subtransaction rollback limitation must be reproduced, not fabricated");
            }
        } else {
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SELECT price FROM products WHERE id=1")) {
                assertTrue(result.next());
                assertEquals(0, initialPrice.compareTo(result.getBigDecimal(1)));
            }
            connection.releaseSavepoint(connection.setSavepoint());
        }
        connection.rollback();
        connection.setAutoCommit(true);
        DeterministicDataset.validate(connection, 37, 257, 987654321);
    }

    private static void assertCommittedUpdate(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            assertEquals(1, statement.executeUpdate("UPDATE accounts SET balance=balance+0.01 WHERE id=1"));
            assertEquals(1, statement.executeUpdate("UPDATE accounts SET balance=balance-0.01 WHERE id=2"));
        }
        connection.commit();
        connection.setAutoCommit(true);
        assertThrows(SQLException.class, () -> DeterministicDataset.validate(connection, 37, 257, 987654321));
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE accounts SET balance=balance-0.01 WHERE id=1");
            statement.executeUpdate("UPDATE accounts SET balance=balance+0.01 WHERE id=2");
        }
        DeterministicDataset.validate(connection, 37, 257, 987654321);
    }

    private static void assertPrecision(Connection connection, String id) throws SQLException {
        BigDecimal high = new BigDecimal("9999999999999999.99");
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("UPDATE products SET price=? WHERE id=1")) {
            statement.setBigDecimal(1, high);
            statement.executeUpdate();
        }
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT price FROM products WHERE id=1")) {
            assertTrue(result.next());
            BigDecimal actual = result.getBigDecimal(1);
            if ("sqlite".equals(id)) {
                assertNotEquals(0, high.compareTo(actual), "SQLite NUMERIC affinity loses high-value decimal precision");
            } else {
                assertEquals(0, high.compareTo(actual), "DECIMAL(18,2) must round-trip its maximum value exactly");
            }
        }
        connection.rollback();
        connection.setAutoCommit(true);
    }

    private static void assertIndependentTransactions(DatabaseAdapter adapter) throws SQLException {
        try (Connection writer = adapter.openConnection(); Connection reader = adapter.openConnection()) {
            BigDecimal initial;
            try (Statement statement = reader.createStatement();
                 ResultSet result = statement.executeQuery("SELECT price FROM products WHERE id=1")) {
                assertTrue(result.next());
                initial = result.getBigDecimal(1);
            }
            writer.setAutoCommit(false);
            try (Statement statement = writer.createStatement()) {
                statement.executeUpdate("UPDATE products SET price=price+0.01 WHERE id=1");
            }
            reader.setAutoCommit(false);
            reader.rollback();
            assertFalse(writer.getAutoCommit());
            writer.commit();
            reader.setAutoCommit(true);
            try (Statement statement = reader.createStatement();
                 ResultSet result = statement.executeQuery("SELECT price FROM products WHERE id=1")) {
                assertTrue(result.next());
                assertEquals(0, initial.add(new BigDecimal("0.01")).compareTo(result.getBigDecimal(1)),
                        "rolling back reader must not roll back another connection's transaction");
            }
            try (Statement statement = writer.createStatement()) {
                statement.executeUpdate("UPDATE products SET price=price-0.01 WHERE id=1");
            }
            writer.commit();
        }
    }

    private static void assertPragmas(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("PRAGMA foreign_keys")) {
            assertTrue(result.next());
            assertEquals(1, result.getInt(1));
        }
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("PRAGMA busy_timeout")) {
            assertTrue(result.next());
            assertEquals(5000, result.getInt(1));
        }
    }

    private static void assertConcurrentNonconflictingWritersAndSnapshotReader(DatabaseAdapter adapter)
            throws Exception {
        var ready = new java.util.concurrent.CountDownLatch(2);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int id = 1; id <= 2; id++) {
                final int row = id;
                tasks.add(executor.submit(() -> {
                    try (Connection connection = adapter.openConnection()) {
                        connection.setAutoCommit(false);
                        try (Statement statement = connection.createStatement()) {
                            assertEquals(1, statement.executeUpdate(
                                    "UPDATE products SET price=price+0.01 WHERE id=" + row));
                        }
                        ready.countDown();
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                        connection.rollback();
                    }
                    return true;
                }));
            }
            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS), "nonconflicting writers must run concurrently");
                try (Connection reader = adapter.openConnection()) {
                    reader.setAutoCommit(false);
                    // Both uncommitted updates must remain invisible to a third session.
                    DeterministicDataset.validate(reader, 37, 257, 987654321);
                    reader.rollback();
                }
            } finally {
                release.countDown();
            }
            for (var task : tasks) {
                assertTrue(task.get(10, TimeUnit.SECONDS));
            }
        }
    }

    private static void assertWriterConflictClassification(DatabaseAdapter adapter) throws SQLException {
        try (Connection first = adapter.openConnection(); Connection second = adapter.openConnection()) {
            assertPragmas(second);
            first.setAutoCommit(false);
            try (Statement statement = first.createStatement()) {
                statement.executeUpdate("UPDATE products SET price=price+1 WHERE id=1");
            }
            try (Statement statement = second.createStatement()) {
                SQLException conflict = assertThrows(SQLException.class,
                        () -> statement.executeUpdate("UPDATE products SET price=price+1 WHERE id=2"));
                assertTrue(SQLiteDatabaseAdapter.isBusyOrLocked(conflict), conflict.toString());
            }
            first.rollback();
        }
        assertTrue(SQLiteDatabaseAdapter.isBusyOrLocked(new SQLException("locked", null, 262)));
        assertTrue(SQLiteDatabaseAdapter.isBusyOrLocked(new SQLException("[SQLITE_BUSY]", null, 0)));
        assertFalse(SQLiteDatabaseAdapter.isBusyOrLocked(new SQLException("foreign key failed", null, 19)));
    }

    private static void assertSerializedLease(DatabaseAdapter adapter) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Connection first = adapter.openConnection();
            try {
                var opened = new java.util.concurrent.CountDownLatch(1);
                var requestStarted = new java.util.concurrent.CountDownLatch(1);
                var pending = executor.submit(() -> {
                    requestStarted.countDown();
                    try (Connection ignored = adapter.openConnection()) {
                        opened.countDown();
                    }
                    return true;
                });
                assertTrue(requestStarted.await(5, TimeUnit.SECONDS));
                assertFalse(opened.await(150, TimeUnit.MILLISECONDS), "second shared-session lease must wait");
                first.close();
                assertTrue(pending.get(10, TimeUnit.SECONDS));
                first.close();
            } finally {
                first.close();
            }
        }
    }
}
