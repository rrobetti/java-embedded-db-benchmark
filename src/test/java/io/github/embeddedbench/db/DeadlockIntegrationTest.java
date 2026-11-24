package io.github.embeddedbench.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DeadlockIntegrationTest {
    private record Outcome(boolean committed, SQLException failure) {
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void twoRealH2TransactionsForceOneDeadlockVictimAndOneAtomicWinner() throws Exception {
        try (DatabaseAdapter adapter = new H2DatabaseAdapter()) {
            adapter.initialize();
            BigDecimal initialSum;
            try (Connection connection = adapter.openConnection()) {
                adapter.createSchema(connection);
                DeterministicDataset.load(connection, 2, 0, 42, 2);
                initialSum = balanceSum(connection);
            }
            CountDownLatch firstWrites = new CountDownLatch(2);
            List<Outcome> outcomes;
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var first = executor.submit(() -> transfer(adapter, 1, 2, firstWrites));
                var second = executor.submit(() -> transfer(adapter, 2, 1, firstWrites));
                outcomes = List.of(first.get(15, TimeUnit.SECONDS), second.get(15, TimeUnit.SECONDS));
            }
            assertEquals(1, outcomes.stream().filter(Outcome::committed).count());
            assertEquals(1, outcomes.stream().filter(outcome -> outcome.failure() != null).count());
            SQLException victim = outcomes.stream().map(Outcome::failure).filter(java.util.Objects::nonNull)
                    .findFirst().orElseThrow();
            assertEquals("40001", victim.getSQLState(), victim.toString());
            assertTrue(victim.getMessage().toLowerCase(java.util.Locale.ROOT).contains("deadlock"), victim.toString());
            try (Connection connection = adapter.openConnection()) {
                assertEquals(0, initialSum.compareTo(balanceSum(connection)),
                        "winning transfer and victim rollback must conserve exact integer cents");
                try (Statement statement = connection.createStatement();
                     ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM accounts")) {
                    assertTrue(result.next());
                    assertEquals(2, result.getLong(1));
                }
            }
        }
    }

    private static Outcome transfer(DatabaseAdapter adapter, int first, int second, CountDownLatch firstWrites)
            throws Exception {
        try (Connection connection = adapter.openConnection(); Statement statement = connection.createStatement()) {
            statement.execute("SET LOCK_TIMEOUT 5000");
            statement.setQueryTimeout(10);
            connection.setAutoCommit(false);
            try {
                assertEquals(1, statement.executeUpdate(
                        "UPDATE accounts SET balance=balance+0.01 WHERE id=" + first));
                firstWrites.countDown();
                assertTrue(firstWrites.await(5, TimeUnit.SECONDS), "both first row locks must be held");
                assertEquals(1, statement.executeUpdate(
                        "UPDATE accounts SET balance=balance-0.01 WHERE id=" + second));
                connection.commit();
                return new Outcome(true, null);
            } catch (SQLException exception) {
                connection.rollback();
                return new Outcome(false, exception);
            }
        }
    }

    private static BigDecimal balanceSum(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT SUM(CAST(ROUND(balance*100,0) AS BIGINT)) FROM accounts")) {
            assertTrue(result.next());
            return result.getBigDecimal(1);
        }
    }
}
