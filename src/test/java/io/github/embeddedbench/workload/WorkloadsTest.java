package io.github.embeddedbench.workload;

import io.github.embeddedbench.db.DatabaseAdapters;
import io.github.embeddedbench.db.DeterministicDataset;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class WorkloadsTest {
    @ParameterizedTest
    @ValueSource(strings = {"point", "zipf", "hot", "range", "indexed-range", "joins",
            "relational", "cte", "mixed", "htap", "cache", "transactions", "rollback", "write",
            "insert", "update", "delete", "batch-update", "upsert", "cache-populate",
            "cache-invalidate", "cache-bulk-invalidate", "cache-refresh", "cache-incremental",
            "cache-expire", "cache-replace", "contended-write", "contended-transactions", "long-transactions"})
    void realJdbcOperationsPreserveInvariants(String name) throws Exception {
        try (var db = DatabaseAdapters.create("h2", "default")) {
            db.initialize();
            try (Connection c = db.openConnection();
                 Workload w = Workloads.create(name, 20, 10, 50, 20, 3, 42)) {
                db.createSchema(c);
                DeterministicDataset.load(c, 20, 128, 42, 10);
                DeterministicDataset.validate(c, 20, 128, 42);
                w.prepare(c);
                SplittableRandom r = new SplittableRandom(42);
                for (int i = 0; i < 30; i++) {
                    Workload.Operation op = w.execute(c, r);
                    assertTrue(op.units() > 0);
                    assertTrue(java.util.Set.of("read", "write", "analytical", "transaction").contains(op.category()));
                    assertTrue(c.getAutoCommit(), "Transaction state leaked");
                }
                w.validate(c);
            }
        }
    }

    @Test
    void committedInsertsAndRollbackHaveExactOutcomes() throws Exception {
        try (var db = DatabaseAdapters.create("h2", "default")) {
            db.initialize();
            try (Connection c = db.openConnection();
                 Workload insert = Workloads.create("insert", 10, 10, 50, 10, 3, 42);
                 Workload rollback = Workloads.create("rollback", 10, 10, 50, 10, 3, 42)) {
                db.createSchema(c);
                DeterministicDataset.load(c, 10, 128, 42, 10);
                insert.prepare(c);
                insert.execute(c, new SplittableRandom(42));
                insert.validate(c);
                assertEquals(13, count(c));
                BigDecimal before = balance(c, 1);
                rollback.prepare(c);
                for (int i = 0; i < 10; i++) rollback.execute(c, new SplittableRandom(i));
                rollback.validate(c);
                assertEquals(before, balance(c, 1));
                assertEquals(13, count(c));
            }
        }
    }

    @Test
    void contendedWorkersShareDatabaseAndDoNotLoseUpdates() throws Exception {
        try (var db = DatabaseAdapters.create("h2", "default")) {
            db.initialize();
            try (Connection setup = db.openConnection();
                 Workload w = Workloads.create("contended-write", 10, 10, 50, 10, 1, 42)) {
                db.createSchema(setup);
                DeterministicDataset.load(setup, 10, 128, 42, 10);
                BigDecimal first = balance(setup, 1), second = balance(setup, 2);
                w.prepare(setup);
                try (var executor = java.util.concurrent.Executors.newFixedThreadPool(4)) {
                    var jobs = new java.util.ArrayList<java.util.concurrent.Callable<Void>>();
                    for (int worker = 0; worker < 4; worker++) {
                        int index = worker;
                        jobs.add(() -> {
                            try (Connection c = db.openConnection()) {
                                SplittableRandom r = new SplittableRandom(index);
                                for (int operation = 0; operation < 100; operation++) w.execute(c, r);
                            }
                            return null;
                        });
                    }
                    for (var future : executor.invokeAll(jobs)) future.get();
                }
                w.validate(setup);
                assertEquals(0, first.add(new BigDecimal("400.00")).compareTo(balance(setup, 1)));
                assertEquals(0, second.subtract(new BigDecimal("400.00")).compareTo(balance(setup, 2)));
            }
        }
    }

    @Test
    void failedMutationRollsBackWithoutCountingSuccess() throws Exception {
        try (var db = DatabaseAdapters.create("h2", "default")) {
            db.initialize();
            try (Connection c = db.openConnection();
                 Workload first = Workloads.create("insert", 10, 10, 50, 10, 3, 42);
                 Workload duplicate = Workloads.create("insert", 10, 10, 50, 10, 3, 42)) {
                db.createSchema(c);
                DeterministicDataset.load(c, 10, 128, 42, 10);
                first.prepare(c);
                first.execute(c, new SplittableRandom(42));
                duplicate.prepare(c);
                assertThrows(SQLException.class, () -> duplicate.execute(c, new SplittableRandom(42)));
                assertTrue(c.getAutoCommit());
                duplicate.validate(c);
                first.validate(c);
                assertEquals(13, count(c));
            }
        }
    }

    @Test
    void corruptionInvalidatesResult() throws Exception {
        try (var db = DatabaseAdapters.create("h2", "default")) {
            db.initialize();
            try (Connection c = db.openConnection();
                 Workload w = Workloads.create("transactions", 10, 10, 50, 10, 3, 42)) {
                db.createSchema(c);
                DeterministicDataset.load(c, 10, 128, 42, 10);
                w.prepare(c);
                try (Statement s = c.createStatement()) {
                    s.executeUpdate("UPDATE accounts SET balance=balance+1 WHERE id=1");
                }
                SQLException failure = assertThrows(SQLException.class, () -> w.validate(c));
                assertEquals("BENCH_CORRECTNESS", failure.getSQLState());
            }
        }
    }

    @Test
    void zipfIsDeterministicBoundedAndSkewed() {
        SplittableRandom a = new SplittableRandom(42), b = new SplittableRandom(42);
        int head = 0, tail = 0;
        for (int i = 0; i < 100_000; i++) {
            long rank = Workloads.zipf(a, 1000);
            assertEquals(rank, Workloads.zipf(b, 1000));
            assertTrue(rank >= 1 && rank <= 1000);
            if (rank <= 10) head++;
            if (rank > 990) tail++;
        }
        assertTrue(head > tail * 20);
        assertEquals(1, Workloads.zipf(a, 1));
        assertThrows(IllegalArgumentException.class, () -> Workloads.create("unknown", 10, 10, 99, 1, 1, 42));
    }

    private long count(Connection c) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM financial_transactions")) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private BigDecimal balance(Connection c, long id) throws SQLException {
        try (var s = c.prepareStatement("SELECT balance FROM accounts WHERE id=?")) {
            s.setLong(1, id);
            try (ResultSet rs = s.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBigDecimal(1);
            }
        }
    }
}
