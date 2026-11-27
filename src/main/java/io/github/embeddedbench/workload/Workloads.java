package io.github.embeddedbench.workload;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import io.github.embeddedbench.db.DeterministicDataset;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;

public final class Workloads {
    public static final Set<String> NAMES = Set.of("ingestion", "point", "zipf", "hot", "range",
            "indexed-range", "joins", "relational", "cte", "aggregate", "mixed", "rw-independent", "rw-hot", "htap", "cache",
            "transactions", "write", "insert", "update", "delete", "batch-update", "upsert",
            "rollback", "cache-populate", "cache-invalidate", "cache-bulk-invalidate",
            "cache-refresh", "cache-incremental", "cache-expire", "cache-replace",
            "contended-write", "contended-transactions", "long-transactions", "deadlocks");

    private Workloads() {}

    public static Workload create(String name, long rows, int rangeRows, int readPercent,
                                  int analyticalPercent, int transactionSize, long seed) {
        if (!NAMES.contains(name)) throw new IllegalArgumentException("Unknown workload: " + name);
        if (name.equals("deadlocks") && rows < 2)
            throw new IllegalArgumentException("Deadlock workloads require at least two accounts");
        if (name.equals("rw-independent") && rows < 4 || name.equals("rw-hot") && rows < 2)
            throw new IllegalArgumentException("Read/write workloads require at least four independent or two hot accounts");
        if (rows < 1 || rangeRows < 1 || transactionSize < 1 || readPercent < 0 || readPercent > 100
                || analyticalPercent < 0 || analyticalPercent > 100)
            throw new IllegalArgumentException("Invalid workload parameters");
        return new JdbcWorkload(name, rows, rangeRows, readPercent, analyticalPercent, transactionSize, seed);
    }

    private static final class JdbcWorkload implements Workload {
        private static final Operation READ = new Operation("read", 1);
        private static final Operation WRITE = new Operation("write", 1);
        private static final Operation ANALYTICAL = new Operation("analytical", 1);
        private static final Operation TRANSACTION = new Operation("transaction", 1);
        private static final Timestamp TIME = Timestamp.valueOf("2020-01-01 00:00:00");
        private final String name;
        private final long rows;
        private final long seed;
        private final int rangeRows, readPercent, analyticalPercent, transactionSize;
        private final AtomicLong nextId, inserted = new AtomicLong(), deleted = new AtomicLong();
        private final Map<Connection, Map<String, PreparedStatement>> statements = new IdentityHashMap<>();
        private final Map<String, Long> originalCounts = new LinkedHashMap<>();
        private BigDecimal originalBalance;
        private volatile long consumed;
        private boolean prepared;
        private int payloadSize;

        private JdbcWorkload(String name, long rows, int rangeRows, int readPercent,
                             int analyticalPercent, int transactionSize, long seed) {
            this.name = name;
            this.rows = rows;
            this.seed = seed;
            this.rangeRows = rangeRows;
            this.readPercent = readPercent;
            this.analyticalPercent = analyticalPercent;
            this.transactionSize = transactionSize;
            this.nextId = new AtomicLong(rows + 1);
        }

        @Override
        public void prepare(Connection c) throws SQLException {
            for (String table : List.of("customers", "accounts", "products", "orders",
                    "order_items", "financial_transactions"))
                originalCounts.put(table, scalar(c, "SELECT COUNT(*) FROM " + table));
            originalBalance = decimal(c, "SELECT SUM(CAST(ROUND(balance*100,0) AS BIGINT)) FROM accounts");
            try (var s = c.prepareStatement("SELECT payload FROM customers WHERE id=1");
                 var rs = s.executeQuery()) {
                if (!rs.next() || rs.getBytes(1) == null) throw incorrect("Missing reference payload");
                payloadSize = rs.getBytes(1).length;
            }
            prepared = true;
        }

        @Override
        public Operation execute(Connection c, SplittableRandom r) throws SQLException {
            if (!prepared) throw new IllegalStateException("Workload.prepare must run before workers");
            return switch (name) {
                case "point" -> point(c, r.nextLong(rows) + 1);
                case "zipf" -> point(c, zipf(r, rows));
                case "hot" -> point(c, r.nextLong(Math.max(1, rows / 100)) + 1);
                case "range", "indexed-range" -> range(c, r, name.equals("indexed-range"));
                case "joins", "relational", "cte" -> relational(c, r);
                case "aggregate" -> aggregate(c);
                case "htap" -> r.nextInt(100) < analyticalPercent ? analytical(c, r) : mixed(c, r);
                case "mixed" -> mixed(c, r);
                case "rw-independent", "rw-hot" -> execute(c, r,
                        r.nextInt(100) < readPercent ? OperationType.READ : OperationType.WRITE);
                case "cache" -> r.nextInt(100) < readPercent ? point(c, r.nextLong(rows) + 1) : maintenance(c, r);
                case "transactions", "contended-transactions", "long-transactions", "deadlocks" -> transfer(c, r, false, transactionSize);
                case "contended-write" -> transfer(c, r, false, 1);
                case "rollback" -> transfer(c, r, true, transactionSize);
                case "update", "cache-incremental" -> transfer(c, r, false, 1);
                case "batch-update" -> transfer(c, r, false, transactionSize);
                case "upsert" -> upsert(c, r);
                case "delete", "cache-invalidate" -> remove(c, r, 1);
                case "cache-bulk-invalidate" -> remove(c, r, transactionSize);
                case "cache-expire" -> expire(c, r);
                case "cache-refresh", "cache-replace" -> refresh(c, transactionSize);
                case "insert", "ingestion", "cache-populate" -> populate(c, transactionSize);
                case "write" -> write(c, r);
                default -> throw new IllegalArgumentException(name);
            };
        }

        @Override
        public Operation execute(Connection c, SplittableRandom r, OperationType type) throws SQLException {
            if (!prepared) throw new IllegalStateException("Workload.prepare must run before workers");
            if (!name.equals("rw-independent") && !name.equals("rw-hot"))
                throw new UnsupportedOperationException("Explicit read/write selection is unavailable for " + name);
            if (type == OperationType.READ) {
                long id = name.equals("rw-hot") ? 1 : 2 * r.nextLong((rows + 1) / 2) + 1;
                PreparedStatement s = statement(c, "SELECT id,balance FROM accounts WHERE id=?");
                s.setLong(1, id);
                try (ResultSet rs = s.executeQuery()) {
                    if (!rs.next() || rs.getLong(1) != id || rs.getBigDecimal(2) == null || rs.next())
                        throw incorrect("Account lookup mismatch");
                }
                return READ;
            }
            if (type != OperationType.WRITE) throw new IllegalArgumentException("Operation type is required");
            if (name.equals("rw-hot")) return transferPair(c, 1, 2, false, false, 1);
            long evenCount = rows / 2;
            long firstIndex = r.nextLong(evenCount);
            long first = 2 * (firstIndex + 1);
            long second = 2 * ((firstIndex + 1) % evenCount + 1);
            return transferPair(c, first, second, false, false, 1);
        }

        private Operation mixed(Connection c, SplittableRandom r) throws SQLException {
            return r.nextInt(100) < readPercent ? point(c, r.nextLong(rows) + 1) : write(c, r);
        }

        private Operation write(Connection c, SplittableRandom r) throws SQLException {
            return switch (r.nextInt(10)) {
                case 0, 1 -> populate(c, 1);
                case 2, 3 -> remove(c, r, 1);
                default -> transfer(c, r, false, 1);
            };
        }

        private Operation point(Connection c, long id) throws SQLException {
            PreparedStatement s = statement(c, "SELECT id,name,email,notes,payload FROM customers WHERE id=?");
            s.setLong(1, id);
            try (ResultSet rs = s.executeQuery()) {
                if (!rs.next() || rs.getLong(1) != id || !("customer-" + id).equals(rs.getString(2))
                        || !("customer-" + id + "@example.test").equals(rs.getString(3))
                        || !java.util.Objects.equals(id % 4 == 0 ? null : "note-" + id, rs.getString(4)))
                    throw incorrect("Missing or malformed customer " + id);
                byte[] payload = rs.getBytes(5);
                if (payload == null || payload.length != payloadSize) throw incorrect("Payload size mismatch");
                consumed = id ^ payload.length ^ rs.getString(2).hashCode();
                if (rs.next()) throw incorrect("Duplicate primary key");
            }
            return READ;
        }

        private Operation range(Connection c, SplittableRandom r, boolean indexed) throws SQLException {
            long size = Math.min(rows, rangeRows);
            long start = r.nextLong(rows - size + 1) + 1;
            PreparedStatement s = statement(c, indexed
                    ? "SELECT id,name,email,payload FROM customers WHERE id>=? AND id<? ORDER BY id"
                    : "SELECT id,name,email,payload FROM customers WHERE CAST(REPLACE(name,'customer-','') AS BIGINT)>=? AND CAST(REPLACE(name,'customer-','') AS BIGINT)<? ORDER BY id");
            s.setLong(1, start);
            s.setLong(2, start + size);
            if (consume(s) != size) throw incorrect("Range cardinality mismatch");
            return READ;
        }

        private Operation relational(Connection c, SplittableRandom r) throws SQLException {
            long id = r.nextLong(rows) + 1;
            int variant = name.equals("cte") ? -1 : r.nextInt(name.equals("joins") ? 3 : 6);
            String sql = name.equals("cte")
                    ? "WITH selected AS (SELECT id,customer_id FROM orders WHERE id=?) SELECT c.id,o.id FROM customers c JOIN selected o ON c.id=o.customer_id"
                    : switch (variant) {
                        case 0 -> "SELECT c.id,a.balance FROM customers c JOIN accounts a ON c.id=a.customer_id WHERE c.id=?";
                        case 1 -> "SELECT c.id,o.id,i.quantity FROM customers c JOIN orders o ON c.id=o.customer_id JOIN order_items i ON o.id=i.order_id WHERE c.id=?";
                        case 2 -> "SELECT c.id,o.id,p.name,i.quantity,p.price FROM customers c JOIN orders o ON c.id=o.customer_id JOIN order_items i ON o.id=i.order_id JOIN products p ON p.id=i.product_id WHERE c.id=? ORDER BY p.price,p.id";
                        case 3 -> "SELECT o.status,COUNT(*),SUM(i.quantity) FROM orders o JOIN order_items i ON o.id=i.order_id WHERE o.customer_id=? GROUP BY o.status ORDER BY o.status";
                        case 4 -> "SELECT id,name FROM products WHERE id IN (SELECT product_id FROM order_items WHERE order_id=?) ORDER BY id";
                        default -> "SELECT id,price FROM products WHERE id>=? ORDER BY price,id LIMIT 100";
                    };
            PreparedStatement s = statement(c, sql);
            s.setLong(1, id);
            long expected = switch (variant) {
                case 1, 2 -> 2;
                case 4 -> Math.min(2, rows);
                case 5 -> Math.min(100, rows - id + 1);
                default -> 1;
            };
            if (consume(s) != expected) throw incorrect("Relational query cardinality mismatch");
            return READ;
        }

        private Operation analytical(Connection c, SplittableRandom r) throws SQLException {
            String sql = switch (r.nextInt(4)) {
                case 0 -> "SELECT SUM(balance),COUNT(*) FROM accounts";
                case 1 -> "SELECT o.status,COUNT(*),SUM(i.quantity) FROM orders o JOIN order_items i ON o.id=i.order_id GROUP BY o.status ORDER BY o.status";
                case 2 -> "SELECT c.id,SUM(i.quantity*p.price) FROM customers c JOIN orders o ON c.id=o.customer_id JOIN order_items i ON o.id=i.order_id JOIN products p ON p.id=i.product_id GROUP BY c.id ORDER BY c.id";
                default -> "SELECT id,name,price FROM products ORDER BY name,price,id";
            };
            consume(statement(c, sql));
            return ANALYTICAL;
        }

        private Operation aggregate(Connection c) throws SQLException {
            PreparedStatement s = statement(c,
                    "SELECT COUNT(*),SUM(CAST(ROUND(balance*100,0) AS BIGINT)) FROM accounts");
            try (ResultSet rs = s.executeQuery()) {
                if (!rs.next() || rs.getLong(1) != originalCounts.get("accounts")
                        || rs.getBigDecimal(2) == null || rs.getBigDecimal(2).compareTo(originalBalance) != 0
                        || rs.next()) throw incorrect("Account aggregate mismatch");
            }
            return ANALYTICAL;
        }

        private Operation transfer(Connection c, SplittableRandom r, boolean rollback, int size) throws SQLException {
            boolean deadlocks = name.equals("deadlocks");
            long first = deadlocks ? (r.nextBoolean() ? 1 : Math.min(2, rows))
                    : name.startsWith("contended-") ? 1 : r.nextLong(rows) + 1;
            long second = rows == 1 ? first : deadlocks ? 3 - first : (first % rows) + 1;
            return transferPair(c, first, second, deadlocks, rollback, size);
        }

        private Operation transferPair(Connection c, long first, long second, boolean deadlocks,
                                       boolean rollback, int size) throws SQLException {
            long low = deadlocks ? first : Math.min(first, second);
            long high = deadlocks ? second : Math.max(first, second);
            transaction(c, rollback, () -> {
                PreparedStatement s = statement(c, "UPDATE accounts SET balance=balance+? WHERE id=?");
                s.clearBatch();
                for (int i = 0; i < size; i++) {
                    s.setBigDecimal(1, BigDecimal.ONE);
                    s.setLong(2, low);
                    if (deadlocks) {
                        if (s.executeUpdate() != 1) throw incorrect("Deadlock account disappeared");
                        java.util.concurrent.locks.LockSupport.parkNanos(10_000_000L);
                        if (Thread.currentThread().isInterrupted())
                            throw new java.sql.SQLTimeoutException("Interrupted deadlock transaction");
                    } else s.addBatch();
                    s.setBigDecimal(1, BigDecimal.ONE.negate());
                    s.setLong(2, high);
                    if (deadlocks) {
                        if (s.executeUpdate() != 1) throw incorrect("Deadlock account disappeared");
                    } else s.addBatch();
                }
                if (!deadlocks) checkBatch(s.executeBatch());
                PreparedStatement read = statement(c, "SELECT balance FROM accounts WHERE id=?");
                read.setLong(1, low);
                if (consume(read) != 1) throw incorrect("Transfer account disappeared");
                if (name.equals("long-transactions")) {
                    java.util.concurrent.locks.LockSupport.parkNanos(10_000_000L);
                    if (Thread.currentThread().isInterrupted())
                        throw new java.sql.SQLTimeoutException("Interrupted long transaction");
                }
            });
            return name.endsWith("transactions") || deadlocks || rollback ? TRANSACTION : WRITE;
        }

        private Operation populate(Connection c, int size) throws SQLException {
            transaction(c, false, () -> {
                PreparedStatement s = statement(c, "INSERT INTO financial_transactions(id,account_id,amount,created_at) VALUES(?,?,?,?)");
                s.clearBatch();
                for (int i = 0; i < size; i++) {
                    s.setLong(1, nextId.getAndIncrement());
                    s.setLong(2, 1);
                    s.setBigDecimal(3, new BigDecimal("1.25"));
                    s.setTimestamp(4, TIME);
                    s.addBatch();
                }
                checkBatch(s.executeBatch());
            });
            inserted.addAndGet(size);
            return WRITE;
        }

        private Operation remove(Connection c, SplittableRandom r, int size) throws SQLException {
            long start = r.nextLong(rows) + 1;
            long[] changed = {0};
            transaction(c, false, () -> {
                PreparedStatement s = statement(c, "DELETE FROM financial_transactions WHERE id>=? AND id<?");
                s.setLong(1, start);
                s.setLong(2, Math.min(rows + 1, start + size));
                changed[0] = s.executeUpdate();
            });
            deleted.addAndGet(changed[0]);
            return WRITE;
        }

        private Operation upsert(Connection c, SplittableRandom r) throws SQLException {
            long id = r.nextBoolean() ? r.nextLong(rows) + 1 : nextId.getAndIncrement();
            boolean[] added = {false};
            transaction(c, false, () -> {
                PreparedStatement u = statement(c, "UPDATE financial_transactions SET amount=? WHERE id=?");
                u.setBigDecimal(1, new BigDecimal("1.25"));
                u.setLong(2, id);
                if (u.executeUpdate() == 0) {
                    PreparedStatement s = statement(c, "INSERT INTO financial_transactions(id,account_id,amount,created_at) VALUES(?,?,?,?)");
                    s.setLong(1, id); s.setLong(2, 1);
                    s.setBigDecimal(3, new BigDecimal("1.25")); s.setTimestamp(4, TIME);
                    if (s.executeUpdate() != 1) throw incorrect("Upsert failed");
                    added[0] = true;
                }
            });
            if (added[0]) inserted.incrementAndGet();
            return WRITE;
        }

        private Operation expire(Connection c, SplittableRandom r) throws SQLException {
            long[] removed = {0};
            transaction(c, false, () -> {
                PreparedStatement s = statement(c, "DELETE FROM financial_transactions WHERE created_at<?");
                s.setTimestamp(1, new Timestamp(1_704_067_200_000L + (r.nextLong(rows) + 1) * 1000));
                removed[0] = s.executeUpdate();
            });
            deleted.addAndGet(removed[0]);
            return WRITE;
        }

        private Operation maintenance(Connection c, SplittableRandom r) throws SQLException {
            return switch (r.nextInt(4)) {
                case 0 -> populate(c, transactionSize);
                case 1 -> remove(c, r, transactionSize);
                case 2 -> transfer(c, r, false, 1);
                default -> refresh(c, transactionSize);
            };
        }

        private Operation refresh(Connection c, int size) throws SQLException {
            long[] removed = {0};
            transaction(c, false, () -> {
                PreparedStatement s = statement(c, "DELETE FROM financial_transactions");
                removed[0] = s.executeUpdate();
                s = statement(c, "INSERT INTO financial_transactions(id,account_id,amount,created_at) VALUES(?,?,?,?)");
                s.clearBatch();
                int pending = 0;
                for (long i = 0; i < rows; i++) {
                    s.setLong(1, nextId.getAndIncrement()); s.setLong(2, 1);
                    s.setBigDecimal(3, new BigDecimal("1.25")); s.setTimestamp(4, TIME); s.addBatch();
                    if (++pending == size) {
                        checkBatch(s.executeBatch());
                        s.clearBatch();
                        pending = 0;
                    }
                }
                if (pending > 0) checkBatch(s.executeBatch());
            });
            deleted.addAndGet(removed[0]);
            inserted.addAndGet(rows);
            return WRITE;
        }

        @FunctionalInterface
        private interface SqlAction { void run() throws SQLException; }

        private void transaction(Connection c, boolean rollback, SqlAction action) throws SQLException {
            boolean auto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                action.run();
                if (rollback) c.rollback(); else c.commit();
            } catch (SQLException | RuntimeException e) {
                try { c.rollback(); } catch (SQLException secondary) { e.addSuppressed(secondary); }
                throw e;
            } finally {
                c.setAutoCommit(auto);
            }
        }

        private void checkBatch(int[] updates) throws SQLException {
            for (int count : updates)
                if (count != 1 && count != java.sql.Statement.SUCCESS_NO_INFO)
                    throw incorrect("Unexpected batch update count: " + count);
        }

        private synchronized PreparedStatement statement(Connection c, String sql) throws SQLException {
            Map<String, PreparedStatement> cache = statements.computeIfAbsent(c, ignored -> new LinkedHashMap<>());
            PreparedStatement s = cache.get(sql);
            if (s == null) { s = c.prepareStatement(sql); cache.put(sql, s); }
            return s;
        }

        private long consume(PreparedStatement s) throws SQLException {
            long count = 0, hash = 0;
            try (ResultSet rs = s.executeQuery()) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    count++;
                    for (int column = 1; column <= columns; column++) {
                        Object value = rs.getObject(column);
                        hash = 31 * hash + (value == null ? 0 : value instanceof byte[] b
                                ? java.util.Arrays.hashCode(b) : value.hashCode());
                    }
                }
            }
            consumed = hash;
            return count;
        }

        @Override
        public void validate(Connection c) throws SQLException {
            if (!prepared) throw incorrect("Missing baseline validation");
            if (Set.of("point", "zipf", "hot", "range", "indexed-range", "joins", "relational", "cte", "aggregate").contains(name))
                DeterministicDataset.validate(c, rows, payloadSize, seed);
            for (Map.Entry<String, Long> entry : originalCounts.entrySet()) {
                long expected = entry.getValue();
                if (entry.getKey().equals("financial_transactions"))
                    expected += inserted.get() - deleted.get();
                if (scalar(c, "SELECT COUNT(*) FROM " + entry.getKey()) != expected)
                    throw incorrect("Committed record count mismatch in " + entry.getKey());
            }
            if (decimal(c, "SELECT SUM(CAST(ROUND(balance*100,0) AS BIGINT)) FROM accounts").compareTo(originalBalance) != 0)
                throw incorrect("Total account balance changed");
            if (scalar(c, "SELECT COUNT(*) FROM financial_transactions t LEFT JOIN accounts a ON t.account_id=a.id WHERE a.id IS NULL") != 0)
                throw incorrect("Orphan financial transaction");
            if (scalar(c, "SELECT COUNT(*) FROM financial_transactions WHERE id>" + rows + " AND amount<>1.25") != 0)
                throw incorrect("Inserted monetary value changed");
        }

        private long scalar(Connection c, String sql) throws SQLException {
            try (PreparedStatement s = c.prepareStatement(sql); ResultSet rs = s.executeQuery()) {
                if (!rs.next()) throw incorrect("Missing scalar");
                return rs.getLong(1);
            }
        }

        private BigDecimal decimal(Connection c, String sql) throws SQLException {
            try (PreparedStatement s = c.prepareStatement(sql); ResultSet rs = s.executeQuery()) {
                if (!rs.next() || rs.getBigDecimal(1) == null) throw incorrect("Missing monetary aggregate");
                return rs.getBigDecimal(1);
            }
        }

        @Override
        public synchronized void close() throws SQLException {
            SQLException failure = null;
            for (Map<String, PreparedStatement> cache : statements.values())
                for (PreparedStatement s : cache.values())
                    try { s.close(); } catch (SQLException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
            statements.clear();
            if (failure != null) throw failure;
        }
    }

    private static SQLException incorrect(String message) { return new SQLException(message, "BENCH_CORRECTNESS"); }

    // Rejection-inversion sampling of the truncated discrete Zipf distribution, exponent 1.07.
    static long zipf(SplittableRandom r, long size) {
        double exponent = 1.07;
        double hLower = integral(1.5, exponent) - 1;
        double hUpper = integral(size + 0.5, exponent);
        double squeeze = 2 - inverse(integral(2.5, exponent) - Math.pow(2, -exponent), exponent);
        while (true) {
            double u = hUpper + r.nextDouble() * (hLower - hUpper);
            double x = inverse(u, exponent);
            long k = Math.max(1, Math.min(size, Math.round(x)));
            if (k - x <= squeeze || u >= integral(k + 0.5, exponent) - Math.pow(k, -exponent))
                return k;
        }
    }

    private static double integral(double x, double exponent) {
        return Math.expm1((1 - exponent) * Math.log(x)) / (1 - exponent);
    }

    private static double inverse(double x, double exponent) {
        return Math.exp(Math.log1p((1 - exponent) * x) / (1 - exponent));
    }
}
