package io.github.embeddedbench.db;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HexFormat;
import java.lang.reflect.Proxy;

/**
 * Generates customers/accounts/products/orders/transactions at N rows each and
 * order_items at 2N rows, using bounded streaming batches.
 */
public final class DeterministicDataset {
    private static final long BASE_MILLIS = 1_704_067_200_000L;

    private DeterministicDataset() {
    }

    public static void load(Connection connection, long rows, int payloadSize, long seed, int batchSize)
            throws SQLException {
        load(connection, rows, payloadSize, seed, batchSize, "batch");
    }

    public static void load(Connection connection, long rows, int payloadSize, long seed, int batchSize, String mode)
            throws SQLException {
        checkArguments(rows, payloadSize);
        if (!"batch".equals(mode) && !"prepared".equals(mode) && !"individual".equals(mode)) {
            throw new IllegalArgumentException("Supported ingestion modes: batch, prepared, individual; got " + mode);
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        boolean ownedTransaction = connection.getAutoCommit();
        Savepoint savepoint = null;
        if (ownedTransaction) {
            connection.setAutoCommit(false);
        } else {
            savepoint = connection.setSavepoint();
        }
        try {
            int effectiveBatchSize = "prepared".equals(mode) ? 0 : batchSize;
            insert(connection, "INSERT INTO customers VALUES (?,?,?,?,?)", rows, effectiveBatchSize, mode, (statement, id) -> {
                statement.setLong(1, id);
                statement.setString(2, customerName(id));
                statement.setString(3, email(id));
                statement.setString(4, notes(id));
                statement.setBytes(5, payload(id, payloadSize, seed));
            });
            insert(connection, "INSERT INTO accounts VALUES (?,?,?)", rows, effectiveBatchSize, mode, (statement, id) -> {
                statement.setLong(1, id);
                statement.setLong(2, id);
                statement.setBigDecimal(3, balance(id, seed));
            });
            insert(connection, "INSERT INTO products VALUES (?,?,?)", rows, effectiveBatchSize, mode, (statement, id) -> {
                statement.setLong(1, id);
                statement.setString(2, productName(id));
                statement.setBigDecimal(3, price(id, seed));
            });
            insert(connection, "INSERT INTO orders VALUES (?,?,?,?)", rows, effectiveBatchSize, mode, (statement, id) -> {
                statement.setLong(1, id);
                statement.setLong(2, id);
                statement.setTimestamp(3, timestamp(id));
                statement.setString(4, status(id));
            });
            insert(connection, "INSERT INTO order_items VALUES (?,?,?,?)", Math.multiplyExact(rows, 2), effectiveBatchSize, mode,
                    (statement, id) -> {
                        long orderId = (id + 1) / 2;
                        statement.setLong(1, id);
                        statement.setLong(2, orderId);
                        statement.setLong(3, itemProduct(id, rows));
                        statement.setInt(4, quantity(id, seed));
                    });
            insert(connection, "INSERT INTO financial_transactions VALUES (?,?,?,?)", rows, effectiveBatchSize, mode,
                    (statement, id) -> {
                        statement.setLong(1, id);
                        statement.setLong(2, id);
                        statement.setBigDecimal(3, amount(id, seed));
                        statement.setTimestamp(4, timestamp(id));
                    });
            if (ownedTransaction) {
                connection.commit();
            } else {
                connection.releaseSavepoint(savepoint);
            }
        } catch (SQLException | RuntimeException exception) {
            try {
                if (ownedTransaction) {
                    connection.rollback();
                } else {
                    connection.rollback(savepoint);
                }
            } catch (SQLException rollbackFailure) {
                exception.addSuppressed(rollbackFailure);
            }
            throw exception;
        } finally {
            if (ownedTransaction) {
                connection.setAutoCommit(true);
            }
        }
    }

    public static void validate(Connection connection, long rows, int payloadSize, long seed) throws SQLException {
        checkArguments(rows, payloadSize);
        scan(connection, "customers", rows, (result, id) -> {
            equal(customerName(id), result.getString("name"), "customer name", id);
            equal(email(id), result.getString("email"), "customer email", id);
            equal(notes(id), result.getString("notes"), "customer notes", id);
            if (!Arrays.equals(payload(id, payloadSize, seed), result.getBytes("payload"))) {
                throw mismatch("customer payload", id);
            }
        });
        BigDecimal[] balances = {BigDecimal.ZERO};
        scan(connection, "accounts", rows, (result, id) -> {
            equal(id, result.getLong("customer_id"), "account customer", id);
            decimal(balance(id, seed), result.getBigDecimal("balance"), "balance", id);
            balances[0] = balances[0].add(balance(id, seed));
        });
        BigDecimal[] prices = {BigDecimal.ZERO};
        scan(connection, "products", rows, (result, id) -> {
            equal(productName(id), result.getString("name"), "product name", id);
            decimal(price(id, seed), result.getBigDecimal("price"), "price", id);
            prices[0] = prices[0].add(price(id, seed));
        });
        scan(connection, "orders", rows, (result, id) -> {
            equal(id, result.getLong("customer_id"), "order customer", id);
            equal(timestamp(id), result.getTimestamp("created_at"), "order timestamp", id);
            equal(status(id), result.getString("status"), "order status", id);
        });
        long[] quantities = {0};
        scan(connection, "order_items", Math.multiplyExact(rows, 2), (result, id) -> {
            equal((id + 1) / 2, result.getLong("order_id"), "item order", id);
            equal(itemProduct(id, rows), result.getLong("product_id"), "item product", id);
            equal(quantity(id, seed), result.getInt("quantity"), "item quantity", id);
            quantities[0] = Math.addExact(quantities[0], quantity(id, seed));
        });
        BigDecimal[] amounts = {BigDecimal.ZERO};
        scan(connection, "financial_transactions", rows, (result, id) -> {
            equal(id, result.getLong("account_id"), "transaction account", id);
            decimal(amount(id, seed), result.getBigDecimal("amount"), "amount", id);
            equal(timestamp(id), result.getTimestamp("created_at"), "transaction timestamp", id);
            amounts[0] = amounts[0].add(amount(id, seed));
        });
        aggregate(connection, "accounts", "balance", balances[0]);
        aggregate(connection, "products", "price", prices[0]);
        aggregate(connection, "financial_transactions", "amount", amounts[0]);
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COALESCE(SUM(quantity),0) FROM order_items")) {
            result.next();
            equal(quantities[0], result.getLong(1), "quantity aggregate", 0);
        }
    }

    /**
     * Logical bytes exclude indexes, allocation, encoding overhead and null markers.
     * BIGINT, timestamp and decimal count as 8 bytes, INTEGER as 4, ASCII text as
     * its length, binary as payloadSize. Thus bytes = N*(202+payloadSize)
     * + 3*sum(digits(id)) + floor(N/2) + sum(id%4!=0 ? 5+digits(id) : 0).
     */
    public static long logicalBytes(long rows, int payloadSize) {
        checkArguments(rows, payloadSize);
        long result = Math.addExact(Math.multiplyExact(rows, 202L + payloadSize), rows / 2);
        long start = 1;
        int digits = 1;
        while (start <= rows) {
            long end = start > Long.MAX_VALUE / 10 ? rows : Math.min(rows, start * 10 - 1);
            long count = end - start + 1;
            long nonNull = count - (end / 4 - (start - 1) / 4);
            result = Math.addExact(result, Math.multiplyExact(count, 3L * digits));
            result = Math.addExact(result, Math.multiplyExact(nonNull, 5L + digits));
            if (end == rows) {
                break;
            }
            start *= 10;
            digits++;
        }
        return result;
    }

    private static void checkArguments(long rows, int payloadSize) {
        if (rows < 0 || rows > Long.MAX_VALUE / 2 || payloadSize < 0) {
            throw new IllegalArgumentException("rows must be nonnegative and <= Long.MAX_VALUE/2; payloadSize nonnegative");
        }
    }

    private static void insert(Connection connection, String sql, long rows, int batchSize, String mode, Binder binder)
            throws SQLException {
        if ("individual".equals(mode)) {
            insertIndividual(connection, sql, rows, binder);
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int pending = 0;
            for (long id = 1; id <= rows; id++) {
                binder.bind(statement, id);
                if (batchSize == 0) {
                    statement.executeUpdate();
                    continue;
                }
                statement.addBatch();
                if (++pending == batchSize) {
                    statement.executeBatch();
                    statement.clearBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                statement.executeBatch();
            }
        }
    }

    private static void insertIndividual(Connection connection, String template, long rows, Binder binder)
            throws SQLException {
        Object[] values = new Object[(int) template.chars().filter(character -> character == '?').count()];
        PreparedStatement parameters = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    if (method.getName().startsWith("set") && arguments != null && arguments.length == 2
                            && arguments[0] instanceof Integer index) {
                        values[index - 1] = arguments[1];
                        return null;
                    }
                    throw new UnsupportedOperationException("Literal binding does not execute prepared SQL");
                });
        String product = connection.getMetaData().getDatabaseProductName();
        boolean sqlite = "SQLite".equalsIgnoreCase(product);
        boolean postgres = "PostgreSQL".equalsIgnoreCase(product);
        String prefix = template.substring(0, template.indexOf('(') + 1);
        try (Statement statement = connection.createStatement()) {
            for (long id = 1; id <= rows; id++) {
                binder.bind(parameters, id);
                StringBuilder sql = new StringBuilder(prefix);
                for (int index = 0; index < values.length; index++) {
                    if (index != 0) {
                        sql.append(',');
                    }
                    sql.append(literal(values[index], sqlite, postgres));
                }
                statement.executeUpdate(sql.append(')').toString());
            }
        }
    }

    private static String literal(Object value, boolean sqlite, boolean postgres) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof byte[] bytes) {
            String hex = HexFormat.of().formatHex(bytes);
            return postgres ? "decode('" + hex + "','hex')" : "X'" + hex + "'";
        }
        if (value instanceof Timestamp timestamp) {
            return sqlite ? Long.toString(timestamp.getTime()) : "TIMESTAMP '" + timestamp + "'";
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Number) {
            return value.toString();
        }
        return "'" + value.toString().replace("'", "''") + "'";
    }

    private static void scan(Connection connection, String table, long rows, RowValidator validator)
            throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT * FROM " + table + " ORDER BY id")) {
            long id = 0;
            while (result.next()) {
                id++;
                equal(id, result.getLong("id"), table + " primary key", id);
                validator.validate(result, id);
            }
            equal(rows, id, table + " row count", 0);
        }
    }

    private static void aggregate(Connection connection, String table, String column, BigDecimal expected)
            throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COALESCE(SUM(CAST(ROUND("
                     + column + "*100,0) AS BIGINT)),0) FROM " + table)) {
            result.next();
            if (expected.movePointRight(2).compareTo(result.getBigDecimal(1)) != 0) {
                throw mismatch(table + " aggregate in exact integer cents", 0);
            }
        }
    }

    private static void decimal(BigDecimal expected, BigDecimal actual, String field, long id) throws SQLException {
        if (actual == null || expected.compareTo(actual.setScale(2, RoundingMode.HALF_UP)) != 0
                || actual.subtract(actual.setScale(2, RoundingMode.HALF_UP)).abs()
                .compareTo(new BigDecimal("0.0000001")) > 0) {
            throw mismatch(field, id);
        }
    }

    private static void equal(Object expected, Object actual, String field, long id) throws SQLException {
        if (!java.util.Objects.equals(expected, actual)) {
            throw mismatch(field, id);
        }
    }

    private static SQLException mismatch(String field, long id) {
        return new SQLException("Dataset validation failed: " + field + " at id " + id);
    }

    private static String customerName(long id) {
        return "customer-" + id;
    }

    private static String email(long id) {
        return customerName(id) + "@example.test";
    }

    private static String productName(long id) {
        return "product-" + id;
    }

    private static String notes(long id) {
        return id % 4 == 0 ? null : "note-" + id;
    }

    private static String status(long id) {
        return id % 2 == 0 ? "PAID" : "NEW";
    }

    private static Timestamp timestamp(long id) {
        return new Timestamp(BASE_MILLIS + (id % 31_536_000L) * 1000L);
    }

    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private static BigDecimal balance(long id, long seed) {
        return BigDecimal.valueOf(1_000_000L + Math.floorMod(mix(id ^ seed), 100_000L), 2);
    }

    private static BigDecimal price(long id, long seed) {
        return BigDecimal.valueOf(1L + Math.floorMod(mix(id ^ seed ^ 0x1234L), 100_000L), 2);
    }

    private static BigDecimal amount(long id, long seed) {
        return BigDecimal.valueOf(Math.floorMod(mix(id ^ seed ^ 0x5678L), 200_001L) - 100_000L, 2);
    }

    private static int quantity(long id, long seed) {
        return 1 + (int) Math.floorMod(mix(id ^ seed), 5L);
    }

    private static long itemProduct(long id, long rows) {
        long orderId = (id + 1) / 2;
        return id % 2 == 1 ? orderId : orderId % rows + 1;
    }

    private static byte[] payload(long id, int size, long seed) {
        byte[] bytes = new byte[size];
        long state = mix(id ^ seed);
        for (int offset = 0; offset < size; offset++) {
            if (offset % 8 == 0) {
                state = mix(state + offset + 0x9e3779b97f4a7c15L);
            }
            bytes[offset] = (byte) (state >>> ((offset % 8) * 8));
        }
        return bytes;
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement, long id) throws SQLException;
    }

    @FunctionalInterface
    private interface RowValidator {
        void validate(ResultSet result, long id) throws SQLException;
    }
}
