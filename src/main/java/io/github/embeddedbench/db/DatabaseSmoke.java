package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Real-engine connectivity, schema, deterministic load, join/CTE and rollback smoke. */
public final class DatabaseSmoke {
    private DatabaseSmoke() {
    }

    public record Result(String database, String version, Capabilities capabilities) {
    }

    public static List<Result> smokeAll() throws SQLException {
        return smoke(List.of("h2", "hsqldb", "sqlite", "pglite"));
    }

    public static List<Result> smoke(List<String> databases) throws SQLException {
        List<Result> results = new ArrayList<>();
        for (String database : databases) {
            try (DatabaseAdapter adapter = DatabaseAdapters.create(database, "default")) {
                adapter.initialize();
                String version = adapter.version();
                try (Connection connection = adapter.openConnection()) {
                    adapter.createSchema(connection);
                    DeterministicDataset.load(connection, 16, 64, 42, 7);
                    DeterministicDataset.validate(connection, 16, 64, 42);
                    try (Statement statement = connection.createStatement();
                         ResultSet result = statement.executeQuery("WITH counts AS (SELECT o.id, COUNT(i.id) AS n "
                                 + "FROM orders o JOIN order_items i ON i.order_id=o.id GROUP BY o.id) "
                                 + "SELECT COUNT(*), SUM(n) FROM counts")) {
                        if (!result.next() || result.getLong(1) != 16 || result.getLong(2) != 32) {
                            throw new SQLException(database + " join/CTE smoke failed");
                        }
                    }
                    connection.setAutoCommit(false);
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("UPDATE accounts SET balance=balance+0.01 WHERE id=1");
                    }
                    connection.rollback();
                    connection.setAutoCommit(true);
                    DeterministicDataset.validate(connection, 16, 64, 42);
                }
                results.add(new Result(database, version, adapter.capabilities()));
            }
        }
        return List.copyOf(results);
    }

    public static void main(String[] arguments) throws SQLException {
        List<String> databases = List.of("h2", "hsqldb", "sqlite", "pglite");
        if (arguments.length == 2 && "--databases".equals(arguments[0])) {
            databases = List.of(arguments[1].split(","));
        } else if (arguments.length != 0) {
            throw new IllegalArgumentException("Usage: DatabaseSmoke [--databases h2,hsqldb,sqlite,pglite]");
        }
        for (Result result : smoke(databases)) {
            System.out.printf("PASS %s version=%s mode=%s xa=%s independentTransactions=%s limitations=%s%n",
                    result.database(), result.version().replace('\n', ' '), result.capabilities().executionMode(),
                    result.capabilities().xa(), result.capabilities().independentTransactions(),
                    result.capabilities().limitations());
        }
    }
}
