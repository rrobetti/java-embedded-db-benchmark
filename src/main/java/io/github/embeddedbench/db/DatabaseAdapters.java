package io.github.embeddedbench.db;

import java.util.Locale;
import java.util.Objects;
import java.sql.SQLException;

public final class DatabaseAdapters {
    private DatabaseAdapters() {
    }

    public static void initializeDriver(String id) throws SQLException {
        String driver = switch (Objects.requireNonNull(id, "id").toLowerCase(Locale.ROOT)) {
            case "h2" -> "org.h2.Driver";
            case "hsqldb", "hsql" -> "org.hsqldb.jdbc.JDBCDriver";
            case "sqlite" -> "org.sqlite.JDBC";
            case "pglite" -> "io.roastedroot.pglite4j.jdbc.PgLiteDriver";
            default -> throw new IllegalArgumentException("Unknown database: " + id);
        };
        try {
            Class.forName(driver, true, DatabaseAdapters.class.getClassLoader());
            if ("pglite".equalsIgnoreCase(id)) {
                Class.forName("org.postgresql.Driver", true, DatabaseAdapters.class.getClassLoader());
            }
        } catch (ClassNotFoundException exception) {
            throw new SQLException("JDBC driver is missing for " + id, exception);
        }
    }

    public static DatabaseAdapter create(String id, String profile) {
        Objects.requireNonNull(id, "id");
        String selectedProfile = profile == null ? "default" : profile.toLowerCase(Locale.ROOT);
        return switch (id.toLowerCase(Locale.ROOT)) {
            case "h2" -> new H2DatabaseAdapter(selectedProfile);
            case "hsqldb", "hsql" -> new HsqlDbDatabaseAdapter(selectedProfile);
            case "sqlite" -> new SQLiteDatabaseAdapter(selectedProfile);
            case "pglite" -> new PgLiteDatabaseAdapter(selectedProfile);
            default -> throw new IllegalArgumentException("Unknown database: " + id);
        };
    }
}
