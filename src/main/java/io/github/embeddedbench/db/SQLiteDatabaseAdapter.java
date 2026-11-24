package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

public final class SQLiteDatabaseAdapter extends AbstractDatabaseAdapter {
    public SQLiteDatabaseAdapter() {
        this("default");
    }

    public SQLiteDatabaseAdapter(String profile) {
        requireDefaultProfile(profile);
    }

    @Override
    public String id() {
        return "sqlite";
    }

    @Override
    String jdbcUrl() {
        return "jdbc:sqlite:file:benchmark?mode=memory&cache=shared";
    }

    @Override
    void configure(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA foreign_keys=ON");
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("PRAGMA temp_store=MEMORY");
            statement.execute("PRAGMA locking_mode=NORMAL");
            statement.execute("PRAGMA read_uncommitted=0");
        }
    }

    @Override
    String binaryType() {
        return "BLOB";
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(true, true, false, false, true, "embedded-jdbc",
                "Shared-cache in-memory SQLite permits one writer; SQLITE_LOCKED is not retried by busy_timeout. "
                        + "DECIMAL uses numeric affinity rather than fixed-point storage.");
    }

    public static boolean isBusyOrLocked(SQLException exception) {
        for (SQLException current = exception; current != null; current = current.getNextException()) {
            int primaryCode = current.getErrorCode() & 0xff;
            String message = current.getMessage();
            if (primaryCode == 5 || primaryCode == 6
                    || (message != null && (message.toUpperCase(Locale.ROOT).contains("SQLITE_BUSY")
                    || message.toUpperCase(Locale.ROOT).contains("SQLITE_LOCKED")))) {
                return true;
            }
        }
        return false;
    }
}
