package io.github.embeddedbench.db;

import java.util.Locale;
import java.util.Objects;

public final class DatabaseAdapters {
    private DatabaseAdapters() {
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
