package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

abstract class AbstractDatabaseAdapter implements DatabaseAdapter {
    private Connection anchor;
    private boolean initialized;

    abstract String jdbcUrl();

    String username() {
        return "";
    }

    String password() {
        return "";
    }

    Properties connectionProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", username());
        properties.setProperty("password", password());
        return properties;
    }

    void configure(Connection connection) throws SQLException {
    }

    void afterInitialize(Connection connection) throws SQLException {
    }

    String shutdownSql() {
        return null;
    }

    String binaryType() {
        return "BLOB";
    }

    @Override
    public synchronized void initialize() throws SQLException {
        if (initialized) {
            return;
        }
        Connection connection = connect();
        try {
            afterInitialize(connection);
            anchor = connection;
            initialized = true;
        } catch (SQLException | RuntimeException exception) {
            connection.close();
            throw exception;
        }
    }

    Connection connect() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl(), connectionProperties());
        try {
            configure(connection);
            return connection;
        } catch (SQLException | RuntimeException exception) {
            connection.close();
            throw exception;
        }
    }

    @Override
    public synchronized Connection openConnection() throws SQLException {
        if (!initialized) {
            throw new SQLException(id() + " adapter is not initialized");
        }
        return connect();
    }

    @Override
    public void createSchema(Connection connection) throws SQLException {
        Schema.create(connection, binaryType());
    }

    @Override
    public String version() throws SQLException {
        try (Connection connection = openConnection()) {
            return connection.getMetaData().getDatabaseProductVersion();
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!initialized) {
            return;
        }
        initialized = false;
        Connection connection = anchor;
        anchor = null;
        try (connection) {
            if (shutdownSql() != null) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(shutdownSql());
                }
            }
        }
    }

    static void requireDefaultProfile(String profile) {
        if (!"default".equals(profile)) {
            throw new IllegalArgumentException("Unsupported profile: " + profile);
        }
    }
}
