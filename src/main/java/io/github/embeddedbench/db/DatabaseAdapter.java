package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public interface DatabaseAdapter extends AutoCloseable {
    String id();
    void initialize() throws SQLException;
    Connection openConnection() throws SQLException;
    void createSchema(Connection connection) throws SQLException;
    Capabilities capabilities();
    String version() throws SQLException;
    default void configureIsolation(Connection connection, int level) throws SQLException {
        if (!connection.getMetaData().supportsTransactionIsolationLevel(level)) {
            throw new SQLFeatureNotSupportedException(id() + " does not support isolation level " + level);
        }
        connection.setTransactionIsolation(level);
        if (connection.getTransactionIsolation() != level) {
            throw new SQLException(id() + " did not apply requested isolation level " + level);
        }
    }
    @Override
    void close() throws SQLException;
}
