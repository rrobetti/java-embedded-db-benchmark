package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseAdapter extends AutoCloseable {
    String id();
    void initialize() throws SQLException;
    Connection openConnection() throws SQLException;
    void createSchema(Connection connection) throws SQLException;
    Capabilities capabilities();
    String version() throws SQLException;
    @Override
    void close() throws SQLException;
}
