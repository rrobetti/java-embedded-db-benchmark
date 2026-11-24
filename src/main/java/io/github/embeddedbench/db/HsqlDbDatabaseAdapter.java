package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class HsqlDbDatabaseAdapter extends AbstractDatabaseAdapter {
    private final String profile;
    private XaProbe.Result xaProbe = XaProbe.untested();

    public HsqlDbDatabaseAdapter() {
        this("default");
    }

    public HsqlDbDatabaseAdapter(String profile) {
        if (!"default".equals(profile) && !"mvcc".equals(profile)) {
            throw new IllegalArgumentException("HSQLDB profile must be default or mvcc: " + profile);
        }
        this.profile = profile;
    }

    @Override
    public String id() {
        return "hsqldb";
    }

    @Override
    String jdbcUrl() {
        return "jdbc:hsqldb:mem:benchmark";
    }

    @Override
    String username() {
        return "SA";
    }

    @Override
    void afterInitialize(Connection connection) throws SQLException {
        if ("mvcc".equals(profile)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET DATABASE TRANSACTION CONTROL MVCC");
            }
        }
        xaProbe = XaProbe.run(connection, "org.hsqldb.jdbc.pool.JDBCXADataSource", "setUrl", jdbcUrl(), username());
    }

    @Override
    String shutdownSql() {
        return "SHUTDOWN";
    }

    public XaProbe.Result xaProbe() {
        return xaProbe;
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(true, true, "mvcc".equals(profile), xaProbe.supported(), true, "embedded-jdbc",
                "mvcc".equals(profile) ? "MVCC; conflicting row writers may block."
                        : "Engine default LOCKS transaction control uses table locks; readers can block behind writers.");
    }
}
