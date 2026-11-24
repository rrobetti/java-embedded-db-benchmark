package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.SQLException;

public final class H2DatabaseAdapter extends AbstractDatabaseAdapter {
    private XaProbe.Result xaProbe = XaProbe.untested();
    public H2DatabaseAdapter() {
        this("default");
    }

    public H2DatabaseAdapter(String profile) {
        requireDefaultProfile(profile);
    }

    @Override
    public String id() {
        return "h2";
    }

    @Override
    String jdbcUrl() {
        return "jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1";
    }

    @Override
    String username() {
        return "sa";
    }

    @Override
    String shutdownSql() {
        return "SHUTDOWN";
    }

    @Override
    void afterInitialize(Connection connection) throws SQLException {
        xaProbe = XaProbe.run(connection, "org.h2.jdbcx.JdbcDataSource", "setURL", jdbcUrl(), username());
    }

    public XaProbe.Result xaProbe() {
        return xaProbe;
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(true, true, true, xaProbe.supported(), true, "embedded-jdbc",
                "Row-level write conflicts may block; XA is engine-specific, not a distributed benchmark.");
    }
}
