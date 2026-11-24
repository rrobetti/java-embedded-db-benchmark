package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/** Small, real XA lifecycle probe run at engine initialization, outside timed workloads. */
public final class XaProbe {
    private static final AtomicInteger IDS = new AtomicInteger();

    public record Result(boolean supported, boolean prepare, boolean recover, boolean commit,
                         boolean rollback, String details) {
    }

    private XaProbe() {
    }

    public static Result result(DatabaseAdapter adapter) {
        if (adapter instanceof H2DatabaseAdapter h2) {
            return h2.xaProbe();
        }
        if (adapter instanceof HsqlDbDatabaseAdapter hsql) {
            return hsql.xaProbe();
        }
        return new Result(false, false, false, false, false, "Engine has no supported independent XA resources");
    }

    static Result untested() {
        return new Result(false, false, false, false, false, "Not initialized; XA lifecycle not yet probed");
    }

    static Result run(Connection control, String sourceClass, String urlSetter, String url, String username)
            throws SQLException {
        try {
            return perform(control, sourceClass, urlSetter, url, username);
        } catch (SQLException exception) {
            String state = exception.getSQLState();
            if (state != null && (state.startsWith("53") || "HY001".equals(state))) {
                throw exception;
            }
            StringBuilder details = new StringBuilder(exception.toString());
            for (Throwable cause = exception.getCause(); cause != null; cause = cause.getCause()) {
                details.append("; caused by ").append(cause);
            }
            return new Result(false, false, false, false, false, details.toString());
        }
    }

    private static Result perform(Connection control, String sourceClass, String urlSetter, String url, String username)
            throws SQLException {
        try (Statement statement = control.createStatement()) {
            statement.executeUpdate("CREATE TABLE embeddedbench_xa_probe (id BIGINT PRIMARY KEY)");
        }
        try {
            XADataSource source = (XADataSource) Class.forName(sourceClass).getConstructor().newInstance();
            source.getClass().getMethod(urlSetter, String.class).invoke(source, url);
            source.getClass().getMethod("setUser", String.class).invoke(source, username);
            source.getClass().getMethod("setPassword", String.class).invoke(source, "");
            XAConnection xa = source.getXAConnection();
            try {
                XAResource resource = xa.getXAResource();
                try (Connection connection = xa.getConnection()) {
                    for (int branch = 1; branch <= 2; branch++) {
                        Xid xid = xid(IDS.incrementAndGet());
                        resource.start(xid, XAResource.TMNOFLAGS);
                        try (Statement statement = connection.createStatement()) {
                            statement.executeUpdate("INSERT INTO embeddedbench_xa_probe VALUES (" + branch + ")");
                        }
                        resource.end(xid, XAResource.TMSUCCESS);
                        if (resource.prepare(xid) != XAResource.XA_OK) {
                            throw new SQLException("XA probe did not prepare a writing branch");
                        }
                        Xid[] recovered = resource.recover(XAResource.TMSTARTRSCAN);
                        resource.recover(XAResource.TMENDRSCAN);
                        if (Arrays.stream(recovered).noneMatch(value -> sameXid(xid, value))) {
                            throw new SQLException("XA probe could not recover its prepared branch");
                        }
                        if (branch == 1) {
                            resource.commit(xid, false);
                        } else {
                            resource.rollback(xid);
                        }
                    }
                }
            } finally {
                xa.close();
            }
            try (Statement statement = control.createStatement();
                 ResultSet result = statement.executeQuery("SELECT COUNT(*), MIN(id), MAX(id) FROM embeddedbench_xa_probe")) {
                if (!result.next() || result.getLong(1) != 1 || result.getLong(2) != 1 || result.getLong(3) != 1) {
                    throw new SQLException("XA commit/rollback probe produced incorrect visibility");
                }
            }
            return new Result(true, true, true, true, true,
                    "Actual XADataSource: two writing branches prepared/recovered, one committed, one rolled back");
        } catch (ReflectiveOperationException | XAException exception) {
            throw new SQLException("Real-engine XA lifecycle probe failed", exception);
        } finally {
            try (Statement statement = control.createStatement()) {
                statement.executeUpdate("DROP TABLE embeddedbench_xa_probe");
            }
        }
    }

    private static Xid xid(int id) {
        byte[] global = java.nio.ByteBuffer.allocate(4).putInt(id).array();
        return new Xid() {
            public int getFormatId() {
                return 0x45424442;
            }

            public byte[] getGlobalTransactionId() {
                return global.clone();
            }

            public byte[] getBranchQualifier() {
                return new byte[]{1};
            }
        };
    }

    private static boolean sameXid(Xid left, Xid right) {
        return left.getFormatId() == right.getFormatId()
                && Arrays.equals(left.getGlobalTransactionId(), right.getGlobalTransactionId())
                && Arrays.equals(left.getBranchQualifier(), right.getBranchQualifier());
    }
}
