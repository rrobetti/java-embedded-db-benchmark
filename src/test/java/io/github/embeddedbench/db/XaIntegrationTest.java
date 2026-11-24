package io.github.embeddedbench.db;

import org.h2.jdbcx.JdbcDataSource;
import org.hsqldb.jdbc.pool.JDBCXADataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class XaIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {"h2", "hsqldb"})
    void actualXaPrepareRecoverCommitAndRollback(String engine) throws Exception {
        try (DatabaseAdapter adapter = DatabaseAdapters.create(engine, "default")) {
            adapter.initialize();
            assertTrue(adapter.capabilities().xa());
            try (Connection schema = adapter.openConnection()) {
                adapter.createSchema(schema);
            }
            XAConnection xa = source(engine).getXAConnection();
            try {
                XAResource resource = xa.getXAResource();
                try (Connection connection = xa.getConnection()) {
                    Xid committed = xid(1);
                    resource.start(committed, XAResource.TMNOFLAGS);
                    insert(connection, 9001);
                    resource.end(committed, XAResource.TMSUCCESS);
                    assertEquals(XAResource.XA_OK, resource.prepare(committed));
                    Xid[] pending = resource.recover(XAResource.TMSTARTRSCAN);
                    resource.recover(XAResource.TMENDRSCAN);
                    assertTrue(Arrays.stream(pending).anyMatch(found -> sameXid(committed, found)),
                            "prepared branch must be recoverable before commit");
                    resource.commit(committed, false);
                    assertCount(adapter, 9001, 1);

                    Xid rolledBack = xid(2);
                    resource.start(rolledBack, XAResource.TMNOFLAGS);
                    insert(connection, 9002);
                    resource.end(rolledBack, XAResource.TMSUCCESS);
                    assertEquals(XAResource.XA_OK, resource.prepare(rolledBack));
                    resource.rollback(rolledBack);
                    assertCount(adapter, 9002, 0);
                    assertEquals(0, resource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
                }
            } finally {
                xa.close();
            }
        }
    }

    @Test
    void narayanaCoordinatesTwoRealEngineResourcesForCommitAndRollback() throws Exception {
        com.arjuna.ats.arjuna.common.arjPropertyManager.getObjectStoreEnvironmentBean()
                .setObjectStoreDir("target/narayana-object-store");
        try (DatabaseAdapter h2 = new H2DatabaseAdapter(); DatabaseAdapter hsql = new HsqlDbDatabaseAdapter()) {
            h2.initialize();
            hsql.initialize();
            try (Connection connection = h2.openConnection()) {
                h2.createSchema(connection);
            }
            try (Connection connection = hsql.openConnection()) {
                hsql.createSchema(connection);
            }
            var manager = com.arjuna.ats.jta.TransactionManager.transactionManager();
            manager.setTransactionTimeout(15);
            for (boolean commit : new boolean[]{true, false}) {
                XAConnection first = source("h2").getXAConnection();
                XAConnection second = source("hsqldb").getXAConnection();
                try {
                    manager.begin();
                    var transaction = manager.getTransaction();
                    assertTrue(transaction.enlistResource(first.getXAResource()));
                    assertTrue(transaction.enlistResource(second.getXAResource()));
                    try (Connection a = first.getConnection(); Connection b = second.getConnection()) {
                        insert(a, commit ? 9101 : 9102);
                        insert(b, commit ? 9101 : 9102);
                        if (commit) {
                            manager.commit();
                        } else {
                            manager.rollback();
                        }
                    }
                    assertCount(h2, commit ? 9101 : 9102, commit ? 1 : 0);
                    assertCount(hsql, commit ? 9101 : 9102, commit ? 1 : 0);
                } finally {
                    if (manager.getTransaction() != null) {
                        manager.rollback();
                    }
                    first.close();
                    second.close();
                }
            }
        }
    }

    private static XADataSource source(String engine) throws SQLException {
        if ("h2".equals(engine)) {
            JdbcDataSource source = new JdbcDataSource();
            source.setURL("jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1");
            source.setUser("sa");
            source.setPassword("");
            return source;
        }
        JDBCXADataSource source = new JDBCXADataSource();
        source.setUrl("jdbc:hsqldb:mem:benchmark");
        source.setUser("SA");
        source.setPassword("");
        return source;
    }

    private static void insert(Connection connection, long id) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            assertEquals(1, statement.executeUpdate("INSERT INTO products VALUES (" + id + ",'xa-product',12.34)"));
        }
    }

    private static void assertCount(DatabaseAdapter adapter, long id, long expected) throws SQLException {
        try (Connection connection = adapter.openConnection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM products WHERE id=" + id)) {
            assertTrue(result.next());
            assertEquals(expected, result.getLong(1));
        }
    }

    private static Xid xid(int branch) {
        return new Xid() {
            @Override
            public int getFormatId() {
                return 0x45424442;
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return new byte[]{1, 2, 3, (byte) branch};
            }

            @Override
            public byte[] getBranchQualifier() {
                return new byte[]{(byte) branch};
            }
        };
    }

    private static boolean sameXid(Xid left, Xid right) {
        return left.getFormatId() == right.getFormatId()
                && Arrays.equals(left.getGlobalTransactionId(), right.getGlobalTransactionId())
                && Arrays.equals(left.getBranchQualifier(), right.getBranchQualifier());
    }
}
