package io.github.embeddedbench.db;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PGlite4j's wire bridge multiplexes sockets onto one PostgreSQL session.
 * A lease therefore covers the entire connection, not just individual SQL calls.
 */
public final class PgLiteDatabaseAdapter extends AbstractDatabaseAdapter {
    private final Semaphore session = new Semaphore(1, true);
    private final Set<Connection> leases = ConcurrentHashMap.newKeySet();
    private Connection physical;
    private String engineVersion;

    public PgLiteDatabaseAdapter() {
        this("default");
    }

    public PgLiteDatabaseAdapter(String profile) {
        requireDefaultProfile(profile);
    }

    @Override
    public String id() {
        return "pglite";
    }

    @Override
    String jdbcUrl() {
        return "jdbc:pglite:memory:benchmark";
    }

    @Override
    String username() {
        return "postgres";
    }

    @Override
    String password() {
        return "password";
    }

    @Override
    String binaryType() {
        return "BYTEA";
    }

    @Override
    Properties connectionProperties() {
        Properties properties = super.connectionProperties();
        // The 0.0.3 bridge does not correctly clear unnamed extended-query
        // portals after SQL errors; simple protocol preserves transaction recovery.
        properties.setProperty("preferQueryMode", "simple");
        return properties;
    }

    @Override
    synchronized Connection connect() throws SQLException {
        if (physical == null || physical.isClosed()) {
            physical = super.connect();
        }
        return physical;
    }

    @Override
    void afterInitialize(Connection connection) throws SQLException {
        engineVersion = connection.getMetaData().getDatabaseProductVersion();
    }

    @Override
    public synchronized String version() throws SQLException {
        if (physical == null || physical.isClosed()) {
            throw new SQLException("PGlite adapter is not initialized");
        }
        return engineVersion;
    }

    @Override
    public Connection openConnection() throws SQLException {
        try {
            if (!session.tryAcquire(30, TimeUnit.SECONDS)) {
                throw new SQLException("PGlite shared-session lease timed out; independent connections are unsupported");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted waiting for PGlite shared session", exception);
        }
        Connection delegate;
        try {
            delegate = super.openConnection();
        } catch (SQLException | RuntimeException exception) {
            session.release();
            throw exception;
        }
        AtomicBoolean closed = new AtomicBoolean();
        Connection proxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (connectionProxy, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        if (closed.compareAndSet(false, true)) {
                            try {
                                if (!delegate.getAutoCommit()) {
                                    delegate.rollback();
                                    delegate.setAutoCommit(true);
                                }
                            } finally {
                                leases.remove(connectionProxy);
                                session.release();
                            }
                        }
                        return null;
                    }
                    if ("isClosed".equals(method.getName()) && closed.get()) {
                        return true;
                    }
                    if ("hashCode".equals(method.getName())) {
                        return System.identityHashCode(connectionProxy);
                    }
                    if ("equals".equals(method.getName())) {
                        return connectionProxy == arguments[0];
                    }
                    if (closed.get()) {
                        throw new SQLException("Logical PGlite connection is closed", "08003");
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
        leases.add(proxy);
        return proxy;
    }

    @Override
    public synchronized void close() throws SQLException {
        SQLException failure = null;
        for (Connection connection : leases) {
            try {
                connection.close();
            } catch (SQLException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        try {
            super.close();
        } catch (SQLException exception) {
            failure = exception;
        } finally {
            physical = null;
            engineVersion = null;
        }
        try {
            // Version 0.0.3 exposes eviction only at package scope. Closing JDBC
            // sockets alone does not stop its wire listener or release WASM memory.
            Class<?> driver = Class.forName("io.roastedroot.pglite4j.jdbc.PgLiteDriver");
            Method shutdown = driver.getDeclaredMethod("closeAndEvict", String.class);
            shutdown.setAccessible(true);
            shutdown.invoke(null, jdbcUrl().substring("jdbc:pglite:".length()));
        } catch (ReflectiveOperationException | RuntimeException exception) {
            SQLException shutdownFailure = new SQLException("Unable to release PGlite 0.0.3 managed instance", exception);
            if (failure == null) {
                failure = shutdownFailure;
            } else {
                failure.addSuppressed(shutdownFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(false, false, false, false, true, "serialized-shared-session",
                "PGlite4j 0.0.3 uses one WASM PostgreSQL session behind a loopback JDBC wire bridge. "
                        + "One physical connection, serialized logical leases; no independent sessions or XA. "
                        + "Rollback-to-savepoint leaves an active unnamed portal; a full rollback is required.");
    }
}
