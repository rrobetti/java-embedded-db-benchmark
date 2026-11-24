package io.github.embeddedbench.db;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
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
    private String jdbcDriverVersion = "not initialized";
    private PgLiteWireBridge bridge;
    private final boolean upstream;

    public PgLiteDatabaseAdapter() {
        this("default");
    }

    public PgLiteDatabaseAdapter(String profile) {
        if (!"default".equals(profile) && !"framed".equals(profile) && !"upstream".equals(profile)) {
            throw new IllegalArgumentException("PGlite profile must be default, framed or upstream: " + profile);
        }
        upstream = "upstream".equals(profile);
    }

    @Override
    public String id() {
        return "pglite";
    }

    @Override
    String jdbcUrl() {
        if (upstream) {
            return "jdbc:pglite:memory:benchmark";
        }
        return bridge == null ? "jdbc:postgresql://127.0.0.1:<ephemeral>/postgres" : bridge.jdbcUrl();
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
        properties.setProperty("preferQueryMode", "simple");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("gssEncMode", "disable");
        properties.setProperty("prepareThreshold", "0");
        properties.setProperty("connectTimeout", "30");
        return properties;
    }

    @Override
    synchronized Connection connect() throws SQLException {
        if (physical != null && physical.isClosed()) {
            throw new SQLException("PGlite physical session was lost; close and explicitly reinitialize the adapter", "08003");
        }
        if (physical == null) {
            if (upstream) {
                physical = DriverManager.getConnection(jdbcUrl(), connectionProperties());
            } else {
                bridge = new PgLiteWireBridge();
                try {
                    physical = DriverManager.getConnection(bridge.jdbcUrl(), connectionProperties());
                } catch (SQLException | RuntimeException exception) {
                    if (bridge.failure() != null && bridge.failure() != exception) {
                        exception.addSuppressed(bridge.failure());
                    }
                    try {
                        bridge.close();
                    } catch (SQLException | RuntimeException cleanupFailure) {
                        exception.addSuppressed(cleanupFailure);
                    } finally {
                        bridge = null;
                    }
                    throw exception;
                }
            }
        }
        return physical;
    }

    @Override
    void afterInitialize(Connection connection) throws SQLException {
        engineVersion = connection.getMetaData().getDatabaseProductVersion();
        jdbcDriverVersion = connection.getMetaData().getDriverVersion();
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
            if (bridge != null) {
                bridge.close();
                bridge = null;
            }
            if (upstream) {
                Class<?> driver = Class.forName("io.roastedroot.pglite4j.jdbc.PgLiteDriver");
                var evict = driver.getDeclaredMethod("closeAndEvict", String.class);
                evict.setAccessible(true);
                evict.invoke(null, "memory:benchmark");
            }
        } catch (SQLException | ReflectiveOperationException | RuntimeException exception) {
            SQLException shutdownFailure = new SQLException("Unable to release PGlite 0.0.3 WASM bridge", exception);
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
        String transport = upstream
                ? "UNMODIFIED UPSTREAM pglite4j-jdbc 0.0.3 driver via " + jdbcUrl()
                + "; pgjdbc " + jdbcDriverVersion
                + ". Batched protocol pipelines are unsafe in upstream arbitrary-socket-read framing; "
                + "use prepared/individual ingestion. "
                : "ADAPTER TRANSPORT DEVIATION: pglite4j 0.0.3 core WASM via a local exact-frame bridge and pgjdbc; "
                + "NOT the unmodified pglite4j-jdbc driver or jdbc:pglite:memory:benchmark URL. Actual URL: "
                + jdbcUrl() + "; pgjdbc " + jdbcDriverVersion + ". ";
        return new Capabilities(false, false, false, false, true, "serialized-shared-session",
                transport
                        + "One physical connection, serialized logical leases; no independent sessions or XA. "
                        + "Upstream 0.0.3 WASM SQL-error cleanup leaves an active portal; restart the instance after SQL errors.");
    }
}
