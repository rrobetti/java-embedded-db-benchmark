package io.github.embeddedbench.db;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Frames TCP input before forwarding it to PGlite's real WASM protocol engine.
 * Upstream 0.0.3 forwards arbitrary socket reads, which can split a PostgreSQL
 * message or combine a pipelined batch beyond the engine's CMA input capacity.
 */
final class PgLiteWireBridge implements AutoCloseable {
    private final Object engine;
    private final Method execute;
    private final Method shutdown;
    private final ServerSocket listener;
    private final Thread thread;
    private volatile Socket socket;
    private volatile boolean closed;
    private volatile Throwable failure;

    PgLiteWireBridge() throws SQLException {
        try {
            Class<?> type = Class.forName("io.roastedroot.pglite4j.core.PGLite");
            execute = type.getMethod("execProtocolRaw", byte[].class);
            shutdown = type.getMethod("close");
            Object builder = type.getMethod("builder").invoke(null);
            engine = builder.getClass().getMethod("build").invoke(builder);
            try {
                listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
            } catch (IOException exception) {
                shutdown.invoke(engine);
                throw exception;
            }
        } catch (InvocationTargetException exception) {
            if (exception.getCause() instanceof Error error) {
                throw error;
            }
            throw new SQLException("Cannot initialize framed PGlite WASM bridge", exception.getCause());
        } catch (ReflectiveOperationException | IOException exception) {
            throw new SQLException("Cannot initialize framed PGlite WASM bridge", exception);
        }
        thread = new Thread(this::serve, "embeddedbench-pglite-framed-wire");
        thread.setDaemon(true);
        try {
            thread.start();
        } catch (RuntimeException | Error exception) {
            try {
                listener.close();
                shutdown.invoke(engine);
            } catch (IOException | ReflectiveOperationException cleanup) {
                exception.addSuppressed(cleanup);
            }
            throw exception;
        }
    }

    String jdbcUrl() {
        return "jdbc:postgresql://127.0.0.1:" + listener.getLocalPort() + "/postgres";
    }

    Throwable failure() {
        return failure;
    }

    private void serve() {
        try (Socket accepted = listener.accept()) {
            socket = accepted;
            accepted.setTcpNoDelay(true);
            DataInputStream input = new DataInputStream(accepted.getInputStream());
            forward(readMessage(input, -1), accepted);
            while (!closed) {
                int type = input.read();
                if (type < 0 || type == 'X') {
                    break;
                }
                forward(readMessage(input, type), accepted);
            }
        } catch (IOException | ReflectiveOperationException | RuntimeException exception) {
            if (!closed) {
                failure = exception instanceof InvocationTargetException invocation
                        ? invocation.getCause() : exception;
            }
        }
    }

    static byte[] readMessage(DataInputStream input, int type) throws IOException {
        int length = input.readInt();
        if (length < 4 || length > 64 * 1024 * 1024) {
            throw new IOException("Invalid PostgreSQL frontend message length: " + length);
        }
        ByteBuffer bytes = ByteBuffer.allocate(length + (type < 0 ? 0 : 1));
        if (type >= 0) {
            bytes.put((byte) type);
        }
        bytes.putInt(length);
        byte[] payload = input.readNBytes(length - 4);
        if (payload.length != length - 4) {
            throw new EOFException("Incomplete PostgreSQL frontend message");
        }
        return bytes.put(payload).array();
    }

    private void forward(byte[] message, Socket accepted) throws ReflectiveOperationException, IOException {
        byte[] reply = (byte[]) execute.invoke(engine, (Object) message);
        if (reply.length != 0) {
            accepted.getOutputStream().write(reply);
            accepted.getOutputStream().flush();
        }
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        try {
            listener.close();
            if (socket != null) {
                socket.close();
            }
            thread.join(5000);
            if (thread.isAlive()) {
                throw new SQLException("PGlite wire thread did not terminate");
            }
            shutdown.invoke(engine);
        } catch (IOException | ReflectiveOperationException exception) {
            throw new SQLException("Cannot release framed PGlite bridge", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted closing framed PGlite bridge", exception);
        }
    }
}
