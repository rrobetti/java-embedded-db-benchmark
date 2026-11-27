package io.github.embeddedbench.workload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.SplittableRandom;

public interface Workload extends AutoCloseable {
    record Operation(String category, long units) {}
    enum OperationType { READ, WRITE }

    default void prepare(Connection connection) throws SQLException {}

    Operation execute(Connection connection, SplittableRandom random) throws SQLException;

    default Operation execute(Connection connection, SplittableRandom random, OperationType type) throws SQLException {
        throw new UnsupportedOperationException("Explicit read/write selection is unsupported by this workload");
    }

    void validate(Connection connection) throws SQLException;

    @Override
    default void close() throws SQLException {}
}
