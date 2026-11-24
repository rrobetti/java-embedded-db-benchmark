package io.github.embeddedbench.workload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.SplittableRandom;

public interface Workload extends AutoCloseable {
    record Operation(String category, long units) {}

    default void prepare(Connection connection) throws SQLException {}

    Operation execute(Connection connection, SplittableRandom random) throws SQLException;

    void validate(Connection connection) throws SQLException;

    @Override
    default void close() throws SQLException {}
}
