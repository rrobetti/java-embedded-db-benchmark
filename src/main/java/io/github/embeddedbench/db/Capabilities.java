package io.github.embeddedbench.db;

public record Capabilities(boolean independentTransactions, boolean concurrentReaders,
                           boolean concurrentWriters, boolean xa, boolean cte,
                           String executionMode, String limitations) {
}
