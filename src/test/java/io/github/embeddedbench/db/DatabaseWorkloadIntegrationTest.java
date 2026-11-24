package io.github.embeddedbench.db;

import io.github.embeddedbench.workload.Workload;
import io.github.embeddedbench.workload.Workloads;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 180, unit = TimeUnit.SECONDS)
class DatabaseWorkloadIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {"h2", "hsqldb", "sqlite", "pglite"})
    void everyWorkloadExecutesAndValidatesAgainstEachRealEngine(String engine) throws Exception {
        for (String name : new TreeSet<>(Workloads.NAMES)) {
            try (DatabaseAdapter adapter = DatabaseAdapters.create(engine, "default")) {
                adapter.initialize();
                try (Connection connection = adapter.openConnection();
                     Workload workload = Workloads.create(name, 50, 7, 65, 30, 3, 42)) {
                    adapter.createSchema(connection);
                    DeterministicDataset.load(connection, 50, 128, 42, 500);
                    DeterministicDataset.validate(connection, 50, 128, 42);
                    workload.prepare(connection);
                    SplittableRandom random = new SplittableRandom(42);
                    for (int operation = 0; operation < 24; operation++) {
                        Workload.Operation result = workload.execute(connection, random);
                        assertNotNull(result, engine + "/" + name);
                        assertFalse(result.category().isBlank(), engine + "/" + name);
                        assertTrue(result.units() >= 0, engine + "/" + name);
                        assertTrue(connection.getAutoCommit(), engine + "/" + name + " transaction leaked");
                    }
                    workload.validate(connection);
                }
            } catch (Exception exception) {
                throw new AssertionError("Real workload failed: " + engine + "/" + name, exception);
            }
        }
    }
}
