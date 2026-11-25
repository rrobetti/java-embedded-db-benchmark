package io.github.embeddedbench.harness;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Watches sampler liveness and phases with observable loader/operation progress. */
final class ProgressWatchdog {
    private static final Set<String> OPERATION_PHASES = Set.of(
            "warmup", "measurement", "measurement-maintenance",
            "refresh-before", "refresh-during", "refresh-after");
    private final long timeoutNanos;
    private final long operationTimeoutNanos;
    private long sequence = -1, lastHeartbeat, lastProgress, records = -1, completed = -1;
    private String phase;

    ProgressWatchdog(double seconds) {
        this(seconds, seconds);
    }

    ProgressWatchdog(double seconds, double operationSeconds) {
        timeoutNanos = (long) (seconds * 1_000_000_000L);
        operationTimeoutNanos = (long) (operationSeconds * 1_000_000_000L);
    }

    String check(Path heartbeat, long now) throws IOException {
        if (Files.isRegularFile(heartbeat)) {
            Object value;
            try { value = Json.decode(Files.readString(heartbeat)); }
            catch (RuntimeException error) { throw new IOException("Invalid progress heartbeat", error); }
            if (!(value instanceof Map<?, ?> map)) throw new IOException("Invalid progress heartbeat shape");
            observe(map, now);
        }
        return timeout(now);
    }

    void observe(Map<?, ?> heartbeat, long now) throws IOException {
        if (!(heartbeat.get("sequence") instanceof Number sequenceNumber)
                || !(heartbeat.get("completedOperations") instanceof Number completedNumber)
                || !(heartbeat.get("phase") instanceof String currentPhase))
            throw new IOException("Invalid progress heartbeat fields");
        long currentSequence = sequenceNumber.longValue();
        if (currentSequence <= sequence) return;
        long currentRecords = heartbeat.get("recordsLoaded") instanceof Number number ? number.longValue() : -1;
        long currentCompleted = completedNumber.longValue();
        boolean phaseChanged = !currentPhase.equals(phase);
        if (sequence < 0 || phaseChanged || currentPhase.equals("loading") && currentRecords > records
                || OPERATION_PHASES.contains(currentPhase) && currentCompleted > completed)
            lastProgress = now;
        phase = currentPhase;
        records = currentRecords;
        completed = currentCompleted;
        sequence = currentSequence;
        lastHeartbeat = now;
    }

    String timeout(long now) {
        if (sequence < 0 || timeoutNanos == 0) return null;
        if (now - lastHeartbeat > timeoutNanos) return "HEARTBEAT_TIMEOUT";
        if (phase.equals("loading") && now - lastProgress > timeoutNanos) return "INACTIVITY_TIMEOUT";
        if (OPERATION_PHASES.contains(phase) && now - lastProgress > operationTimeoutNanos)
            return "INACTIVITY_TIMEOUT";
        return null;
    }
}
