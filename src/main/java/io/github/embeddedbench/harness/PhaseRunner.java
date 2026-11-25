package io.github.embeddedbench.harness;

import io.github.embeddedbench.workload.Workload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

final class PhaseRunner {
    record Result(Measurements measurements, double elapsedSeconds, double offeredSeconds,
                  Map<String, Long> arrivalCounts) {}
    static final class Failure extends java.sql.SQLException {
        private final Result partial;
        Failure(Throwable cause, Result partial) {
            super(cause.getMessage(), cause instanceof java.sql.SQLException sql ? sql.getSQLState() : null,
                    cause instanceof java.sql.SQLException sql ? sql.getErrorCode() : 0, cause);
            this.partial = partial;
        }
        Result partial() { return partial; }
    }
    private record Request(long scheduledNanos, Workload.OperationType type) {}
    private PhaseRunner() {}

    static Result run(Workload workload, List<Connection> connections, int workers, boolean serialized,
                      BenchmarkOptions options, double seconds, long seed) throws Exception {
        return run(workload, connections, workers, serialized, options, seconds, seed, null);
    }

    static Result run(Workload workload, List<Connection> connections, int workers, boolean serialized,
                      BenchmarkOptions options, double seconds, long seed, ResourceSampler sampler) throws Exception {
        Measurements measurements = new Measurements();
        if (seconds == 0) return new Result(measurements, 0, 0, Map.of());
        long durationNanos = (long) (seconds * 1_000_000_000L);
        var queue = new ArrayBlockingQueue<Request>(options.queueCapacity);
        boolean pooled = !serialized && (options.connectionMode.equals("pooled") || connections.size() < workers);
        var connectionPool = new ArrayBlockingQueue<Connection>(Math.max(1, connections.size()));
        if (pooled) connectionPool.addAll(connections);
        var ready = new CountDownLatch(workers);
        var start = new CountDownLatch(1);
        var finished = new CountDownLatch(workers);
        var fatal = new AtomicReference<Throwable>();
        var epoch = new AtomicLong();
        var issued = new AtomicLong();
        var producerDone = new java.util.concurrent.atomic.AtomicBoolean();
        var threads = new ArrayList<Thread>();
        var workerMeasurements = new ArrayList<Measurements>();
        boolean openLoop = options.requestRate > 0 || options.fixedReadRate > 0;
        Object sessionLock = new Object();
        for (int worker = 0; worker < workers; worker++) {
            int index = worker;
            var localMeasurements = new Measurements();
            workerMeasurements.add(localMeasurements);
            Thread thread = new Thread(() -> {
                var random = new SplittableRandom(seed + 0x9e3779b97f4a7c15L * index);
                Connection dedicatedConnection = pooled ? null : connections.get(serialized ? 0 : index);
                ready.countDown();
                try {
                    start.await();
                    while (fatal.get() == null) {
                        long scheduled;
                        Workload.OperationType kind = null;
                        if (openLoop) {
                            Request request = queue.poll(20, TimeUnit.MILLISECONDS);
                            if (request == null) {
                                if (producerDone.get() && queue.isEmpty()) break;
                                continue;
                            }
                            scheduled = request.scheduledNanos();
                            kind = request.type();
                        } else {
                            if (System.nanoTime() - epoch.get() >= durationNanos) break;
                            if (options.maxOperations > 0 && !claim(issued, options.maxOperations)) break;
                            scheduled = System.nanoTime();
                            if (options.readerWorkers != null)
                                kind = index < options.readerWorkers ? Workload.OperationType.READ : Workload.OperationType.WRITE;
                        }
                        Connection connection = pooled ? connectionPool.take() : dedicatedConnection;
                        try {
                            Workload.Operation operation;
                            if (serialized) {
                                synchronized (sessionLock) { operation = execute(workload, connection, random, kind); }
                            } else operation = execute(workload, connection, random, kind);
                            localMeasurements.success(operation.category(), operation.units(), System.nanoTime() - scheduled);
                        } catch (java.sql.SQLException | UnsupportedOperationException error) {
                            localMeasurements.failure(error);
                            if (List.of("UNSUPPORTED", "CORRECTNESS").contains(Measurements.classify(error))) {
                                fatal.compareAndSet(null, error);
                                break;
                            }
                        } catch (Throwable error) {
                            localMeasurements.failure(error);
                            fatal.compareAndSet(null, error);
                            break;
                        } finally {
                            if (pooled) connectionPool.add(connection);
                        }
                    }
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    fatal.compareAndSet(null, error);
                } finally { finished.countDown(); }
            }, "benchmark-worker-" + index);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
        var sources = new ArrayList<>(workerMeasurements);
        sources.add(measurements);
        if (sampler != null) sampler.startPerformance(sources);
        try (AutoCloseable tracking = sampler == null ? () -> {} : sampler::stopPerformance) {
        ready.await();
        long begin = System.nanoTime();
        epoch.set(begin);
        start.countDown();
        long offerEnd = begin + durationNanos;
        long offeredReads = 0, offeredWrites = 0, acceptedReads = 0, acceptedWrites = 0;
        try {
            if (options.fixedReadRate > 0) {
                double writeRate = options.fixedReadRate * (100 - options.readPercent) / options.readPercent;
                long readIndex = 0, writeIndex = 0, nextRead = begin;
                long nextWrite = writeRate > 0 ? begin : Long.MAX_VALUE;
                while (Math.min(nextRead, nextWrite) < begin + durationNanos && fatal.get() == null) {
                    if (options.maxOperations > 0 && issued.get() >= options.maxOperations) break;
                    boolean read = nextRead <= nextWrite;
                    long scheduled = read ? nextRead : nextWrite;
                    awaitArrival(scheduled, fatal);
                    if (fatal.get() != null) break;
                    boolean accepted = queue.offer(new Request(scheduled,
                            read ? Workload.OperationType.READ : Workload.OperationType.WRITE));
                    if (!accepted) measurements.reject();
                    if (read) {
                        offeredReads++;
                        if (accepted) acceptedReads++;
                        nextRead = nextArrival(begin, durationNanos, ++readIndex, options.fixedReadRate);
                    } else {
                        offeredWrites++;
                        if (accepted) acceptedWrites++;
                        nextWrite = nextArrival(begin, durationNanos, ++writeIndex, writeRate);
                    }
                    issued.incrementAndGet();
                }
                if (options.maxOperations == 0 || issued.get() < options.maxOperations)
                    awaitArrival(begin + durationNanos, fatal);
                offerEnd = Math.min(System.nanoTime(), begin + durationNanos);
            } else if (options.requestRate > 0) {
                long scheduled = begin;
                long arrival = 0;
                while (scheduled < begin + durationNanos && fatal.get() == null) {
                    if (options.maxOperations > 0 && issued.get() >= options.maxOperations) break;
                    awaitArrival(scheduled, fatal);
                    if (fatal.get() != null) break;
                    if (!queue.offer(new Request(scheduled, null))) measurements.reject();
                    issued.incrementAndGet();
                    scheduled = nextArrival(begin, durationNanos, ++arrival, options.requestRate);
                }
                if (options.maxOperations == 0 || issued.get() < options.maxOperations) {
                    awaitArrival(begin + durationNanos, fatal);
                }
                offerEnd = Math.min(System.nanoTime(), begin + durationNanos);
            }
        } finally {
            producerDone.set(true);
        }
        if (!finished.await(options.childDeadline, TimeUnit.SECONDS)) {
            for (Thread thread : threads) thread.interrupt();
            throw new java.sql.SQLTimeoutException("Workers exceeded phase drain deadline");
        }
        long end = System.nanoTime();
        if (sampler != null) sampler.stopPerformance();
        for (Measurements local : workerMeasurements) measurements.merge(local);
        Result result = new Result(measurements, (end - begin) / 1_000_000_000.0,
                Math.max(0, Math.min(offerEnd, end) - begin) / 1_000_000_000.0,
                options.fixedReadRate > 0 ? Map.of("offeredReads", offeredReads, "offeredWrites", offeredWrites,
                        "acceptedReads", acceptedReads, "acceptedWrites", acceptedWrites) : Map.of());
        Throwable failure = fatal.get();
        if (failure != null) throw new Failure(failure, result);
        return result;
        }
    }

    private static Workload.Operation execute(Workload workload, Connection connection, SplittableRandom random,
                                               Workload.OperationType type) throws java.sql.SQLException {
        return type == null ? workload.execute(connection, random) : workload.execute(connection, random, type);
    }

    private static void awaitArrival(long scheduled, AtomicReference<Throwable> fatal) throws InterruptedException {
        long remaining;
        while ((remaining = scheduled - System.nanoTime()) > 0 && fatal.get() == null) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Arrival scheduler interrupted");
            LockSupport.parkNanos(Math.min(remaining, 10_000_000L));
        }
    }

    private static long nextArrival(long begin, long duration, long index, double rate) {
        double offset = index * 1_000_000_000.0 / rate;
        return begin + (offset >= duration ? duration : (long) offset);
    }

    private static boolean claim(AtomicLong issued, long maximum) {
        while (true) {
            long current = issued.get();
            if (current >= maximum) return false;
            if (issued.compareAndSet(current, current + 1)) return true;
        }
    }
}
