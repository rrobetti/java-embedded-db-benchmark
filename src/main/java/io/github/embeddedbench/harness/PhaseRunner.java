package io.github.embeddedbench.harness;

import io.github.embeddedbench.workload.Workload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

final class PhaseRunner {
    record Result(Measurements measurements, double elapsedSeconds, double offeredSeconds) {}
    private record Request(long scheduledNanos) {}
    private PhaseRunner() {}

    static Result run(Workload workload, List<Connection> connections, int workers, boolean serialized,
                      BenchmarkOptions options, double seconds, long seed) throws Exception {
        Measurements measurements = new Measurements();
        if (seconds == 0) return new Result(measurements, 0, 0);
        long durationNanos = (long) (seconds * 1_000_000_000L);
        var queue = new ArrayBlockingQueue<Request>(options.queueCapacity);
        var ready = new CountDownLatch(workers);
        var start = new CountDownLatch(1);
        var finished = new CountDownLatch(workers);
        var fatal = new AtomicReference<Throwable>();
        var epoch = new AtomicLong();
        var issued = new AtomicLong();
        var producerDone = new java.util.concurrent.atomic.AtomicBoolean();
        var threads = new ArrayList<Thread>();
        Object sessionLock = new Object();
        for (int worker = 0; worker < workers; worker++) {
            int index = worker;
            Thread thread = new Thread(() -> {
                var random = new SplittableRandom(seed + 0x9e3779b97f4a7c15L * index);
                Connection connection = connections.get(serialized ? 0 : index);
                ready.countDown();
                try {
                    start.await();
                    while (fatal.get() == null) {
                        long scheduled;
                        if (options.requestRate > 0) {
                            Request request = queue.poll(20, TimeUnit.MILLISECONDS);
                            if (request == null) {
                                if (producerDone.get() && queue.isEmpty()) break;
                                continue;
                            }
                            scheduled = request.scheduledNanos();
                        } else {
                            if (System.nanoTime() - epoch.get() >= durationNanos) break;
                            if (options.maxOperations > 0 && !claim(issued, options.maxOperations)) break;
                            scheduled = System.nanoTime();
                        }
                        try {
                            Workload.Operation operation;
                            if (serialized) {
                                synchronized (sessionLock) { operation = workload.execute(connection, random); }
                            } else operation = workload.execute(connection, random);
                            measurements.success(operation.category(), operation.units(), System.nanoTime() - scheduled);
                        } catch (java.sql.SQLException | UnsupportedOperationException error) {
                            measurements.failure(error);
                            if (List.of("UNSUPPORTED", "CORRECTNESS").contains(Measurements.classify(error))) {
                                fatal.compareAndSet(null, error);
                                break;
                            }
                        } catch (Throwable error) {
                            measurements.failure(error);
                            fatal.compareAndSet(null, error);
                            break;
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
        ready.await();
        long begin = System.nanoTime();
        epoch.set(begin);
        start.countDown();
        long offerEnd = begin + durationNanos;
        try {
            if (options.requestRate > 0) {
                long interval = Math.max(1, (long) (1_000_000_000.0 / options.requestRate));
                long scheduled = begin;
                while (scheduled < begin + durationNanos && fatal.get() == null) {
                    if (options.maxOperations > 0 && issued.get() >= options.maxOperations) break;
                    long remaining;
                    while ((remaining = scheduled - System.nanoTime()) > 0 && fatal.get() == null) {
                        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Arrival scheduler interrupted");
                        LockSupport.parkNanos(Math.min(remaining, 10_000_000L));
                    }
                    if (fatal.get() != null) break;
                    if (!queue.offer(new Request(scheduled))) measurements.reject();
                    issued.incrementAndGet();
                    scheduled += interval;
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
        Throwable failure = fatal.get();
        if (failure != null) {
            if (failure instanceof Exception exception) throw exception;
            if (failure instanceof Error error) throw error;
            throw new IllegalStateException(failure);
        }
        long end = System.nanoTime();
        return new Result(measurements, (end - begin) / 1_000_000_000.0,
                Math.max(0, Math.min(offerEnd, end) - begin) / 1_000_000_000.0);
    }

    private static boolean claim(AtomicLong issued, long maximum) {
        while (true) {
            long current = issued.get();
            if (current >= maximum) return false;
            if (issued.compareAndSet(current, current + 1)) return true;
        }
    }
}
