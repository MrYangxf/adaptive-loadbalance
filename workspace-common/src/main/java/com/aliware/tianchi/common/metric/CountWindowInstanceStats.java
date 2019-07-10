package com.aliware.tianchi.common.metric;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class CountWindowInstanceStats implements InstanceStats {
    private static final long serialVersionUID = 6012160831896006679L;

    private String address;
    private ServerStats serverStats;

    private volatile int activeCount;
    private volatile int domainThreads;

    private int windowSize;
    private volatile long startMillis = System.currentTimeMillis();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, ConcurrentLinkedDeque<Long>> successesQueueMap = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedDeque<Long>> failuresQueueMap = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedDeque<Long>> rejectionsQueueMap = new ConcurrentHashMap<>();

    public CountWindowInstanceStats(String address, ServerStats serverStats, int windowSize) {
        this.address = address;
        this.serverStats = serverStats;
        this.windowSize = windowSize;
    }

    @Override
    public SnapshotStats snapshot(String serviceId) {
        ConcurrentLinkedDeque<Long> successesQueue, failuresQueue, rejectionsQueue,
                newSuccessesQueue = new ConcurrentLinkedDeque<>(),
                newFailuresQueue = new ConcurrentLinkedDeque<>(),
                newRejectionsQueue = new ConcurrentLinkedDeque<>();
        lock.writeLock().lock();
        long intervalMillis, startMs = startMillis;
        try {
            long currentTimeMillis = System.currentTimeMillis();
            intervalMillis = currentTimeMillis - startMs;
            startMillis = currentTimeMillis;
            successesQueue = successesQueueMap.replace(serviceId, newSuccessesQueue);
            failuresQueue = failuresQueueMap.replace(serviceId, newFailuresQueue);
            rejectionsQueue = rejectionsQueueMap.replace(serviceId, newRejectionsQueue);
        } finally {
            lock.writeLock().unlock();
        }

        lock.readLock().lock();
        try {
            long successesSum = replaceAndSum(successesQueue, newSuccessesQueue);
            long successesRT = successesSum & SUM_MASK;
            long successes = successesSum >>> SIZE_SHIFT;

            long failuresSum = replaceAndSum(failuresQueue, newFailuresQueue);
            long failures = failuresSum >>> SIZE_SHIFT;

            long rejectionsSum = replaceAndSum(rejectionsQueue, newRejectionsQueue);
            long rejections = rejectionsSum & SUM_MASK;

            double avgRT = successes == 0 ? 0d : successesRT / (double) successes;

            return new SnapshotStats() {
                private static final long serialVersionUID = 3380395362875553534L;

                @Override
                public String getAddress() {
                    return address;
                }

                @Override
                public String getServiceId() {
                    return serviceId;
                }

                @Override
                public long startTimeMs() {
                    return startMs;
                }

                @Override
                public long intervalTimeMs() {
                    return intervalMillis;
                }

                @Override
                public int getDomainThreads() {
                    return domainThreads;
                }

                @Override
                public int getActiveCount() {
                    return activeCount;
                }

                @Override
                public ServerStats getServerStats() {
                    return serverStats;
                }


                @Override
                public long getThroughput() {
                    return 0;
                }

                @Override
                public long getNumberOfSuccesses() {
                    return successes;
                }

                @Override
                public long getNumberOfFailures() {
                    return failures;
                }

                @Override
                public long getNumberOfRejections() {
                    return rejections;
                }
            };
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public ServerStats getServerStats() {
        return serverStats;
    }

    @Override
    public Set<String> getServiceIds() {
        Set<String> ids = new HashSet<>();
        ids.addAll(successesQueueMap.keySet());
        ids.addAll(failuresQueueMap.keySet());
        ids.addAll(rejectionsQueueMap.keySet());
        return ids;
    }

    @Override
    public void setDomainThreads(int nThreads) {
        domainThreads = nThreads;
    }

    @Override
    public int getDomainThreads() {
        return domainThreads;
    }

    @Override
    public void setActiveCount(int activeCount) {
        this.activeCount = activeCount;
    }

    @Override
    public int getActiveCount() {
        return activeCount;
    }

    @Override
    public void success(String serviceId, long responseMs) {
        lock.readLock().lock();
        try {
            successesQueueMap.computeIfAbsent(serviceId, k -> new ConcurrentLinkedDeque<>())
                             .offerFirst(responseMs);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void failure(String serviceId, long responseMs) {
        lock.readLock().lock();
        try {
            failuresQueueMap.computeIfAbsent(serviceId, k -> new ConcurrentLinkedDeque<>())
                            .offerFirst(responseMs);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void rejection(String serviceId) {
        lock.readLock().lock();
        try {
            rejectionsQueueMap.computeIfAbsent(serviceId, k -> new ConcurrentLinkedDeque<>())
                              .offerFirst(1L);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void clean() {

    }

    @Override
    public double getAvgResponseMs() {
        return 0;
    }

    @Override
    public double getAvgResponseMs(String serviceId) {
        return 0;
    }

    @Override
    public long getThroughput() {
        return 0;
    }

    @Override
    public long getThroughput(String serviceId) {
        return 0;
    }

    @Override
    public long getTotalResponseMs() {
        return 0;
    }

    @Override
    public long getTotalResponseMs(String serviceId) {
        return 0;
    }

    @Override
    public long getNumberOfRequests() {
        return 0;
    }

    @Override
    public long getNumberOfRequests(String serviceId) {
        return 0;
    }

    @Override
    public long getNumberOfSuccesses() {
        return 0;
    }

    @Override
    public long getNumberOfSuccesses(String serviceId) {
        return 0;
    }

    @Override
    public long getNumberOfFailures() {
        return 0;
    }

    @Override
    public long getNumberOfFailures(String serviceId) {
        return 0;
    }

    @Override
    public long getNumberOfRejections() {
        return 0;
    }

    @Override
    public long getNumberOfRejections(String serviceId) {
        return 0;
    }

    private static final int SIZE_SHIFT = 32;
    private static final long SUM_MASK = (1L << 32) - 1;

    private long replaceAndSum(ConcurrentLinkedDeque<Long> oldQueue, ConcurrentLinkedDeque<Long> newQueue) {
        long sum = 0;
        if (nonNull(oldQueue)) {
            long size = 0;
            for (long l : oldQueue) {
                if (size == windowSize) {
                    break;
                }
                sum += l;
                size++;
            }
            sum |= (size << SIZE_SHIFT);
        }

        return sum;
    }

    public static void main(String[] args) {
        CountWindowInstanceStats stats = new CountWindowInstanceStats("a", new ServerStats("a"), 10);

        for (int i = 1; i < 20; i++) {
            stats.success("ok", 2);
        }

        SnapshotStats ok = stats.snapshot("ok");
        System.out.println(ok);
    }
}
