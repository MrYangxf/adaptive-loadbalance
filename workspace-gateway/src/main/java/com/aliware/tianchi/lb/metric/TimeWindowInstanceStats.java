package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.util.SegmentCounter;
import com.aliware.tianchi.common.util.SegmentCounterFactory;
import com.aliware.tianchi.common.util.SkipListCounter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.defaultIfNull;

/**
 * 基于时间窗口的统计信息
 *
 * @author yangxf
 */
public class TimeWindowInstanceStats implements InstanceStats {
    private static final long serialVersionUID = -1040897703729118186L;

    private static final long DEFAULT_WINDOW_SIZE = 10;
    private static final long DEFAULT_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    private static final SegmentCounterFactory DEFAULT_COUNTER_FACTORY = SkipListCounter::new;

    /**
     * 服务实例的地址 host:port
     */
    private final String address;

    /**
     * 实例系统、jvm信息
     */
    private final ServerStats serverStats;

    // windowNanos = windowSize * timeIntervalNanos
    private final long windowSize;
    private final long timeIntervalNanos;
    private final long windowNanos;

    private final SegmentCounterFactory counterFactory;

    /*
     * 4组分段计数器
     * key = serviceId, value = counter
     * totalResponseMs = successesMs + failuresMs
     * requests = successes + failures + rejections
     */
    private final Map<String, SegmentCounter> totalResponseMsCounterMap = new ConcurrentHashMap<>();
    private final Map<String, SegmentCounter> requestsCounterMap = new ConcurrentHashMap<>();
    private final Map<String, SegmentCounter> failuresCounterMap = new ConcurrentHashMap<>();
    private final Map<String, SegmentCounter> rejectionsCounterMap = new ConcurrentHashMap<>();


    public TimeWindowInstanceStats(String address,
                                   ServerStats serverStats,
                                   long windowSize,
                                   long timeInterval,
                                   TimeUnit timeUnit,
                                   SegmentCounterFactory counterFactory) {
        checkNotNull(address, "address");
        this.address = address;
        this.serverStats = serverStats;
        this.windowSize = windowSize > 0 ? windowSize : DEFAULT_WINDOW_SIZE;
        long nanos = defaultIfNull(timeUnit, DEFAULT_TIME_UNIT).toNanos(timeInterval);
        this.timeIntervalNanos = nanos > 0 ? nanos : DEFAULT_INTERVAL_NANOS;
        windowNanos = this.windowSize * timeIntervalNanos;
        this.counterFactory = defaultIfNull(counterFactory, DEFAULT_COUNTER_FACTORY);
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getTimeIntervalNanos() {
        return timeIntervalNanos;
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
    public State evalState() {
        return null;
    }

    @Override
    public Set<String> getServiceIds() {
        return requestsCounterMap.keySet();
    }

    @Override
    public void success(String serviceId, long responseMs) {
        long offset = offset();
        getOrCreate(totalResponseMsCounterMap, serviceId).add(offset, responseMs);
        getOrCreate(requestsCounterMap, serviceId).increment(offset);
    }

    @Override
    public void failure(String serviceId, long responseMs) {
        long offset = offset();
        getOrCreate(totalResponseMsCounterMap, serviceId).add(offset, responseMs);
        getOrCreate(requestsCounterMap, serviceId).increment(offset);
        getOrCreate(failuresCounterMap, serviceId).increment(offset);
    }

    @Override
    public void rejection(String serviceId) {
        long offset = offset();
        getOrCreate(requestsCounterMap, serviceId).increment(offset);
        getOrCreate(rejectionsCounterMap, serviceId).increment(offset);
    }

    @Override
    public void clean() {
        // 留一个间隔作为缓冲
        long toKey = offset() - (windowNanos << 1);
        cleanMap(totalResponseMsCounterMap, toKey);
        cleanMap(requestsCounterMap, toKey);
        cleanMap(failuresCounterMap, toKey);
        cleanMap(rejectionsCounterMap, toKey);
    }

    @Override
    public long getAvgResponseMs() {
        long high = offset(),
                low = high - windowNanos;
        // avg = totalResponseMs / (requests - rejections + 1)
        return sumMap(totalResponseMsCounterMap, low, high) /
               (sumMap(requestsCounterMap, low, high) - sumMap(rejectionsCounterMap, low, high) + 1);
    }

    @Override
    public long getAvgResponseMs(String serviceId) {
        long high = offset(),
                low = high - windowNanos;
        // avg = totalResponseMs / (requests - rejections + 1)
        return getOrCreate(totalResponseMsCounterMap, serviceId).sum(low, high) /
               (getOrCreate(requestsCounterMap, serviceId).sum(low, high) -
                getOrCreate(rejectionsCounterMap, serviceId).sum(low, high) + 1);
    }

    @Override
    public long getThroughput() {
        long high = offset(),
                low = high - windowNanos;
        return sumMap(requestsCounterMap, low, high) /
               TimeUnit.NANOSECONDS.toSeconds(windowNanos);
    }

    @Override
    public long getThroughput(String serviceId) {
        long high = offset(),
                low = high - windowNanos;
        return getOrCreate(requestsCounterMap, serviceId).sum(low, high) /
               TimeUnit.NANOSECONDS.toSeconds(windowNanos);
    }

    @Override
    public long getTotalResponseMs() {
        long high = offset(),
                low = high - windowNanos;
        return sumMap(totalResponseMsCounterMap, low, high);
    }

    @Override
    public long getTotalResponseMs(String serviceId) {
        long high = offset(),
                low = high - windowNanos;
        return getOrCreate(totalResponseMsCounterMap, serviceId).sum(low, high);
    }

    @Override
    public long getNumberOfRequests() {
        long high = offset(),
                low = high - windowNanos;
        return sumMap(requestsCounterMap, low, high);
    }

    @Override
    public long getNumberOfRequests(String serviceId) {
        long high = offset(),
                low = high - windowNanos;
        return getOrCreate(requestsCounterMap, serviceId).sum(low, high);
    }

    @Override
    public long getNumberOfFailures() {
        long high = offset(),
                low = high - windowNanos;
        return sumMap(failuresCounterMap, low, high);
    }

    @Override
    public long getNumberOfFailures(String serviceId) {
        long high = offset(),
                low = high - windowNanos;
        return getOrCreate(failuresCounterMap, serviceId).sum(low, high);
    }

    @Override
    public long getNumberOfRejections() {
        long high = offset(),
                low = high - windowNanos;
        return sumMap(rejectionsCounterMap, low, high);
    }

    @Override
    public long getNumberOfRejections(String serviceId) {
        long high = offset(),
                low = high - windowNanos;
        return getOrCreate(rejectionsCounterMap, serviceId).sum(low, high);
    }

    private SegmentCounter getOrCreate(Map<String, SegmentCounter> counterMap, String key) {
        return counterMap.computeIfAbsent(key, k -> counterFactory.newCounter());
    }

    private long offset() {
        return System.nanoTime() / timeIntervalNanos;
    }

    private long sumMap(Map<String, SegmentCounter> counterMap, long fromKey, long toKey) {
        return counterMap.values().stream()
                         .mapToLong(c -> c.sum(fromKey, toKey))
                         .sum();
    }

    private void cleanMap(Map<String, SegmentCounter> counterMap, long toKey) {
        for (SegmentCounter segmentCounter : counterMap.values()) {
            segmentCounter.clean(toKey);
        }
    }
}
