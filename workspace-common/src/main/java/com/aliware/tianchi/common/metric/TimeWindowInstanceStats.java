package com.aliware.tianchi.common.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.common.util.SegmentCounter;
import com.aliware.tianchi.common.util.SegmentCounterFactory;
import com.aliware.tianchi.common.util.SkipListCounter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.aliware.tianchi.common.util.ObjectUtil.*;

/**
 * 基于时间窗口的统计信息
 *
 * @author yangxf
 */
public class TimeWindowInstanceStats implements InstanceStats {
    private static final long serialVersionUID = -1040897703729118186L;

    private static final String SEPARATOR = "_";
    private static final String GROUP_SEPARATOR = "@";

    private static final long DEFAULT_WINDOW_SIZE = 10;
    private static final long DEFAULT_TIME_INTERVAL = TimeUnit.SECONDS.toNanos(1);
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

    // windowMillis = windowSize * timeInterval
    private final long windowSize;
    private final long timeInterval;
    private final TimeUnit timeUnit;

    private final SegmentCounterFactory counterFactory;

    /*
     * 4组分段计数器
     * key = serviceId, value = counter
     * totalResponseMs = successesMs + failuresMs
     */
    private final Map<String, SegmentCounter> totalResponseMsCounterMap = new ConcurrentHashMap<>();
    private final Map<String, SegmentCounter> successesCounterMap = new ConcurrentHashMap<>();
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
        this.timeUnit = defaultIfNull(timeUnit, DEFAULT_TIME_UNIT);
        this.timeInterval = timeInterval > 0 ? timeInterval : DEFAULT_TIME_INTERVAL;
        this.counterFactory = defaultIfNull(counterFactory, DEFAULT_COUNTER_FACTORY);
    }

    public static SnapshotStats fromString(String text) {
        checkNotEmpty(text, "text");

        String[] groups = text.split(GROUP_SEPARATOR);
        if (groups.length != 3) {
            throwIllegalArg();
        }

        String serviceId = groups[0];
        String[] insts = groups[1].split(SEPARATOR);
        if (insts.length != 9) {
            throwIllegalArg();
        }
        String address = insts[0];
        long startTimeMs = Long.parseLong(insts[1]);
        long endTimeMs = Long.parseLong(insts[2]);
        long totalReqs = Long.parseLong(insts[3]);
        long successes = Long.parseLong(insts[4]);
        long failures = Long.parseLong(insts[5]);
        long rejections = Long.parseLong(insts[6]);
        long avgResponseMs = Long.parseLong(insts[7]);
        long throughput = Long.parseLong(insts[8]);
        ServerStats serverStats = new ServerStats(address);
        RuntimeInfo runInfo = isEmpty(groups[2]) || groups[2].equals("null") ?
                null : RuntimeInfo.fromString(groups[2]);
        serverStats.setRuntimeInfo(runInfo);

        return new SnapshotStats() {
            private static final long serialVersionUID = 6197862269143364929L;

            @Override
            public String serviceId() {
                return serviceId;
            }

            @Override
            public long startTimeMs() {
                return startTimeMs;
            }

            @Override
            public long endTimeMs() {
                return endTimeMs;
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
            public long getAvgResponseMs() {
                return avgResponseMs;
            }

            @Override
            public long getThroughput() {
                return throughput;
            }

            @Override
            public long getTotalResponseMs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getNumberOfRequests() {
                return totalReqs;
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
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getTimeIntervalMs(TimeUnit unit) {
        checkNotNull(unit, "unit");
        return unit.convert(timeInterval, timeUnit);
    }

    @Override
    public long startTimeMs() {
        return _startTimeMs(offset());
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
        return successesCounterMap.keySet();
    }

    @Override
    public void success(String serviceId, long responseMs) {
        long offset = offset();
        getOrCreate(totalResponseMsCounterMap, serviceId).add(offset, responseMs);
        getOrCreate(successesCounterMap, serviceId).increment(offset);
    }

    @Override
    public void failure(String serviceId, long responseMs) {
        long offset = offset();
        // getOrCreate(totalResponseMsCounterMap, serviceId).add(offset, responseMs);
        getOrCreate(failuresCounterMap, serviceId).increment(offset);
    }

    @Override
    public void rejection(String serviceId) {
        long offset = offset();
        getOrCreate(rejectionsCounterMap, serviceId).increment(offset);
    }

    @Override
    public void clean() {
        // 留一个间隔作为缓冲
        long toKey = offset() - (windowSize << 1);
        cleanMap(totalResponseMsCounterMap, toKey);
        cleanMap(successesCounterMap, toKey);
        cleanMap(failuresCounterMap, toKey);
        cleanMap(rejectionsCounterMap, toKey);
    }

    @Override
    public long getAvgResponseMs() {
        long high = offset();
        return _getAvgResponseMs(high);
    }


    @Override
    public long getAvgResponseMs(String serviceId) {
        if (!totalResponseMsCounterMap.containsKey(serviceId)) {
            return -1;
        }

        long high = offset();
        return _getAvgResponseMs(serviceId, high);
    }

    @Override
    public long getThroughput() {
        long high = offset();
        return _getThroughput(high);
    }

    @Override
    public long getThroughput(String serviceId) {
        if (!successesCounterMap.containsKey(serviceId)) {
            return -1;
        }

        long high = offset();
        return _getThroughput(serviceId, high);
    }

    @Override
    public long getTotalResponseMs() {
        long high = offset();
        return _getTotalResponseMs(high);
    }

    @Override
    public long getTotalResponseMs(String serviceId) {
        if (!totalResponseMsCounterMap.containsKey(serviceId)) {
            return -1;
        }

        long high = offset();
        return _getTotalResponseMs(serviceId, high);
    }

    @Override
    public long getNumberOfRequests() {
        long high = offset();
        return _getNumberOfRequests(high);
    }

    @Override
    public long getNumberOfRequests(String serviceId) {
        if (!successesCounterMap.containsKey(serviceId)) {
            return -1;
        }

        long high = offset();
        return _getNumberOfRequests(serviceId, high);
    }

    @Override
    public long getNumberOfSuccesses() {
        long high = offset();
        return _getNumberOfSuccesses(high);
    }

    @Override
    public long getNumberOfSuccesses(String serviceId) {
        if (!successesCounterMap.containsKey(serviceId)) {
            return -1;
        }

        long high = offset();
        return _getNumberOfSuccesses(serviceId, high);
    }

    @Override
    public long getNumberOfFailures() {
        long high = offset();
        return _getNumberOfFailures(high);
    }

    @Override
    public long getNumberOfFailures(String serviceId) {
        if (!failuresCounterMap.containsKey(serviceId)) {
            return -1;
        }

        long high = offset();
        return _getNumberOfFailures(serviceId, high);
    }

    @Override
    public long getNumberOfRejections() {
        long high = offset();
        return _getNumberOfRejections(high);
    }

    @Override
    public long getNumberOfRejections(String serviceId) {
        if (!rejectionsCounterMap.containsKey(serviceId)) {
            return -1;
        }

        long high = offset();
        return _getNumberOfRejections(serviceId, high);
    }

    @Override
    public String snapshot(String serviceId) {
        long offset = offset();
        return serviceId + GROUP_SEPARATOR
               + address + SEPARATOR
               + _startTimeMs(offset) + SEPARATOR
               + TimeUnit.MILLISECONDS.convert(offset * timeInterval, timeUnit) + SEPARATOR
               + _getNumberOfRequests(serviceId, offset) + SEPARATOR
               + _getNumberOfSuccesses(serviceId, offset) + SEPARATOR
               + _getNumberOfFailures(serviceId, offset) + SEPARATOR
               + _getNumberOfRejections(serviceId, offset) + SEPARATOR
               + _getAvgResponseMs(serviceId, offset) + SEPARATOR
               + _getThroughput(serviceId, offset)
               + GROUP_SEPARATOR + serverStats.getRuntimeInfo();
    }

    @Override
    public String toString() {
        return getServiceIds()
                .stream()
                .map(this::snapshot)
                .collect(Collectors.toList())
                .toString();
    }

    private long _startTimeMs(long offset) {
        long start = offset - windowSize;
        return TimeUnit.MILLISECONDS.convert(start * timeInterval, timeUnit);
    }

    private long _getAvgResponseMs(long high) {
        long low = high - windowSize;
        // avg = totalResponseMs / (successes + 1)
        return sumMap(totalResponseMsCounterMap, low, high) / (sumMap(successesCounterMap, low, high) + 1);
    }

    private long _getAvgResponseMs(String serviceId, long high) {
        long low = high - windowSize;
        // avg = totalResponseMs / (successes + 1)
        return getOrCreate(totalResponseMsCounterMap, serviceId).sum(low, high) /
               (getOrCreate(successesCounterMap, serviceId).sum(low, high) + 1);
    }

    private long _getThroughput(long high) {
        long low = high - windowSize;
        return sumMap(successesCounterMap, low, high) /
               TimeUnit.SECONDS.convert(windowSize * timeInterval, timeUnit);
    }

    private long _getThroughput(String serviceId, long high) {
        long low = high - windowSize;
        return getOrCreate(successesCounterMap, serviceId).sum(low, high) /
               TimeUnit.SECONDS.convert(windowSize * timeInterval, timeUnit);
    }

    private long _getTotalResponseMs(long high) {
        long low = high - windowSize;
        return sumMap(totalResponseMsCounterMap, low, high);
    }

    private long _getTotalResponseMs(String serviceId, long high) {
        long low = high - windowSize;
        return getOrCreate(totalResponseMsCounterMap, serviceId).sum(low, high);
    }

    private long _getNumberOfRequests(long high) {
        return _getNumberOfSuccesses(high) +
               _getNumberOfFailures(high) +
               _getNumberOfRejections(high);
    }

    private long _getNumberOfRequests(String serviceId, long high) {
        return _getNumberOfSuccesses(serviceId, high) +
               _getNumberOfFailures(serviceId, high) +
               _getNumberOfRejections(serviceId, high);
    }

    private long _getNumberOfSuccesses(long high) {
        long low = high - windowSize;
        return sumMap(successesCounterMap, low, high);
    }

    private long _getNumberOfSuccesses(String serviceId, long high) {
        long low = high - windowSize;
        return getOrCreate(successesCounterMap, serviceId).sum(low, high);
    }

    private long _getNumberOfFailures(long high) {
        long low = high - windowSize;
        return sumMap(failuresCounterMap, low, high);
    }

    private long _getNumberOfFailures(String serviceId, long high) {
        long low = high - windowSize;
        return getOrCreate(failuresCounterMap, serviceId).sum(low, high);
    }

    private long _getNumberOfRejections(long high) {
        long low = high - windowSize;
        return sumMap(rejectionsCounterMap, low, high);
    }

    private long _getNumberOfRejections(String serviceId, long high) {
        long low = high - windowSize;
        return getOrCreate(rejectionsCounterMap, serviceId).sum(low, high);
    }

    private SegmentCounter getOrCreate(Map<String, SegmentCounter> counterMap, String key) {
        return counterMap.computeIfAbsent(key, k -> counterFactory.newCounter());
    }

    private long offset() {
        long currentTimeMillis = System.currentTimeMillis();
        return timeUnit.convert(currentTimeMillis, TimeUnit.MILLISECONDS) / timeInterval;
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

    private static void throwIllegalArg() {
        throw new IllegalArgumentException("text format error, see TimeWindowInstanceStats.snapshot(java.lang.String)");
    }

}
