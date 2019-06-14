package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.common.util.SegmentCounter;
import com.aliware.tianchi.common.util.SegmentCounterFactory;
import com.aliware.tianchi.common.util.SkipListCounter;

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

    private static final long DEFAULT_INTERVAL_SECONDS = 10;
    private static final SegmentCounterFactory DEFAULT_COUNTER_FACTORY = SkipListCounter::new;

    /**
     * 服务实例的地址 host:port
     */
    private final String address;

    /**
     * 窗口的时间间隔
     */
    private final long intervalSeconds;

    /**
     * 该实例服务器信息
     */
    private volatile ServerStats serverStats;

    // 计数器
    private final SegmentCounter totalResponseMsCounter; // successes + failures MS
    private final SegmentCounter numberOfRequestsCounter; // successes + failures + rejections
    private final SegmentCounter numberOfFailuresCounter;
    private final SegmentCounter numberOfRejectionsCounter;

    public TimeWindowInstanceStats(ServerStats serverStats, String address) {
        this(DEFAULT_INTERVAL_SECONDS, serverStats, address, null);
    }

    public TimeWindowInstanceStats(long intervalSeconds, ServerStats serverStats, String address) {
        this(intervalSeconds, serverStats, address, null);
    }

    public TimeWindowInstanceStats(long intervalSeconds, ServerStats serverStats, String address, SegmentCounterFactory counterFactory) {
        checkNotNull(address, "address");
        if (intervalSeconds <= 0) {
            throw new IllegalArgumentException("intervalSeconds must be > 0");
        }
        this.intervalSeconds = intervalSeconds;
        this.serverStats = serverStats;
        this.address = address;
        counterFactory = defaultIfNull(counterFactory, DEFAULT_COUNTER_FACTORY);
        totalResponseMsCounter = counterFactory.newCounter();
        numberOfRequestsCounter = counterFactory.newCounter();
        numberOfFailuresCounter = counterFactory.newCounter();
        numberOfRejectionsCounter = counterFactory.newCounter();
    }

    @Override
    public void success(long responseMs) {
        long s = getCurrentSeconds();
        totalResponseMsCounter.add(s, responseMs);
        numberOfRequestsCounter.increment(s);
    }

    @Override
    public void failure(long responseMs) {
        long s = getCurrentSeconds();
        // totalResponseMsCounter.add(s, responseMs);
        numberOfRequestsCounter.increment(s);
        numberOfFailuresCounter.increment(s);
    }

    @Override
    public void rejection() {
        long s = getCurrentSeconds();
        numberOfRequestsCounter.increment(s);
        numberOfRejectionsCounter.increment(s);
    }

    @Override
    public void clean() {
        // 留一个间隔作为缓冲
        long toKey = getCurrentSeconds() - (intervalSeconds << 1);
        totalResponseMsCounter.clean(toKey);
        numberOfRequestsCounter.clean(toKey);
        numberOfFailuresCounter.clean(toKey);
        numberOfRejectionsCounter.clean(toKey);
    }

    @Override
    public long evalMaxRequestsPerSeconds() {
        ServerStats serverStats = this.serverStats;
        if (serverStats == null) {
            return Long.MAX_VALUE;
        }
        RuntimeInfo runtimeInfo = serverStats.getRuntimeInfo();
        if (runtimeInfo == null) {
            return Long.MAX_VALUE;
        }

        long timestamp = runtimeInfo.getTimestamp();
        long high = TimeUnit.MILLISECONDS.toSeconds(timestamp);
        if (high < getCurrentSeconds()) {
            high++;
        }
        long low = high - intervalSeconds;

        long total = numberOfRequestsCounter.sum(low, high);
        long notSuccesses = numberOfFailuresCounter.sum(low, high) +
                            numberOfRejectionsCounter.sum(low, high);
        long successes = total - notSuccesses;
        long successTpt = successes / intervalSeconds;

        if (successTpt == 0) {
            return Long.MAX_VALUE;
        }

        double rate = notSuccesses / (double) total;
        if (rate > .1) {
            return successTpt;
        }
        if (rate > .07) {
            return (long) (successTpt * 1.1);
        }
        if (rate > .05) {
            return (long) (successTpt * 1.2);
        }

        double processCpuLoad = runtimeInfo.getProcessCpuLoad();

        return Long.MAX_VALUE;
    }

    @Override
    public long getAvgResponseMs() {
        long high = getCurrentSeconds(),
                low = high - intervalSeconds;
        // avg = totalResponseMs / (requests - rejections + 1)
        return totalResponseMsCounter.sum(low, true, high, false) /
               (numberOfRequestsCounter.sum(low, true, high, false) -
                numberOfFailuresCounter.sum(low, true, high, false) -
                numberOfRejectionsCounter.sum(low, true, high, false) + 1);
    }

    @Override
    public long getThroughput() {
        long high = getCurrentSeconds(),
                low = high - intervalSeconds;
        return numberOfRequestsCounter.sum(low, true, high, false) / intervalSeconds;
    }

    @Override
    public long getTotalResponseMs() {
        long high = getCurrentSeconds(),
                low = high - intervalSeconds;
        return totalResponseMsCounter.sum(low, true, high, false);
    }

    @Override
    public long getNumberOfRequests() {
        long high = getCurrentSeconds(),
                low = high - intervalSeconds;
        return numberOfRequestsCounter.sum(low, true, high, false);
    }

    @Override
    public long getNumberOfRequests(long second) {
        return numberOfRequestsCounter.get(getCurrentSeconds());
    }

    @Override
    public long getNumberOfFailures() {
        long high = getCurrentSeconds(),
                low = high - intervalSeconds;
        return numberOfFailuresCounter.sum(low, true, high, false);
    }

    @Override
    public long getNumberOfRejections() {
        long high = getCurrentSeconds(),
                low = high - intervalSeconds;
        return numberOfRejectionsCounter.sum(low, true, high, false);
    }

    @Override
    public ServerStats getServerStats() {
        return serverStats;
    }

    @Override
    public void setServerStats(ServerStats serverStats) {
        this.serverStats = serverStats;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return getAddress() +
               " - req=" + getNumberOfRequests() +
               ", fai=" + getNumberOfFailures() +
               ", rej=" + getNumberOfRejections() +
               ", avg=" + getAvgResponseMs() +
               ", tpt=" + getThroughput() +
               ", max=" + evalMaxRequestsPerSeconds() +
               ", runtime=" + getServerStats().getRuntimeInfo();
    }

    private static long getCurrentSeconds() {
        long ms = System.currentTimeMillis();
        return TimeUnit.MILLISECONDS.toSeconds(ms);
    }
}
