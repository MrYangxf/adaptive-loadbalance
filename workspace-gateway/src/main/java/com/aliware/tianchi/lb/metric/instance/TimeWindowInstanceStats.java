package com.aliware.tianchi.lb.metric.instance;

import com.aliware.tianchi.lb.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.ServerStats;
import com.aliware.tianchi.util.SegmentCounter;
import com.aliware.tianchi.util.SegmentCounterFactory;
import com.aliware.tianchi.util.SkipListCounter;

import java.util.concurrent.TimeUnit;

import static com.aliware.tianchi.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.util.ObjectUtil.defaultIfNull;

/**
 * 基于时间窗口的统计信息
 *
 * @author yangxf
 */
public class TimeWindowInstanceStats implements InstanceStats {
    private static final long serialVersionUID = -1040897703729118186L;

    private static final long DEFAULT_INTERVAL_SECONDS = 10;
    private static final SegmentCounterFactory DEFAULT_COUNTER_FACTORY = SkipListCounter::new;

    private final String address;

    /**
     * 窗口的时间间隔
     */
    private final long intervalSeconds;

    /**
     * 该实例服务器信息
     */
    private volatile ServerStats serverStats;

    // 计数器， totalResponseMs和numberOfRequests不记录被拒绝的请求
    private final SegmentCounter totalResponseMsCounter;
    private final SegmentCounter numberOfRequestsCounter;
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
        // numberOfRequestsCounter.increment(s);
        numberOfFailuresCounter.increment(s);
    }

    @Override
    public void rejection() {
        long s = getCurrentSeconds();
        numberOfRejectionsCounter.increment(s);
    }

    @Override
    public void clean() {
        long toKey = getCurrentSeconds() - intervalSeconds;
        totalResponseMsCounter.clean(toKey);
        numberOfRequestsCounter.clean(toKey);
        numberOfFailuresCounter.clean(toKey);
        numberOfRejectionsCounter.clean(toKey);
    }

    @Override
    public long getAvgResponseMs() {
        long high = getCurrentSeconds(),
                low = high - intervalSeconds;
        return totalResponseMsCounter.sum(low, true, high, false) /
               (numberOfRequestsCounter.sum(low, true, high, false) + 1);
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
               " req=" + getNumberOfRequests() +
               ", fai=" + getNumberOfFailures() +
               ", rej=" + getNumberOfRejections() +
               ", avg=" + getAvgResponseMs() +
               ", tpt=" + getThroughput();
    }

    private static long getCurrentSeconds() {
        long ms = System.currentTimeMillis();
        return TimeUnit.MILLISECONDS.toSeconds(ms);
    }
}
