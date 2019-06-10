package com.aliware.tianchi.lb.metric.instance;

import com.aliware.tianchi.lb.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.ServerStats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;

/**
 * @author yangxf
 */
public class BaseInstanceStats implements InstanceStats {
    private static final long serialVersionUID = -7162869203189284104L;

    private LongAdder totalResponseMs = new LongAdder(); // successes + failures ms
    private LongAdder numberOfRequests = new LongAdder(); // successes + failures
    private AtomicLong numberOfFailures = new AtomicLong();
    private AtomicLong numberOfRejections = new AtomicLong();

    private final long lastStatsTimestamp;
    private volatile ServerStats serverStats;

    public BaseInstanceStats(ServerStats serverStats) {
        this(serverStats, System.currentTimeMillis());
    }

    public BaseInstanceStats(ServerStats serverStats, long lastStatsTimestamp) {
        checkNotNull(serverStats, "serverStats");
        this.serverStats = serverStats;
        this.lastStatsTimestamp = lastStatsTimestamp;
    }

    protected void doAccumulate(long responseMs) {
    }

    private void accumulate(long responseMs) {
        totalResponseMs.add(responseMs);
        numberOfRequests.increment();
        doAccumulate(responseMs);
    }

    public void success(long responseMs) {
        accumulate(responseMs);
    }

    public void failure(long responseMs) {
        numberOfFailures.incrementAndGet();
        accumulate(responseMs);
    }

    public void rejection() {
        numberOfRejections.incrementAndGet();
    }

    @Override
    public void clean() {

    }

    @Override
    public long evalMaxRequestsPerSeconds() {
        return 0;
    }

    public long getAvgResponseMs() {
        return getTotalResponseMs() / getNumberOfRequests();
    }

    public long getThroughput() {
        if (System.currentTimeMillis() <= lastStatsTimestamp) {
            return 0;
        }
        long s = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - lastStatsTimestamp);
        return getNumberOfRequests() / s;
    }

    public long getTotalResponseMs() {
        return totalResponseMs.sum();
    }

    public long getNumberOfRequests() {
        return numberOfRequests.sum();
    }

    @Override
    public long getNumberOfRequests(long second) {
        return 0;
    }

    public long getNumberOfFailures() {
        return numberOfFailures.get();
    }

    public long getNumberOfRejections() {
        return numberOfRejections.get();
    }

    public long getLastStatsTimestamp() {
        return lastStatsTimestamp;
    }

    public ServerStats getServerStats() {
        return serverStats;
    }

    @Override
    public void setServerStats(ServerStats serverStats) {
        this.serverStats = serverStats;
    }

    @Override
    public String getAddress() {
        return null;
    }


}