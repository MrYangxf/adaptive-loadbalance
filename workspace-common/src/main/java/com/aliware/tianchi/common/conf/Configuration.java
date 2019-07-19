package com.aliware.tianchi.common.conf;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.RingCounter;
import com.aliware.tianchi.common.util.SegmentCounterFactory;
import com.aliware.tianchi.common.util.SkipListCounter;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxf
 */
public class Configuration implements Serializable {
    private static final long serialVersionUID = -420784964533522751L;

    /**
     * 平均响应时间误差范围ms
     */
    private static final int ERROR_RANGE = 0;

    /**
     * 请求数占生产者线程池的最大比例 0~1
     */
    private double maxRateOfWaitingRequests = .85d;

    /**
     * 生产者进程最大cpu负载 0~1
     */
    private double maxProcessCpuLoad = .99d;

    private Comparator<SnapshotStats> statsComparator = null;

    /**
     * 指标统计时间窗口配置
     */
    private long windowSizeOfStats = 500;

    private long timeIntervalOfStats = 1;

    private TimeUnit timeUnitOfStats = TimeUnit.MILLISECONDS;

    private SegmentCounterFactory counterFactory = () -> new SkipListCounter();

    private boolean openAvgRT = true;

    private boolean openThroughput = false;

    /**
     * 开启运行时信息统计
     */
    private boolean openRuntimeStats = false;

    /**
     * 进程运行时信息统计队列大小
     */
    private int runtimeInfoQueueSize = 5;

    /**
     * 统计信息推送配置
     */
    private long statsPushInitDelayMs = 500;

    private long statsPushDelayMs = 150;

    public boolean isLogger() {
        return  (ThreadLocalRandom.current().nextInt() & 511) == 0;
        // return false;
    }
    
    public double getMaxRateOfWaitingRequests() {
        return maxRateOfWaitingRequests;
    }

    public Configuration setMaxRateOfWaitingRequests(double maxRateOfWaitingRequests) {
        this.maxRateOfWaitingRequests = maxRateOfWaitingRequests;
        return this;
    }

    public double getMaxProcessCpuLoad() {
        return maxProcessCpuLoad;
    }

    public Configuration setMaxProcessCpuLoad(double maxProcessCpuLoad) {
        this.maxProcessCpuLoad = maxProcessCpuLoad;
        return this;
    }

    public Comparator<SnapshotStats> getStatsComparator() {
        return statsComparator;
    }

    public Configuration setStatsComparator(Comparator<SnapshotStats> statsComparator) {
        this.statsComparator = statsComparator;
        return this;
    }

    public long getWindowSizeOfStats() {
        return windowSizeOfStats;
    }

    public Configuration setWindowSizeOfStats(long windowSizeOfStats) {
        this.windowSizeOfStats = windowSizeOfStats;
        return this;
    }

    public long getTimeIntervalOfStats() {
        return timeIntervalOfStats;
    }

    public Configuration setTimeIntervalOfStats(long timeIntervalOfStats) {
        this.timeIntervalOfStats = timeIntervalOfStats;
        return this;
    }

    public TimeUnit getTimeUnitOfStats() {
        return timeUnitOfStats;
    }

    public Configuration setTimeUnitOfStats(TimeUnit timeUnitOfStats) {
        this.timeUnitOfStats = timeUnitOfStats;
        return this;
    }

    public SegmentCounterFactory getCounterFactory() {
        return counterFactory;
    }

    public Configuration setCounterFactory(SegmentCounterFactory counterFactory) {
        this.counterFactory = counterFactory;
        return this;
    }

    public int getRuntimeInfoQueueSize() {
        return runtimeInfoQueueSize;
    }

    public Configuration setRuntimeInfoQueueSize(int runtimeInfoQueueSize) {
        this.runtimeInfoQueueSize = runtimeInfoQueueSize;
        return this;
    }

    public long getStatsPushInitDelayMs() {
        return statsPushInitDelayMs;
    }

    public Configuration setStatsPushInitDelayMs(long statsPushInitDelayMs) {
        this.statsPushInitDelayMs = statsPushInitDelayMs;
        return this;
    }

    public long getStatsPushDelayMs() {
        return statsPushDelayMs;
    }

    public Configuration setStatsPushDelayMs(long statsPushDelayMs) {
        this.statsPushDelayMs = statsPushDelayMs;
        return this;
    }

    public boolean isOpenRuntimeStats() {
        return openRuntimeStats;
    }

    public Configuration setOpenRuntimeStats(boolean openRuntimeStats) {
        this.openRuntimeStats = openRuntimeStats;
        return this;
    }

    public boolean isOpenAvgRT() {
        return openAvgRT;
    }

    public Configuration setOpenAvgRT(boolean openAvgRT) {
        this.openAvgRT = openAvgRT;
        return this;
    }

    public boolean isOpenThroughput() {
        return openThroughput;
    }

    public Configuration setOpenThroughput(boolean openThroughput) {
        this.openThroughput = openThroughput;
        return this;
    }
}
