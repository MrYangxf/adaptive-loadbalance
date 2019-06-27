package com.aliware.tianchi.common.conf;

import com.aliware.tianchi.common.util.SegmentCounterFactory;
import com.aliware.tianchi.common.util.SkipListCounter;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxf
 */
public class Configuration implements Serializable {
    private static final long serialVersionUID = -420784964533522751L;

    /**
     * 平均响应时间误差范围ms
     */
    private int avgRTMsErrorRange = 1;

    /**
     * 请求数占生产者线程池的最大比例 0~1
     */
    private double maxRateOfWaitingRequests = .8d;

    /**
     * 生产者进程最大cpu负载 0~1
     */
    private double maxProcessCpuLoad = .8d;

    /**
     * 指标统计时间窗口配置
     */
    private long windowSizeOfStats = 10;

    private long timeIntervalOfStats = 50;

    private TimeUnit timeUnitOfStats = TimeUnit.MILLISECONDS;

    private SegmentCounterFactory counterFactory = SkipListCounter::new;

    /**
     * 进程运行时信息统计队列大小
     */
    private int runtimeInfoQueueSize = 5;

    /**
     * 统计信息推送配置
     */
    private long statsPushInitDelayMs = 500;

    private long statsPushDelayMs = 500;

    public int getAvgRTMsErrorRange() {
        return avgRTMsErrorRange;
    }

    public Configuration setAvgRTMsErrorRange(int avgRTMsErrorRange) {
        this.avgRTMsErrorRange = avgRTMsErrorRange;
        return this;
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
}
