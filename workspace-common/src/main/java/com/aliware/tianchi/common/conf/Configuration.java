package com.aliware.tianchi.common.conf;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.RingCounter;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.common.util.SegmentCounterFactory;
import com.aliware.tianchi.common.util.SkipListCounter;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static com.aliware.tianchi.common.util.MathUtil.isApproximate;
import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class Configuration implements Serializable {
    private static final long serialVersionUID = -420784964533522751L;
    
    /**
     * 平均响应时间误差范围ms
     */
    private static final int ERROR_RANGE = 0;
    
    public static final boolean OPEN_LOGGER = true;
    
    /**
     * 请求数占生产者线程池的最大比例 0~1
     */
    private double maxRateOfWaitingRequests = .8d;

    /**
     * 生产者进程最大cpu负载 0~1
     */
    private double maxProcessCpuLoad = .99d;

    private Comparator<SnapshotStats> statsComparator = LOAD_FIRST;

    /**
     * 指标统计时间窗口配置
     */
    private long windowSizeOfStats = 4;

    private long timeIntervalOfStats = 100;

    private TimeUnit timeUnitOfStats = TimeUnit.MILLISECONDS;

    private SegmentCounterFactory counterFactory = () -> new SkipListCounter();

    /**
     * 开启运行时信息统计
     */
    private boolean openRuntimeStats;
    
    /**
     * 进程运行时信息统计队列大小
     */
    private int runtimeInfoQueueSize = 5;

    /**
     * 统计信息推送配置
     */
    private long statsPushInitDelayMs = 500;

    private long statsPushDelayMs = 400;

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

    public static final Comparator<SnapshotStats> LOAD_FIRST = (o1, o2) -> {
        long a1 = o1.getAvgResponseMs(),
                a2 = o2.getAvgResponseMs();

        if (isApproximate(a1, a2, ERROR_RANGE)) {
            RuntimeInfo r1 = o1.getServerStats().getRuntimeInfo(),
                    r2 = o2.getServerStats().getRuntimeInfo();
            if (nonNull(r1) && nonNull(r2)) {
                double d = (1 - r1.getProcessCpuLoad()) * r1.getAvailableProcessors() -
                           (1 - r2.getProcessCpuLoad()) * r2.getAvailableProcessors();
                return d > 0 ? -1 : d < 0 ? 1 : 0;
            }
        }

        return (int) (a1 - a2);
    };

    public static final Comparator<SnapshotStats> THREADS_FIRST = (o1, o2) -> {
        long a1 = o1.getAvgResponseMs(),
                a2 = o2.getAvgResponseMs();

        if (isApproximate(a1, a2, ERROR_RANGE)) {
            RuntimeInfo r1 = o1.getServerStats().getRuntimeInfo(),
                    r2 = o2.getServerStats().getRuntimeInfo();
            int idles1 = o1.getDomainThreads() - o1.getActiveCount(),
                    idles2 = o2.getDomainThreads() - o2.getActiveCount();
            if (nonNull(r1) && nonNull(r2)) {
                idles1 /= r1.getAvailableProcessors();
                idles2 /= r2.getAvailableProcessors();
            }
            return idles2 - idles1;
        }

        return (int) (a1 - a2);
    };
}
