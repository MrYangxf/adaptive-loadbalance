package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.lb.metric.instance.BaseInstanceStats;
import com.aliware.tianchi.lb.metric.instance.TimeWindowInstanceStats;
import com.aliware.tianchi.util.SegmentCounterFactory;
import com.aliware.tianchi.util.SkipListCounter;
import org.apache.dubbo.rpc.Invoker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.aliware.tianchi.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.util.ObjectUtil.defaultIfNull;

/**
 * @author yangxf
 */
public class LBStatistics {

    public static final LBStatistics STATS = LBStatistics.builder().build();

    /**
     * 是否统计服务器指标，默认关闭
     */
    private boolean serverStats;

    /**
     * 是否统计高级指标，默认关闭
     */
    private boolean advancedStats;

    /**
     * 是否采用时间窗口统计，默认开启
     */
    private boolean timeWindowStats = true;
    private long windowSize = 10;

    private SegmentCounterFactory counterFactory;

    /**
     * InstanceStats 创建函数
     */
    private Function<Invoker<?>, InstanceStats> statsGenerator;

    private Map<Class<?>, Map<String, InstanceStats>> registry = new ConcurrentHashMap<>();

    private LBStatistics() {
        if (statsGenerator == null) {
            if (timeWindowStats) {
                statsGenerator = invoker ->
                        new TimeWindowInstanceStats(windowSize, new ServerStats(), counterFactory);
            } else {
                statsGenerator = invoker ->
                        new BaseInstanceStats(new ServerStats(), System.currentTimeMillis());
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, InstanceStats> getInstanceStatsMap(Invoker<?> invoker) {
        checkNotNull(invoker, "invoker");
        Class<?> interClass = invoker.getInterface();
        Map<String, InstanceStats> instanceStatsMap = registry.get(interClass);
        if (instanceStatsMap == null) {
            Map<String, InstanceStats> newMap = registry.putIfAbsent(interClass, new ConcurrentHashMap<>());
            if (newMap != null) {
                instanceStatsMap = newMap;
            }
        }
        return instanceStatsMap;
    }

    public InstanceStats getInstanceStats(Invoker<?> invoker) {
        Map<String, InstanceStats> instanceStatsMap = getInstanceStatsMap(invoker);
        String address = invoker.getUrl().getAddress();
        InstanceStats stats = instanceStatsMap.get(address);
        if (stats == null) {
            InstanceStats newStats = instanceStatsMap.putIfAbsent(address, statsGenerator.apply(invoker));
            if (newStats != null) {
                stats = newStats;
            }
        }
        return stats;
    }

    public void evict(Invoker<?> invoker) {
        checkNotNull(invoker, "invoker");
        Class<?> interClass = invoker.getInterface();
        Map<String, InstanceStats> statsMap = registry.get(interClass);
        if (statsMap != null) {
            statsMap.remove(invoker.getUrl().getAddress());
        }
    }

    public static class Builder {
        private boolean serverStats;
        private boolean advancedStats;
        private boolean timeWindowStats = true;
        private long windowSize = 10;
        private SegmentCounterFactory counterFactory;
        private Function<Invoker<?>, InstanceStats> statsGenerator;

        Builder() {
        }

        public Builder serverStats(boolean serverStats) {
            this.serverStats = serverStats;
            return this;
        }

        public Builder advancedStats(boolean advancedStats) {
            this.advancedStats = advancedStats;
            return this;
        }

        public Builder timeWindowStats(boolean timeWindowStats) {
            this.timeWindowStats = timeWindowStats;
            return this;
        }

        public Builder windowSize(long windowSize) {
            this.windowSize = windowSize;
            return this;
        }

        public Builder counterFactory(SegmentCounterFactory counterFactory) {
            this.counterFactory = counterFactory;
            return this;
        }

        public Builder statsGenerator(Function<Invoker<?>, InstanceStats> statsGenerator) {
            this.statsGenerator = statsGenerator;
            return this;
        }

        public LBStatistics build() {
            LBStatistics stats = new LBStatistics();
            stats.serverStats = serverStats;
            stats.advancedStats = advancedStats;
            stats.timeWindowStats = timeWindowStats;
            stats.windowSize = windowSize;
            stats.counterFactory = defaultIfNull(counterFactory, SkipListCounter::new);
            stats.statsGenerator = statsGenerator;
            return stats;
        }
    }

}
