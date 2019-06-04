package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.metric.instance.BaseInstanceStats;
import com.aliware.tianchi.lb.metric.instance.TimeWindowInstanceStats;
import com.aliware.tianchi.util.FastSkipListCounter;
import com.aliware.tianchi.util.SegmentCounterFactory;
import com.aliware.tianchi.util.SkipListCounter;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
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
    private static final Logger logger = LoggerFactory.getLogger(LBStatistics.class);

    public static final LBStatistics STATS = LBStatistics.builder()
                                                         .counterFactory(SkipListCounter::new)
                                                         .windowSize(3)
                                                         .build();

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

    /**
     * key = interface, sub_key = address, value = InstanceStats
     */
    private Map<Class<?>, Map<String, InstanceStats>> registry = new ConcurrentHashMap<>();

    /**
     * key = address, value = ServerStats
     */
    private Map<String, ServerStats> serverRegistry = new ConcurrentHashMap<>();

    private LBStatistics() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, InstanceStats> getInstanceStatsMap(Invoker<?> invoker) {
        checkNotNull(invoker, "invoker");
        Class<?> interClass = invoker.getInterface();
        Map<String, InstanceStats> instanceStatsMap = registry.get(interClass);
        if (instanceStatsMap == null) {
            Map<String, InstanceStats> newMap = new ConcurrentHashMap<>();
            instanceStatsMap = registry.putIfAbsent(interClass, newMap);
            if (instanceStatsMap == null) {
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
            InstanceStats newStats = statsGenerator.apply(invoker);
            stats = instanceStatsMap.putIfAbsent(address, newStats);
            if (stats == null) {
                ServerStats serverStats = serverRegistry.get(address);
                if (serverStats == null) {
                    ServerStats newServerStats = new ServerStats(invoker.getUrl().getHost());
                    serverStats = serverRegistry.putIfAbsent(address, newServerStats);
                    if (serverStats == null) {
                        serverStats = newServerStats;
                    }
                }
                newStats.setServerStats(serverStats);
                stats = newStats;
            }
        }
        return stats;
    }

    public Map<Class<?>, Map<String, InstanceStats>> getRegistry() {
        return registry;
    }

    public void updateRuntimeInfo(String address, RuntimeInfo runtimeInfo) {
        checkNotNull(address, "address");
        ServerStats serverStats = serverRegistry.get(address);
        if (serverStats != null) {
            serverStats.setRuntimeInfo(runtimeInfo);
        }
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
            if (statsGenerator == null) {
                if (timeWindowStats) {
                    statsGenerator = invoker ->
                            new TimeWindowInstanceStats(windowSize,
                                                        null,
                                                        invoker.getUrl().getAddress(),
                                                        counterFactory);
                } else {
                    statsGenerator = invoker ->
                            new BaseInstanceStats(null, System.currentTimeMillis());
                }
            }
            stats.statsGenerator = statsGenerator;
            return stats;
        }
    }

}
