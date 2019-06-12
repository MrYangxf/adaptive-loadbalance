package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.defaultIfNull;

/**
 * @author yangxf
 */
public class LBStatistics {

    private static final Logger logger = LoggerFactory.getLogger(LBStatistics.class);

    private static final Function<String, InstanceStats> DEFAULT_STATS_GENERATOR =
            address -> new TimeWindowInstanceStats(address, new ServerStats(address),
                                                   5, 1, TimeUnit.SECONDS,
                                                   null);
    
    public static final LBStatistics STATS = new LBStatistics(null);


    /**
     * InstanceStats 创建函数
     */
    private Function<String, InstanceStats> statsGenerator;

    /**
     * key = host:port, value = InstanceStats
     */
    private final Map<String, InstanceStats> registry = new ConcurrentHashMap<>();

    public LBStatistics(Function<String, InstanceStats> statsGenerator) {
        this.statsGenerator = defaultIfNull(statsGenerator, DEFAULT_STATS_GENERATOR);
    }

    public InstanceStats getInstanceStats(String address) {
        checkNotNull(address, "address");
        InstanceStats stats = registry.get(address);
        if (stats == null) {
            InstanceStats newStats = statsGenerator.apply(address);
            stats = registry.putIfAbsent(address, newStats);
            if (stats == null) {
                stats = newStats;
            }
        }
        return stats;
    }

    public Map<String, InstanceStats> getRegistry() {
        return registry;
    }

    public void updateRuntimeInfo(String address, RuntimeInfo runtimeInfo) {
        checkNotNull(address, "address");
        InstanceStats stats = registry.get(address);
        if (stats != null) {
            ServerStats serverStats = stats.getServerStats();
            if (serverStats != null) {
                serverStats.setRuntimeInfo(runtimeInfo);
                logger.info("update " + address + " to " + runtimeInfo);
            } else {
                logger.info(address + " runtime info is null");
            }
        } else {
            logger.info(address + " instance stats is null");
        }
    }

}