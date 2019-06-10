package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.rpc.Invoker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.defaultIfNull;

/**
 * @author yangxf
 */
public class LBStatistics {

    public static final LBStatistics STATS = new LBStatistics(null);

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

    public LBStatistics(Function<Invoker<?>, InstanceStats> statsGenerator) {
        this.statsGenerator = defaultIfNull(statsGenerator,
                                            invoker -> new TimeWindowInstanceStats(null,
                                                                                   invoker.getUrl().getAddress()));
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
                    ServerStats newServerStats = new ServerStats(invoker.getUrl().getAddress());
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

}