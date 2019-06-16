package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.metric.SnapshotStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;

/**
 * @author yangxf
 */
public class LBStatistics {

    private static final Logger logger = LoggerFactory.getLogger(LBStatistics.class);

    // key=serviceId, value={key=address, value=SnapshotStats}
    private static final Map<String, Map<String, SnapshotStats>> registry = new ConcurrentHashMap<>();

    public static Map<String, SnapshotStats> getInstanceStatsMap(Invoker<?> invoker, Invocation invocation) {
        checkNotNull(invoker, "invoker");
        checkNotNull(invocation, "invocation");
        String serviceId = invoker.getInterface().getName() + '#' + invocation.getMethodName();
        Map<String, SnapshotStats> instanceStatsMap = registry.get(serviceId);
        if (instanceStatsMap == null) {
            Map<String, SnapshotStats> newMap = new ConcurrentHashMap<>();
            instanceStatsMap = registry.putIfAbsent(serviceId, newMap);
            if (instanceStatsMap == null) {
                instanceStatsMap = newMap;
            }
        }
        return instanceStatsMap;
    }

    public static SnapshotStats getInstanceStats(Invoker<?> invoker, Invocation invocation) {
        Map<String, SnapshotStats> instanceStatsMap = getInstanceStatsMap(invoker, invocation);
        String address = invoker.getUrl().getAddress();
        return instanceStatsMap.get(address);
    }

    public static void updateInstanceStats(Invoker<?> invoker, Invocation invocation, SnapshotStats snapshotStats) {
        Map<String, SnapshotStats> instanceStatsMap = getInstanceStatsMap(invoker, invocation);
        String address = invoker.getUrl().getAddress();
        instanceStatsMap.put(address, snapshotStats);
    }
    
    public static Map<String, Map<String, SnapshotStats>> getRegistry() {
        return registry;
    }

}