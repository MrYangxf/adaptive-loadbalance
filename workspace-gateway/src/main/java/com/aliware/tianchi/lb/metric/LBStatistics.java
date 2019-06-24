package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.metric.SnapshotStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class LBStatistics {

    public static final LBStatistics INSTANCE = new LBStatistics();

    private static final Logger logger = LoggerFactory.getLogger(LBStatistics.class);

    // key=getServiceId, value={key=address, value=SnapshotStats}
    private final Map<String, Map<String, SnapshotStats>> registry = new ConcurrentHashMap<>();

    private final Map<String, LongAdder> waitCounterMap = new ConcurrentHashMap<>();

    private LBStatistics() {
    }

    public Map<String, SnapshotStats> getInstanceStatsMap(String serviceId) {
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

    public SnapshotStats getInstanceStats(String serviceId, String address) {
        Map<String, SnapshotStats> instanceStatsMap = getInstanceStatsMap(serviceId);
        return instanceStatsMap.get(address);
    }

    public void updateInstanceStats(String serviceId, String address, SnapshotStats snapshotStats) {
        getInstanceStatsMap(serviceId).put(address, snapshotStats);
    }

    public void queue(Invoker<?> invoker) {
        waitCounterMap.computeIfAbsent(invoker.getUrl().getAddress(), k -> new LongAdder())
                      .increment();
    }

    public void dequeue(Invoker<?> invoker) {
        waitCounterMap.computeIfAbsent(invoker.getUrl().getAddress(), k -> new LongAdder())
                      .decrement();
    }

    public int getWaits(String address) {
        LongAdder counter = waitCounterMap.get(address);
        if (nonNull(counter)) {
            return counter.intValue();
        }
        return 0;
    }

    public Map<String, Map<String, SnapshotStats>> getRegistry() {
        Map<String, Map<String, SnapshotStats>> snap = new HashMap<>();
        for (Map.Entry<String, Map<String, SnapshotStats>> e : registry.entrySet()) {
            HashMap<String, SnapshotStats> valueMap = new HashMap<>();
            snap.put(e.getKey(), valueMap);
            for (Map.Entry<String, SnapshotStats> se : e.getValue().entrySet()) {
                valueMap.put(se.getKey(), se.getValue());
            }
        }
        return snap;
    }

}