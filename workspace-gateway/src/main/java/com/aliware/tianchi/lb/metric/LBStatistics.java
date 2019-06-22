package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.metric.SnapshotStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
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

    public Map<String, SnapshotStats> getInstanceStatsMap(Invoker<?> invoker, Invocation invocation) {
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

    public SnapshotStats getInstanceStats(Invoker<?> invoker, Invocation invocation) {
        Map<String, SnapshotStats> instanceStatsMap = getInstanceStatsMap(invoker, invocation);
        String address = invoker.getUrl().getAddress();
        return instanceStatsMap.get(address);
    }

    public void updateInstanceStats(Invoker<?> invoker,
                                    Invocation invocation,
                                    SnapshotStats snapshotStats) {
        String address = invoker.getUrl().getAddress();
        getInstanceStatsMap(invoker, invocation).put(address, snapshotStats);
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