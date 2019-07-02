package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static com.aliware.tianchi.common.util.MathUtil.isApproximate;
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

    private Configuration configuration = new Configuration();

    private final Map<String, List<SnapshotStats>> queueMap = new ConcurrentHashMap<>();

    private final Comparator<SnapshotStats> comparator =
            (o1, o2) -> {
                long a1 = o1.getAvgResponseMs(),
                        a2 = o2.getAvgResponseMs();
                if (isApproximate(a1, a2, 0)) {
                    int w1 = getWaits(o1.getAddress());
                    int w2 = getWaits(o2.getAddress());
                    int d1 = w1 - o1.getActiveCount();
                    int d2 = w2 - o2.getActiveCount();
                    return d2 - d1;
                }

                return (int) (a1 - a2);
            };

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

    public synchronized void updateInstanceStats(String serviceId, String address, SnapshotStats snapshotStats) {
        Map<String, SnapshotStats> instanceStatsMap = getInstanceStatsMap(serviceId);
        instanceStatsMap.put(address, snapshotStats);
        List<SnapshotStats> statsArrayList = new ArrayList<>(instanceStatsMap.values());
        statsArrayList.sort(comparator);
        queueMap.put(serviceId, Collections.unmodifiableList(statsArrayList));
    }

    public List<SnapshotStats> getSortStats(String serviceId) {
        return queueMap.get(serviceId);
    }

    public void queue(String address) {
        waitCounterMap.computeIfAbsent(address, k -> new LongAdder())
                      .increment();
    }

    public void dequeue(String address) {
        waitCounterMap.computeIfAbsent(address, k -> new LongAdder())
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

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}