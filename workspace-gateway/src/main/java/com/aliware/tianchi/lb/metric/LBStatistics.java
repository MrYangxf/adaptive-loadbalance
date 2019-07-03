package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final Map<String, AtomicInteger> waitCounterMap = new ConcurrentHashMap<>();

    private Configuration configuration = new Configuration();

    private final Map<String, List<SnapshotStats>> queueMap = new ConcurrentHashMap<>();

    private final Comparator<SnapshotStats> comparator =
            (o1, o2) -> {

                int s1 = o1.getActiveCount();
                int s2 = o2.getActiveCount();

                RuntimeInfo r1 = o1.getServerStats().getRuntimeInfo();
                RuntimeInfo r2 = o2.getServerStats().getRuntimeInfo();

                if (nonNull(r1) && nonNull(r2)) {
                    s1 /= r1.getAvailableProcessors();
                    s2 /= r2.getAvailableProcessors();
                }
                
                return (s1 - s2);
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
        waitCounterMap.computeIfAbsent(address, k -> new AtomicInteger())
                      .getAndIncrement();
    }

    public void dequeue(String address) {
        waitCounterMap.computeIfAbsent(address, k -> new AtomicInteger())
                      .getAndDecrement();
    }

    public int getWaits(String address) {
        AtomicInteger counter = waitCounterMap.get(address);
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