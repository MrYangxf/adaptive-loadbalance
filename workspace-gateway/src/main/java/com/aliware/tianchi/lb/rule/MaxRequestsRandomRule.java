package com.aliware.tianchi.lb.rule;

import com.aliware.tianchi.lb.LBRule;
import com.aliware.tianchi.lb.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxf
 */
public class MaxRequestsRandomRule implements LBRule {
    public static final Logger logger = LoggerFactory.getLogger(MaxRequestsRandomRule.class);

    private final Map<Class<?>, Map<String, Long>> maxTptMap = new ConcurrentHashMap<>();

    public MaxRequestsRandomRule() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(
                         () -> {
                             for (Map.Entry<Class<?>, Map<String, InstanceStats>>
                                     entry : LBStatistics.STATS.getRegistry().entrySet()) {
                                 Class<?> key = entry.getKey();
                                 Map<String, InstanceStats> statsMap = entry.getValue();

                                 Map<String, Long> map = new HashMap<>();
                                 for (Map.Entry<String, InstanceStats> statsEntry : statsMap.entrySet()) {
                                     InstanceStats value = statsEntry.getValue();
                                     map.put(statsEntry.getKey(), value.evalMaxRequestsPerSeconds());
                                     logger.info(value.toString());
                                 }
                                 maxTptMap.put(key, map);
                                 logger.info(map.toString());
                             }
                         },
                         1000,
                         500,
                         TimeUnit.MILLISECONDS);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> candidates) {
        List<Invoker<T>> copyCands = new ArrayList<>(candidates);
        Map<String, Long> map = maxTptMap.get(copyCands.get(0).getInterface());
        int size;
        while ((size = copyCands.size()) != 0) {
            int r = ThreadLocalRandom.current().nextInt(size);
            Invoker<T> invoker = copyCands.get(r);
            InstanceStats stats = LBStatistics.STATS.getInstanceStats(invoker);
            long tpt = stats.getNumberOfRequests(1);
            if (map == null) {
                return invoker;
            }
            Long maxTpt = map.get(invoker.getUrl().getAddress());
            if (maxTpt == null || tpt < maxTpt) {
                return invoker;
            }
            copyCands.remove(r);
        }
        return null;
    }
}
