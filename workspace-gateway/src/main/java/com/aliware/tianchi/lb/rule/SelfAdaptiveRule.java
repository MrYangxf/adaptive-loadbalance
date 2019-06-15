package com.aliware.tianchi.lb.rule;

import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.LBRule;
import com.aliware.tianchi.lb.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import com.aliware.tianchi.lb.metric.ServerStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static com.aliware.tianchi.common.util.ObjectUtil.*;

/**
 * @author yangxf
 */
public class SelfAdaptiveRule implements LBRule {

    private static final Logger logger = LoggerFactory.getLogger(SelfAdaptiveRule.class);

    private LBStatistics statistics = LBStatistics.STATS;

    private final Map<Class<?>, Map<String, Long>> maxTptMap = new ConcurrentHashMap<>();

    private final Map<Class<?>, Map<String, Integer>> weightCache = new ConcurrentHashMap<>();

    private LongAdder rejAdder = new LongAdder();

    public SelfAdaptiveRule() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(
                         new WeightedTask(),
                         1000,
                         1000,
                         TimeUnit.MILLISECONDS);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> candidates) {
        int size = candidates.size();
        int[] weights = weighting(candidates);

        assert size == weights.length;

        int sum = 0;
        for (int i = 0; i < size; i++) {
            sum += weights[i];
        }

        // Map<String, Long> tptMap = maxTptMap.get(candidates.get(0).getInterface());
        // long numberOfRequests = 0;
        // while (sum > 0) {
        //     int r = ThreadLocalRandom.current().nextInt(sum);
        //     for (int i = 0; i < size; i++) {
        //         r -= weights[i];
        //         if (r < 0) {
        //             Invoker<T> select = candidates.get(i);
        //             if (tptMap != null) {
        //                 InstanceStats stats = LBStatistics.STATS.getInstanceStats(select);
        //                 Long maxTpt = tptMap.get(select.getUrl().getAddress());
        //                 numberOfRequests = stats.getNumberOfRequests(0);
        //                 if (maxTpt != null && numberOfRequests > maxTpt) {
        //                     sum -= weights[i];
        //                     weights[i] = 0;
        //                     break;
        //                 }
        //             }
        //             return select;
        //         }
        //     }
        // }
        // rejAdder.increment();
        // logger.info("rejects=" + rejAdder.sum() + ", req=" + numberOfRequests + ", max=" + tptMap);
        // return null;
        //
        if (sum > 0) {
            int r = ThreadLocalRandom.current().nextInt(sum);
            for (int i = 0; i < size; i++) {
                r -= weights[i];
                if (r < 0) {
                    return candidates.get(i);
                }
            }
        }

        return candidates.get(ThreadLocalRandom.current().nextInt(size));
    }

    private <T> int[] weighting(List<Invoker<T>> invokers) {
        int size = invokers.size();
        assert size > 1;

        int[] weights = new int[size];
        Class<T> interClass = invokers.get(0).getInterface();
        Map<String, Integer> weightMap = getWeight(interClass);

        if (isNull(weightMap)) {
            Arrays.fill(weights, 100);
            return weights;
        }

        for (int i = 0; i < size; i++) {
            Invoker<T> invoker = invokers.get(i);
            weights[i] = defaultIfNull(weightMap.get(invoker.getUrl().getAddress()), 1);
        }

        return weights;
    }

    private Map<String, Integer> getWeight(Class<?> interClass) {
        return weightCache.get(interClass);
    }

    private void updateWeight(Class<?> interClass, Map<String, Integer> weightMap) {
        weightCache.put(interClass, weightMap);
    }

    private void removeWeight(Class<?> interClass) {
        weightCache.remove(interClass);
    }

    Map<String, Integer> calculate(Map<String, InstanceStats> statsMap, Map<String, Integer> prevWeightMap) {
        Map<String, Integer> weightMap = new HashMap<>();

        double loadTotal = 0, avgTotal = 0, failTotal = 0;
        Map<String, Long> avgMap = new HashMap<>();
        Map<String, Double> loadMap = new HashMap<>();
        Map<String, Long> failMap = new HashMap<>();

        for (Map.Entry<String, InstanceStats> statsEntry : statsMap.entrySet()) {
            String address = statsEntry.getKey();
            InstanceStats stats = statsEntry.getValue();

            long rejections = stats.getNumberOfRejections();
            long notSuccesses = stats.getNumberOfFailures() + rejections;

            failTotal += notSuccesses;
            failMap.put(address, notSuccesses);

            long avgResponseMs = stats.getAvgResponseMs();
            avgTotal += avgResponseMs;
            avgMap.put(address, avgResponseMs);

            ServerStats serverStats = stats.getServerStats();
            RuntimeInfo info = serverStats.getRuntimeInfo();
            logger.info("STATS : " + stats);
            if (nonNull(info)) {
                double processCpuLoad = info.getProcessCpuLoad();
                loadTotal += processCpuLoad;
                loadMap.put(address, processCpuLoad);
            }
        }

        for (String key : statsMap.keySet()) {
            Long avg = avgMap.get(key);
            double w1 = (avgTotal - avg) / (avgTotal + 1);
            Long failRate = failMap.get(key);
            double w2 = (failTotal - failRate) / (failTotal + 1);

            double w3 = 0;
            Double load = loadMap.get(key);
            if (nonNull(load)) {
                w3 += (loadTotal - load) / loadTotal;
            }

            int w = (int) ((w1 * .2d + w2 * .5d + w3 * .3d) * 100) + 1;
            weightMap.put(key, w);
        }

        return weightMap;
    }

    class WeightedTask implements Runnable {
        @Override
        public void run() {
            Map<Class<?>, Map<String, InstanceStats>> registry = statistics.getRegistry();
            for (Map.Entry<Class<?>, Map<String, InstanceStats>> entry : registry.entrySet()) {
                Class<?> interClass = entry.getKey();
                Map<String, InstanceStats> statsMap = entry.getValue();
                Map<String, Integer> weightMap = calculate(statsMap, getWeight(interClass));
                updateWeight(interClass, weightMap);

                Map<String, Long> map = new HashMap<>();
                for (Map.Entry<String, InstanceStats> statsEntry : statsMap.entrySet()) {
                    InstanceStats value = statsEntry.getValue();
                    map.put(statsEntry.getKey(), value.evalMaxRequestsPerSeconds());
                }
                maxTptMap.put(interClass, map);

                logger.info(interClass.getName() + " update weight " + weightMap);
            }
        }
    }
}
