package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import com.aliware.tianchi.common.metric.ServerStats;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.aliware.tianchi.common.util.ObjectUtil.*;

/**
 * @author yangxf
 */
public class WeightedLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(WeightedLoadBalance.class);

    /**
     * key = serviceId, value = {key = address, value = weight}
     */
    private final Map<String, Map<String, Integer>> weightCache = new ConcurrentHashMap<>();

    public WeightedLoadBalance() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(
                         new WeightedTask(),
                         1000,
                         1000,
                         TimeUnit.MILLISECONDS);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        String serviceId = invokers.get(0).getInterface().getName() + '#' + invocation.getMethodName();

        int size = invokers.size();
        int[] weights = weighting(serviceId, invokers);

        assert size == weights.length;

        int sum = 0;
        for (int i = 0; i < size; i++) {
            sum += weights[i];
            weights[i] = sum;
        }

        if (sum > size) {
            int r = ThreadLocalRandom.current().nextInt(sum);
            for (int i = 0; i < size; i++) {
                if (r < weights[i]) {
                    return invokers.get(i);
                }
            }
        }

        return invokers.get(ThreadLocalRandom.current().nextInt(size));
    }

    Map<String, Integer> calculate(String serviceId, Map<String, SnapshotStats> statsMap, Map<String, Integer> oldWeightMap) {
        Map<String, Integer> weightMap = new HashMap<>();

        double loadTotal = 0, avgTotal = 0, failTotal = 0;
        Map<String, Long> avgMap = new HashMap<>();
        Map<String, Double> loadMap = new HashMap<>();
        Map<String, Long> failMap = new HashMap<>();

        for (Map.Entry<String, SnapshotStats> statsEntry : statsMap.entrySet()) {
            String address = statsEntry.getKey();
            SnapshotStats stats = statsEntry.getValue();

            long rejections = stats.getNumberOfRejections();
            long notSuccesses = stats.getNumberOfFailures() + rejections;

            failTotal += notSuccesses;
            failMap.put(address, notSuccesses);

            long avgResponseMs = stats.getAvgResponseMs();
            avgTotal += avgResponseMs;
            avgMap.put(address, avgResponseMs);

            ServerStats serverStats = stats.getServerStats();
            RuntimeInfo info = serverStats.getRuntimeInfo();
            logger.info(stats.toString() + ", runtime info : " + info);
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

            int w = (int) ((w1 * .3d + w2 * .5d + w3 * .2d) * 100) + 1;
            weightMap.put(key, w);
        }

        return weightMap;
    }

    private <T> int[] weighting(String serviceId, List<Invoker<T>> invokers) {
        int size = invokers.size();
        assert size > 1;

        int[] weights = new int[size];
        Map<String, Integer> weightMap = weightCache.get(serviceId);

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

    class WeightedTask implements Runnable {
        @Override
        public void run() {
            // Map<String, Map<String, SnapshotStats>> allStatsMap = LBStatistics.getRegistry();
            //
            // for (Map.Entry<String, Map<String, SnapshotStats>> entry : allStatsMap.entrySet()) {
            //     String serviceId = entry.getKey();
            //     Map<String, SnapshotStats> statsMap = entry.getValue();
            //     Map<String, Integer> oldWeightMap = weightCache.get(serviceId);
            //     Map<String, Integer> newWeightMap = calculate(serviceId, statsMap, oldWeightMap);
            //     weightCache.put(serviceId, newWeightMap);
            //     logger.info(String.format("update weight %s > %s", oldWeightMap, newWeightMap));
            // }
        }
    }
}
