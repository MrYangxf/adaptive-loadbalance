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

import static com.aliware.tianchi.util.ObjectUtil.isNull;
import static com.aliware.tianchi.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class SelfAdaptiveRule implements LBRule {

    private static final Logger logger = LoggerFactory.getLogger(SelfAdaptiveRule.class);

    private LBStatistics statistics = LBStatistics.STATS;

    private final Map<Class<?>, Map<String, Integer>> weightCache = new ConcurrentHashMap<>();

    public SelfAdaptiveRule() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(new WeightedTask(), 3, 3, TimeUnit.SECONDS);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> candidates) {
        int size = candidates.size();
        int[] weights = weighting(candidates);

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
        Map<String, Integer> weightMap = weightCache.get(interClass);

        if (isNull(weightMap)) {
            Arrays.fill(weights, 100);
            return weights;
        }

        for (int i = 0; i < size; i++) {
            Invoker<T> invoker = invokers.get(i);
            statistics.getInstanceStats(invoker);
            weights[i] = weightMap.get(invoker.getUrl().getAddress());
        }

        return weights;
    }

    private void updateWeight(Class<?> interClass, Map<String, Integer> weightMap) {
        weightCache.put(interClass, weightMap);
    }

    private void removeWeight(Class<?> interClass) {
        weightCache.remove(interClass);
    }

    private Map<String, Integer> calculate(Map<String, InstanceStats> statsMap) {
        Map<String, Integer> weightMap = new HashMap<>();
        for (Map.Entry<String, InstanceStats> statsEntry : statsMap.entrySet()) {
            String address = statsEntry.getKey();
            InstanceStats stats = statsEntry.getValue();
            // todo: 调整加权算法
            int serverWeight = 100;
            ServerStats serverStats = stats.getServerStats();
            RuntimeInfo info = serverStats.getRuntimeInfo();
            if (nonNull(info)) {
                double processCpuLoad = info.getProcessCpuLoad();
                serverWeight = (int) (serverWeight / (processCpuLoad + .2d));
            }
            weightMap.putIfAbsent(address, serverWeight);
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
                Map<String, Integer> weightMap = calculate(statsMap);
                updateWeight(interClass, calculate(statsMap));
                logger.error(interClass.getName() + " update weight " + weightMap);
            }
        }
    }

}
