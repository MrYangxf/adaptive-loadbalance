package com.aliware.tianchi.lb.rule;

import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.ServerStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.aliware.tianchi.common.util.ObjectUtil.defaultIfNull;
import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class WeightedLoadAndErrorRateRule extends SelfAdaptiveRule {

    private static final Logger logger = LoggerFactory.getLogger(WeightedLoadAndErrorRateRule.class);

    Map<String, Integer> calculate(Map<String, InstanceStats> statsMap, Map<String, Integer> prevWeightMap) {
        boolean notFirst = nonNull(prevWeightMap);
        Map<String, Integer> weightMap = new HashMap<>();
        Map<String, Long> avgMap = new HashMap<>();
        double total = 0, avgTotal = 0;
        for (Map.Entry<String, InstanceStats> statsEntry : statsMap.entrySet()) {
            String address = statsEntry.getKey();
            InstanceStats stats = statsEntry.getValue();
            logger.info(stats.toString());

            long rejections = stats.getNumberOfRejections();
            long notSuccesses = stats.getNumberOfFailures() + rejections;
            long requests = stats.getNumberOfRequests();
            double faiRate = notSuccesses / (requests + 1.0d);

            int serverWeight = 100;
            if (notFirst) {
                serverWeight = defaultIfNull(prevWeightMap.get(address), 100);
            }

            ServerStats serverStats = stats.getServerStats();
            RuntimeInfo info = serverStats.getRuntimeInfo();
            if (nonNull(info)) {
                double processCpuLoad = info.getProcessCpuLoad();
                double w = 1 / (processCpuLoad + 1) + 1 / (faiRate + 1);
                serverWeight = (int) (serverWeight * w);
            }

            long avgResponseMs = stats.getAvgResponseMs();
            avgTotal += avgResponseMs;
            avgMap.put(address, avgResponseMs);

            total += serverWeight;
            weightMap.putIfAbsent(address, serverWeight < 1 ? 1 : serverWeight);
        }

        for (Map.Entry<String, Integer> entry : weightMap.entrySet()) {
            String key = entry.getKey();
            Long avg = avgMap.get(key);
            double w = entry.getValue() / total;
            double w2 = (avgTotal - avg) / avgTotal;
            weightMap.put(key, (int) (w * w2 * 300) + 1);
        }

        return weightMap;
    }

}
