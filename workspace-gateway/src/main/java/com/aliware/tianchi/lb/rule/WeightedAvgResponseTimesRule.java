package com.aliware.tianchi.lb.rule;

import com.aliware.tianchi.lb.metric.InstanceStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yangxf
 */
public class WeightedAvgResponseTimesRule extends SelfAdaptiveRule {

    private static final Logger logger = LoggerFactory.getLogger(WeightedAvgResponseTimesRule.class);

    Map<String, Integer> calculate(Map<String, InstanceStats> statsMap, Map<String, Integer> prevWeightMap) {
        Map<String, Integer> weightMap = new HashMap<>();

        double avgTotal = 0, failTotal = 0;
        Map<String, Long> avgMap = new HashMap<>();
        Map<String, Long> failMap = new HashMap<>();

        for (Map.Entry<String, InstanceStats> statsEntry : statsMap.entrySet()) {
            String address = statsEntry.getKey();
            InstanceStats stats = statsEntry.getValue();
            logger.info(stats.toString());

            long rejections = stats.getNumberOfRejections();
            long notSuccesses = stats.getNumberOfFailures() + rejections;

            failTotal += notSuccesses;
            failMap.put(address, notSuccesses);

            long avgResponseMs = stats.getAvgResponseMs();
            avgTotal += avgResponseMs;
            avgMap.put(address, avgResponseMs);
        }

        for (String key : statsMap.keySet()) {
            Long avg = avgMap.get(key);
            double w1 = (avgTotal - avg) / (avgTotal + 1);
            Long failRate = failMap.get(key);
            double w2 = (failTotal - failRate) / (failTotal + 1);

            int w = (int) ((w1 * .5d + w2 * .5d) * 100) + 1;
            weightMap.put(key, w);
        }

        return weightMap;
    }

}
