package com.aliware.tianchi.lb.rule;

import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.ServerStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class WeightedLoadRule extends SelfAdaptiveRule {

    private static final Logger logger = LoggerFactory.getLogger(WeightedLoadRule.class);

    private static final int DEFAULT_WEIGHT = 100;
    private static final double DEFAULT_IDLE_LOAD = .2d;

    Map<String, Integer> calculate(Map<String, InstanceStats> statsMap, Map<String, Integer> prevWeightMap) {
        Map<String, Integer> weightMap = new HashMap<>();
        for (Map.Entry<String, InstanceStats> statsEntry : statsMap.entrySet()) {
            String address = statsEntry.getKey();
            InstanceStats stats = statsEntry.getValue();
            int serverWeight = DEFAULT_WEIGHT;
            ServerStats serverStats = stats.getServerStats();
            RuntimeInfo info = serverStats.getRuntimeInfo();
            logger.info(stats.toString() + ", runtime info: " + info);
            if (nonNull(info)) {
                double processCpuLoad = info.getProcessCpuLoad();
                serverWeight = (int) (serverWeight / (processCpuLoad + DEFAULT_IDLE_LOAD));
            }
            weightMap.putIfAbsent(address, serverWeight);
        }
        return weightMap;
    }

}
