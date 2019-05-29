package com.aliware.tianchi.lb.rule;

import com.aliware.tianchi.lb.LBRule;
import com.aliware.tianchi.lb.metric.InstanceStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.rpc.Invoker;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yangxf
 */
public class ResponseTimeWeightedRule implements LBRule {


    private LBStatistics lbStats = LBStatistics.STATS;

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> candidates) {

        int size = candidates.size();
        long sum = 0;
        long[] timeSegments = new long[size];
        for (int i = 0; i < size; i++) {
            Invoker<T> candidate = candidates.get(i);
            InstanceStats stats = lbStats.getInstanceStats(candidate);
            long avgResponseMs = stats.getAvgResponseMs();
            sum += avgResponseMs;
            timeSegments[i] = sum;
        }

        if (sum < size) {
            return candidates.get(ThreadLocalRandom.current().nextInt(size));
        }
        
        int select = 0;
        long ra = ThreadLocalRandom.current().nextLong(sum);
        for (int i = 0; i < size; i++) {
            if (ra < timeSegments[i]) {
                select = i;
                break;
            }
        }

        return candidates.get(select);
    }
    
}
