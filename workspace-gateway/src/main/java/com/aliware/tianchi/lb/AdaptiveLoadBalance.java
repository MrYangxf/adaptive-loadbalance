package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.aliware.tianchi.common.util.ObjectUtil.isNull;

/**
 * @author yangxf
 */
public class AdaptiveLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveLoadBalance.class);

    private static Comparator<SnapshotStats> CMP = (o1, o2) -> {
        long a1 = o1.getAvgResponseMs(),
                a2 = o2.getAvgResponseMs();
        if (a1 != a2) {
            return (int) (a1 - a2);
        }

        return (int) (o1.getServerStats().getRuntimeInfo().getProcessCpuLoad() -
                      o2.getServerStats().getRuntimeInfo().getProcessCpuLoad());
    };

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        Map<SnapshotStats, Invoker<T>> mapping = new HashMap<>();
        PriorityQueue<SnapshotStats> queue = new PriorityQueue<>(CMP);
        for (Invoker<T> invoker : invokers) {
            SnapshotStats stats = LBStatistics.getInstanceStats(invoker, invocation);
            if (isNull(stats)) {
                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            }
            mapping.put(stats, invoker);
            queue.add(stats);
        }

        for (; ; ) {
            SnapshotStats stats = queue.poll();
            if (stats == null) {
                break;
            }

            RuntimeInfo runtimeInfo = stats.getServerStats().getRuntimeInfo();
            if (runtimeInfo.getProcessCpuLoad() > 0.8) {
                continue;
            }

            Invoker<T> tInvoker = mapping.get(stats);
            logger.info(tInvoker.getUrl().getAddress() +
                    ", avg=" + stats.getAvgResponseMs() +
                        ", succ=" + stats.getNumberOfSuccesses() +
                        ", fai=" + stats.getNumberOfFailures() +
                        ", tpt=" + stats.getThroughput() +
                        ", run=" + stats.getServerStats().getRuntimeInfo()
                       );
            return tInvoker;
        }

        return null;
    }
}
