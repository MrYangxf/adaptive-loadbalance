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
import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class AdaptiveLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveLoadBalance.class);

    private static Comparator<SnapshotStats> CMP = (o1, o2) -> {
        long a1 = o1.getAvgResponseMs(),
                a2 = o2.getAvgResponseMs();

        if (a1 == a2) {
            RuntimeInfo r1 = o1.getServerStats().getRuntimeInfo(),
                    r2 = o2.getServerStats().getRuntimeInfo();
            if (nonNull(r1) && nonNull(r2)) {
                double d = (1 - r1.getProcessCpuLoad()) * r1.getAvailableProcessors() -
                           (1 - r2.getProcessCpuLoad()) * r2.getAvailableProcessors();
                return d > 0 ? -1 : d < 0 ? 1 : 0;
            }
        }

        return (int) (a1 - a2);
    };

    private static final ThreadLocal<PriorityQueue<SnapshotStats>>
            LOCAL_Q = ThreadLocal.withInitial(() -> new PriorityQueue<>(CMP));

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        Map<SnapshotStats, Invoker<T>> mapping = new HashMap<>();

        if (ThreadLocalRandom.current().nextInt() % invokers.size() == 0) {
            return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        }

        PriorityQueue<SnapshotStats> queue = LOCAL_Q.get();

        // double totalIdleCpus = 0;
        // double minIdleCpus = Long.MAX_VALUE;
        double maxIdleCpus = Long.MIN_VALUE;
        Invoker<T> mostIdleIvk = null;
        for (Invoker<T> invoker : invokers) {
            SnapshotStats stats = LBStatistics.getInstanceStats(invoker, invocation);
            RuntimeInfo runtimeInfo;
            if (isNull(stats) || isNull(runtimeInfo = stats.getServerStats().getRuntimeInfo())) {
                queue.clear();
                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            }

            long waits = LBStatistics.getWaits(invoker.getUrl().getAddress());
            // if ((ThreadLocalRandom.current().nextInt() & 15) == 0) {
            //     logger.info(invoker.getUrl().getAddress() +
            //                 ", waits=" + waits +
            //                 ", avg=" + stats.getAvgResponseMs() +
            //                 ", suc=" + stats.getNumberOfSuccesses() +
            //                 ", fai=" + stats.getNumberOfFailures() +
            //                 ", tpt=" + stats.getThroughput() +
            //                 ", run=" + runtimeInfo
            //                );
            // }

            double idleCpus = (1 - runtimeInfo.getProcessCpuLoad()) *
                              runtimeInfo.getAvailableProcessors();
            if (idleCpus > maxIdleCpus) {
                maxIdleCpus = idleCpus;
                mostIdleIvk = invoker;
            }
            // if (idleCpus < minIdleCpus) {
            //     minIdleCpus = idleCpus;
            // }
            // totalIdleCpus += idleCpus;

            if (waits > runtimeInfo.getThreadCount() * .8) {
                continue;
            }

            mapping.put(stats, invoker);
            queue.offer(stats);
        }

        // double idleRate = minIdleCpus * invokers.size() / (totalIdleCpus + 0.0001);
        // if (ThreadLocalRandom.current().nextDouble(1) <= idleRate) {
        //     queue.clear();
        //     return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        // }
        //
        // if (queue.isEmpty()) {
        //     assert mostIdleIvk != null;
        //     logger.info("queue is empty, mostIdleIvk" + mostIdleIvk.getUrl().getAddress());
        // }

        int mask = 0x80000001, n = 0;
        for (; ; ) {
            SnapshotStats stats = queue.poll();
            if (stats == null) {
                break;
            }

            RuntimeInfo runtimeInfo = stats.getServerStats().getRuntimeInfo();
            if (runtimeInfo.getProcessCpuLoad() > 0.8
                ||
                (ThreadLocalRandom.current().nextInt() & (n = (n << 1) | mask)) == 0
                    ) {
                continue;
            }
            queue.clear();
            return mapping.get(stats);
        }

        queue.clear();
        return mostIdleIvk;
    }
}
