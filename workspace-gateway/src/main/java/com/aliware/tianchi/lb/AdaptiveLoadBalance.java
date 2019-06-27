package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.common.util.SmallPriorityQueue;
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

        if (isNear(a1, a2, 3)) {
            return (o2.getDomainThreads() - o2.getActiveCount()) -
                   (o1.getDomainThreads() - o1.getActiveCount());
        }

        return (int) (a1 - a2);
    };

    private static boolean isNear(long left, long right, int n) {
        return left <= right + n && right <= left + n;
    }

    private static final ThreadLocal<Queue<SnapshotStats>>
            LOCAL_Q = ThreadLocal.withInitial(() -> new SmallPriorityQueue<>(8, CMP));

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        Map<SnapshotStats, Invoker<T>> mapping = new HashMap<>();

        Queue<SnapshotStats> queue = LOCAL_Q.get();
        LBStatistics lbStatistics = LBStatistics.INSTANCE;

        String serviceId = invokers.get(0).getInterface().getName() + '#' +
                           invocation.getMethodName() +
                           Arrays.toString(invocation.getParameterTypes());

        double maxIdleCpus = Long.MIN_VALUE;
        Invoker<T> mostIdleIvk = null;
        for (Invoker<T> invoker : invokers) {
            String address = invoker.getUrl().getAddress();
            SnapshotStats stats = lbStatistics.getInstanceStats(serviceId, address);
            RuntimeInfo runtimeInfo;
            if (isNull(stats) ||
                isNull(runtimeInfo = stats.getServerStats().getRuntimeInfo())) {
                queue.clear();
                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            }

            long waits = lbStatistics.getWaits(address);

            double idleCpus = (1 - runtimeInfo.getProcessCpuLoad()) *
                              runtimeInfo.getAvailableProcessors();
            if (idleCpus > maxIdleCpus) {
                maxIdleCpus = idleCpus;
                mostIdleIvk = invoker;
            }

            int threads = stats.getDomainThreads();

            // todo: config
            if (waits > threads * .8) {
                continue;
            }

            mapping.put(stats, invoker);
            queue.offer(stats);
        }

        if (queue.isEmpty()) {
            assert mostIdleIvk != null;
            logger.info("queue is empty, mostIdleIvk" + mostIdleIvk.getUrl().getAddress());
        }

        for (int mask = 1; ; ) {
            SnapshotStats stats = queue.poll();
            if (stats == null) {
                break;
            }

            RuntimeInfo runtimeInfo = stats.getServerStats().getRuntimeInfo();
            // todo: config
            if (runtimeInfo.getProcessCpuLoad() > .8 ||
                (ThreadLocalRandom.current().nextInt() & mask) == 0) {
                mask = (mask << 1) | mask;
                continue;
            }
            queue.clear();
            return mapping.get(stats);
        }

        queue.clear();
        return mostIdleIvk;
    }
}
