package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.conf.Configuration;
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

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.isNull;

/**
 * @author yangxf
 */
public class AdaptiveLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveLoadBalance.class);

    private static final int HEAP_THRESHOLD = 8;

    private Configuration conf;

    private final ThreadLocal<Queue<SnapshotStats>> localSmallQ;

    private final ThreadLocal<Queue<SnapshotStats>> localHeapQ;

    public AdaptiveLoadBalance(Configuration conf) {
        checkNotNull(conf, "conf");
        this.conf = conf;
        Comparator<SnapshotStats> comparator = conf.getStatsComparator();
        localSmallQ = ThreadLocal.withInitial(() -> new SmallPriorityQueue<>(HEAP_THRESHOLD, comparator));
        localHeapQ = ThreadLocal.withInitial(() -> new PriorityQueue<>(comparator));
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        LBStatistics lbStatistics = LBStatistics.INSTANCE;
        Map<SnapshotStats, Invoker<T>> mapping = new HashMap<>();

        int size = invokers.size();
        Queue<SnapshotStats> queue = size > HEAP_THRESHOLD ? localHeapQ.get() : localSmallQ.get();

        String serviceId = invokers.get(0).getInterface().getName() + '#' +
                           invocation.getMethodName() +
                           Arrays.toString(invocation.getParameterTypes());

        // double maxIdleCpus = Long.MIN_VALUE;
        long maxIdleThreads = Long.MIN_VALUE;
        Invoker<T> mostIdleIvk = null;
        for (Invoker<T> invoker : invokers) {
            String address = invoker.getUrl().getAddress();
            SnapshotStats stats = lbStatistics.getInstanceStats(serviceId, address);
            RuntimeInfo runtimeInfo;
            if (isNull(stats) ||
                isNull(runtimeInfo = stats.getServerStats().getRuntimeInfo())) {
                queue.clear();
                return invokers.get(ThreadLocalRandom.current().nextInt(size));
            }

            // double idleCpus = (1 - runtimeInfo.getProcessCpuLoad()) *
            //                   runtimeInfo.getAvailableProcessors();
            // if (idleCpus > maxIdleCpus) {
            //     maxIdleCpus = idleCpus;
            //     mostIdleIvk = invoker;
            // }

            long waits = lbStatistics.getWaits(address);
            int threads = stats.getDomainThreads();

            long idleThreads = threads - waits;
            if (idleThreads > maxIdleThreads) {
                maxIdleThreads = idleThreads;
                mostIdleIvk = invoker;
            }
            
            if (waits > threads * conf.getMaxRateOfWaitingRequests() ||
                runtimeInfo.getProcessCpuLoad() > conf.getMaxProcessCpuLoad()) {
                continue;
            }

            mapping.put(stats, invoker);
            queue.offer(stats);
        }

        if (queue.isEmpty()) {
            assert mostIdleIvk != null;
            String address = mostIdleIvk.getUrl().getAddress();
            SnapshotStats stats = lbStatistics.getInstanceStats(serviceId, address);
            logger.info("queue is empty, mostIdleIvk " + address +
                        ", waits=" + lbStatistics.getWaits(address) +
                        ", active=" + stats.getActiveCount() +
                        ", threads=" + stats.getDomainThreads() +
                        ", avg=" + stats.getAvgResponseMs() +
                        ", suc=" + stats.getNumberOfSuccesses() +
                        ", fai=" + stats.getNumberOfFailures() +
                        ", tpt=" + stats.getThroughput() +
                        ", load=" + stats.getServerStats().getRuntimeInfo().getProcessCpuLoad()
                       );
            // throw new RpcException(RpcException.BIZ_EXCEPTION, "all providers are overloaded");
        }

        for (int mask = 1; ; ) {
            SnapshotStats stats = queue.poll();
            if (stats == null) {
                break;
            }

            if ((ThreadLocalRandom.current().nextInt() & mask) == 0) {
                mask = (mask << 1) | mask;
                continue;
            }
            queue.clear();
            return mapping.get(stats);
        }

        return mostIdleIvk;
    }
}
