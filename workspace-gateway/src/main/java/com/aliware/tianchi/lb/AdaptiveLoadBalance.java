package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.DubboUtil;
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
        Comparator<SnapshotStats> comparator =
                (o1, o2) -> {
                    long a1 = o1.getAvgResponseMs(),
                            a2 = o2.getAvgResponseMs();
                    if (a1 == a2) {
                        int w1 = LBStatistics.INSTANCE.getWaits(o1.getAddress());
                        int w2 = LBStatistics.INSTANCE.getWaits(o2.getAddress());
                        int ac1 = o1.getActiveCount();
                        int ac2 = o2.getActiveCount();
                        int n1 = w1 - ac1 >>> 1;
                        int n2 = w2 - ac2 >>> 1;
                        int d1 = o1.getDomainThreads() - ac1 - (n1 > 0 ? n1 : 0);
                        int d2 = o2.getDomainThreads() - ac2 - (n2 > 0 ? n2 : 0);
                        return d2 - d1;
                    }

                    return (int) (a1 - a2);
                };
        // Comparator<SnapshotStats> comparator = conf.getStatsComparator();
        localSmallQ = ThreadLocal.withInitial(() -> new SmallPriorityQueue<>(HEAP_THRESHOLD, comparator));
        localHeapQ = ThreadLocal.withInitial(() -> new PriorityQueue<>(comparator));
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int size = invokers.size();

        LBStatistics lbStatistics = LBStatistics.INSTANCE;
        Map<SnapshotStats, Invoker<T>> mapping = new HashMap<>();

        Queue<SnapshotStats> queue = size > HEAP_THRESHOLD ? localHeapQ.get() : localSmallQ.get();

        String serviceId = DubboUtil.getServiceId(invokers.get(0), invocation);
        long maxIdleThreads = Long.MIN_VALUE;
        Invoker<T> mostIdleIvk = null;
        for (Invoker<T> invoker : invokers) {
            String address = DubboUtil.getIpAddress(invoker);
            SnapshotStats stats = lbStatistics.getInstanceStats(serviceId, address);
            RuntimeInfo runtimeInfo = null;
            if (isNull(stats) ||
                conf.isOpenRuntimeStats() &&
                isNull(runtimeInfo = stats.getServerStats().getRuntimeInfo())) {
                queue.clear();
                return invokers.get(ThreadLocalRandom.current().nextInt(size));
            }

            long waits = lbStatistics.getWaits(address);
            int threads = stats.getDomainThreads();

            long idleThreads = threads - waits;
            if (idleThreads > maxIdleThreads) {
                maxIdleThreads = idleThreads;
                mostIdleIvk = invoker;
            }

            if (waits > threads * conf.getMaxRateOfWaitingRequests() ||
                conf.isOpenRuntimeStats() &&
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
                        (conf.isOpenRuntimeStats() ?
                                ", load=" + stats.getServerStats().getRuntimeInfo().getProcessCpuLoad() : "")
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

            if ((ThreadLocalRandom.current().nextInt() & 511) == 0)
            logger.info(" select " + stats.getAddress() +
                        ", waits=" + lbStatistics.getWaits(stats.getAddress()) +
                        ", active=" + stats.getActiveCount() +
                        ", threads=" + stats.getDomainThreads() +
                        ", avg=" + stats.getAvgResponseMs() +
                        ", suc=" + stats.getNumberOfSuccesses() +
                        ", fai=" + stats.getNumberOfFailures() +
                        ", tpt=" + stats.getThroughput() +
                        (conf.isOpenRuntimeStats() ?
                                ", load=" + stats.getServerStats().getRuntimeInfo().getProcessCpuLoad() : "")
                       );
            
            queue.clear();
            return mapping.get(stats);
        }

        return mostIdleIvk;
    }
}
