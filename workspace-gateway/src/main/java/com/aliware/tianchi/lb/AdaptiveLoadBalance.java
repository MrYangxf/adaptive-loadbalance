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
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.isNull;

/**
 * @author yangxf
 */
public class AdaptiveLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveLoadBalance.class);

    private static final int HEAP_THRESHOLD = 8;

    private final long start = System.nanoTime();

    private Configuration conf;

    private final ThreadLocal<Queue<SnapshotStats>> localSmallQ;

    private final ThreadLocal<Queue<SnapshotStats>> localHeapQ;

    private final long windowMillis;

    public AdaptiveLoadBalance(Configuration conf) {
        checkNotNull(conf, "conf");
        this.conf = conf;
        Comparator<SnapshotStats> comparator =
                (o1, o2) -> {
                    double a1 = o1.getAvgRTMs(),
                            a2 = o2.getAvgRTMs();

                    return Double.compare(a1, a2);
                };
        // Comparator<SnapshotStats> comparator = conf.getStatsComparator();
        localSmallQ = ThreadLocal.withInitial(() -> new SmallPriorityQueue<>(HEAP_THRESHOLD, comparator));
        localHeapQ = ThreadLocal.withInitial(() -> new PriorityQueue<>(comparator));

        long size = conf.getWindowSizeOfStats() * conf.getTimeIntervalOfStats();
        windowMillis = TimeUnit.MILLISECONDS.convert(size, conf.getTimeUnitOfStats());
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int size = invokers.size();

        if (ThreadLocalRandom.current().nextInt() % (size + 1) == 0) {
            return invokers.get(ThreadLocalRandom.current().nextInt(size));
        }

        LBStatistics lbStatistics = LBStatistics.INSTANCE;
        Map<String, Invoker<T>> mapping = new HashMap<>();

        Queue<SnapshotStats> queue = size > HEAP_THRESHOLD ? localHeapQ.get() : localSmallQ.get();

        String serviceId = DubboUtil.getServiceId(invokers.get(0), invocation);

        Map<String, SnapshotStats> instanceStatsMap = lbStatistics.getInstanceStatsMap(serviceId);
        if (isNull(instanceStatsMap)) {
            return invokers.get(ThreadLocalRandom.current().nextInt(size));
        }

        for (Invoker<T> invoker : invokers) {
            String address = DubboUtil.getIpAddress(invoker);
            SnapshotStats stats = instanceStatsMap.get(address);
            if (isNull(stats)) {
                return invokers.get(ThreadLocalRandom.current().nextInt(size));
            }

            
            mapping.put(stats.getAddress(), invoker);
            queue.offer(stats);
        }

        for (int mask = 1; ; ) {
            SnapshotStats stats = queue.poll();
            if (stats == null) {
                break;
            }


            if (stats.getToken()) {
                String address = stats.getAddress();
                if ((ThreadLocalRandom.current().nextInt() & 511) == 0) {
                    logger.info(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " select " + address +
                                ", epoch=" + stats.getEpoch() +
                                ", tokens=" + stats.tokens() +
                                // ", waits=" + lbStatistics.getWaits(address) +
                                ", active=" + stats.getActiveCount() +
                                ", threads=" + stats.getDomainThreads() +
                                ", avg=" + stats.getAvgRTMs() +
                                ", suc=" + stats.getNumberOfSuccesses() +
                                ", fai=" + stats.getNumberOfFailures() +
                                ", tpt=" + stats.getThroughput() +
                                (conf.isOpenRuntimeStats() ?
                                        ", load=" + stats.getServerStats().getRuntimeInfo().getProcessCpuLoad() : "")
                               );
                }
                invocation.getAttachments().put("CURRENT_STATS_EPOCH", stats.getEpoch() + "");
                queue.clear();
                return mapping.get(address);
            }
        }

        logger.info("all providers are overloaded");

        throw new RpcException(RpcException.BIZ_EXCEPTION, "all providers are overloaded");
    }
}
