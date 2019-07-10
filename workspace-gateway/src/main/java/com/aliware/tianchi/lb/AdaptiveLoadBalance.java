package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.DubboUtil;
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
import java.util.concurrent.TimeUnit;

import static com.aliware.tianchi.common.util.ObjectUtil.*;

/**
 * @author yangxf
 */
public class AdaptiveLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveLoadBalance.class);

    private static final int HEAP_THRESHOLD = 8;

    private final long start = System.nanoTime();

    private final Configuration conf;

    private final Comparator<SnapshotStats> comparator;

    public AdaptiveLoadBalance(Configuration conf) {
        checkNotNull(conf, "conf");
        this.conf = conf;
        comparator = Comparator.comparingDouble(SnapshotStats::getAvgRTMs);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int size = invokers.size();

        LBStatistics lbStatistics = LBStatistics.INSTANCE;
        Map<String, Invoker<T>> mapping = new HashMap<>();

        Queue<SnapshotStats> queue = size > HEAP_THRESHOLD ?
                new PriorityQueue<>(comparator) :
                new SmallPriorityQueue<>(HEAP_THRESHOLD, comparator);

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
            mapping.put(address, invoker);
            queue.offer(stats);
        }

        long maxToken = Long.MIN_VALUE;
        SnapshotStats idleStats = null;
        for (int mask = 0x80000001; ; ) {
            SnapshotStats stats = queue.poll();
            if (isNull(stats)) {
                break;
            }

            if (stats.acquireToken()) {

                if ((ThreadLocalRandom.current().nextInt() & mask) == 0) {
                    mask = (mask << 1) | mask;
                    long tokens = stats.releaseToken();
                    if (tokens > maxToken) {
                        maxToken = tokens;
                        idleStats = stats;
                    }
                    continue;
                }

                String address = stats.getAddress();
                if ((ThreadLocalRandom.current().nextInt() & 511) == 0) {
                    logger.info(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) +
                                ", select " + address +
                                ", epoch=" + stats.getEpoch() +
                                ", tokens=" + stats.tokens() +
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
                return mapping.get(address);
            }
        }

        if (nonNull(idleStats) && idleStats.acquireToken()) {
            invocation.getAttachments().put("CURRENT_STATS_EPOCH", idleStats.getEpoch() + "");
            return mapping.get(idleStats.getAddress());
        }

        logger.info("all providers are overloaded");

        throw new RpcException(RpcException.BIZ_EXCEPTION, "all providers are overloaded");
    }
}
