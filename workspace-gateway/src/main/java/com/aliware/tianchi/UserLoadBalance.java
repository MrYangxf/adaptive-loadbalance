package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.metric.StatsTokenBucket;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.common.util.SmallPriorityQueue;
import com.aliware.tianchi.util.LBHelper;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.aliware.tianchi.common.util.ObjectUtil.isNull;
import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author daofeng.xjf
 * <p>
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final int HEAP_THRESHOLD = 8;

    private final long start = System.nanoTime();

    private final Configuration conf;

    private final Comparator<StatsTokenBucket> comparator;
    private final Comparator<StatsTokenBucket> idleComparator;

    private final ThreadLocal<Queue<StatsTokenBucket>> localQueue;
    private final ThreadLocal<Queue<StatsTokenBucket>> localIdleQueue;

    public UserLoadBalance() {
        conf = LBHelper.CUSTOM.getConfiguration();
        comparator = Comparator.comparingDouble(b -> b.getStats().getAvgRTMs());
        idleComparator = Comparator.comparingLong(StatsTokenBucket::remainTokens);
        localQueue = ThreadLocal.withInitial(() -> new SmallPriorityQueue<>(8, comparator));
        localIdleQueue = ThreadLocal.withInitial(() -> new SmallPriorityQueue<>(8, idleComparator));
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int size = invokers.size();

        if (size == 0) {
            return null;
        }

        if (size == 1) {
            return invokers.get(0);
        }

        LBHelper helper = LBHelper.CUSTOM;
        Map<String, Invoker<T>> mapping = new HashMap<>();

        // Queue<SnapshotStats> queue = size > HEAP_THRESHOLD ?
        //         new PriorityQueue<>(comparator) :
        //         new SmallPriorityQueue<>(HEAP_THRESHOLD, comparator);

        Queue<StatsTokenBucket> queue = localQueue.get();

        String serviceId = DubboUtil.getServiceId(invokers.get(0), invocation);

        Map<String, StatsTokenBucket> statsBucketGroup = helper.getStatsBucketGroup(serviceId);

        List<StatsTokenBucket> statsList = new ArrayList<>(size);
        for (Invoker<T> invoker : invokers) {
            String address = DubboUtil.getIpAddress(invoker);
            StatsTokenBucket bucket = statsBucketGroup.get(address);
            if (isNull(bucket.getStats())) {
                queue.clear();
                return invokers.get(ThreadLocalRandom.current().nextInt(size));
            }
            mapping.put(address, invoker);
            // todo:
            queue.offer(bucket);
            statsList.add(bucket);
        }

        Queue<StatsTokenBucket> idleQueue = null;
        for (int mask = 0x00000001; ; ) {
            StatsTokenBucket bucket = queue.poll();
            if (isNull(bucket)) {
                break;
            }

            if (bucket.acquireToken()) {

                if ((ThreadLocalRandom.current().nextInt() & mask) == 0) {
                    bucket.releaseToken();

                    if (isNull(idleQueue)) {
                        // idleQueue = size > HEAP_THRESHOLD ?
                        //         new PriorityQueue<>(idleComparator) :
                        //         new SmallPriorityQueue<>(HEAP_THRESHOLD, idleComparator);
                        idleQueue = localIdleQueue.get();
                    }

                    idleQueue.offer(bucket);
                    mask = (mask << 1) | mask;
                    continue;
                }

                String address = bucket.getStats().getAddress();
                helper.ensureTokenReleased(bucket, invocation);
                queue.clear();
                return mapping.get(address);
            }
        }

        while (nonNull(idleQueue)) {
            StatsTokenBucket bucket = idleQueue.poll();
            if (isNull(bucket)) {
                break;
            }
            if (bucket.acquireToken()) {
                helper.ensureTokenReleased(bucket, invocation);
                idleQueue.clear();
                return mapping.get(bucket.getStats().getAddress());
            }
        }

        // weighted random ?

        double total = 0d;
        double[] weights = new double[size];
        for (int i = 0; i < size; i++) {
            SnapshotStats stats = statsList.get(i).getStats();
            double weight = stats.getDomainThreads() - stats.getWeight();
            total += weight;
            weights[i] = total;
        }

        if (total == 0) {
            return invokers.get(ThreadLocalRandom.current().nextInt(size));
        }

        double r = ThreadLocalRandom.current().nextDouble(total);
        for (int i = 0; i < size; i++) {
            if (r < weights[i]) {
                return invokers.get(i);
            }
        }
        return invokers.get(ThreadLocalRandom.current().nextInt(size));
    }
}
