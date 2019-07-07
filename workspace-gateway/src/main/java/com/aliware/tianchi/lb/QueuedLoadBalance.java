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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.isNull;

/**
 * @author yangxf
 */
public class QueuedLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(QueuedLoadBalance.class);

    private static final int HEAP_THRESHOLD = 8;

    private Configuration conf;

    private final long windowMillis;

    private final Comparator<SnapshotStats> comparator;

    private final Lock lock = new ReentrantLock();

    private final Condition cond = lock.newCondition();

    private final Map<String, Integer> waitMap = new ConcurrentHashMap<>();

    public QueuedLoadBalance(Configuration conf) {
        checkNotNull(conf, "conf");
        this.conf = conf;
        comparator =
                (o1, o2) -> {
                    double a1 = o1.getAvgResponseMs(),
                            a2 = o2.getAvgResponseMs();

                    return Double.compare(a1, a2);
                };
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

        Queue<SnapshotStats> queue = newQueue(size);
        Queue<SnapshotStats> tempQueue = newQueue(size);

        String serviceId = DubboUtil.getServiceId(invokers.get(0), invocation);
        for (Invoker<T> invoker : invokers) {
            String address = DubboUtil.getIpAddress(invoker);
            SnapshotStats stats = lbStatistics.getInstanceStats(serviceId, address);
            if (isNull(stats) ||
                conf.isOpenRuntimeStats() &&
                isNull(stats.getServerStats().getRuntimeInfo())) {
                return invokers.get(ThreadLocalRandom.current().nextInt(size));
            }

            mapping.put(stats.getAddress(), invoker);
            queue.offer(stats);
        }

        lock.lock();
        try {
            while (true) {
                for (; ; ) {
                    SnapshotStats stats = queue.poll();
                    if (stats == null) {
                        break;
                    }

                    String address = stats.getAddress();
                    int waits = waitMap.getOrDefault(address, 0);
                    int activeCount = stats.getActiveCount();
                    long netWaits = (waits - activeCount) / 2;
                    netWaits = netWaits < 0 ? 0 : netWaits;

                    double mc = stats.getAvgResponseMs() * stats.getNumberOfSuccesses() / windowMillis;
                    int threads = stats.getDomainThreads();
                    if (waits > threads * .5 &&
                        waits > mc + netWaits) {
                        tempQueue.offer(stats);
                        continue;
                    }

                    if ((ThreadLocalRandom.current().nextInt() & 511) == 0)
                        logger.info(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()) + " select " + address +
                                    ", waits=" + waits +
                                    ", active=" + stats.getActiveCount() +
                                    ", threads=" + stats.getDomainThreads() +
                                    ", avg=" + stats.getAvgResponseMs() +
                                    ", suc=" + stats.getNumberOfSuccesses() +
                                    ", fai=" + stats.getNumberOfFailures() +
                                    ", tpt=" + stats.getThroughput() +
                                    (conf.isOpenRuntimeStats() ?
                                            ", load=" + stats.getServerStats()
                                                             .getRuntimeInfo()
                                                             .getProcessCpuLoad() : "")
                                   );
                    return mapping.get(address);
                }

                if (!cond.await(3000, TimeUnit.MILLISECONDS)) {
                    break;
                }
                queue = tempQueue;
                tempQueue = newQueue(size);
            }

            // throw new RpcException(RpcException.BIZ_EXCEPTION, "all providers are overloaded");
        } catch (InterruptedException e) {
            // 
        } finally {
            lock.unlock();
        }

        throw new RpcException(RpcException.BIZ_EXCEPTION, "all providers are overloaded");
    }

    public void queue(String address) {
        lock.lock();
        try {
            int waits = waitMap.getOrDefault(address, 0);
            waitMap.put(address, ++waits);
        } finally {
            lock.unlock();
        }
    }

    public void dequeue(String address) {
        lock.lock();
        try {
            int waits = waitMap.getOrDefault(address, 0);
            waitMap.put(address, --waits);
            cond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private Queue<SnapshotStats> newQueue(int size) {
        return size > HEAP_THRESHOLD ?
                new PriorityQueue<>(comparator) :
                new SmallPriorityQueue<>(HEAP_THRESHOLD, comparator);
    }
}
