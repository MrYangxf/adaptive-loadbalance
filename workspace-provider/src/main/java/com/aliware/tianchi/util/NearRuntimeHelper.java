package com.aliware.tianchi.util;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.CountWindowInstanceStats;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class NearRuntimeHelper {

    private static final Logger logger = LoggerFactory.getLogger(NearRuntimeHelper.class);

    public static final NearRuntimeHelper INSTANCE = new NearRuntimeHelper(new Configuration());

    private Configuration conf;

    private final LinkedList<RuntimeInfo> buf = new LinkedList<>();

    private volatile InstanceStats stats;

    private volatile RuntimeInfo current;

    private final ScheduledExecutorService scheduledExecutor;

    public NearRuntimeHelper(Configuration conf) {
        checkNotNull(conf);
        this.conf = conf;
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public void updateRuntimeInfo() {
        synchronized (buf) {
            buf.addFirst(new RuntimeInfo());
            RuntimeInfo info = RuntimeInfo.merge(buf.toArray(new RuntimeInfo[0]));
            if (nonNull(stats)) {
                if (conf.isOpenRuntimeStats()) {
                    stats.getServerStats().setRuntimeInfo(info);
                }
                current = info;
                logger.info("update " + info);
            }
            if (buf.size() >= conf.getRuntimeInfoQueueSize()) {
                buf.pollLast();
            }
        }
    }

    public RuntimeInfo getRuntimeInfo() {
        return current;
    }

    public InstanceStats getInstanceStats() {
        return stats;
    }

    public InstanceStats getOrCreateInstanceStats(Invoker<?> invoker) {
        if (stats == null) {
            synchronized (this) {
                if (stats == null) {
                    String nThreadsString = invoker.getUrl().getParameter(Constants.THREADS_KEY);
                    int nThreads = Integer.parseInt(nThreadsString);
                    InstanceStats newStats = newStats(DubboUtil.getIpAddress(invoker), nThreads);
                    newStats.setDomainThreads(nThreads);
                    stats = newStats;
                }
            }
        }
        return stats;
    }

    public void cleanStats() {
        if (nonNull(stats)) {
            stats.clean();
        }
    }

    public Configuration getConfiguration() {
        return conf;
    }

    private InstanceStats newStats(String address, int nThreads) {
        // TimeWindowInstanceStats stats =
        //         new TimeWindowInstanceStats(conf,
        //                                     address,
        //                                     new ServerStats(address),
        //                                     conf.getWindowSizeOfStats(),
        //                                     conf.getTimeIntervalOfStats(),
        //                                     conf.getTimeUnitOfStats(),
        //                                     conf.getCounterFactory());
        // stats.setDomainThreads(nThreads);
        // return stats;
        return new CountWindowInstanceStats(address, new ServerStats(address), nThreads * 2);
    }

    private volatile ConcurrentLinkedQueue<Integer> activeQueue = new ConcurrentLinkedQueue<>();

    public void putActiveCount(int activeCount) {
        activeQueue.offer(activeCount);
    }

    public synchronized int getActiveCount() {
        ConcurrentLinkedQueue<Integer> queue = activeQueue;
        activeQueue = new ConcurrentLinkedQueue<>();

        int sum = 0, size = 0, min = Integer.MAX_VALUE, max = 0;
        for (int ac : queue) {
            if (ac == 0) {
                continue;
            }
            sum += ac;
            size++;
            if (ac > max) max = ac;
            if (ac < min) min = ac;
        }

        if (size > 10) {
            sum -= max;
            sum -= min;
            size -= 2;
        }

        if (size == 0) {
            return 0;
        }

        return sum / size;
    }
}
