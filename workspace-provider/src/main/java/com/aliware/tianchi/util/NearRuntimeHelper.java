package com.aliware.tianchi.util;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.metric.TimeWindowInstanceStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.rpc.Invoker;

import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author yangxf
 */
public class NearRuntimeHelper {

    private static final Logger logger = LoggerFactory.getLogger(NearRuntimeHelper.class);

    public static final NearRuntimeHelper INSTANCE = new NearRuntimeHelper(new Configuration());

    private Configuration conf;

    private final ScheduledExecutorService scheduledExecutor;

    private final LinkedList<RuntimeInfo> buf = new LinkedList<>();

    private volatile InstanceStats stats;

    private volatile RuntimeInfo current;

    private AtomicLong epoch;

    private int threads = 200;

    private long startNanos;

    private Executor executor;

    public NearRuntimeHelper(Configuration conf) {
        checkNotNull(conf);
        this.conf = conf;
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public long getAndIncrementEpoch() {
        if (epoch == null) {
            return 0;
        }
        return epoch.getAndIncrement();
    }

    public void updateRuntimeInfo() {
        if (conf.isOpenRuntimeStats()) {
            synchronized (buf) {
                buf.addFirst(new RuntimeInfo());
                RuntimeInfo info = RuntimeInfo.merge(buf.toArray(new RuntimeInfo[0]));
                if (nonNull(stats)) {
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

    public int getThreads() {
        return threads;
    }

    public Executor getExecutor() {
        return executor;
    }

    public ThreadPoolStats getThreadPoolStats() {
        return ThreadPoolUtil.getThreadPoolStats(executor);
    }

    public long getStartNanos() {
        return startNanos;
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
                    URL url = invoker.getUrl();
                    int nThreads = url.getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);

                    stats = newStats(DubboUtil.getIpAddress(invoker), threads = nThreads);

                    DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();

                    executor = (Executor) dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY, Integer.toString(url.getPort()));

                    epoch = new AtomicLong(1);

                    startNanos = System.nanoTime();

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
        TimeWindowInstanceStats stats =
                new TimeWindowInstanceStats(conf,
                                            address,
                                            new ServerStats(address),
                                            conf.getWindowSizeOfStats(),
                                            conf.getTimeIntervalOfStats(),
                                            conf.getTimeUnitOfStats(),
                                            conf.getCounterFactory());
        stats.setDomainThreads(nThreads);
        return stats;
    }
}
