package com.aliware.tianchi.util;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.metric.TimeWindowInstanceStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;

import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    private volatile RuntimeInfo current;

    private volatile InstanceStats stats;

    public NearRuntimeHelper(Configuration conf) {
        checkNotNull(conf);
        this.conf = conf;
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(this::updateRuntimeInfo,
                                         1000,
                                         1000,
                                         TimeUnit.MILLISECONDS);
    }

    public void updateInstanceRuntimeInfo() {
        if (conf.isOpenRuntimeStats() && nonNull(stats)) {
            stats.getServerStats().setRuntimeInfo(current);
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
                    String ipAddress = DubboUtil.getIpAddress(invoker);
                    InstanceStats newStats = newStats(ipAddress);
                    String nThreadsString = invoker.getUrl().getParameter(Constants.THREADS_KEY);
                    int nThreads = Integer.parseInt(nThreadsString);
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

    private void updateRuntimeInfo() {
        synchronized (buf) {
            buf.addFirst(new RuntimeInfo());
            current = RuntimeInfo.merge(buf.toArray(new RuntimeInfo[0]));
            logger.info("update " + current);
            if (buf.size() >= conf.getRuntimeInfoQueueSize()) {
                buf.pollLast();
            }
        }
    }

    private InstanceStats newStats(String address) {
        return new TimeWindowInstanceStats(address,
                                           new ServerStats(address),
                                           conf.getWindowSizeOfStats(),
                                           conf.getTimeIntervalOfStats(),
                                           conf.getTimeUnitOfStats(),
                                           conf.getCounterFactory());
    }
}
