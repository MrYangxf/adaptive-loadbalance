package com.aliware.tianchi.util;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.metric.TimeWindowInstanceStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author yangxf
 */
public class NearRuntimeHelper {

    private static final Logger logger = LoggerFactory.getLogger(NearRuntimeHelper.class);

    public static final NearRuntimeHelper INSTANCE = new NearRuntimeHelper();

    // todo: config
    private int bufSize = 5;

    private final LinkedList<RuntimeInfo> buf = new LinkedList<>();

    private volatile InstanceStats stats;

    private static final AtomicReferenceFieldUpdater<NearRuntimeHelper, InstanceStats> STATS_U =
            AtomicReferenceFieldUpdater.newUpdater(NearRuntimeHelper.class, InstanceStats.class, "stats");

    private NearRuntimeHelper() {
    }

    public void updateRuntimeInfo() {
        synchronized (buf) {
            buf.addFirst(new RuntimeInfo());
            RuntimeInfo info = RuntimeInfo.merge(buf.toArray(new RuntimeInfo[0]));
            stats.getServerStats().setRuntimeInfo(info);
            logger.info("update " + info);
            if (buf.size() >= bufSize) {
                buf.pollLast();
            }
        }
    }

    public RuntimeInfo getRuntimeInfo() {
        return stats.getServerStats().getRuntimeInfo();
    }

    public InstanceStats getInstanceStats() {
        return stats;
    }

    public InstanceStats getOrCreateInstanceStats(Invoker<?> invoker) {
        InstanceStats newStats = newStats(invoker.getUrl().getAddress());
        if (STATS_U.compareAndSet(this, null, newStats)) {
            String nThreadsString = invoker.getUrl().getParameter(Constants.THREADS_KEY);
            int nThreads = Integer.parseInt(nThreadsString);
            newStats.setDomainThreads(nThreads);
            return newStats;
        }
        return STATS_U.get(this);
    }

    private InstanceStats newStats(String address) {
        // todo: config
        return new TimeWindowInstanceStats(address,
                                           new ServerStats(address),
                                           10, 100, TimeUnit.MILLISECONDS,
                                           null);
    }
}
