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

    public NearRuntimeHelper(Configuration conf) {
        checkNotNull(conf);
        this.conf = conf;
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

    private InstanceStats newStats(String address) {
        return new TimeWindowInstanceStats(address,
                                           new ServerStats(address),
                                           conf.getWindowSizeOfStats(),
                                           conf.getTimeIntervalOfStats(),
                                           conf.getTimeUnitOfStats(),
                                           conf.getCounterFactory());
    }
}
