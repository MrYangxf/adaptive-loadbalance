package com.aliware.tianchi.util;

import com.aliware.tianchi.StatsThreadPoolExecutor;
import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.metric.TimeWindowInstanceStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.common.util.RuntimeInfo;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.rpc.Invoker;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

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

    private volatile ThreadPoolExecutor domainExecutor;

    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    public NearRuntimeHelper(Configuration conf) {
        checkNotNull(conf);
        this.conf = conf;
        scheduledExecutor.scheduleWithFixedDelay(this::updateRuntimeInfo,
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
                    int nThreads = invoker.getUrl().getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
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

    public ThreadPoolExecutor getDomainExecutor() {
        if (domainExecutor == null) {
            synchronized (this) {
                if (domainExecutor == null) {
                    DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
                    Map<String, Object> executors = dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY);
                    for (Map.Entry<String, Object> entry : executors.entrySet()) {
                        Object value = entry.getValue();
                        if (value instanceof StatsThreadPoolExecutor) {
                            domainExecutor = (ThreadPoolExecutor) value;
                            return domainExecutor;
                        }
                    }
                    throw new RuntimeException("StatsThreadPoolExecutor not found");
                }
            }
        }
        return domainExecutor;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
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

    private final Set<String> serviceIds = new ConcurrentHashSet<>();
    
    public void putServiceId(String serviceId) {
        serviceIds.add(serviceId);
    }
    
    public Set<String> getServiceIds() {
        return serviceIds;
    }

    private volatile ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();

    public void incr(int n) {
        queue.offer(n);
    }

    public synchronized ConcurrentLinkedQueue<Integer> getAndClear() {
        ConcurrentLinkedQueue<Integer> q = queue;
        queue = new ConcurrentLinkedQueue<>();
        return q;
    }
}
