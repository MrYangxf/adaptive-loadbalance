package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.OSUtil;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    static final long START = System.nanoTime();

    private static final Logger logger = LoggerFactory.getLogger(CallbackServiceImpl.class);

    public CallbackServiceImpl() {
        NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
        Configuration conf = helper.getConfiguration();
        helper.getScheduledExecutor()
              .scheduleWithFixedDelay(() -> _updateAndNotify(true),
                                      conf.getStatsPushInitDelayMs(),
                                      conf.getStatsPushDelayMs(),
                                      TimeUnit.MILLISECONDS);
    }

    /**
     * key: listener type
     * value: callback listener
     */
    private static final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
    }

    public static void notifyStats(SnapshotStats snapshot) {
        NearRuntimeHelper.INSTANCE
                .getScheduledExecutor()
                .schedule(() -> _notifyStats(snapshot), 0, TimeUnit.MILLISECONDS);
    }

    private static void _notifyStats(SnapshotStats snapshot) {
        try {
            NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
            helper.updateRuntimeInfo();
            long processCpuTime = OSUtil.getProcessCpuTime();
            long cpus = TimeUnit.NANOSECONDS.toMillis(processCpuTime);
            long s = cpus - cpuTime;
            cpuTime = cpus;
            // notify 
            for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                try {
                    CallbackListener listener = entry.getValue();
                    listener.receiveServerMsg(snapshot.toString());
                    logger.info(new StringJoiner(", ")
                                        .add("sec=" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - START))
                                        .add("act=" + snapshot.getActiveCount())
                                        .add("ms=" + snapshot.intervalTimeMs())
                                        .add("time=" + snapshot.getAvgResponseMs() * snapshot.getNumberOfSuccesses())
                                        .add("avg=" + snapshot.getAvgResponseMs())
                                        .add("suc=" + snapshot.getNumberOfSuccesses())
                                        .add("cpu=" + s)
                                        .add("run=" + snapshot.getServerStats().getRuntimeInfo())
                                        .toString());
                } catch (Throwable t) {
                    logger.error("send error", t);
                }
            }
        } catch (Throwable throwable) {
            logger.error("schedule error", throwable);
        }
    }

    private volatile static long cpuTime = 0;

    private static void _updateAndNotify(boolean clean) {

        try {
            // update runtime info
            NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
            helper.updateRuntimeInfo();

            long processCpuTime = OSUtil.getProcessCpuTime();
            long cpus = TimeUnit.NANOSECONDS.toMillis(processCpuTime);
            long s = cpus - cpuTime;
            cpuTime = cpus;
            // notify 
            for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                try {
                    InstanceStats instanceStats = helper.getInstanceStats();
                    if (nonNull(instanceStats)) {
                        // int threads = instanceStats.getDomainThreads();
                        // int oldActiveCount = instanceStats.getActiveCount();
                        int activeCount = helper.getActiveCount();
                        // if (activeCount < threads * .5) {
                        //     activeCount = (int) (threads * .6);
                        // } else if (activeCount + 20 < oldActiveCount) {
                        //     activeCount += 30;
                        // }
                        instanceStats.setActiveCount(activeCount);
                        CallbackListener listener = entry.getValue();
                        Set<String> serviceIds = instanceStats.getServiceIds();
                        for (String serviceId : serviceIds) {
                            SnapshotStats snapshot = instanceStats.snapshot(serviceId);
                            listener.receiveServerMsg(snapshot.toString());
                            logger.info(new StringJoiner(", ")
                                                .add("sec=" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - START))
                                                .add("act=" + snapshot.getActiveCount())
                                                .add("time=" + snapshot.getAvgResponseMs() * snapshot.getNumberOfSuccesses())
                                                .add("avg=" + snapshot.getAvgResponseMs())
                                                .add("suc=" + snapshot.getNumberOfSuccesses())
                                                .add("cpu=" + s)
                                                .add("run=" + snapshot.getServerStats().getRuntimeInfo())
                                                .toString());
                        }
                    }
                } catch (Throwable t) {
                    logger.error("send error", t);
                }
            }

            if (clean) {
                helper.cleanStats();
            }
        } catch (Throwable throwable) {
            logger.error("schedule error", throwable);
        }
    }
}
