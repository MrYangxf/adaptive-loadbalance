package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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

    private static final Logger logger = LoggerFactory.getLogger(CallbackServiceImpl.class);

    static Executor executor = Executors.newSingleThreadExecutor();

    public CallbackServiceImpl() {
        Configuration conf = NearRuntimeHelper.INSTANCE.getConfiguration();
        Executors.newSingleThreadScheduledExecutor()
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

    public static void updateAndNotify() {
        executor.execute(() -> _updateAndNotify(false));
    }

    private static void _updateAndNotify(boolean clean) {

        try {
            // update runtime info
            NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
            helper.updateRuntimeInfo();

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
                                                .add("" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()))
                                                .add("act=" + instanceStats.getActiveCount())
                                                .add("time=" + instanceStats.getTotalResponseMs(serviceId))
                                                .add("avg=" + instanceStats.getAvgResponseMs(serviceId))
                                                .add("suc=" + instanceStats.getNumberOfSuccesses(serviceId))
                                                .add("run=" + instanceStats.getServerStats().getRuntimeInfo())
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
