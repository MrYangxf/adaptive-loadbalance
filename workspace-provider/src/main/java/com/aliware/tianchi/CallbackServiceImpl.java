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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    private static final AtomicLong EPOCH = new AtomicLong();

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
            NearRuntimeHelper.INSTANCE.updateInstanceRuntimeInfo();

            // notify 
            InstanceStats instanceStats = NearRuntimeHelper.INSTANCE.getInstanceStats();
            if (nonNull(instanceStats)) {
                instanceStats.setEpoch(EPOCH.getAndIncrement());
                for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                    try {
                        CallbackListener listener = entry.getValue();
                        Set<String> serviceIds = instanceStats.getServiceIds();
                        for (String serviceId : serviceIds) {
                            SnapshotStats snapshot = instanceStats.snapshot(serviceId);
                            listener.receiveServerMsg(snapshot.toString());
                        }
                    } catch (Throwable t) {
                        logger.error("send error", t);
                    }
                }
            }

            if (clean) {
                NearRuntimeHelper.INSTANCE.cleanStats();
            }
        } catch (Throwable throwable) {
            logger.error("schedule error", throwable);
        }
    }
}
