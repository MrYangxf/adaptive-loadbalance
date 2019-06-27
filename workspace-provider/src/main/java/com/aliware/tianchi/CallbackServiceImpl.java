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

    public CallbackServiceImpl() {
        Configuration conf = NearRuntimeHelper.INSTANCE.getConfiguration();
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(new PushTask(),
                                         conf.getStatsPushInitDelayMs(),
                                         conf.getStatsPushDelayMs(),
                                         TimeUnit.MILLISECONDS);
    }

    /**
     * key: listener type
     * value: callback listener
     */
    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
    }

    class PushTask implements Runnable {

        @Override
        public void run() {
            try {
                // update runtime info
                NearRuntimeHelper.INSTANCE.updateRuntimeInfo();

                // notify 
                for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                    try {
                        InstanceStats instanceStats = NearRuntimeHelper.INSTANCE.getInstanceStats();
                        if (nonNull(instanceStats)) {
                            CallbackListener listener = entry.getValue();
                            Set<String> serviceIds = instanceStats.getServiceIds();
                            for (String serviceId : serviceIds) {
                                SnapshotStats snapshot = instanceStats.snapshot(serviceId);
                                listener.receiveServerMsg(snapshot.toString());
                            }
                        }
                    } catch (Throwable t) {
                        logger.error("send error", t);
                    }
                }

                NearRuntimeHelper.INSTANCE.cleanStats();
            } catch (Throwable throwable) {
                logger.error("schedule error", throwable);
            }
        }
    }
}
