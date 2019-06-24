package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
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

    public CallbackServiceImpl() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(() -> {
                     if (!listeners.isEmpty()) {
                         for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                             try {
                                 InstanceStats instanceStats = NearRuntimeHelper.INSTANCE.getInstanceStats();
                                 if (nonNull(instanceStats)) {
                                     Set<String> serviceIds = instanceStats.getServiceIds();
                                     for (String serviceId : serviceIds) {
                                         SnapshotStats snapshot = instanceStats.snapshot(serviceId);
                                         entry.getValue().receiveServerMsg(snapshot.toString());
                                     }
                                 }
                             } catch (Throwable t) {
                                 // listeners.remove(entry.getKey());
                             }
                         }
                     }
                     // todo: config
                 }, 500, 500, TimeUnit.MILLISECONDS);
    }

    /**
     * key: listener type
     * value: callback listener
     */
    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
        // listener.receiveServerMsg(helper.getRuntimeInfo().toString()); // send notification for change
    }
}
