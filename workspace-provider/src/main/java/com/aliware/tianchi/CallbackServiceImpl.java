package com.aliware.tianchi;

import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    private final NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;

    public CallbackServiceImpl() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(() -> {
                     if (!listeners.isEmpty()) {
                         String msg = helper.getCurrent().toString();
                         for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                             try {
                                 entry.getValue().receiveServerMsg(msg);
                             } catch (Throwable t) {
                                 // listeners.remove(entry.getKey());
                             }
                         }
                     }
                 }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * key: listener type
     * value: callback listener
     */
    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
        // listener.receiveServerMsg(helper.getCurrent().toString()); // send notification for change
    }
}
