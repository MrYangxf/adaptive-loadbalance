package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
        NearRuntimeHelper.INSTANCE.getScheduledExecutor()
                                  .scheduleWithFixedDelay(() -> _updateAndNotify(true),
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

    private void _updateAndNotify(boolean clean) {

        try {
            // update runtime info
            NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
            helper.updateInstanceRuntimeInfo();

            // notify 
            InstanceStats instanceStats = helper.getInstanceStats();
            if (nonNull(instanceStats)) {
                ConcurrentLinkedQueue<Integer> queue = helper.getAndClear();
                int sum = 0, size = 0;
                for (int n : queue) {
                    sum += n;
                    size++;
                }
                int avgActive = sum / (size + 1);
                for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                    try {
                        CallbackListener listener = entry.getValue();
                        Set<String> serviceIds = helper.getServiceIds();
                        for (String serviceId : serviceIds) {
                            listener.receiveServerMsg(newSS(instanceStats, avgActive, serviceId).toString());
                        }
                    } catch (Throwable t) {
                        logger.error("send error", t);
                    }
                }
            }
            if (clean) {
                helper.cleanStats();
            }
        } catch (Throwable throwable) {
            logger.error("schedule error", throwable);
        }
    }

    private SnapshotStats newSS(InstanceStats instanceStats, int avgActive, String serviceId) {
       return new SnapshotStats() {
            private static final long serialVersionUID = -2441001503497939317L;

            @Override
            public String getAddress() {
                return instanceStats.getAddress();
            }

            @Override
            public String getServiceId() {
                return serviceId;
            }

            @Override
            public long epoch() {
                return 0;
            }

            @Override
            public long startTimeMs() {
                return 0;
            }

            @Override
            public long endTimeMs() {
                return 0;
            }

            @Override
            public int getDomainThreads() {
                return instanceStats.getDomainThreads();
            }

            @Override
            public int getActiveCount() {
                return avgActive;
            }

            @Override
            public ServerStats getServerStats() {
                return instanceStats.getServerStats();
            }

            @Override
            public long getAvgResponseMs() {
                return 0;
            }

            @Override
            public long getThroughput() {
                return 0;
            }

            @Override
            public long getNumberOfSuccesses() {
                return 0;
            }

            @Override
            public long getNumberOfFailures() {
                return 0;
            }

            @Override
            public long getNumberOfRejections() {
                return 0;
            }
        };
    }
}
