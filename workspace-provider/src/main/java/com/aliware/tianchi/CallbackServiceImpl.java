package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
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
            ConcurrentLinkedQueue<Long> queue = NearRuntimeHelper.INSTANCE.get();
            long total = 0, size = 0;
            for (Long d : queue) {
                total += d;
                size++;
            }

            long avg = total / (size + 1);
            
            InstanceStats instanceStats = NearRuntimeHelper.INSTANCE.getInstanceStats();
            if (nonNull(instanceStats)) {
                for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                    try {
                        CallbackListener listener = entry.getValue();
                        for (String serviceId : NearRuntimeHelper.INSTANCE.serviceIdMap.keySet()) {
                            SnapshotStats ss = new SnapshotStats() {
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
                                public int getDomainThreads() {
                                    return instanceStats.getDomainThreads();
                                }

                                @Override
                                public int getActiveCount() {
                                    return TestServerFilter.longAdder.intValue();
                                }

                                @Override
                                public ServerStats getServerStats() {
                                    return instanceStats.getServerStats();
                                }

                                @Override
                                public long getAvgResponseMs() {
                                    return avg;
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
                            listener.receiveServerMsg(ss.toString());
                        }
                    } catch (Throwable t) {
                        logger.error("send error", t);
                    }
                }
            }
            
            
            // InstanceStats instanceStats = NearRuntimeHelper.INSTANCE.getInstanceStats();
            // if (nonNull(instanceStats)) {
            //     instanceStats.setEpoch(EPOCH.getAndIncrement());
            //     for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
            //         try {
            //             CallbackListener listener = entry.getValue();
            //             Set<String> serviceIds = instanceStats.getServiceIds();
            //             for (String serviceId : serviceIds) {
            //                 SnapshotStats snapshot = instanceStats.snapshot(serviceId);
            //                 listener.receiveServerMsg(snapshot.toString());
            //             }
            //         } catch (Throwable t) {
            //             logger.error("send error", t);
            //         }
            //     }
            // }

            if (clean) {
                NearRuntimeHelper.INSTANCE.cleanStats();
            }
        } catch (Throwable throwable) {
            logger.error("schedule error", throwable);
        }
    }
}
