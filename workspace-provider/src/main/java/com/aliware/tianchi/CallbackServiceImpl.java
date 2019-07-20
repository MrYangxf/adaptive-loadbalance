package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import com.aliware.tianchi.util.ThreadPoolStats;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
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
        NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
        Configuration conf = helper.getConfiguration();
        long initDelayMs = conf.getStatsPushInitDelayMs();
        long delayMs = conf.getStatsPushDelayMs();
        ScheduledExecutorService executor = helper.getScheduledExecutor();
        executor.scheduleWithFixedDelay(new PushTask(), initDelayMs, delayMs, TimeUnit.MILLISECONDS);
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

        private int weightCache;

        private long previousMillis = System.currentTimeMillis();

        private Map<String, SnapshotStats> prevStatsMap = new ConcurrentHashMap<>();

        @Override
        public void run() {

            NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;

            // update runtime info
            helper.updateRuntimeInfo();

            int weight = 0;

            // TestThreadPool threadPool = (TestThreadPool) ExtensionLoader.getExtensionLoader(ThreadPool.class)
            //                                                             .getAdaptiveExtension();
            //
            // ThreadPoolStats threadPoolStats = threadPool.getThreadPoolStats();
            ThreadPoolStats threadPoolStats = helper.getThreadPoolStats();
            int frees = threadPoolStats.freeCount(),
                    waits = threadPoolStats.waitCount(),
                    works = threadPoolStats.workCount();

            if (waits > 0) {
                weight = works;
                weightCache = weight;
                previousMillis = System.currentTimeMillis();
            } else if (works > weightCache) {
                weight = works;
                weightCache = weight;
            } else if (works + 10 > weightCache ||
                       System.currentTimeMillis() < previousMillis + 2500) {
                weight = weightCache;
            } else {
                weightCache = works;
            }

            if (weight < helper.getThreads() / 2) {
                weight = helper.getThreads();
                weightCache = works;
            }

            long epoch = helper.getAndIncrementEpoch();

            // notify 
            InstanceStats instanceStats = helper.getInstanceStats();
            if (nonNull(instanceStats)) {
                for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                    try {
                        CallbackListener listener = entry.getValue();
                        Set<String> serviceIds = instanceStats.getServiceIds();
                        for (String serviceId : serviceIds) {
                            if (!serviceId.contains("hash")) {
                                continue;
                            }

                            SnapshotStats snapshot = instanceStats.snapshot(serviceId);
                            snapshot.setEpoch(epoch);
                            snapshot.setWeight(weight);
                            listener.receiveServerMsg(snapshot.toString());

                            prevStatsMap.put(serviceId, snapshot);

                            long time = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - helper.getStartNanos());
                            logger.info(new StringJoiner(", ")
                                                .add("time=" + time)
                                                .add("epoch=" + epoch)
                                                .add("act=" + snapshot.getActiveCount())
                                                .add("frees=" + frees)
                                                .add("waits=" + waits)
                                                .add("works=" + works)
                                                .add("weight=" + weight)
                                                .add("wCache=" + weightCache)
                                                .add("duration=" + snapshot.getAvgRTMs() * snapshot.getNumberOfSuccesses())
                                                .add("avg=" + snapshot.getAvgRTMs())
                                                .add("suc=" + snapshot.getNumberOfSuccesses())
                                                .add("run=" + snapshot.getServerStats().getRuntimeInfo())
                                                .toString());
                        }

                    } catch (Throwable t) {
                        logger.error("send error", t);
                    }
                }

                helper.cleanStats();
            }

        }

    }

}
