package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadPool;
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

    private static final Logger logger = LoggerFactory.getLogger(CallbackServiceImpl.class);

    public CallbackServiceImpl() {
        NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
        Configuration conf = helper.getConfiguration();
        helper.getScheduledExecutor()
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

        private int weightCache;

        private long previousMillis = System.currentTimeMillis();

        @Override
        public void run() {

            NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;

            // update runtime info
            helper.updateRuntimeInfo();

            TestThreadPool threadPool = (TestThreadPool) ExtensionLoader.getExtensionLoader(ThreadPool.class)
                                                                        .getAdaptiveExtension();
            TestThreadPool.ThreadStats threadStats = threadPool.getThreadStats();
            int queues = threadStats.queues(), waits = threadStats.waits(), works = threadStats.works();

            int weight;
            if (waits > 0 || works > weightCache) {
                weight = works;
                weightCache = weight;
            } else {
                weight = weightCache;
            }

            if (weight < helper.getThreads() / 2) {
                weight = helper.getThreads();
                weightCache = works;
            }

            long epoch = helper.getAndIncrementEpoch();

            // notify 
            for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                try {
                    InstanceStats instanceStats = helper.getInstanceStats();
                    if (nonNull(instanceStats)) {
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
                            long time = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - helper.getStartNanos());
                            logger.info(new StringJoiner(", ")
                                                .add("time=" + time)
                                                .add("epoch=" + epoch)
                                                .add("act=" + snapshot.getActiveCount())
                                                .add("queues=" + queues)
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
                    }
                } catch (Throwable t) {
                    logger.error("send error", t);
                }
            }

            helper.cleanStats();

        }

    }

}
