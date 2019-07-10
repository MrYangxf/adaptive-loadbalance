package com.aliware.tianchi;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.MathUtil;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    private static long START = 0;

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
    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
    }

    private volatile double weightCache;

    private final AtomicInteger counter = new AtomicInteger();

    private volatile long previousMillis = System.currentTimeMillis();

    private void _updateAndNotify(boolean clean) {
        if (START == 0) {
            START = System.nanoTime();
        }
        try {

            // update runtime info
            NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
            helper.updateRuntimeInfo();

            long epoch = helper.getAndIncrementEpoch();

            TestThreadPool threadPool = (TestThreadPool) ExtensionLoader.getExtensionLoader(ThreadPool.class)
                                                                        .getAdaptiveExtension();

            TestThreadPool.ThreadStats threadStats = threadPool.getThreadStats();
            int qwaits = threadStats.queues(), sem = threadStats.waits(), other = threadStats.works();

            double weight = 0;

            if (sem > 0) {
                weight = other;
                weightCache = weight;
                previousMillis = System.currentTimeMillis();
            } else if (MathUtil.isApproximate(other, weightCache, 10)) {
                weight = weightCache;
            } else if (other > weightCache) {
                weight = other;
                weightCache = weight;
            } else if (System.currentTimeMillis() < previousMillis + 3000) {
                weight = weightCache;
            }

            logger.info(new StringJoiner(", ")
                                .add("time=" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - START))
                                .add("qwaits=" + qwaits)
                                .add("sem=" + sem)
                                .add("other=" + other)
                                .toString());

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
                            int threads = snapshot.getDomainThreads();
                            if (weight < threads / 2) {
                                weight = threads * 1;
                                weightCache = other;
                            }
                            snapshot.setWeight(weight * 1);
                            listener.receiveServerMsg(snapshot.toString());
                            logger.info(new StringJoiner(", ")
                                                .add("sec=" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - START))
                                                .add("act=" + snapshot.getActiveCount())
                                                .add("weight=" + weight)
                                                .add("wCache=" + weightCache)
                                                .add("time=" + snapshot.getAvgRTMs() * snapshot.getNumberOfSuccesses())
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

            if (clean) {
                helper.cleanStats();
            }
        } catch (Throwable throwable) {
            logger.error("schedule error", throwable);
        }
    }


}
