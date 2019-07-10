package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aliware.tianchi.common.util.ObjectUtil.*;

/**
 * @author yangxf
 */
public class AdaptiveRandomLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveRandomLoadBalance.class);

    private final Configuration conf;

    public AdaptiveRandomLoadBalance(Configuration conf) {
        checkNotNull(conf, "conf");
        this.conf = conf;
    }

    static final double[][] WEIGHTS = {
            {14178, 43518, 56304},
            {36108, 54503, 65387},
            {39528, 72600, 67872},
            {10476, 26076, 53448},
            {25374, 40302, 78324},
            {18990, 36942, 64068},
            {22386, 36438, 79176},
            {36558, 59142, 72300},
            {18354, 38436, 75210},
            {12126, 40320, 43554}
    };

    AtomicBoolean init = new AtomicBoolean();
    static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    volatile double[] weights = {1, 2, 3};
    
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers,
                                 URL url, Invocation invocation) throws RpcException {

        if (!init.get() &&
                init.compareAndSet(false, true)) {
            long startTime = 30;
            for (double[] w : WEIGHTS) {
                scheduler.schedule(() -> {
                    weights = w;
                }, startTime, TimeUnit.SECONDS);
                startTime += 6;
            }
        }
        
        int size = invokers.size();

        // LBStatistics lbStatistics = LBStatistics.INSTANCE;
        //
        // String serviceId = DubboUtil.getServiceId(invokers.get(0), invocation);
        // Map<String, SnapshotStats> instanceStatsMap = lbStatistics.getInstanceStatsMap(serviceId);
        // if (isEmpty(instanceStatsMap)) {
        //     return invokers.get(ThreadLocalRandom.current().nextInt(size));
        // }
        //
        // double total = 0d;
        // double[] weights = new double[size];
        // for (int i = 0; i < size; i++) {
        //     Invoker<T> invoker = invokers.get(i);
        //     String address = DubboUtil.getIpAddress(invoker);
        //     SnapshotStats stats = instanceStatsMap.get(address);
        //     if (isNull(stats)) {
        //         return invokers.get(ThreadLocalRandom.current().nextInt(size));
        //     }
        //     double avgResponseMs = stats.getAvgResponseMs();
        //     // long successes = stats.getNumberOfSuccesses();
        //     // double weight = avgResponseMs * successes / stats.intervalTimeMs();
        //     // double weight = LBStatistics.INSTANCE.getWaits(address);
        //     // double weight = stats.getNumberOfSuccesses();
        //     double weight = stats.getWeight() / (avgResponseMs + .001d);
        //     if (weight == 0) {
        //         return invokers.get(ThreadLocalRandom.current().nextInt(size));
        //     }
        //
        //     // if (waits < stats.getDomainThreads() * .5 &&
        //     //         ThreadLocalRandom.current().nextBoolean()) {
        //     //     return invoker;
        //     // }
        //     total += weight;
        //     weights[i] = weight;
        // }

        // if ((ThreadLocalRandom.current().nextInt() % 511) == 0) {
        //     logger.info(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()) +
        //                 " weights=" + Arrays.toString(weights)
        //                );
        // }
        //
        // if (total == 0) {
        //     return invokers.get(ThreadLocalRandom.current().nextInt(size));
        // }


        int total = 0;
        for (double weight : weights) {
            total += weight;
        }
        
        int rm = 0;
        while (rm < size) {
            double select = ThreadLocalRandom.current().nextDouble(total);
            for (int i = 0; i < weights.length; i++) {
                if (select < weights[i]) {
                    Invoker<T> invoker = invokers.get(i);
                    // String address = DubboUtil.getIpAddress(invoker);
                    // SnapshotStats stats = instanceStatsMap.get(address);
                    // if (stats.acquireToken()) {
                    //     rm++;
                    //     weights[i] = 0;
                    //     total -= weights[i];
                    //     break;
                    // }
                    return invoker;
                }
                select -= weights[i];
            }
        }

        throw new RpcException(RpcException.BIZ_EXCEPTION, "all providers are overloaded");
    }

}
