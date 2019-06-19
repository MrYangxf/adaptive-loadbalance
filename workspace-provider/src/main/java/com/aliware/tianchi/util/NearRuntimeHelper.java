package com.aliware.tianchi.util;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.ServerStats;
import com.aliware.tianchi.common.metric.TimeWindowInstanceStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.common.util.SkipListCounter;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.RpcContext;

import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxf
 */
public class NearRuntimeHelper {

    private static final Logger logger = LoggerFactory.getLogger(NearRuntimeHelper.class);
    
    public static final NearRuntimeHelper INSTANCE = new NearRuntimeHelper();

    private int bufSize = 5;

    private final LinkedList<RuntimeInfo> buf = new LinkedList<>();

    private final InstanceStats stats = createStats();

    private NearRuntimeHelper() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(
                         () -> {
                             synchronized (buf) {
                                 buf.addFirst(new RuntimeInfo());
                                 if (buf.size() >= bufSize) {
                                     RuntimeInfo info = RuntimeInfo.merge(buf.toArray(new RuntimeInfo[0]));
                                     stats.getServerStats().setRuntimeInfo(info);
                                     logger.info("update " + info);
                                     buf.pollLast();
                                 }
                             }
                         },
                         500,
                         500,
                         TimeUnit.MILLISECONDS);
    }

    public RuntimeInfo getRuntimeInfo() {
        return stats.getServerStats().getRuntimeInfo();
    }
    
    public InstanceStats getInstanceStats() {
        return stats;
    }

    private InstanceStats createStats() {
        String address = RpcContext.getServerContext().getLocalAddressString();
        return new TimeWindowInstanceStats(address,
                                           new ServerStats(address),
                                           1, 1, TimeUnit.SECONDS,
                                           SkipListCounter::new);
    }
}
