package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.listener.CallbackListener;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.aliware.tianchi.common.util.ObjectUtil.nonEmpty;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 */
public class CallbackListenerImpl implements CallbackListener {

    private static final Logger logger = LoggerFactory.getLogger(CallbackListenerImpl.class);
    static final long START = System.nanoTime();

    @Override
    public void receiveServerMsg(String msg) {
        if (nonEmpty(msg)) {
            try {
                SnapshotStats stats = SnapshotStats.fromString(msg);
                String serviceId = stats.getServiceId();
                String address = stats.getAddress();

                LBStatistics lbStatistics = LBStatistics.INSTANCE;
                lbStatistics.updateInstanceStats(serviceId, address, stats);
                

                if (serviceId.contains("hash")) {
                    logger.info("sec=" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - START) +
                                " UPDATE " + address +
                                // ", waits=" + LBStatistics.INSTANCE.getWaits(address) +
                                ", epoch=" + stats.getEpoch() +
                                ", token=" + stats.tokens() +
                                ", active=" + stats.getActiveCount() +
                                ", ms=" + stats.intervalTimeMs() +
                                ", threads=" + stats.getDomainThreads() +
                                ", avg=" + stats.getAvgRTMs() +
                                ", suc=" + stats.getNumberOfSuccesses() +
                                ", fai=" + stats.getNumberOfFailures() +
                                ", tpt=" + stats.getThroughput() +
                                ", run=" + stats.getServerStats().getRuntimeInfo()
                               );
                }
            } catch (Exception e) {
                // ... 
                logger.error("update error", e);
            }
        }
    }

}
