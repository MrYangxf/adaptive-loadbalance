package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.listener.CallbackListener;

import java.net.InetSocketAddress;

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

    @Override
    public void receiveServerMsg(String msg) {
        if (nonEmpty(msg)) {
            try {
                SnapshotStats stats = SnapshotStats.fromString(msg);
                String serviceId = stats.getServiceId();
                String address = stats.getAddress();
                LBStatistics.INSTANCE.updateInstanceStats(serviceId, address, stats);
                InetSocketAddress socketAddress = NetUtils.toAddress(address);
                String hostAddress = socketAddress.getHostName() + ':' + socketAddress.getPort();
                boolean alias = !address.equals(hostAddress);
                if (alias && nonEmpty(hostAddress)) {
                    SnapshotStats hostStats = SnapshotStats.fromString(hostAddress, msg);
                    LBStatistics.INSTANCE.updateInstanceStats(serviceId, hostAddress, hostStats);
                }

                if (serviceId.contains("hash")) {
                    logger.info("UPDATE " + address +
                                (alias ? ", " + hostAddress : "") +
                                ", active=" + stats.getActiveCount() +
                                ", threads=" + stats.getDomainThreads() +
                                ", avg=" + stats.getAvgResponseMs() +
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
