package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;

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
                SnapshotStats snapshotStats = SnapshotStats.fromString(msg);
                LBStatistics.INSTANCE.updateInstanceStats(snapshotStats.getServiceId(),
                                                          snapshotStats.getAddress(),
                                                          snapshotStats);
                logger.info("update " + snapshotStats);
            } catch (Exception e) {
                // ... 
            }
        }
    }

}
