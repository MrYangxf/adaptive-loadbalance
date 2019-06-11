package com.aliware.tianchi;

import com.aliware.tianchi.common.util.OSUtil;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.RpcContext;
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
            RuntimeInfo runtimeInfo = OSUtil.newRuntimeInfo(msg);
            String remoteAddressString = RpcContext.getContext().getRemoteAddressString();
            logger.info(String.format("update " + remoteAddressString + " runtime info to %s", runtimeInfo));
            LBStatistics.STATS.updateRuntimeInfo(remoteAddressString, runtimeInfo);
            InetSocketAddress remoteAddress = RpcContext.getContext().getRemoteAddress();
            String hostAddress = remoteAddress.getHostName() + ":" + remoteAddress.getPort();
            if (!hostAddress.equals(remoteAddressString)) {
                LBStatistics.STATS.updateRuntimeInfo(hostAddress, runtimeInfo);
                logger.info(String.format("update " + hostAddress + " runtime info to %s", runtimeInfo));
            }
        }
    }
}
