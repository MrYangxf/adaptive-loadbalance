package com.aliware.tianchi;

import com.aliware.tianchi.common.util.OSUtil;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.listener.CallbackListener;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 */
public class CallbackListenerImpl implements CallbackListener {

    @Override
    public void receiveServerMsg(String msg) {
        String remoteAddressString = RpcContext.getContext().getRemoteAddressString();
        RuntimeInfo runtimeInfo = OSUtil.newRuntimeInfo(msg);
        LBStatistics.STATS.updateRuntimeInfo(remoteAddressString, runtimeInfo);
        System.out.println(String.format("update " + remoteAddressString + " runtime info to %s", runtimeInfo));
    }

}
