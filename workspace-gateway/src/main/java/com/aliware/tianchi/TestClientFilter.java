package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.locks.Lock;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // LBStatistics.INSTANCE.queue(DubboUtil.getIpAddress(invoker));
        // UserLoadBalance.rule.queue(DubboUtil.getIpAddress(invoker));
        return invoker.invoke(invocation);
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String address = DubboUtil.getIpAddress(invoker);
        String serviceId = DubboUtil.getServiceId(invoker, invocation);
        SnapshotStats stats = LBStatistics.INSTANCE.getInstanceStats(serviceId, address);
        if (stats != null) {
            String epoch = invocation.getAttachment("CURRENT_STATS_EPOCH", "0");
            if (epoch.equals(String.valueOf(stats.getEpoch()))) {
                stats.releaseToken();
            }
        }
        // LBStatistics.INSTANCE.dequeue(address);
        // UserLoadBalance.rule.dequeue(DubboUtil.getIpAddress(invoker));
        return result;
    }
}
    