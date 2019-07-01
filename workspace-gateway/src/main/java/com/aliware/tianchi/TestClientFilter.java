package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import io.netty.channel.EventLoopGroup;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(TestClientFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        LBStatistics.INSTANCE.queue(invoker.getUrl().getAddress());
        return invoker.invoke(invocation);
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        // executor.execute(() -> {
        String address = invoker.getUrl().getAddress();
        LBStatistics.INSTANCE.dequeue(address);
        String statsAtt = result.getAttachment("STATS");
        if (statsAtt != null) {
            SnapshotStats stats = SnapshotStats.fromString(address, statsAtt);
            LBStatistics.INSTANCE.updateInstanceStats(stats.getServiceId(), address, stats);
            // logger.info("UPDATE " + address +
            //             ", waits = " + LBStatistics.INSTANCE.getWaits(address) +
            //             ", active=" + stats.getActiveCount() +
            //             ", threads=" + stats.getDomainThreads() +
            //             ", avg=" + stats.getAvgResponseMs() +
            //             ", suc=" + stats.getNumberOfSuccesses() +
            //             ", fai=" + stats.getNumberOfFailures() +
            //             ", tpt=" + stats.getThroughput() +
            //             ", run=" + (stats.getServerStats().getRuntimeInfo() == null ? "null":
            //                     stats.getServerStats().getRuntimeInfo().getProcessCpuLoad())
            //            );
        }
        // });
        
        return result;
    }
}
    