package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.common.util.MathUtil;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.aliware.tianchi.common.util.ObjectUtil.isNull;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端过滤器
 * 可选接口
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.PROVIDER)
public class TestServerFilter implements Filter {

    private static final String START_MILLIS = "START_MILLIS";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        invocation.getAttachments().put(START_MILLIS, String.valueOf(System.currentTimeMillis()));
        // String serviceId = DubboUtil.getServiceId(invoker, invocation);
        // InstanceStats stats = NearRuntimeHelper.INSTANCE.getOrCreateInstanceStats(invoker);
        // stats.success(serviceId, 1);
        return invoker.invoke(invocation);
    }

    AtomicBoolean lock = new AtomicBoolean();

    volatile SnapshotStats previous;

    AtomicLong counter = new AtomicLong();

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String serviceId = DubboUtil.getServiceId(invoker, invocation);

        NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;
        InstanceStats stats = helper.getOrCreateInstanceStats(invoker);

        String att = invocation.getAttachment(START_MILLIS);

        long startTimeMs = att == null ? System.currentTimeMillis() : Long.parseLong(att);
        long duration = System.currentTimeMillis() - startTimeMs;
        if (result.hasException()) {
            stats.failure(serviceId, duration);
        } else {
            stats.success(serviceId, duration);
        }

        int threads = invoker.getUrl().getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
        if ((counter.getAndIncrement() % (threads << 1)) == 0) {
            stats.setActiveCount(helper.getActiveCount());
            SnapshotStats snapshot = stats.snapshot(serviceId);

            if (isNull(previous)) {
                snapshot.setWeight(snapshot.getDomainThreads());
                CallbackServiceImpl.notifyStats(snapshot);
                previous = snapshot;
            } else {
                double avgRT = snapshot.getAvgResponseMs();
                double prevAvgRT = previous.getAvgResponseMs();
                if (MathUtil.isApproximate(avgRT, prevAvgRT, 5) && avgRT > prevAvgRT + 1) {
                    CallbackServiceImpl.notifyStats(previous);
                } else {
                    CallbackServiceImpl.notifyStats(snapshot);
                    previous = snapshot;
                }
            }

        }

        return result;
    }

}
