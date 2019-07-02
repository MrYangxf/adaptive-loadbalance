package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import static com.aliware.tianchi.common.util.DubboUtil.getServiceId;

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

    public static final LongAdder longAdder = new LongAdder();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        longAdder.increment();
        invocation.getAttachments().put(START_MILLIS, String.valueOf(System.currentTimeMillis()));
        return invoker.invoke(invocation);
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        // InstanceStats stats = NearRuntimeHelper.INSTANCE.getInstanceStats();
        // String serviceId = getServiceId(invoker, invocation);
        // if (stats == null && serviceId.contains("hash")) {
        //     stats = NearRuntimeHelper.INSTANCE.getOrCreateInstanceStats(invoker);
        //     stats.success(serviceId, 1);
        // }

        // if (stats != null) {
        //     stats.setActiveCount(longAdder.intValue());
        // }
        NearRuntimeHelper.INSTANCE.getOrCreateInstanceStats(invoker);
        NearRuntimeHelper.INSTANCE.serviceIdMap.putIfAbsent(getServiceId(invoker, invocation), "");

        longAdder.decrement();

        if (!result.hasException() &&
            (ThreadLocalRandom.current().nextInt() & 15) == 0) {
            String att = invocation.getAttachment(START_MILLIS);
            long startTimeMs = att == null ? System.currentTimeMillis() : Long.parseLong(att);
            long duration = System.currentTimeMillis() - startTimeMs;
            NearRuntimeHelper.INSTANCE.queue(duration);
        }
        // if (result.hasException()) {
        //     stats.failure(serviceId, duration);
        // } else {
        //     stats.success(serviceId, duration);
        // }
        return result;
    }

}
