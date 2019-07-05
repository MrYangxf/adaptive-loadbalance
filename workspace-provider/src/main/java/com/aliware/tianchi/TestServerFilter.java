package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.util.DubboUtil;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String serviceId = DubboUtil.getServiceId(invoker, invocation);

        InstanceStats stats = NearRuntimeHelper.INSTANCE.getOrCreateInstanceStats(invoker);
        // if (serviceId.contains("hash") && !lock.get() && lock.compareAndSet(false, true)) {
        //
        //     stats.success(serviceId, 1);
        // }
        String att = invocation.getAttachment(START_MILLIS);

        long startTimeMs = att == null ? System.currentTimeMillis() : Long.parseLong(att);
        long duration = System.currentTimeMillis() - startTimeMs;
        if (result.hasException()) {
            stats.failure(serviceId, duration);
        } else {
            stats.success(serviceId, duration);
        }
        return result;
    }

}
