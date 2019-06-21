package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端过滤器
 * 可选接口
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.PROVIDER)
public class TestServerFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long startMs = System.currentTimeMillis();
        invocation.getAttachments().put("startTimeMs", String.valueOf(startMs));
        return invoker.invoke(invocation);
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String serviceId = invoker.getInterface().getName() + '#' + invocation.getMethodName();
        InstanceStats stats = NearRuntimeHelper.INSTANCE.getInstanceStats();
        String att = invocation.getAttachment("startTimeMs");
        long startTimeMs = att == null ? System.currentTimeMillis() : Long.parseLong(att);
        if (result.hasException()) {
            stats.failure(serviceId, System.currentTimeMillis() - startTimeMs);
        } else {
            stats.success(serviceId, System.currentTimeMillis() - startTimeMs);
        }

        if ((ThreadLocalRandom.current().nextInt() & 0x80010001) == 0)
            result.setAttachment("STATS", stats.snapshot(serviceId));
        return result;
    }

}
