package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.concurrent.CompletableFuture;

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
        boolean isAsync = RpcUtils.isAsync(invoker.getUrl(), invocation);
        if (isAsync) {
            return invokeAsync(invoker, invocation);
        } else {
            return invokeSync(invoker, invocation);
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String serviceId = invoker.getInterface().getName() + '#' + invocation.getMethodName();
        result.setAttachment("STATS", NearRuntimeHelper.INSTANCE.getInstanceStats().snapshot(serviceId));
        return result;
    }

    private Result invokeAsync(Invoker<?> invoker, Invocation invocation) throws RpcException {
        String serviceId = invoker.getInterface().getName() + '#' + invocation.getMethodName();
        InstanceStats stats = NearRuntimeHelper.INSTANCE.getInstanceStats();
        long startMs = System.currentTimeMillis();
        Result result = invoker.invoke(invocation);
        CompletableFuture<Object> f = RpcContext.getContext().getCompletableFuture();
        if (f != null) {
            f.whenComplete((x, y) -> stats.success(serviceId, System.currentTimeMillis() - startMs))
             .exceptionally(t -> {
                 stats.failure(serviceId, System.currentTimeMillis() - startMs);
                 return null;
             });
        }
        return result;
    }

    private Result invokeSync(Invoker<?> invoker, Invocation invocation) throws RpcException {
        String serviceId = invoker.getInterface().getName() + '#' + invocation.getMethodName();
        InstanceStats stats = NearRuntimeHelper.INSTANCE.getInstanceStats();
        long startMs = System.currentTimeMillis();
        try {
            Result result = invoker.invoke(invocation);
            stats.success(serviceId, System.currentTimeMillis() - startMs);
            return result;
        } catch (Exception e) {
            stats.failure(serviceId, System.currentTimeMillis() - startMs);
            throw e;
        }
    }

}
