package com.aliware.tianchi;

import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端限流
 * 可选接口
 * 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {

    private static final double THRESHOLD = .95d;
    private final NearRuntimeHelper helper = NearRuntimeHelper.INSTANCE;

    /**
     * @param request         服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException
     * true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        double processCpuLoad = helper.getRuntimeInfo().getProcessCpuLoad();
        if (processCpuLoad > THRESHOLD) {
            double rate = processCpuLoad - THRESHOLD;
            double r = ThreadLocalRandom.current().nextDouble(1);
            return r > rate;
        }
        return true;
    }

}
