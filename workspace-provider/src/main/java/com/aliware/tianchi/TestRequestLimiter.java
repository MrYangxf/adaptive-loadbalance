package com.aliware.tianchi;

import com.aliware.tianchi.common.metric.InstanceStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.common.util.SegmentCounter;
import com.aliware.tianchi.common.util.SkipListCounter;
import com.aliware.tianchi.util.NearRuntimeHelper;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

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

    private SegmentCounter counter = new SkipListCounter();
    private SegmentCounter activeCounter = new SkipListCounter();
    
    /**
     * @param request         服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException
     * true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        long offset = getOffset();
        counter.increment(offset);
        activeCounter.add(offset, activeTaskCount);
        InstanceStats stats = helper.getInstanceStats();
        if (nonNull(stats)) {
            stats.setActiveCount((int) (activeCounter.get(offset) / counter.get(offset)));
            RuntimeInfo runtimeInfo = helper.getRuntimeInfo();
            if (nonNull(runtimeInfo)) {
                double processCpuLoad = runtimeInfo.getProcessCpuLoad();
                if (processCpuLoad > THRESHOLD) {
                    double rate = processCpuLoad - THRESHOLD;
                    double r = ThreadLocalRandom.current().nextDouble(1);
                    return r > rate;
                }
            }
            return activeTaskCount < stats.getDomainThreads();
        }
        return true;
    }
    
    private long getOffset() {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }

}
