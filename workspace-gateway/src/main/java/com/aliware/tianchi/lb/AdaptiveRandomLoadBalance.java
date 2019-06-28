package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.util.RuntimeInfo;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.isNull;

/**
 * @author yangxf
 */
public class AdaptiveRandomLoadBalance implements LoadBalance {
    
    private Configuration conf;

    public AdaptiveRandomLoadBalance(Configuration conf) {
        checkNotNull(conf, "conf");
        this.conf = conf;
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, 
                                 URL url, Invocation invocation) throws RpcException {
        LBStatistics lbStatistics = LBStatistics.INSTANCE;
        String serviceId = invokers.get(0).getInterface().getName() + '#' +
                           invocation.getMethodName() +
                           Arrays.toString(invocation.getParameterTypes());
        
        List<Invoker<T>> copyList = new ArrayList<>(invokers);
        ThreadLocalRandom r = ThreadLocalRandom.current();

        while (!copyList.isEmpty()) {
            int bound = copyList.size();
            int i = r.nextInt(bound);
            Invoker<T> invoker = copyList.get(i);
            String address = invoker.getUrl().getAddress();
            SnapshotStats stats = lbStatistics.getInstanceStats(serviceId, address);
            RuntimeInfo runtimeInfo;
            if (isNull(stats) ||
                isNull(runtimeInfo = stats.getServerStats().getRuntimeInfo())) {
                return invokers.get(r.nextInt(bound));
            }

            long waits = lbStatistics.getWaits(address);
            int threads = stats.getDomainThreads();
            if (waits > threads * conf.getMaxRateOfWaitingRequests() ||
                    runtimeInfo.getProcessCpuLoad() > conf.getMaxProcessCpuLoad()) {
                copyList.remove(i);
                continue;
            }

            return invoker;
        }

        throw new RpcException(RpcException.BIZ_EXCEPTION, "all providers are overloaded");
    }
    
}
