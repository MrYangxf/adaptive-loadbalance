package com.aliware.tianchi.lb;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.lb.metric.LBStatistics;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.aliware.tianchi.common.conf.Configuration.OPEN_LOGGER;
import static com.aliware.tianchi.common.util.DubboUtil.getIpAddress;
import static com.aliware.tianchi.common.util.DubboUtil.getServiceId;
import static com.aliware.tianchi.common.util.ObjectUtil.isEmpty;

/**
 * @author yangxf
 */
public class AdaptiveLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveLoadBalance.class);

    private Configuration conf;

    public AdaptiveLoadBalance(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int size = invokers.size();

        LBStatistics lbStatistics = LBStatistics.INSTANCE;

        String serviceId = getServiceId(invokers.get(0), invocation);

        List<SnapshotStats> sortStats = lbStatistics.getSortStats(serviceId);
        if (isEmpty(sortStats)) {
            return invokers.get(ThreadLocalRandom.current().nextInt(size));
        }

        for (SnapshotStats stats : sortStats) {
            String address = stats.getAddress();
            long waits = lbStatistics.getWaits(address);
            int threads = stats.getDomainThreads();

            if (waits > threads * conf.getMaxRateOfWaitingRequests() ||
                conf.isOpenRuntimeStats() &&
                stats.getServerStats().getRuntimeInfo().getProcessCpuLoad() > conf.getMaxProcessCpuLoad()) {
                continue;
            }

            if (OPEN_LOGGER &&
                (ThreadLocalRandom.current().nextInt() & 511) == 0) {
                logger.info("SELECT " + address +
                            ", waits=" + waits +
                            ", active=" + stats.getActiveCount() +
                            ", threads=" + stats.getDomainThreads() +
                            ", avg=" + stats.getAvgResponseMs() +
                            ", suc=" + stats.getNumberOfSuccesses() +
                            ", fai=" + stats.getNumberOfFailures() +
                            ", tpt=" + stats.getThroughput() +
                            (conf.isOpenRuntimeStats() ?
                                    ", load=" + stats.getServerStats().getRuntimeInfo().getProcessCpuLoad() : "")
                           );
            }
            return findInvoker(invokers, address);
        }

        throw new RpcException(RpcException.BIZ_EXCEPTION, "all servers are  overload");

    }

    private static <T> Invoker<T> findInvoker(List<Invoker<T>> invokers, String address) {
        for (Invoker<T> invoker : invokers) {
            if (getIpAddress(invoker).equals(address)) {
                return invoker;
            }
        }
        return null;
    }
}
