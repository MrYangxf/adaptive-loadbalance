package com.aliware.tianchi.lb.metric;


import java.io.Serializable;

/**
 * 实例统计信息
 *
 * @author yangxf
 */
public interface InstanceStats extends Serializable {

    /**
     * 请求成功
     *
     * @param responseMs 响应时间
     */
    void success(long responseMs);

    /**
     * 请求失败
     *
     * @param responseMs 响应时间
     */
    void failure(long responseMs);

    /**
     * 请求被拒绝
     */
    void rejection();

    /**
     * 清理统计信息
     */
    void clean();

    /**
     * 评估该实例最大请求数，如果未获取到服务器信息，返回 {@link Long#MAX_VALUE}
     */
    long evalMaxRequestsPerSeconds();

    long getAvgResponseMs();

    long getThroughput();

    long getTotalResponseMs();

    long getNumberOfRequests();
    
    long getNumberOfRequests(long second);

    long getNumberOfFailures();

    long getNumberOfRejections();

    ServerStats getServerStats();

    void setServerStats(ServerStats serverStats);

    String getAddress();
}
