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
     * @param responseMs 响应时间
     */
    void success(int responseMs);

    /**
     * 请求失败
     * @param responseMs 响应时间
     */
    void failure(int responseMs);

    /**
     * 请求被拒绝
     */
    void rejection();

    /**
     * 清理统计信息
     */
    void clean();
    
    long getAvgResponseMs();

    long getThroughput();

    long getTotalResponseMs();

    long getNumberOfRequests();

    long getNumberOfFailures();

    long getNumberOfRejections();

    ServerStats getServerStats();

}
