package com.aliware.tianchi.common.metric;


import java.io.Serializable;
import java.util.Set;

/**
 * 实例统计信息
 *
 * @author yangxf
 */
public interface InstanceStats extends Serializable {

    SnapshotStats snapshot(String serviceId);

    String getAddress();

    /**
     * 获取系统、jvm统计指标
     */
    ServerStats getServerStats();

    Set<String> getServiceIds();

    void setDomainThreads(int nThreads);

    int getDomainThreads();

    void setActiveCount(int activeCount);

    int getActiveCount();

    /**
     * 请求成功
     *
     * @param serviceId  服务id
     * @param responseMs 响应时间
     */
    void success(String serviceId, long responseMs);

    /**
     * 请求失败
     *
     * @param serviceId  服务id
     * @param responseMs 响应时间
     */
    void failure(String serviceId, long responseMs);

    /**
     * 请求被拒绝
     */
    void rejection(String serviceId);

    /**
     * 清理统计信息
     */
    void clean();

    // 指标查询

    double getAvgResponseMs();

    double getAvgResponseMs(String serviceId);

    long getThroughput();

    long getThroughput(String serviceId);

    long getTotalResponseMs();

    long getTotalResponseMs(String serviceId);

    long getNumberOfRequests();

    long getNumberOfRequests(String serviceId);

    long getNumberOfSuccesses();

    long getNumberOfSuccesses(String serviceId);

    long getNumberOfFailures();

    long getNumberOfFailures(String serviceId);

    long getNumberOfRejections();

    long getNumberOfRejections(String serviceId);

}
