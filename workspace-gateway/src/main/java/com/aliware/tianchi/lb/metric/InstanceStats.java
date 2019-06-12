package com.aliware.tianchi.lb.metric;


import java.io.Serializable;
import java.util.Set;

/**
 * 实例统计信息
 *
 * @author yangxf
 */
public interface InstanceStats extends Serializable {

    enum State {
        OVERLOAD,
        UNDERLOAD
    }

    String getAddress();

    /**
     * 获取系统、jvm统计指标
     */
    ServerStats getServerStats();

    /**
     * 评估实例状态
     */
    State evalState();

    Set<String> getServiceIds();

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

    long getAvgResponseMs();

    long getAvgResponseMs(String serviceId);

    long getThroughput();

    long getThroughput(String serviceId);

    long getTotalResponseMs();

    long getTotalResponseMs(String serviceId);

    long getNumberOfRequests();

    long getNumberOfRequests(String serviceId);

    long getNumberOfFailures();

    long getNumberOfFailures(String serviceId);

    long getNumberOfRejections();

    long getNumberOfRejections(String serviceId);

}
