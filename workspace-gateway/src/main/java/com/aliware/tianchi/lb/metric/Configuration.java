package com.aliware.tianchi.lb.metric;

/**
 * @author yangxf
 */
public class Configuration {

    /**
     * 是否统计服务器指标，默认关闭
     */
    private boolean serverStats;

    /**
     * 是否统计高级指标，默认关闭
     */
    private boolean advancedStats;

    /**
     * 是否采用时间窗口统计，默认开启
     */
    private boolean timeWindowStats = true;

    private long windowSize = 10;
    
}
