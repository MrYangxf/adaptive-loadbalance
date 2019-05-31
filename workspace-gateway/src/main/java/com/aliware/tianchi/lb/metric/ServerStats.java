package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;

import static com.aliware.tianchi.util.ObjectUtil.checkNotNull;

/**
 * @author yangxf
 */
public class ServerStats {

    private String hostname;

    private volatile RuntimeInfo runtimeInfo;

    public ServerStats(String hostname) {
        checkNotNull(hostname, "hostname");
        this.hostname = hostname;
    }

    public void setRuntimeInfo(RuntimeInfo runtimeInfo) {
        this.runtimeInfo = runtimeInfo;
    }

    public RuntimeInfo getRuntimeInfo() {
        return runtimeInfo;
    }
    
    public String getHostname() {
        return hostname;
    }
}
