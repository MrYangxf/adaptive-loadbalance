package com.aliware.tianchi.lb.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;

/**
 * @author yangxf
 */
public class ServerStats {

    private final String address;

    private volatile RuntimeInfo runtimeInfo;

    public ServerStats(String address) {
        checkNotNull(address, "address");
        this.address = address;
    }

    public void setRuntimeInfo(RuntimeInfo runtimeInfo) {
        this.runtimeInfo = runtimeInfo;
    }

    public RuntimeInfo getRuntimeInfo() {
        return runtimeInfo;
    }

    public String getAddress() {
        return address;
    }
}
