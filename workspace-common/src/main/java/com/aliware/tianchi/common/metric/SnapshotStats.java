package com.aliware.tianchi.common.metric;

import java.util.Set;

/**
 * @author yangxf
 */
public abstract class SnapshotStats implements InstanceStats {
    private static final long serialVersionUID = 2358996314645011219L;

    public abstract String serviceId();
    
    @Override
    public Set<String> getServiceIds() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void success(String serviceId, long responseMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failure(String serviceId, long responseMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rejection(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String snapshot(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAvgResponseMs(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getThroughput(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTotalResponseMs(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfRequests(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfSuccesses(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfFailures(String serviceId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfRejections(String serviceId) {
        throw new UnsupportedOperationException();
    }
}
