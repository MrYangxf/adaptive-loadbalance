package com.aliware.tianchi.util;

import com.aliware.tianchi.common.conf.Configuration;
import com.aliware.tianchi.common.metric.SnapshotStats;
import com.aliware.tianchi.common.metric.StatsTokenBucket;
import org.apache.dubbo.rpc.Invocation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangxf
 */
public final class LBHelper {
    public static final LBHelper CUSTOM = new LBHelper(new Configuration());

    private static final String TOKEN_RELEASE_KEY = "CURRENT_STATS_EPOCH";

    private Configuration configuration;

    private final Map<String, Map<String, StatsTokenBucket>> registry = new ConcurrentHashMap<>();

    public LBHelper(Configuration configuration) {
        this.configuration = configuration;
    }

    public Map<String, StatsTokenBucket> getStatsBucketGroup(String serviceId) {
        Map<String, StatsTokenBucket> statsMap = registry.get(serviceId);
        if (statsMap == null) {
            Map<String, StatsTokenBucket> newMap = new ConcurrentHashMap<>();
            statsMap = registry.putIfAbsent(serviceId, newMap);
            if (statsMap == null) {
                statsMap = newMap;
            }
        }
        return statsMap;
    }

    public StatsTokenBucket getStatsBucket(String serviceId, String address) {
        Map<String, StatsTokenBucket> statsGroup = getStatsBucketGroup(serviceId);
        StatsTokenBucket bucket = statsGroup.get(address);
        if (bucket == null) {
            StatsTokenBucket newBucket = new StatsTokenBucket();
            bucket = statsGroup.putIfAbsent(address, newBucket);
            if (bucket == null) {
                bucket = newBucket;
            }
        }
        return bucket;
    }

    public synchronized void updateInstanceStats(SnapshotStats snapshotStats) {
        String serviceId = snapshotStats.getServiceId();
        String address = snapshotStats.getAddress();
        StatsTokenBucket bucket = getStatsBucket(serviceId, address);
        bucket.resetTokens(snapshotStats.getWeight());
        bucket.setStats(snapshotStats);
    }

    public void ensureTokenReleased(StatsTokenBucket bucket, Invocation invocation) {
        invocation.getAttachments().put(TOKEN_RELEASE_KEY, "RELEASE");
    }

    public void releaseTokenIfRequire(String serviceId, String address, Invocation invocation) {
        StatsTokenBucket bucket = getStatsBucket(serviceId, address);
        String epoch = invocation.getAttachment(TOKEN_RELEASE_KEY, "");
        if ("RELEASE".equals(epoch)) {
            bucket.releaseToken();
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
