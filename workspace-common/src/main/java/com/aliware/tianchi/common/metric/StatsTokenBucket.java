package com.aliware.tianchi.common.metric;

import com.aliware.tianchi.common.util.Sequence;

/**
 * @author yangxf
 */
public class StatsTokenBucket {

    private long maxTokens;

    private final Sequence token;

    private volatile SnapshotStats stats;

    public StatsTokenBucket() {
        this(null, 0L);
    }

    public StatsTokenBucket(SnapshotStats stats, long maxTokens) {
        this.stats = stats;
        this.maxTokens = maxTokens;
        token = new Sequence(0, maxTokens);
    }

    public boolean acquireToken() {
        long n = token.getValue();
        while (n > 0) {
            if (token.compareAndSetValue(n, n - 1)) {
                return true;
            }
            n = token.getValue();
        }
        return false;
    }

    public long releaseToken() {
        return token.incrementAndGet();
    }

    public long remainTokens() {
        return token.getValue();
    }

    public synchronized void resetTokens(long tokens) {
        token.getAndAdd(tokens - maxTokens);
        maxTokens = tokens;
    }

    public SnapshotStats getStats() {
        return stats;
    }

    public void setStats(SnapshotStats stats) {
        this.stats = stats;
    }
}
