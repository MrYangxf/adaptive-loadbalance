package com.aliware.tianchi.lb.metric.instance;


import com.aliware.tianchi.lb.metric.ServerStats;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangxf
 */
public class AdvancedInstanceStats extends BaseInstanceStats {
    private static final long serialVersionUID = -7162869203189284104L;

    private static final int DEFAULT_HEAP_CAP = 1024;

    private final Map<Thread, AdaptiveHeap> q50 = new ConcurrentHashMap<>();
    private final Map<Thread, AdaptiveHeap> q90 = new ConcurrentHashMap<>();
    private final Map<Thread, AdaptiveHeap> q99 = new ConcurrentHashMap<>();

    public AdvancedInstanceStats(ServerStats serverStats) {
        super(serverStats);
    }

    public AdvancedInstanceStats(ServerStats serverStats, long lastStatsTimestamp) {
        super(serverStats, lastStatsTimestamp);
    }

    public long maxDelay50() {
        return getMaxDelay(q50);
    }

    public long maxDelay90() {
        return getMaxDelay(q90);
    }

    public long maxDelay99() {
        return getMaxDelay(q99);
    }

    @Override
    protected void doAccumulate(long responseMs) {
        Thread ct = Thread.currentThread();
        q50.computeIfAbsent(ct, k -> new AdaptiveHeap(1))
           .add(responseMs);
        q90.computeIfAbsent(ct, k -> new AdaptiveHeap(9))
           .add(responseMs);
        q99.computeIfAbsent(ct, k -> new AdaptiveHeap(99))
           .add(responseMs);
    }

    private long getMaxDelay(Map<Thread, AdaptiveHeap> qMap) {
        long min = Long.MAX_VALUE;
        for (Map.Entry<Thread, AdaptiveHeap> entry : qMap.entrySet()) {
            AdaptiveHeap q = entry.getValue();
            Long peek = q.peek();
            if (peek != null && peek < min) {
                min = peek;
            }
        }
        return min;
    }

    static class AdaptiveHeap {
        int mod, counter;
        final PriorityQueue<Long> qMin;
        final PriorityQueue<Long> qMax;

        AdaptiveHeap(int rate) {
            mod = rate + 1;
            qMin = new PriorityQueue<>(DEFAULT_HEAP_CAP);
            qMax = new PriorityQueue<>(DEFAULT_HEAP_CAP, Comparator.reverseOrder());
        }

        void add(long value) {
            counter++;
            Long max = qMax.peek();
            Long min = qMin.peek();
            if (min == null || max == null) {
                if (min == null) {
                    qMin.offer(value);
                } else if (value > min) {
                    qMax.offer(qMin.poll());
                    qMin.offer(value);
                } else {
                    qMax.offer(value);
                }
                return;
            }

            if (value > min) {
                if (counter % mod != 0) {
                    qMax.offer(qMin.poll());
                }
                qMin.offer(value);
            } else {
                if (counter % mod == 0) {
                    if (value >= max) {
                        qMin.offer(value);
                        return;
                    } else {
                        qMin.offer(qMax.poll());
                    }
                }
                qMax.offer(value);
            }
        }

        Long peek() {
            return qMin.peek();
        }
    }
}