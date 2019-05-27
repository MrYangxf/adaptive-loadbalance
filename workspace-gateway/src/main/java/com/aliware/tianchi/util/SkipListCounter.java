package com.aliware.tianchi.util;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yangxf
 */
public class SkipListCounter implements SegmentCounter {

    private ConcurrentSkipListMap<Long, AtomicLong> dataMap =
            new ConcurrentSkipListMap<>(Comparator.reverseOrder());

    @Override
    public void increment(long offset) {
        getOrCreate(offset).incrementAndGet();
    }

    @Override
    public void decrement(long offset) {
        getOrCreate(offset).decrementAndGet();
    }

    @Override
    public void add(long offset, long n) {
        getOrCreate(offset).addAndGet(n);
    }

    @Override
    public void set(long offset, long n) {
        getOrCreate(offset).set(n);
    }

    @Override
    public long get(long offset) {
        return getOrCreate(offset).get();
    }

    @Override
    public long sum(long fromOffset, boolean fromInclusive, long toOffset, boolean toInclusive) {
        if (fromOffset < 0 || fromOffset >= toOffset) {
            throw new IllegalArgumentException("fromOffset must be < toOffset and fromOffset must be >= 0");
        }

        // reversed index
        return dataMap.subMap(toOffset, toInclusive, fromOffset, fromInclusive)
                      .values()
                      .stream()
                      .mapToLong(AtomicLong::get)
                      .sum();
    }

    @Override
    public void clean(long toOffset) {
        clean(toOffset, false);
    }

    @Override
    public void clean(long toOffset, boolean toInclusive) {
        if (toOffset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }

        dataMap.tailMap(toOffset, toInclusive).clear();
    }

    private AtomicLong getOrCreate(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        return dataMap.computeIfAbsent(offset, k -> new AtomicLong());
    }
}
