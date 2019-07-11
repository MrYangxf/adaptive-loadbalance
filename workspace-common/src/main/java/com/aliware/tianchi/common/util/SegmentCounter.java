package com.aliware.tianchi.common.util;

/**
 * @author yangxf
 */
public interface SegmentCounter {

    void increment(long offset);

    void decrement(long offset);

    void add(long offset, long n);

    void set(long offset, long n);

    long get(long offset);

    /**
     * @param fromOffset inclusive
     * @param toOffset   exclusive
     */
    default long sum(long fromOffset, long toOffset) {
        return sum(fromOffset, true, toOffset, true);
    }


    long sum(long fromOffset, boolean fromInclusive, long toOffset, boolean toInclusive);

    /**
     * Clean startOffset to toOffset(exclusive).
     */
    default void clean(long toOffset) {
        clean(toOffset, false);
    }

    void clean(long toOffset, boolean toInclusive);
}
