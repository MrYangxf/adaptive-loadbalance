package com.aliware.tianchi.common.util;

import sun.misc.Unsafe;

class LhsPadding {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

class Value 
        extends LhsPadding
{
    protected volatile long key, value;
}

class RhsPadding extends Value {
    protected long p9, p10, p11, p12, p13, p14, p15;
}

public class Sequence 
        extends RhsPadding
{
    
    public Sequence(final long key) {
        UNSAFE.putOrderedLong(this, KEY_OFFSET, key);
    }

    public Sequence(final long key, final long initialValue) {
        UNSAFE.putOrderedLong(this, KEY_OFFSET, key);
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, initialValue);
    }

    public long getKey() {
        return key;
    }

    public void setKey(final long key) {
        UNSAFE.putOrderedLong(this, KEY_OFFSET, key);
    }

    public void setKeyVolatile(final long key) {
        UNSAFE.putLongVolatile(this, VALUE_OFFSET, key);
    }

    public boolean compareAndSetKey(final long expectedKey, final long newKey) {
        return UNSAFE.compareAndSwapLong(this, VALUE_OFFSET, expectedKey, newKey);
    }

    public long getValue() {
        return value;
    }

    public void setValue(final long value) {
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
    }

    public void setValueVolatile(final long value) {
        UNSAFE.putLongVolatile(this, VALUE_OFFSET, value);
    }

    public boolean compareAndSetValue(final long expectedValue, final long newValue) {
        return UNSAFE.compareAndSwapLong(this, VALUE_OFFSET, expectedValue, newValue);
    }

    public long incrementAndGet() {
        return addAndGet(1L);
    }

    public long decrementAndGet() {
        return addAndGet(-1L);
    }

    public long addAndGet(final long increment) {
        return UNSAFE.getAndAddLong(this, VALUE_OFFSET, increment) + increment;
    }

    @Override
    public String toString() {
        return getKey() + ":" + getValue();
    }

    private static final Unsafe UNSAFE;
    private static final long KEY_OFFSET;
    private static final long VALUE_OFFSET;

    static {
        UNSAFE = UnsafeUtil.getUnsafe();
        try {
            VALUE_OFFSET = UNSAFE.objectFieldOffset(Value.class.getDeclaredField("value"));
            KEY_OFFSET = UNSAFE.objectFieldOffset(Value.class.getDeclaredField("key"));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}