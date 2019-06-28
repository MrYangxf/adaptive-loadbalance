package com.aliware.tianchi.common.util;

import sun.misc.Unsafe;

/**
 * @author yangxf
 */
public class RingCounter implements SegmentCounter {

    private static final Sequence SENTINEL = new Sequence(-1);

    private final Sequence[] data;
    private final int capacity;
    private final int indexMask;

    public RingCounter(int expectCapacity) {
        capacity = MathUtil.nextPowerOf2(expectCapacity);
        data = new Sequence[capacity + 2 * BUFFER_PAD];
        indexMask = capacity - 1;
        for (int i = 0; i < capacity; i++) {
            data[BUFFER_PAD + i] = new Sequence(0);
        }
    }

    @Override
    public void increment(long offset) {
        add(offset, 1L);
    }

    @Override
    public void decrement(long offset) {
        add(offset, -1L);
    }

    @Override
    public void add(long offset, long n) {
        long epoch = epoch(offset);
        int index = indexOf(offset);

        Sequence seq = elementAt(index);
        while (epoch > seq.getKey()) {
            if (seq != SENTINEL &&
                cas(index, seq, SENTINEL)) {
                seq = new Sequence(epoch);
                setValue(index, seq);
                break;
            }
            seq = elementAt(index);
        }

        seq.addAndGet(n);
    }

    @Override
    public void set(long offset, long n) {
        long epoch = epoch(offset);
        int index = indexOf(offset);

        Sequence seq = elementAt(index);
        while (epoch > seq.getKey()) {
            if (seq != SENTINEL &&
                cas(index, seq, SENTINEL)) {
                seq = new Sequence(epoch);
                setValue(index, seq);
                break;
            }
            seq = elementAt(index);
        }

        seq.setValue(n);
    }

    @Override
    public long get(long offset) {
        return elementAt(indexOf(offset)).getValue();
    }

    @Override
    public long sum(long fromOffset, boolean fromInclusive, long toOffset, boolean toInclusive) {
        int fromIndex = indexOf(fromOffset);
        int toIndex = indexOf(toOffset);

        long fromEpoch = epoch(fromOffset);
        long toEpoch = epoch(toOffset);

        long sum = 0;
        if (fromInclusive) {
            Sequence fromSeq = elementAt(fromIndex);
            long fromSeqEpoch = fromSeq.getKey();
            if (fromSeqEpoch == fromEpoch) {
                sum += fromSeq.getValue();
            }
        }
        if (toIndex > fromIndex) {
            for (int i = fromIndex + 1; i < toIndex; i++) {
                Sequence seq = elementAt(i);
                long epoch = seq.getKey();
                if (epoch >= fromEpoch && epoch <= toEpoch) {
                    sum += seq.getValue();
                }
            }
        } else if (toIndex < fromIndex) {
            for (int i = 0; i < toIndex; i++) {
                Sequence seq = elementAt(i);
                long epoch = seq.getKey();
                if (epoch >= fromEpoch && epoch <= toEpoch) {
                    sum += seq.getValue();
                }
            }
            for (int i = fromIndex + 1; i < capacity; i++) {
                Sequence seq = elementAt(i);
                long epoch = seq.getKey();
                if (epoch >= fromEpoch && epoch <= toEpoch) {
                    sum += seq.getValue();
                }
            }
        } else if (fromOffset != toOffset) {
            for (int i = 0; i < fromIndex; i++) {
                Sequence seq = elementAt(i);
                long epoch = seq.getKey();
                if (epoch >= fromEpoch && epoch <= toEpoch) {
                    sum += seq.getValue();
                }
            }
            for (int i = fromIndex + 1; i < capacity; i++) {
                Sequence seq = elementAt(i);
                long epoch = seq.getKey();
                if (epoch >= fromEpoch && epoch <= toEpoch) {
                    sum += seq.getValue();
                }
            }
        }
        
        if (toInclusive && fromIndex != toIndex) {
            sum += elementAt(toIndex).getValue();
        }
        
        return sum;
    }

    @Override
    public void clean(long toOffset, boolean toInclusive) {

    }

    private long epoch(long offset) {
        return offset / capacity;
    }

    private int indexOf(long offset) {
        return (int) (offset & indexMask);
    }

    private void setValue(long index, Object value) {
        UNSAFE.putObjectVolatile(data, REF_ARRAY_BASE + (index << REF_ELEMENT_SHIFT), value);
    }

    private boolean cas(long index, Object expect, Object newValue) {
        return UNSAFE.compareAndSwapObject(data, REF_ARRAY_BASE + (index << REF_ELEMENT_SHIFT), expect, newValue);
    }

    private Sequence elementAt(long index) {
        return (Sequence) UNSAFE.getObjectVolatile(data, REF_ARRAY_BASE + (index << REF_ELEMENT_SHIFT));
    }

    private static final Unsafe UNSAFE = UnsafeUtil.getUnsafe();
    private static final int BUFFER_PAD;
    private static final long REF_ARRAY_BASE;
    private static final int REF_ELEMENT_SHIFT;

    static {
        try {
            final int scale = UNSAFE.arrayIndexScale(Sequence[].class);
            REF_ELEMENT_SHIFT = 31 - Integer.numberOfLeadingZeros(scale);
            BUFFER_PAD = 128 / scale;
            // Including the buffer pad in the array base offset
            REF_ARRAY_BASE = UNSAFE.arrayBaseOffset(Sequence[].class) + (BUFFER_PAD << REF_ELEMENT_SHIFT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
