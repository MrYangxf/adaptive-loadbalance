package com.aliware.tianchi.common.util;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author yangxf
 */
public class SmallPriorityQueue<E> extends AbstractQueue<E> {

    private final Comparator<E> comparator;
    private final Object[] data;
    private final boolean[] marks;
    private int size;

    public SmallPriorityQueue(int capacity) {
        this(capacity, null);
    }

    public SmallPriorityQueue(int capacity, Comparator<E> comparator) {
        data = new Object[capacity];
        marks = new boolean[capacity];
        this.comparator = comparator;
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean offer(E e) {
        if (size < data.length) {
            for (int i = 0; i < marks.length; i++) {
                if (!marks[i]) {
                    marks[i] = true;
                    data[i] = e;
                    size++;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public E poll() {
        if (size == 0) {
            return null;
        }

        int i = peekIndex();
        size--;
        marks[i] = false;
        return elementAt(i);
    }

    @Override
    public E peek() {
        if (size == 0) {
            return null;
        }
        return elementAt(peekIndex());
    }

    @Override
    public void clear() {
        for (int i = 0; i < marks.length; i++) {
            marks[i] = false;
        }
        size = 0;
    }

    private int peekIndex() {
        int m = -1;
        E em = null;
        for (int i = 0; i < marks.length; i++) {
            if (marks[i]) {
                E ei = elementAt(i);
                if (em == null ||
                    cpr(ei, em) < 0) {
                    em = ei;
                    m = i;
                }
            }
        }
        return m;
    }

    @SuppressWarnings("unchecked")
    private int cpr(E left, E right) {
        return comparator == null ?
                ((Comparable) left).compareTo(right) :
                comparator.compare(left, right);
    }

    @SuppressWarnings("unchecked")
    private E elementAt(int index) {
        return (E) data[index];
    }

}
