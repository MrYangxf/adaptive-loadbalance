package com.aliware.tianchi.util;

/**
 * @author yangxf
 */
@FunctionalInterface
public interface SegmentCounterFactory {
    SegmentCounter newCounter();
}
