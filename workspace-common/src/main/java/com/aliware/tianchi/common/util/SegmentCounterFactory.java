package com.aliware.tianchi.common.util;

/**
 * @author yangxf
 */
@FunctionalInterface
public interface SegmentCounterFactory {
    SegmentCounter newCounter();
}
