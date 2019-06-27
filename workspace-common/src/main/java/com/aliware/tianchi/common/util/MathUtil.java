package com.aliware.tianchi.common.util;

/**
 * @author yangxf
 */
public final class MathUtil {

    private MathUtil() {
        throw new InstantiationError("ArrayUtil can't be instantiated");
    }

    private static final int MAXIMUM = 1 << 30;

    public static int nextPowerOf2(int i) {
        int n = i - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM) ? MAXIMUM : n + 1;
    }

    /**
     * 比较两个数是否是近似相等的
     *
     * @param error 误差
     */
    public static boolean isApproximate(long left, long right, int error) {
        return left <= right + error && right <= left + error;
    }

    public static boolean isApproximate(double left, double right, double error) {
        return left <= right + error && right <= left + error;
    }

}
