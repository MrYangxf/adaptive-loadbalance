package com.aliware.tianchi.util;

import java.util.concurrent.ThreadLocalRandom;

import static com.aliware.tianchi.util.ObjectUtil.isEmpty;

/**
 * @author yangxf
 */
public final class ArrayUtil {
    private ArrayUtil() {
        throw new InstantiationError("ArrayUtil can't be instantiated");
    }

    public static void shuffle(Object[] array) {
        if (isEmpty(array)) {
            return;
        }
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int len = array.length;
        for (int i = 0; i < len - 1; i++) {
            int si = r.nextInt(i + 1, len);
            Object t = array[i];
            array[i] = array[si];
            array[si] = t;
        }
    }
}
