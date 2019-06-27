package com.aliware.tianchi.common.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * @author yangxf
 */
public final class UnsafeUtil {
    private UnsafeUtil() {
        throw new InstantiationError("UnsafeUtil can't be instantiated");
    }

    public static Unsafe getUnsafe() {
        try {
            if (System.getSecurityManager() == null) {
                return (Unsafe) getUnsafeByField();
            }

            Object maybeUnsafe = AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                try {
                    return getUnsafeByField();
                } catch (Throwable t) {
                    return t;
                }
            });
            
            if (maybeUnsafe instanceof Throwable) {
                throw (Throwable) maybeUnsafe;
            }
            
            return (Unsafe) maybeUnsafe;
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static Object getUnsafeByField() throws Throwable {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        return theUnsafe.get(null);
    }
}
