package com.aliware.tianchi.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

/**
 * @author yangxf
 */
public final class ObjectUtil {

    private ObjectUtil() {
        throw new InstantiationError("ObjectUtil can't be instantiated");
    }

    @SuppressWarnings("unchecked")
    public static int compare(Comparator comparator, Object o1, Object o2) {
        return comparator == null ?
                ((Comparable) o1).compareTo(o2) :
                comparator.compare(o1, o2);
    }

    // ============ null or nonnull or null ... ============

    public static void checkNotNull(Object obj) {
        if (isNull(obj)) {
            throw new NullPointerException("null");
        }
    }

    public static void checkNotNull(Object obj, String msg) {
        if (isNull(obj)) {
            throw new NullPointerException(msg + " must be not null.");
        }
    }

    public static void checkNotEmpty(String obj, String msg) {
        if (isEmpty(obj)) {
            throw new NullPointerException(msg + " must be not empty.");
        }
    }

    public static void checkNotEmpty(Object[] obj, String msg) {
        if (isEmpty(obj)) {
            throw new NullPointerException(msg + " must be not empty.");
        }
    }

    public static boolean isNull(Object obj) {
        return obj == null;
    }

    public static boolean nonNull(Object arg) {
        return arg != null;
    }

    public static boolean isEmpty(String arg) {
        return arg == null || arg.length() == 0;
    }

    public static boolean isEmpty(Collection arg) {
        return arg == null || arg.size() == 0;
    }

    public static boolean isEmpty(Map arg) {
        return arg == null || arg.size() == 0;
    }

    public static boolean isEmpty(Object[] arg) {
        return arg == null || arg.length == 0;
    }

    public static boolean nonEmpty(String arg) {
        return arg != null && arg.length() != 0;
    }

    public static boolean nonEmpty(Collection arg) {
        return arg != null && arg.size() != 0;
    }

    public static boolean nonEmpty(Map arg) {
        return arg != null && arg.size() != 0;
    }

    public static boolean nonEmpty(Object[] arg) {
        return arg != null && arg.length != 0;
    }

    public static <T> T defaultIfNull(T t, T defaultValue) {
        return isNull(t) ? defaultValue : t;
    }

    public static String defaultIfEmpty(String t, String defaultValue) {
        return isEmpty(t) ? defaultValue : t;
    }

    public static <T extends Collection> T defaultIfEmpty(T t, T defaultValue) {
        return isEmpty(t) ? defaultValue : t;
    }

    public static <T extends Map> T defaultIfEmpty(T t, T defaultValue) {
        return isEmpty(t) ? defaultValue : t;
    }

    public static Object[] defaultIfEmpty(Object[] t, Object[] defaultValue) {
        return isEmpty(t) ? defaultValue : t;
    }

}
