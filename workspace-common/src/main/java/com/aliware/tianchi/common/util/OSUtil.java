package com.aliware.tianchi.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author yangxf
 */
public final class OSUtil {
    private OSUtil() {
        throw new InstantiationError("OSUtil can't be instantiated");
    }

    public static RuntimeInfo newRuntimeInfo() {
        return new RuntimeInfo();
    }

    public static RuntimeInfo newRuntimeInfo(String formatText) {
        return RuntimeInfo.fromString(formatText);
    }

    public static String getName() {
        return OS.getName();
    }

    public static String getArch() {
        return OS.getArch();
    }

    public static String getVersion() {
        return OS.getVersion();
    }

    public static int getAvailableProcessors() {
        return OS.getAvailableProcessors();
    }

    public static int getNumberOfCpus() {
        return CPUS;
    }

    public static double getSystemLoadAverage() {
        return OS.getSystemLoadAverage();
    }

    public static long getCommittedVirtualMemorySize() {
        return getFromOperatingSystem("getCommittedVirtualMemorySize", long.class);
    }

    public static long getTotalSwapSpaceSize() {
        return getFromOperatingSystem("getTotalSwapSpaceSize", long.class);
    }

    public static long getFreeSwapSpaceSize() {
        return getFromOperatingSystem("getFreeSwapSpaceSize", long.class);
    }

    public static long getProcessCpuTime() {
        return getFromOperatingSystem("getProcessCpuTime", long.class);
    }

    public static long getFreePhysicalMemorySize() {
        return getFromOperatingSystem("getFreePhysicalMemorySize", long.class);
    }

    public static long getTotalPhysicalMemorySize() {
        return getFromOperatingSystem("getTotalPhysicalMemorySize", long.class);
    }

    public static long getOpenFileDescriptorCount() {
        return getFromOperatingSystem("getOpenFileDescriptorCount", long.class);
    }

    public static long getMaxFileDescriptorCount() {
        return getFromOperatingSystem("getMaxFileDescriptorCount", long.class);
    }

    public static double getSystemCpuLoad() {
        return getFromOperatingSystem("getSystemCpuLoad", double.class);
    }

    public static double getProcessCpuLoad() {
        return getFromOperatingSystem("getProcessCpuLoad", double.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getFromOperatingSystem(String methodName, Class<T> type) {
        try {
            final Method method = OS.getClass().getMethod(methodName, (Class<?>[]) null);
            method.setAccessible(true);
            return (T) method.invoke(OS, (Object[]) null);
        } catch (final InvocationTargetException e) {
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new IllegalStateException(e.getCause());
        } catch (final NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        } catch (final IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final OperatingSystemMXBean OS = ManagementFactory.getOperatingSystemMXBean();
    private static final int CPUS;

    static {
        int cpus = OS.getAvailableProcessors();
        try {
            // todo: non linux
            if (sun.awt.OSInfo.getOSType() == sun.awt.OSInfo.OSType.LINUX) {
                String[] command = {"/bin/sh", "-c", "cat /proc/cpuinfo | grep process | wc -l"};
                Process exec = Runtime.getRuntime().exec(command);
                try (InputStream is = exec.getInputStream();
                     InputStreamReader isr = new InputStreamReader(is);
                     BufferedReader br = new BufferedReader(isr)) {
                    String line = br.readLine();
                    if (line != null) {
                        cpus = Integer.parseInt(line.replaceAll("[^0-9]", ""));
                    }
                } finally {
                    exec.destroy();
                }
            }
        } catch (IOException e) {
            // ...
            e.printStackTrace();
        } finally {
            CPUS = cpus;
        }
    }
}
