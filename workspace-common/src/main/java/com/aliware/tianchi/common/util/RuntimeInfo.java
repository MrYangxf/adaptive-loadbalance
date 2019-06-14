package com.aliware.tianchi.common.util;

import java.util.stream.Stream;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.isEmpty;

/**
 * @author yangxf
 */
public final class RuntimeInfo {
    private static final int FIELDS = 6;
    private static final String SEPARATOR = "_";

    private final long timestamp;

    private final int cpus;
    private final int availableProcessors;
    private final double processCpuLoad;

    // jvm metric

    private final int threadCount;
    private final int daemonThreadCount;

    public RuntimeInfo() {
        this(System.currentTimeMillis(),
             OSUtil.getNumberOfCpus(),
             OSUtil.getAvailableProcessors(),
             OSUtil.getProcessCpuLoad(),
             JvmUtil.getThreadCount(),
             JvmUtil.getDaemonThreadCount()
            );
    }

    public RuntimeInfo(long timestamp, int cpus, int availableProcessors, double processCpuLoad, int threadCount, int daemonThreadCount) {
        this.timestamp = timestamp;
        this.cpus = cpus;
        this.availableProcessors = availableProcessors;
        this.processCpuLoad = processCpuLoad;
        this.threadCount = threadCount;
        this.daemonThreadCount = daemonThreadCount;
    }

    public static RuntimeInfo fromString(String formatText) {
        checkNotNull(formatText, "formatText");
        return newRuntimeInfo(formatText.split(SEPARATOR, -1));
    }

    /**
     * 将多个RuntimeInfo合并成一个，然后对除timestamp之外的字段求平均数
     */
    public static RuntimeInfo merge(RuntimeInfo... runtimeInfos) {
        return newRuntimeInfo(runtimeInfos);
    }

    @Override
    public String toString() {
        return timestamp + SEPARATOR +
               cpus + SEPARATOR +
               availableProcessors + SEPARATOR +
               ((int) (processCpuLoad * 10000)) / 10000.00d + SEPARATOR +
               threadCount + SEPARATOR +
               daemonThreadCount;
    }

    public double getProcessCpuLoad() {
        if (cpus == availableProcessors) {
            return processCpuLoad;
        } else {
            return processCpuLoad * cpus / availableProcessors;
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getCpus() {
        return cpus;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public int getThreadCount() {
        return threadCount;
    }


    public int getDaemonThreadCount() {
        return daemonThreadCount;
    }


    private static RuntimeInfo newRuntimeInfo(String[] values) {

        if (values.length != FIELDS) {
            throw new IllegalArgumentException("the format text is like com.aliware.tianchi.common.util.RuntimeInfo#toString()");
        }
        return new RuntimeInfo(Long.parseLong(values[0]),
                               Integer.parseInt(values[1]),
                               Integer.parseInt(values[2]),
                               Double.parseDouble(values[3]),
                               Integer.parseInt(values[4]),
                               Integer.parseInt(values[5]));
    }

    private static RuntimeInfo newRuntimeInfo(RuntimeInfo... infos) {
        if (isEmpty(infos)) {
            return new RuntimeInfo();
        }

        int size = infos.length;
        if (size == 1) {
            return infos[0];
        }

        double[] array = Stream
                .of(infos)
                .map(RuntimeInfo::toNumArray)
                .reduce((x, y) -> {
                    for (int i = 0; i < x.length; i++) {
                        x[i] += y[i];
                    }
                    return x;
                })
                .orElse(new double[FIELDS]);

        for (int i = 0; i < array.length; i++) {
            array[i] = array[i] / size;
        }

        return new RuntimeInfo(
                System.currentTimeMillis(),
                (int) array[1], (int) array[2], array[3], (int) array[4], (int) array[5]);
    }

    private static double[] toNumArray(RuntimeInfo info) {
        double[] numbers = new double[FIELDS];
        numbers[0] = info.timestamp;
        numbers[1] = info.cpus;
        numbers[2] = info.availableProcessors;
        numbers[3] = info.processCpuLoad;
        numbers[4] = info.threadCount;
        numbers[5] = info.daemonThreadCount;
        return numbers;
    }
}