package com.aliware.tianchi.common.util;

import java.util.stream.Stream;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.isEmpty;

/**
 * @author yangxf
 */
public final class RuntimeInfo {
    private static final int FIELDS = 19;
    private static final String SPLIT_REG = "[,_:; ]";
    private static final String SEPARATOR = "_";

    private final long timestamp;

    private final int cpus;
    private final int availableProcessors;
    private final long freePhysicalMemory;
    private final long totalPhysicalMemory;
    private final double processCpuLoad;
    private final double systemCpuLoad;
    private final double systemLoadAverage;
    private final long processCpuTimeNanos;

    // jvm metric

    private final int threadCount;
    private final int peakThreadCount;
    private final int daemonThreadCount;
    private final long totalStartedThreadCount;

    private final long heapUsed;
    private final long heapCommitted;
    private final long heapMax;

    private final long nonHeapUsed;
    private final long nonHeapCommitted;
    private final long nonHeapMax;

    public RuntimeInfo() {
        this(System.currentTimeMillis(),
             OSUtil.getNumberOfCpus(),
             OSUtil.getAvailableProcessors(),
             OSUtil.getFreePhysicalMemorySize(),
             OSUtil.getTotalPhysicalMemorySize(),
             OSUtil.getProcessCpuLoad(),
             OSUtil.getSystemCpuLoad(),
             OSUtil.getSystemLoadAverage(),
             OSUtil.getProcessCpuTime(),
             JvmUtil.getThreadCount(),
             JvmUtil.getPeakThreadCount(),
             JvmUtil.getDaemonThreadCount(),
             JvmUtil.getTotalStartedThreadCount(),
             JvmUtil.getHeapUsed(),
             JvmUtil.getHeapCommitted(),
             JvmUtil.getHeapMax(),
             JvmUtil.getNonHeapUsed(),
             JvmUtil.getNonHeapCommitted(),
             JvmUtil.getNonHeapMax()
            );
    }

    private RuntimeInfo(long timestamp,
                        int cpus,
                        int availableProcessors,
                        long freePhysicalMemory,
                        long totalPhysicalMemory,
                        double processCpuLoad,
                        double systemCpuLoad,
                        double systemLoadAverage,
                        long processCpuTimeNanos,
                        int threadCount,
                        int peakThreadCount,
                        int daemonThreadCount,
                        long totalStartedThreadCount,
                        long heapUsed,
                        long heapCommitted,
                        long heapMax,
                        long nonHeapUsed,
                        long nonHeapCommitted,
                        long nonHeapMax) {
        this.timestamp = System.currentTimeMillis();
        this.cpus = cpus;
        this.availableProcessors = availableProcessors;
        this.freePhysicalMemory = freePhysicalMemory;
        this.totalPhysicalMemory = totalPhysicalMemory;
        this.processCpuLoad = processCpuLoad;
        this.systemCpuLoad = systemCpuLoad;
        this.systemLoadAverage = systemLoadAverage;
        this.processCpuTimeNanos = processCpuTimeNanos;
        this.threadCount = threadCount;
        this.peakThreadCount = peakThreadCount;
        this.totalStartedThreadCount = totalStartedThreadCount;
        this.daemonThreadCount = daemonThreadCount;
        this.heapUsed = heapUsed;
        this.heapCommitted = heapCommitted;
        this.heapMax = heapMax;
        this.nonHeapUsed = nonHeapUsed;
        this.nonHeapCommitted = nonHeapCommitted;
        this.nonHeapMax = nonHeapMax;
    }

    public static RuntimeInfo fromString(String formatText) {
        checkNotNull(formatText, "formatText");
        return newRuntimeInfo(formatText.split(SPLIT_REG, -1));
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
               freePhysicalMemory + SEPARATOR +
               totalPhysicalMemory + SEPARATOR +
               ((int) (processCpuLoad * 10000)) / 10000.00d + SEPARATOR +
               ((int) (systemCpuLoad * 10000)) / 10000.00d + SEPARATOR +
               ((int) (systemLoadAverage * 10000)) / 10000.00d + SEPARATOR +
               processCpuTimeNanos + SEPARATOR +
               threadCount + SEPARATOR +
               peakThreadCount + SEPARATOR +
               daemonThreadCount + SEPARATOR +
               totalStartedThreadCount + SEPARATOR +
               heapUsed + SEPARATOR +
               heapCommitted + SEPARATOR +
               heapMax + SEPARATOR +
               nonHeapUsed + SEPARATOR +
               nonHeapCommitted + SEPARATOR +
               nonHeapMax;
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

    public double getSystemCpuLoad() {
        return systemCpuLoad;
    }

    public double getSystemLoadAverage() {
        return systemLoadAverage;
    }

    public int getCpus() {
        return cpus;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public long getProcessCpuTimeNanos() {
        return processCpuTimeNanos;
    }

    public long getFreePhysicalMemory() {
        return freePhysicalMemory;
    }

    public long getTotalPhysicalMemory() {
        return totalPhysicalMemory;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public int getPeakThreadCount() {
        return peakThreadCount;
    }

    public long getTotalStartedThreadCount() {
        return totalStartedThreadCount;
    }

    public int getDaemonThreadCount() {
        return daemonThreadCount;
    }

    public long getHeapUsed() {
        return heapUsed;
    }

    public long getHeapCommitted() {
        return heapCommitted;
    }

    public long getHeapMax() {
        return heapMax;
    }

    public long getNonHeapUsed() {
        return nonHeapUsed;
    }

    public long getNonHeapCommitted() {
        return nonHeapCommitted;
    }

    public long getNonHeapMax() {
        return nonHeapMax;
    }


    private static RuntimeInfo newRuntimeInfo(String[] values) {

        if (values.length != FIELDS) {
            throw new IllegalArgumentException("the format text is like com.aliware.tianchi.common.util.RuntimeInfo#toString()");
        }
        return new RuntimeInfo(Long.parseLong(values[0]),
                               Integer.parseInt(values[1]),
                               Integer.parseInt(values[2]),
                               Long.parseLong(values[3]),
                               Long.parseLong(values[4]),
                               Double.parseDouble(values[5]),
                               Double.parseDouble(values[6]),
                               Double.parseDouble(values[7]),
                               Long.parseLong(values[8]),
                               Integer.parseInt(values[9]),
                               Integer.parseInt(values[10]),
                               Integer.parseInt(values[11]),
                               Long.parseLong(values[12]),
                               Long.parseLong(values[13]),
                               Long.parseLong(values[14]),
                               Long.parseLong(values[15]),
                               Long.parseLong(values[16]),
                               Long.parseLong(values[17]),
                               Long.parseLong(values[18]));
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
                (int) array[1], (int) array[2], (long) array[3], (long) array[4], array[5], array[6],
                array[7], (long) array[8], (int) array[9], (int) array[10], (int) array[11], (long) array[12],
                (long) array[13], (long) array[14], (long) array[15], (long) array[16], (long) array[17], (long) array[18]
        );
    }

    private static double[] toNumArray(RuntimeInfo info) {
        double[] numbers = new double[FIELDS];
        numbers[0] = info.timestamp;
        numbers[1] = info.cpus;
        numbers[2] = info.availableProcessors;
        numbers[3] = info.freePhysicalMemory;
        numbers[4] = info.totalPhysicalMemory;
        numbers[5] = info.processCpuLoad;
        numbers[6] = info.systemCpuLoad;
        numbers[7] = info.systemLoadAverage;
        numbers[8] = info.processCpuTimeNanos;
        numbers[9] = info.threadCount;
        numbers[10] = info.peakThreadCount;
        numbers[11] = info.daemonThreadCount;
        numbers[12] = info.totalStartedThreadCount;
        numbers[13] = info.heapUsed;
        numbers[14] = info.heapCommitted;
        numbers[15] = info.heapMax;
        numbers[16] = info.nonHeapUsed;
        numbers[17] = info.nonHeapCommitted;
        numbers[18] = info.nonHeapMax;
        return numbers;
    }

}