package com.aliware.tianchi.common.util;

/**
 * @author yangxf
 */
public final class RuntimeInfo {
    private static final String SPLIT_REG = "[,_:; ]";
    private static final String SEPARATOR = "_";

    private final int cpus;
    private final int availableProcessors;
    private final long freePhysicalMemory;
    private final long totalPhysicalMemory;
    private final double processCpuLoad;
    private final double systemCpuLoad;
    private final double systemLoadAverage;
    private final long processCpuTimeNanos;

    public RuntimeInfo() {
        this(OSUtil.getNumberOfCpus(),
             OSUtil.getAvailableProcessors(),
             OSUtil.getFreePhysicalMemorySize(),
             OSUtil.getTotalPhysicalMemorySize(),
             OSUtil.getProcessCpuLoad(),
             OSUtil.getSystemCpuLoad(),
             OSUtil.getSystemLoadAverage(),
             OSUtil.getProcessCpuTime());
    }

    private RuntimeInfo(int cpus,
                        int availableProcessors,
                        long freePhysicalMemory,
                        long totalPhysicalMemory,
                        double processCpuLoad,
                        double systemCpuLoad,
                        double systemLoadAverage,
                        long processCpuTimeNanos) {
        this.cpus = cpus;
        this.availableProcessors = availableProcessors;
        this.freePhysicalMemory = freePhysicalMemory;
        this.totalPhysicalMemory = totalPhysicalMemory;
        this.processCpuLoad = processCpuLoad;
        this.systemCpuLoad = systemCpuLoad;
        this.systemLoadAverage = systemLoadAverage;
        this.processCpuTimeNanos = processCpuTimeNanos;
    }

    public static RuntimeInfo fromString(String formatText) {
        if (formatText == null) {
            throw new NullPointerException("formatText must be not null");
        }

        String[] values = formatText.split(SPLIT_REG, -1);
        if (values.length != 8) {
            throw new IllegalArgumentException("the format text is like " +
                                               "\"1_2_3_4_0.5_0.6_7.0_8\" that be split by \"_\"");
        }

        return new RuntimeInfo(Integer.parseInt(values[0]),
                               Integer.parseInt(values[1]),
                               Long.parseLong(values[2]),
                               Long.parseLong(values[3]),
                               Double.parseDouble(values[4]),
                               Double.parseDouble(values[5]),
                               Double.parseDouble(values[6]),
                               Long.parseLong(values[7]));
    }

    @Override
    public String toString() {
        return cpus + SEPARATOR +
               availableProcessors + SEPARATOR +
               freePhysicalMemory + SEPARATOR +
               totalPhysicalMemory + SEPARATOR +
               processCpuLoad + SEPARATOR +
               systemCpuLoad + SEPARATOR +
               systemLoadAverage + SEPARATOR +
               processCpuTimeNanos;
    }

    public double getProcessCpuLoad() {
        if (cpus == availableProcessors) {
            return processCpuLoad;
        } else {
            return processCpuLoad * cpus / availableProcessors;
        }
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
}