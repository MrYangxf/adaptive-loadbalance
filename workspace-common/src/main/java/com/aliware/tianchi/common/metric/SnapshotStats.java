package com.aliware.tianchi.common.metric;

import com.aliware.tianchi.common.util.RuntimeInfo;

import java.io.Serializable;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotEmpty;
import static com.aliware.tianchi.common.util.ObjectUtil.defaultIfEmpty;
import static com.aliware.tianchi.common.util.ObjectUtil.isEmpty;

/**
 * @author yangxf
 */
public abstract class SnapshotStats implements Serializable {
    private static final long serialVersionUID = 6032175431996366552L;

    private static final String SEPARATOR = "_";
    private static final String GROUP_SEPARATOR = "@";

    public static SnapshotStats fromString(String text) {
        return fromString(null, text);
    }
    
    public static SnapshotStats fromString(String address, String text) {
        checkNotEmpty(text, "text");

        String[] groups = text.split(GROUP_SEPARATOR);
        if (groups.length != 3) {
            throwIllegalArg();
        }

        String serviceId = groups[0];
        String[] insts = groups[1].split(SEPARATOR);
        if (insts.length != 10) {
            throwIllegalArg();
        }
        String finalAddress = defaultIfEmpty(address, insts[0]);
        long startTimeMs = Long.parseLong(insts[1]);
        long endTimeMs = Long.parseLong(insts[2]);
        int threads = Integer.parseInt(insts[3]);
        int activeCount = Integer.parseInt(insts[4]);
        long successes = Long.parseLong(insts[5]);
        long failures = Long.parseLong(insts[6]);
        long rejections = Long.parseLong(insts[7]);
        long avgResponseMs = Long.parseLong(insts[8]);
        long throughput = Long.parseLong(insts[9]);
        ServerStats serverStats = new ServerStats(address);
        RuntimeInfo runInfo = isEmpty(groups[2]) || groups[2].equals("null") ?
                null : RuntimeInfo.fromString(groups[2]);
        serverStats.setRuntimeInfo(runInfo);

        return new SnapshotStats() {
            private static final long serialVersionUID = 6197862269143364929L;

            @Override
            public String getServiceId() {
                return serviceId;
            }

            @Override
            public long startTimeMs() {
                return startTimeMs;
            }

            @Override
            public long endTimeMs() {
                return endTimeMs;
            }

            @Override
            public int getDomainThreads() {
                return threads;
            }

            @Override
            public int getActiveCount() {
                return activeCount;
            }

            @Override
            public String getAddress() {
                return finalAddress;
            }

            @Override
            public ServerStats getServerStats() {
                return serverStats;
            }

            @Override
            public long getAvgResponseMs() {
                return avgResponseMs;
            }

            @Override
            public long getThroughput() {
                return throughput;
            }

            @Override
            public long getNumberOfSuccesses() {
                return successes;
            }

            @Override
            public long getNumberOfFailures() {
                return failures;
            }

            @Override
            public long getNumberOfRejections() {
                return rejections;
            }
        };
    }

    @Override
    public final String toString() {
        return getServiceId() + GROUP_SEPARATOR
               + getAddress() + SEPARATOR
               + startTimeMs() + SEPARATOR
               + endTimeMs() + SEPARATOR
               + getDomainThreads() + SEPARATOR
               + getActiveCount() + SEPARATOR
               + getNumberOfSuccesses() + SEPARATOR
               + getNumberOfFailures() + SEPARATOR
               + getNumberOfRejections() + SEPARATOR
               + getAvgResponseMs() + SEPARATOR
               + getThroughput()
               + GROUP_SEPARATOR + getServerStats().getRuntimeInfo();
    }

    public String getAddress() {
        throw new UnsupportedOperationException();
    }

    public String getServiceId() {
        throw new UnsupportedOperationException();
    }

    public long startTimeMs() {
        throw new UnsupportedOperationException();
    }

    public long endTimeMs() {
        throw new UnsupportedOperationException();
    }

    public int getDomainThreads() {
        throw new UnsupportedOperationException();
    }

    public int getActiveCount() {
        throw new UnsupportedOperationException();
    }

    public ServerStats getServerStats() {
        throw new UnsupportedOperationException();
    }

    public long getAvgResponseMs() {
        throw new UnsupportedOperationException();
    }

    public long getThroughput() {
        throw new UnsupportedOperationException();
    }

    public long getNumberOfSuccesses() {
        throw new UnsupportedOperationException();
    }

    public long getNumberOfFailures() {
        throw new UnsupportedOperationException();
    }

    public long getNumberOfRejections() {
        throw new UnsupportedOperationException();
    }

    private static void throwIllegalArg() {
        throw new IllegalArgumentException("text format error, see SnapshotStats.toString()");
    }
}
