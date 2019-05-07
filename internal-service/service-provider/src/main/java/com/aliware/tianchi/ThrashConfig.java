package com.aliware.tianchi;

import java.util.concurrent.Semaphore;

/**
 * @author guohaoice@gmail.com
 */
public class ThrashConfig {
    public static final ThrashConfig INIT_CONFIG=new ThrashConfig(4000,Integer.MAX_VALUE,10);
    final long durationInMs;
    final int averageRTTInMs;
    final Semaphore permit;

    public ThrashConfig(long durationInMs, int maxConcurrency, int averageRTTInMs) {
        this.durationInMs = durationInMs;
        this.averageRTTInMs = averageRTTInMs;
        this.permit=new Semaphore(maxConcurrency);
    }
}
