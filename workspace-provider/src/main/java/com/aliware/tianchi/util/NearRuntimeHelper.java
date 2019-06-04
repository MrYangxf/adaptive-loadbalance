package com.aliware.tianchi.util;

import com.aliware.tianchi.common.util.RuntimeInfo;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxf
 */
public class NearRuntimeHelper {

    public static final NearRuntimeHelper INSTANCE = new NearRuntimeHelper();

    private volatile RuntimeInfo current = new RuntimeInfo();

    private NearRuntimeHelper() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(
                         () -> current = new RuntimeInfo(),
                         1000,
                         1000,
                         TimeUnit.MILLISECONDS);
    }
    
    public RuntimeInfo getCurrent() {
        return current;
    }

}
