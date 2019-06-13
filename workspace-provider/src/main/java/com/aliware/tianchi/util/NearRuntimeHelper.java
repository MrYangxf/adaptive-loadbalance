package com.aliware.tianchi.util;

import com.aliware.tianchi.common.util.RuntimeInfo;

import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxf
 */
public class NearRuntimeHelper {

    public static final NearRuntimeHelper INSTANCE = new NearRuntimeHelper();

    private int bufSize = 5;
    
    private final LinkedList<RuntimeInfo> buf = new LinkedList<>();

    private volatile RuntimeInfo current = new RuntimeInfo();

    private NearRuntimeHelper() {
        Executors.newSingleThreadScheduledExecutor()
                 .scheduleWithFixedDelay(
                         () -> {
                             synchronized (buf) {
                                 buf.addFirst(new RuntimeInfo());
                                 if (buf.size() >= bufSize) {
                                     current = RuntimeInfo.merge(buf.toArray(new RuntimeInfo[0]));
                                     buf.pollLast();
                                 }
                             }
                         },
                         500,
                         300,
                         TimeUnit.MILLISECONDS);
    }

    public RuntimeInfo getCurrent() {
        return current;
    }

}
