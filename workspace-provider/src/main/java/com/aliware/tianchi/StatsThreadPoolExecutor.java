package com.aliware.tianchi;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author yangxf
 */
public class StatsThreadPoolExecutor extends ThreadPoolExecutor {

    private final LongAdder counter = new LongAdder();

    public StatsThreadPoolExecutor(int corePoolSize,
                                   int maximumPoolSize,
                                   long keepAliveTime,
                                   TimeUnit unit,
                                   BlockingQueue<Runnable> workQueue,
                                   ThreadFactory threadFactory,
                                   RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    public int getActiveCount() {
        return counter.intValue();
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        counter.increment();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        counter.decrement();
        super.afterExecute(r, t);
    }
}
