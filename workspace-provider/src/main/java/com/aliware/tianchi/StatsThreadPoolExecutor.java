package com.aliware.tianchi;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yangxf
 */
public class StatsThreadPoolExecutor extends ThreadPoolExecutor {

    private final AtomicLong counter = new AtomicLong();
    private static final Logger logger = LoggerFactory.getLogger(StatsThreadPoolExecutor.class);

    private final HashSet<Thread> threads = new HashSet<>();

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
        // threads.add(t);
        counter.getAndIncrement();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        counter.getAndDecrement();
        super.afterExecute(r, t);
    }
}
