package com.aliware.tianchi;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * @author yangxf
 */
public class StatsThreadPoolExecutor extends ThreadPoolExecutor {

    private final AtomicLong counter = new AtomicLong();
    private static final Logger logger = LoggerFactory.getLogger(StatsThreadPoolExecutor.class);

    private final HashSet<Thread> threads = new HashSet<>();

    private final Lock lock;

    public StatsThreadPoolExecutor(Lock lock,
                                   int corePoolSize,
                                   int maximumPoolSize,
                                   long keepAliveTime,
                                   TimeUnit unit,
                                   BlockingQueue<Runnable> workQueue,
                                   ThreadFactory threadFactory,
                                   RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.lock = lock;
    }

    @Override
    public void execute(Runnable command) {
        lock.lock();
        try {
            super.execute(command);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Runnable task) {
        return super.remove(task);
    }

    @Override
    public int getActiveCount() {
        return counter.intValue();
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        if (!(r instanceof TestTask)) {
            counter.getAndIncrement();
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        if (!(r instanceof TestTask)) {
            counter.getAndDecrement();
        }
        super.afterExecute(r, t);
    }

    public static class TestTask implements Runnable {

        synchronized void close() {
            notifyAll();
        }

        @Override
        public synchronized void run() {
            try {
                wait();
            } catch (InterruptedException e) {
                //
            }
        }

    }
}
